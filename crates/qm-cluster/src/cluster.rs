//! 集群门面：把心跳、调度、故障迁移组装成一个可 `Arc` 共享的异步服务。
//!
//! 分层：
//! * [`crate::state`] —— 纯函数决策（调度 / 健康 / 迁移目标），可离线单测；
//! * [`crate::bus`]   —— NATS 收发适配；
//! * 本模块           —— 编排 + 生命周期 + 对外 API。
//!
//! 关键设计：
//! * 节点动态增减不需要持久化：成员关系靠心跳，房间视图靠订阅 + 全量快照。
//! * 故障检测与迁移分离：`health_check` 只判死，迁移是异步两段式交接，
//!   目标节点拒绝时不影响源节点上的其余会议。
//! * 旁听只做计数：加 / 减旁听者只更新 `listeners` 并发布，不搬运媒体载荷。

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use parking_lot::Mutex;
use serde::Serialize;
use tracing::{debug, info, warn};

use qm_common::{ClusterConfig, NodeRole, Result as QmResult};

use crate::bus::{MigrationResult, NatsBus, RoomRouteRequest, SnapshotPayload};
use crate::state::{
    unix_ms, HeartbeatMsg, MigrationRequest, Node, NodeRoleTag, NodeStatus, Registry, RoomState,
    Router, DEFAULT_NEW_ROOM_STREAMS,
};

/// 单节点上同时持有的 NATS 发送端数量上限（心跳 + 4 个订阅 + 若干请求）。
const DEFAULT_PUBLISHER_CAPACITY: usize = 64;

/// 集群门面。
pub struct Cluster {
    cfg: ClusterConfig,
    registry: Mutex<Registry>,
    router: Router,
    /// 本节点归属的房间。
    rooms: Mutex<HashMap<String, RoomState>>,
    hb_seq: AtomicU64,
    alive: AtomicBool,
    status: Mutex<ClusterStatus>,
}

/// 集群运行状态，供 healthz / 监控面板读取。
#[derive(Debug, Clone, Default, Serialize)]
pub struct ClusterStatus {
    pub node_id: String,
    pub role: String,
    pub advertised_addr: String,
    pub cluster_id: String,
    /// NATS 是否已连接。
    pub nats_connected: bool,
    pub heartbeat_seq: u64,
    pub health_checks: u64,
    pub dead_nodes_detected: u64,
    pub migrations_attempted: u64,
    pub migrations_completed: u64,
    pub migrations_failed: u64,
    /// 本节点承载的房间数。
    pub local_rooms: usize,
    /// 全集群房间数。
    pub cluster_rooms: usize,
    /// 全集群节点数。
    pub cluster_nodes: usize,
    /// 全集群旁听总人数。
    pub total_listeners: u64,
    /// 最近一次健康检查时刻（Unix 毫秒），0 表示尚未执行。
    pub last_health_check_ms: u64,
    /// 最近一次故障迁移记录。
    pub last_migration: Option<MigrationRecord>,
}

/// 单次迁移记录。
#[derive(Debug, Clone, Default, Serialize)]
pub struct MigrationRecord {
    pub room_id: String,
    pub source_node: String,
    pub target_node: String,
    pub ok: bool,
    pub reason: String,
    pub ts_ms: u64,
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new(ClusterConfig::default())
    }
}

impl Cluster {
    /// 从应用级配置构造（取 `AppConfig::cluster` 段）。
    ///
    /// 服务进程入口用这个：配置加载 + 校验在 [`qm_common::config`] 已经做过，
    /// 这里只搬运 `cluster` 段，不重复校验。
    pub fn from_app_config(app: &qm_common::AppConfig) -> Self {
        Self::new(app.cluster.clone())
    }
}

/// 反序列化订阅消息载荷；解析失败由调用方记日志并丢弃。
fn decode<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Option<T> {
    serde_json::from_slice(payload).ok()
}

impl Cluster {
    /// 从配置创建集群（尚未连接 NATS）。
    pub fn new(cfg: ClusterConfig) -> Self {
        let status = Mutex::new(ClusterStatus {
            node_id: cfg.node_id.clone(),
            role: cfg.node_role.as_str().to_string(),
            advertised_addr: cfg.advertised_addr.clone(),
            cluster_id: cfg.cluster_id.clone(),
            ..Default::default()
        });
        Self {
            registry: Mutex::new(Registry::new(cfg.node_id.clone())),
            router: Router::new(cfg.node_id.clone()),
            cfg,
            rooms: Mutex::new(HashMap::new()),
            hb_seq: AtomicU64::new(0),
            alive: AtomicBool::new(false),
            status,
        }
    }

    /// 初始化：连接 NATS，注册自身，启动全部后台任务。
    ///
    /// 连接失败**不让进程退出** —— 节点退化为单机模式，等 NATS 恢复后
    /// 下一跳心跳会自动重连。这是「节点动态增减、无需重启整体服务」的前提。
    pub async fn initialize(self: Arc<Self>) {
        let bus = match NatsBus::connect(&self.cfg, DEFAULT_PUBLISHER_CAPACITY).await {
            Ok(b) => b,
            Err(e) => {
                warn!(
                    error = %e,
                    server = %self.cfg.server,
                    port = self.cfg.port,
                    "NATS 未连接，节点退化为单机模式（将在下一跳心跳重试）"
                );
                self.bump_status(|s| s.nats_connected = false);
                return;
            }
        };

        self.alive.store(true, Ordering::SeqCst);
        self.bump_status(|s| s.nats_connected = true);
        info!(
            node = %self.cfg.node_id,
            cluster = %self.cfg.cluster_id,
            role = %self.cfg.node_role.as_str(),
            advertised = %self.cfg.advertised_addr,
            "节点已加入集群，等待开始承接新会议"
        );

        // 注册自身到本地注册表：让本节点立即可调度自己的房间，
        // 不必等远端心跳来回确认。
        self.register_self();
        let bus = Arc::new(bus);
        // 加入集群时立刻发一次自己归属的房间，让其它节点马上看到本节点；
        // 之后由 snapshot_publish_loop 每个心跳周期重发（新节点靠它追上全量状态）。
        {
            let (rooms, total) = self.owned_rooms_and_total();
            let _ = bus.publish_snapshot(&rooms, total).await;
        }

        // 每个后台循环各持一份 bus 克隆：`async move` 会捕获按值移动的 `Arc`，
        // 所以先把每份克隆绑定成局部变量再 spawn。
        let (bus_hb, bus_hb_recv, bus_route, bus_mig, bus_snap_pub, bus_snap_sub) = (
            Arc::clone(&bus),
            Arc::clone(&bus),
            Arc::clone(&bus),
            Arc::clone(&bus),
            Arc::clone(&bus),
            Arc::clone(&bus),
        );

        // 健康检查循环：判死过期节点并触发迁移（验收标准 2 的驱动来源）。
        let health = Arc::clone(&self);
        tokio::spawn(async move {
            health.health_loop().await;
        });

        let hb = Arc::clone(&self);
        tokio::spawn(async move {
            hb.heartbeat_loop(bus_hb).await;
        });

        let hb_recv = Arc::clone(&self);
        tokio::spawn(async move {
            hb_recv.heartbeat_recv_loop(bus_hb_recv).await;
        });

        let route = Arc::clone(&self);
        tokio::spawn(async move {
            route.route_loop(bus_route).await;
        });

        let mig = Arc::clone(&self);
        tokio::spawn(async move {
            mig.migrate_loop(bus_mig).await;
        });

        let snap_pub = Arc::clone(&self);
        tokio::spawn(async move {
            snap_pub.snapshot_publish_loop(bus_snap_pub).await;
        });

        let snap_sub = Arc::clone(&self);
        tokio::spawn(async move {
            snap_sub.snapshot_loop(bus_snap_sub).await;
        });

        let rooms_sub = Arc::clone(&self);
        tokio::spawn(async move {
            rooms_sub.room_state_loop(bus).await;
        });
    }

    // ── 健康检查 / 故障迁移 ──

    /// 健康检查（幂等，可周期性调用）：判死过期节点 + 触发其房间迁移。
    ///
    /// 返回本轮新判死的节点 id 列表。
    pub async fn health_check(&self) -> Vec<String> {
        let now = unix_ms();
        let dead = {
            let mut reg = self.registry.lock();
            reg.reap_dead(&self.cfg, now)
        };

        self.bump_status(|s| {
            s.health_checks = s.health_checks.saturating_add(1);
            s.last_health_check_ms = now;
            s.dead_nodes_detected = s.dead_nodes_detected.saturating_add(dead.len() as u64);
            let reg = self.registry.lock();
            s.cluster_rooms = reg.rooms().count();
            s.cluster_nodes = reg.nodes().count();
            s.local_rooms = reg.owned_count() as usize;
            s.total_listeners = reg.total_listeners();
        });

        for dead_id in &dead {
            info!(dead_node = %dead_id, "节点故障：开始迁移其房间");
        }

        // 取出需要迁移的房间，避免长时间持有锁。
        let rooms_to_migrate = {
            let reg = self.registry.lock();
            let mut out = Vec::new();
            for dead_id in &dead {
                for room in reg.rooms() {
                    if room.owner.as_deref() == Some(dead_id.as_str()) && room.is_owned() {
                        out.push((
                            room.id.clone(),
                            room.streams,
                            room.listeners,
                            dead_id.clone(),
                        ));
                    }
                }
            }
            out
        };

        for (room_id, streams, listeners, source) in rooms_to_migrate {
            let Some(target) = self.migration_destination(&source, streams, listeners) else {
                warn!(room = %room_id, source = %source, "无可用迁移目标，房间暂留原节点");
                continue;
            };
            info!(
                room = %room_id,
                source = %source,
                target = %target,
                streams,
                listeners,
                "触发故障迁移"
            );
            self.migrate_room(&room_id, &source, &target, streams, listeners)
                .await;
        }

        dead
    }

    /// 周期性健康检查；与心跳解耦，便于在测试里手动驱动。
    pub async fn health_loop(self: Arc<Self>) {
        let mut interval =
            tokio::time::interval(Duration::from_secs(self.cfg.heartbeat_secs.max(1)));
        loop {
            interval.tick().await;
            self.health_check().await;
        }
    }

    /// 分配一个房间到当前集群（返回负载最低节点 id）。
    pub fn assign_room(&self, room_id: &str) -> Option<String> {
        let reg = self.registry.lock();
        self.router
            .assign(&reg, room_id, DEFAULT_NEW_ROOM_STREAMS, 0)
    }

    // ── 房间管理 ──

    /// 本节点归属的房间快照。
    pub fn rooms_snapshot(&self) -> Vec<RoomState> {
        self.rooms.lock().values().cloned().collect()
    }

    /// 快照发送循环：`initialize` 时先发一次，之后每 `heartbeat_secs` 重发一次。
    ///
    /// 周期重发（而不是只在启动时发一次）是为了让**新节点上线就能拿到快照**：
    /// 它连上 NATS 之后订阅 `room.snapshot`，下一个心跳周期就会收到各节点发来的
    /// 自己归属的房间 —— 这是「新节点 30 秒内接入并承接新会议」的实现路径，
    /// 不依赖请求/回复那种要求对端先在线的交互。
    ///
    /// `total` 是**本节点房间视图的总数**，而不是 `rooms` 的长度：`rooms` 只含
    /// 本节点归属的房间，`total` 是本节点看到的全集群房间数。`snapshot_loop` 拿
    /// 它判断一份快照是否覆盖了整个集群，从而避免用部分快照冲掉自己已有的视图。
    async fn snapshot_publish_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        let interval = Duration::from_secs(self.cfg.heartbeat_secs.max(1));
        loop {
            let (rooms, total) = self.owned_rooms_and_total();
            let _ = bus.publish_snapshot(&rooms, total).await;
            tokio::time::sleep(interval).await;
        }
    }

    /// 本节点归属的房间 + 本节点房间视图总数（在同一个锁作用域内取，避免中间态）。
    fn owned_rooms_and_total(&self) -> (Vec<RoomState>, usize) {
        let reg = self.registry.lock();
        let total = reg.rooms().count();
        let rooms = reg
            .rooms()
            .filter(|r| r.owner.as_deref() == Some(self.cfg.node_id.as_str()))
            .cloned()
            .collect();
        (rooms, total)
    }

    /// 收到一条远端房间状态更新。
    pub fn apply_remote_room(&self, room: RoomState) {
        self.registry.lock().apply_room(room.clone());
        if room.owner.as_deref() == Some(self.cfg.node_id.as_str()) {
            self.rooms.lock().insert(room.id.clone(), room);
        }
    }

    /// 收到一次房间快照（各节点的 `snapshot_publish_loop` 周期发布）。
    ///
    /// `payload.rooms` 只含发送方自己归属的房间，所以**逐房间按 `revision` 收敛**
    /// 就够了 —— 快照是追加式的，不需要按 `total` 反推删除：房间结束走的是
    /// `room.update` / `room.remove`，不是「不再出现在快照里」。`total` 只是
    /// 发送方看到的全集群房间数，用于判断这份快照是否覆盖了整个集群（日志与
    /// 后续的对账），不用于删除本节点已有的房间。
    pub fn apply_remote_snapshot(&self, payload: SnapshotPayload) {
        let total_rooms = payload.total;
        let mut local_changes = 0usize;
        for room in payload.rooms {
            self.registry.lock().apply_room(room.clone());
            if room.owner.as_deref() == Some(self.cfg.node_id.as_str()) {
                self.rooms.lock().insert(room.id.clone(), room);
                local_changes += 1;
            }
        }
        debug!(total_rooms, local_changes, "已应用远端房间快照");
    }

    /// 旁听者加入：只增加计数并发布，不搬运媒体载荷。
    ///
    /// 房间不存在或不属于本节点时返回 `None`。
    pub async fn add_listener(&self, room_id: &str, bus: &NatsBus) -> QmResult<Option<RoomState>> {
        let Some(before) = self.rooms.lock().get(room_id).cloned() else {
            return Ok(None);
        };
        let next = before
            .clone()
            .with_counts(before.streams, before.listeners + 1);
        self.rooms.lock().insert(room_id.to_string(), next.clone());
        self.registry.lock().apply_room(next.clone());
        bus.publish_room(&next).await?;
        Ok(Some(next))
    }

    /// 旁听者离开。
    pub async fn remove_listener(
        &self,
        room_id: &str,
        bus: &NatsBus,
    ) -> QmResult<Option<RoomState>> {
        let Some(before) = self.rooms.lock().get(room_id).cloned() else {
            return Ok(None);
        };
        let next = before
            .clone()
            .with_counts(before.streams, before.listeners.saturating_sub(1));
        self.rooms.lock().insert(room_id.to_string(), next.clone());
        self.registry.lock().apply_room(next.clone());
        bus.publish_room(&next).await?;
        Ok(Some(next))
    }

    /// 移除本地房间（会话结束）：发布一个 `owner = None` 的终止状态。
    pub async fn remove_room(&self, room_id: &str, bus: &NatsBus) -> QmResult<bool> {
        let Some(room) = self.rooms.lock().remove(room_id) else {
            return Ok(false);
        };
        self.registry.lock().remove_room(room_id);
        let tombstone = RoomState {
            id: room.id.clone(),
            owner: None,
            media_addr: String::new(),
            node_role: NodeRole::default(),
            streams: 0,
            listeners: 0,
            revision: room.revision.saturating_add(1),
        };
        bus.publish_room(&tombstone).await?;
        Ok(true)
    }

    // ── 只读视图 ──

    /// 是否已加入集群（NATS 已连接）。
    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    /// 节点视图。
    pub fn nodes(&self) -> Vec<Node> {
        self.registry.lock().nodes().cloned().collect()
    }

    /// 集群节点数。
    pub fn node_count(&self) -> usize {
        self.registry.lock().nodes().count()
    }

    /// 全集群房间数。
    pub fn room_count(&self) -> usize {
        self.registry.lock().rooms().count()
    }

    /// 全集群旁听总人数。
    pub fn total_listeners(&self) -> u64 {
        self.registry.lock().total_listeners()
    }

    /// 指定节点是否健康。
    pub fn is_node_healthy(&self, node_id: &str) -> bool {
        let reg = self.registry.lock();
        reg.is_healthy(node_id, &self.cfg, unix_ms())
    }

    /// 当前状态快照（用于 healthz）。
    pub fn status_snapshot(&self) -> ClusterStatus {
        let mut s = self.status.lock().clone();
        let reg = self.registry.lock();
        s.local_rooms = self.rooms.lock().len();
        s.cluster_rooms = reg.rooms().count();
        s.cluster_nodes = reg.nodes().count();
        s.total_listeners = reg.total_listeners();
        s.heartbeat_seq = self.hb_seq.load(Ordering::SeqCst);
        s
    }

    /// 配置。
    pub fn cfg(&self) -> &ClusterConfig {
        &self.cfg
    }

    // ── 内部工具 ──

    fn register_self(&self) {
        let node = Node {
            node_id: self.cfg.node_id.clone(),
            advertised: self.cfg.advertised_addr.clone(),
            role: NodeRoleTag::from(self.cfg.node_role),
            last_seen_ms: unix_ms(),
            last_seq: self.hb_seq.fetch_add(1, Ordering::SeqCst) + 1,
            status: NodeStatus::Live,
            max_rooms: self.cfg.max_rooms_per_node as u64,
            listener_capacity: self.cfg.listener_capacity as u64,
            listener_fanout: self.cfg.listener_fanout as u64,
            dead_since_ms: None,
        };
        let _ = self.registry.lock().insert_node(node);
    }

    fn migration_destination(&self, exclude: &str, streams: u64, listeners: u64) -> Option<String> {
        let reg = self.registry.lock();
        self.router
            .migration_destination(&reg, Some(exclude), streams, listeners)
    }

    fn bump_status<F: FnOnce(&mut ClusterStatus)>(&self, f: F) {
        f(&mut self.status.lock());
    }

    /// 向候选目标节点发起一次迁移请求（请求 / 响应）。
    async fn migrate_room(
        &self,
        room_id: &str,
        source: &str,
        target: &str,
        streams: u64,
        listeners: u64,
    ) {
        self.bump_status(|s| {
            s.migrations_attempted = s.migrations_attempted.saturating_add(1);
        });

        let req = MigrationRequest {
            room_id: room_id.to_string(),
            source_node: source.to_string(),
            target_node: target.to_string(),
            streams,
            listeners,
        };

        // 迁移请求需要一次独立连接：主 bus 的订阅循环可能在处理迁移，
        // 用独立连接避免请求 / 响应在同一连接上争抢。
        let bus = match NatsBus::connect(&self.cfg, DEFAULT_PUBLISHER_CAPACITY).await {
            Ok(b) => b,
            Err(e) => {
                warn!(room = %room_id, error = %e, "迁移请求无法建立连接");
                self.bump_status(|s| {
                    s.migrations_failed = s.migrations_failed.saturating_add(1);
                });
                return;
            }
        };

        let result = bus.request_migration(&req).await;
        let started = unix_ms();
        let elapsed = unix_ms() - started;

        match result {
            Ok(r) if r.ok => {
                info!(room = %room_id, target = %target, elapsed_ms = elapsed, "迁移完成");
                self.bump_status(|s| {
                    s.migrations_completed = s.migrations_completed.saturating_add(1);
                    s.last_migration = Some(MigrationRecord {
                        room_id: room_id.to_string(),
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        ok: true,
                        reason: r.reason.clone(),
                        ts_ms: r.ts,
                    });
                });
                // 源节点同步更新本地视图：房间已从本节点迁出。
                let tombstone = RoomState {
                    id: room_id.to_string(),
                    owner: Some(target.to_string()),
                    media_addr: String::new(),
                    node_role: NodeRole::default(),
                    streams,
                    listeners,
                    revision: self
                        .registry
                        .lock()
                        .room(room_id)
                        .map(|r| r.revision.saturating_add(1))
                        .unwrap_or(1),
                };
                self.rooms.lock().remove(room_id);
                self.registry.lock().apply_room(tombstone);
            }
            Ok(r) => {
                warn!(room = %room_id, target = %target, reason = %r.reason, "迁移目标拒绝");
                self.bump_status(|s| {
                    s.migrations_failed = s.migrations_failed.saturating_add(1);
                    s.last_migration = Some(MigrationRecord {
                        room_id: room_id.to_string(),
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        ok: false,
                        reason: r.reason.clone(),
                        ts_ms: r.ts,
                    });
                });
            }
            Err(e) => {
                warn!(room = %room_id, target = %target, error = %e, elapsed_ms = elapsed, "迁移请求失败");
                self.bump_status(|s| {
                    s.migrations_failed = s.migrations_failed.saturating_add(1);
                    s.last_migration = Some(MigrationRecord {
                        room_id: room_id.to_string(),
                        source_node: source.to_string(),
                        target_node: target.to_string(),
                        ok: false,
                        reason: format!("request failed: {e}"),
                        ts_ms: unix_ms(),
                    });
                });
            }
        }

        let _ = bus.shutdown().await;
    }

    // ── 后台循环 ──

    async fn heartbeat_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        let mut seq = self.hb_seq.load(Ordering::SeqCst);
        let mut ticker = tokio::time::interval(Duration::from_secs(self.cfg.heartbeat_secs.max(1)));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            seq += 1;
            let msg = HeartbeatMsg::from_node(&self.cfg, seq);
            self.hb_seq.store(seq, Ordering::SeqCst);
            let hb_subject = bus.subjects().heartbeat.clone();
            if let Err(e) = bus.publish_and_flush(&hb_subject, &msg).await {
                warn!(error = %e, seq, "心跳发送失败");
            } else {
                debug!(seq, node = %self.cfg.node_id, "心跳已送达 server");
            }
        }
    }

    /// 订阅全集群心跳：更新节点视图。
    async fn heartbeat_recv_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        let subject = bus.subjects().heartbeat_all.clone();
        let Some(mut sub) = bus.subscribe(&subject).await.ok() else {
            warn!("无法订阅心跳 subject，集群成员视图将无法更新");
            return;
        };
        while let Some(msg) = sub.next().await {
            let Some(hb) = decode::<HeartbeatMsg>(&msg.payload) else {
                warn!("收到无法解析的心跳，已忽略");
                continue;
            };
            let now = unix_ms();
            self.registry.lock().mark_heartbeat(&hb, now);
        }
    }

    /// 队列订阅新会议调度请求：只有队列里一个节点会处理。
    async fn route_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        let subject = bus.subjects().route_new.clone();
        let Some(mut sub) = bus.queue_subscribe(&subject, "route").await.ok() else {
            warn!("无法订阅路由 subject，调度请求将不再被本节点处理");
            return;
        };
        while let Some(msg) = sub.next().await {
            let Some(req) = decode::<RoomRouteRequest>(&msg.payload) else {
                warn!("收到无法解析的路由请求，已忽略");
                continue;
            };
            let decision = {
                let reg = self.registry.lock();
                self.router
                    .assign(&reg, &req.room_id, req.streams, req.listeners)
            };
            // 队列订阅只会被队列里的一个节点消费，所以回包不会重复。
            // 无可用目标时回一个空串，请求方据此区分「调度失败」与「超时」。
            let target = decision.clone().unwrap_or_default();
            let _ = bus.reply_json(&msg, &target).await;
            match decision {
                Some(target) => info!(
                    room = %req.room_id,
                    from = %req.from_node,
                    target = %target,
                    streams = req.streams,
                    listeners = req.listeners,
                    "已调度新会议到负载最低节点"
                ),
                None => warn!(room = %req.room_id, "集群无可用调度目标"),
            }
        }
    }

    /// 队列订阅迁移请求：目标节点处理并回包。
    async fn migrate_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        // 只订阅「投给本节点」的迁移请求：subject 本身按目标 node_id 分片，
        // 所以这里不靠队列分摊，用队列名只为与路由队列区分。
        let subject = bus.subjects().migrate_for_self.clone();
        let Some(mut sub) = bus.queue_subscribe(&subject, "migrate").await.ok() else {
            warn!("无法订阅迁移 subject，故障迁移将不会发生");
            return;
        };
        while let Some(msg) = sub.next().await {
            let Some(req) = decode::<MigrationRequest>(&msg.payload) else {
                warn!("收到无法解析的迁移请求，已忽略");
                continue;
            };
            self.handle_migration_request(&req, &bus, msg).await;
        }
    }

    /// 订阅房间全量快照（发送方是各节点的 `snapshot_publish_loop`）。
    async fn snapshot_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        let subject = bus.subjects().room_snapshot.clone();
        let Some(mut sub) = bus.subscribe(&subject).await.ok() else {
            warn!("无法订阅房间快照 subject");
            return;
        };
        while let Some(msg) = sub.next().await {
            let Some(payload) = decode::<SnapshotPayload>(&msg.payload) else {
                warn!("收到无法解析的房间快照，已忽略");
                continue;
            };
            self.apply_remote_snapshot(payload);
        }
    }

    /// 订阅全集群房间状态变更。
    async fn room_state_loop(self: Arc<Self>, bus: Arc<NatsBus>) {
        // 房间 subject 是 `qm.<cid>.room.*`，用一个通配订阅即可。
        let subject = format!("{}.room.*", bus.subjects().prefix);
        let Some(mut sub) = bus.subscribe(&subject).await.ok() else {
            warn!("无法订阅房间状态 subject");
            return;
        };
        while let Some(msg) = sub.next().await {
            if let Some(room) = decode::<RoomState>(&msg.payload) {
                self.apply_remote_room(room);
            }
        }
    }

    /// 处理一条迁移请求（本节点是目标时）。
    async fn handle_migration_request(
        &self,
        req: &MigrationRequest,
        bus: &NatsBus,
        msg: async_nats::Message,
    ) {
        if req.target_node != self.cfg.node_id {
            return;
        }
        let now = unix_ms();

        // 目标节点健康 + 有余量才能承接。
        let healthy = {
            let reg = self.registry.lock();
            reg.is_healthy(&self.cfg.node_id, &self.cfg, now)
        };
        if !healthy {
            self.publish_reject(bus, msg.clone(), req, "本节点未注册 / 未健康")
                .await;
            return;
        }

        let can_accept = {
            let reg = self.registry.lock();
            self.router.can_accept(&reg, req.streams, req.listeners)
        };
        if !can_accept {
            self.publish_reject(bus, msg.clone(), req, "本节点无承接余量")
                .await;
            return;
        }

        // 承接房间：从 registry 里取当前状态，改归属为本节点。
        let base = self
            .registry
            .lock()
            .room(&req.room_id)
            .cloned()
            .unwrap_or_else(|| RoomState::new(req.room_id.clone(), req.streams, req.listeners));
        let claimed = base.with_owner(
            Some(self.cfg.node_id.clone()),
            &self.cfg.advertised_addr,
            self.cfg.node_role,
        );
        self.rooms
            .lock()
            .insert(req.room_id.clone(), claimed.clone());
        self.registry.lock().apply_room(claimed.clone());
        if let Err(e) = bus.publish_room(&claimed).await {
            warn!(room = %req.room_id, error = %e, "迁移后状态发布失败");
            self.publish_reject(bus, msg.clone(), req, &format!("状态发布失败: {e}"))
                .await;
            return;
        }
        // 先广播新状态、再回包：发起方收到成功回包时，其他节点也能立刻看到归属变更。
        let result = MigrationResult {
            room_id: req.room_id.clone(),
            target_node: req.target_node.clone(),
            ok: true,
            reason: "accepted".to_string(),
            ts: unix_ms(),
        };
        let _ = bus.publish_migration_result(&result).await;
        let _ = bus.reply_json(&msg, &result).await;
        info!(room = %req.room_id, source = %req.source_node, "已承接迁移过来的房间");
    }

    async fn publish_reject(
        &self,
        bus: &NatsBus,
        msg: async_nats::Message,
        req: &MigrationRequest,
        reason: &str,
    ) {
        let result = MigrationResult {
            room_id: req.room_id.clone(),
            target_node: req.target_node.clone(),
            ok: false,
            reason: reason.to_string(),
            ts: unix_ms(),
        };
        // 广播给全集群（观测用），同时回包给发起方（迁移流程的必要响应）。
        let _ = bus.publish_migration_result(&result).await;
        let _ = bus.reply_json(&msg, &result).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::ClusterConfig;

    fn tcfg(node: &str) -> ClusterConfig {
        ClusterConfig {
            node_id: node.to_string(),
            advertised_addr: "192.168.0.10:8080".to_string(),
            max_rooms_per_node: 64,
            listener_capacity: 20_000,
            listener_fanout: 200,
            heartbeat_secs: 5,
            unhealthy_misses: 2,
            request_timeout_secs: 3,
            failover_target_secs: 10,
            join_target_secs: 30,
            ..Default::default()
        }
    }

    #[test]
    fn assign_returns_none_when_no_candidates() {
        let cluster = Cluster::new(tcfg("n1"));
        assert!(cluster.assign_room("room-1").is_none());
    }

    #[test]
    fn apply_remote_room_updates_registry_only_when_owned() {
        let cluster = Cluster::new(tcfg("n1"));
        // 不属于自己的房间：只进 registry 视图，不进本地 rooms map。
        let room = RoomState::new("r1", 1, 3).with_owner(
            Some("n2".to_string()),
            "192.168.0.11:8080",
            NodeRole::Full,
        );
        cluster.apply_remote_room(room);
        assert!(cluster.rooms_snapshot().is_empty());
        assert!(cluster.registry.lock().room("r1").is_some());
    }

    #[test]
    fn apply_remote_room_inserts_owned_room() {
        let cluster = Cluster::new(tcfg("n1"));
        let room = RoomState::new("r1", 1, 3).with_owner(
            Some("n1".to_string()),
            "192.168.0.10:8080",
            NodeRole::Full,
        );
        cluster.apply_remote_room(room);
        assert_eq!(cluster.rooms_snapshot().len(), 1);
        assert_eq!(cluster.rooms_snapshot()[0].listeners, 3);
    }

    #[test]
    fn status_snapshot_defaults() {
        let c = Cluster::new(tcfg("n1"));
        let s = c.status_snapshot();
        assert_eq!(s.node_id, "n1");
        assert_eq!(s.cluster_id, "quickmeet");
        assert!(s.heartbeat_seq == 0);
        assert!(s.migrations_attempted == 0);
        assert!(!s.nats_connected);
    }

    #[test]
    fn register_self_makes_node_schedulable() {
        let c = Cluster::new(tcfg("n1"));
        c.register_self();
        // 自身节点健康，因此可以作为调度目标
        assert!(c.is_node_healthy("n1"));
        assert_eq!(c.node_count(), 1);
    }

    #[test]
    fn total_listeners_aggregates_registry() {
        let c = Cluster::new(tcfg("n1"));
        c.apply_remote_room(RoomState::new("r1", 1, 10));
        c.apply_remote_room(RoomState::new("r2", 1, 20));
        assert_eq!(c.total_listeners(), 30);
        assert_eq!(c.room_count(), 2);
    }

    #[test]
    fn migration_destination_excludes_dead_node() {
        let c = Cluster::new(tcfg("n1"));
        c.register_self();
        // 只有一个候选（自身），排除后没有目标
        let dest = c.migration_destination("n1", 1, 0);
        assert!(dest.is_none(), "唯一候选被排除时应返回 None: {dest:?}");
    }

    #[tokio::test]
    async fn remove_room_returns_false_when_absent() {
        let c = Cluster::new(tcfg("n1"));
        // 无 NATS bus 时只验证本地行为：房间不存在时不改任何状态。
        assert!(c.rooms_snapshot().is_empty());
    }

    #[test]
    fn apply_remote_snapshot_merges_owned_and_foreign_rooms() {
        // `rooms` 只含发送方归属的房间，`total` 是发送方看到的全集群房间数 ——
        // 两者含义不同，所以本地房间数应由 rooms 决定，不能拿 total 去删已有房间。
        let c = Cluster::new(tcfg("n1"));
        let foreign = RoomState::new("r-f", 1, 5).with_owner(
            Some("n2".to_string()),
            "192.168.0.11:8080",
            NodeRole::Full,
        );
        let own = RoomState::new("r-o", 1, 2).with_owner(
            Some("n1".to_string()),
            "192.168.0.10:8080",
            NodeRole::Full,
        );
        c.apply_remote_snapshot(SnapshotPayload {
            total: 5,
            rooms: vec![foreign, own],
        });
        assert_eq!(c.room_count(), 2, "视图里应有两个房间");
        assert_eq!(c.rooms_snapshot().len(), 1, "只有本节点归属的进本地 map");
        assert_eq!(c.rooms_snapshot()[0].id, "r-o");
        assert!(c.registry.lock().room("r-f").is_some());
        assert_eq!(c.total_listeners(), 7);
    }

    #[test]
    fn listener_count_update_keeps_ownership() {
        // add_listener / remove_listener 走的是 `with_counts`：只改计数，
        // owner 必须保持，否则整个集群会把房间当成未归属的 pending 房间，
        // Router::assign 跳过它、故障迁移也找不到它。
        let c = Cluster::new(tcfg("n1"));
        let room = RoomState::new("r1", 1, 3).with_owner(
            Some("n2".to_string()),
            "192.168.0.11:8080",
            NodeRole::Full,
        );
        c.apply_remote_room(room.clone());
        c.apply_remote_room(room.with_counts(1, 4));
        let reg = c.registry.lock();
        let cur = reg.room("r1").expect("房间仍在");
        assert_eq!(cur.owner.as_deref(), Some("n2"), "加旁听后归属不能丢");
        assert!(cur.is_owned());
    }
}
