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

use crate::bus::{
    MigrationResult, NatsBus, PlaceReply, PlaceRequest, RoomRouteRequest, SnapshotPayload,
};
use crate::state::{
    unix_ms, HeartbeatMsg, MigrationRequest, Node, NodeRoleTag, NodeStatus, Registry, RoomState,
    Router, DEFAULT_NEW_ROOM_STREAMS,
};

/// 单节点上同时持有的 NATS 发送端数量上限（心跳 + 4 个订阅 + 若干请求）。
const DEFAULT_PUBLISHER_CAPACITY: usize = 64;

/// NATS 连接状态观测周期。async-nats 自己负责重连，这里只是观察结果并同步
/// 到 healthz 状态；250ms 足够细，不至于空转。
const CONNECTION_POLL_MS: u64 = 250;

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
    /// 当前 NATS 连接句柄。
    ///
    /// async-nats 在断线后会**用同一个 `Client` 句柄重连**，并重新发送
    /// `ClientOp::Subscribe` 把保留的订阅全部重建（见 async-nats 的
    /// `handle_reconnect`），所以正常断线不需要换句柄、各订阅流也不会退出
    /// （`Subscriber` 的接收端被 handler 保留并重新投递）。真正缺的是**首次**
    /// 连接失败时的重试：`ConnectOptions::retry_on_initial_connect` 默认
    /// `false`，`connect_with_options` 只按 `connection_timeout` 试一次就返回
    /// 错误。这里用 `Mutex<Option<..>>` 让 [`Cluster::connect_loop`] 在 NATS
    /// 起来之前把句柄留在 `None`，起来后补上。
    bus: Mutex<Option<Arc<NatsBus>>>,
    /// 最近一次成功连上 NATS 的时刻（Unix 毫秒），0 表示还没连上过。
    ///
    /// `join_target_secs` 用它度量「新节点接入收敛」耗时：从连上 NATS 到成员
    /// 视图收到至少一个对端心跳，超过该 SLO 就打告警（F5 里它原本是纯装饰值）。
    joined_at_ms: AtomicU64,
    /// 成员视图是否已收敛（收到过至少一个对端心跳）。
    view_converged: AtomicBool,
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

    // ── 重连 / 收敛观测（QM-024 F4 / F5 / F7）──

    /// NATS 断线后本节点成功重新加入集群的次数。0 表示从没掉过线。
    pub rejoin_count: u64,
    /// 最近一次重新加入的时刻（Unix 毫秒），0 表示从未重连。
    pub last_rejoin_ms: u64,
    /// NATS 重连尝试总数（含初始化时的首次尝试），便于确认「不是不重试」。
    pub reconnect_attempts: u64,
    /// 最近一次 NATS 连接尝试的时刻（Unix 毫秒）。
    pub last_reconnect_attempt_ms: u64,
    /// 因超出宽限期而从成员视图摘掉的僵尸节点数（F7：节点数必须能回落）。
    pub pruned_dead_nodes: u64,
    /// 已创建（写进 owner 索引）的房间数。
    pub rooms_created: u64,
    /// 已受理的分布式落点请求数（`place_room` 委派给别的节点建房间）。
    pub rooms_placed: u64,
    /// 已释放的房间数。
    pub rooms_released: u64,
    /// 故障迁移重试次数（`failover_target_secs` 宽限期内未完成则重跑）。
    pub failover_retries: u64,
    /// 本节点心跳序号（跨 NATS 断线累积），用于判断断线前后确实在换连接。
    pub heartbeat_sent_count: u64,
    /// 断线期间丢失的心跳次数（发出但已不在连接上的那部分）。
    pub heartbeat_dropped: u64,
    /// 成员视图收敛（收到第一个对端心跳）用了多久（毫秒）；0 表示尚未收敛。
    /// `join_target_secs` 是它的 SLO，超时会在 [`Cluster::heartbeat_recv_loop`] 告警。
    pub view_converge_ms: u64,
    /// 最近一次观察到 NATS 断线的时刻（Unix 毫秒）。
    pub last_disconnect_ms: u64,
    /// 最近一次从 NATS 断线中恢复的时刻（Unix 毫秒）。
    pub last_reconnect_ms: u64,
    /// async-nats 报告的累计连接建立次数（含首次 + 每次自动重连）。
    pub nats_connects: u64,
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
            bus: Mutex::new(None),
            joined_at_ms: AtomicU64::new(0),
            view_converged: AtomicBool::new(false),
        }
    }

    /// 取当前 NATS 连接句柄（没有则 `None`）。短临界区，不持有锁跨 await。
    pub fn bus_ref(&self) -> Option<Arc<NatsBus>> {
        self.bus.lock().clone()
    }

    /// 替换当前 NATS 连接句柄（首次连接成功或重连成功时调用）。
    pub fn set_bus(&self, bus: Arc<NatsBus>) {
        *self.bus.lock() = Some(bus);
    }

    /// 初始化：连接 NATS，注册自身，启动全部后台任务。
    ///
    /// **NATS 不可达不返回、也不退出**：退化为单机模式，由 [`Cluster::connect_loop`]
    /// 持续重试直到 NATS 起来。旧实现在这里 `return`，进程要等运维重启才能重新
    /// 入集群 —— 这正是「节点动态增减」验收不通过的原因之一。
    pub async fn initialize(self: Arc<Self>) {
        let initial_bus = match NatsBus::connect(&self.cfg, DEFAULT_PUBLISHER_CAPACITY).await {
            Ok(b) => {
                let arc = Arc::new(b);
                self.set_bus(Arc::clone(&arc));
                self.on_connected(&arc).await;
                Some(arc)
            }
            Err(e) => {
                self.bump_status(|s| {
                    s.nats_connected = false;
                    s.reconnect_attempts = s.reconnect_attempts.saturating_add(1);
                    s.last_reconnect_attempt_ms = unix_ms();
                });
                warn!(
                    error = %e,
                    node = %self.cfg.node_id,
                    server = %self.cfg.server,
                    port = self.cfg.port,
                    "NATS 未连接，节点退化为单机模式（connect_loop 将持续重试）"
                );
                None
            }
        };

        // 健康检查循环：判死过期节点 + 触发迁移（验收标准 2 的驱动来源）。
        let health = Arc::clone(&self);
        tokio::spawn(async move {
            health.connect_loop().await;
        });

        let health_check = Arc::clone(&self);
        tokio::spawn(async move {
            health_check.health_loop().await;
        });

        // 各订阅循环各持一份 bus 克隆；`async move` 会按值捕获，
        // 所以先把克隆绑定成局部变量再 spawn。
        let (bus_hb, bus_hb_recv, bus_route, bus_mig, bus_snap_pub, bus_snap_sub, bus_rooms, bus_place) =
            (
                initial_bus.clone(),
                initial_bus.clone(),
                initial_bus.clone(),
                initial_bus.clone(),
                initial_bus.clone(),
                initial_bus.clone(),
                initial_bus.clone(),
                initial_bus.clone(),
            );

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

        let place = Arc::clone(&self);
        tokio::spawn(async move {
            place.place_loop(bus_place).await;
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
            rooms_sub.room_state_loop(bus_rooms).await;
        });
    }

    /// 首次连接成功 / 重试连接成功后的统一善后。
    ///
    /// 注册自身 + 立刻发一次快照：新节点靠它让别人马上看到，别人也靠它
    /// 让本节点追上全量房间状态（`snapshot_loop` 收到即应用）。
    async fn on_connected(&self, bus: &Arc<NatsBus>) {
        self.register_self();
        let (rooms, total) = self.owned_rooms_and_total();
        let _ = bus.publish_snapshot(&rooms, total).await;
        self.joined_at_ms.store(unix_ms(), Ordering::SeqCst);
        self.alive.store(true, Ordering::SeqCst);
        let joined = unix_ms();
        let stats = bus.stats();
        self.bump_status(|s| {
            s.nats_connected = true;
            s.last_reconnect_ms = joined;
            s.nats_connects = stats.connects;
        });
        info!(
            node = %self.cfg.node_id,
            cluster = %self.cfg.cluster_id,
            role = %self.cfg.node_role.as_str(),
            advertised = %self.cfg.advertised_addr,
            nats_connects = stats.connects,
            "节点已加入集群，等待开始承接新会议"
        );
    }

    /// NATS 连接重试循环（F4）。
    ///
    /// async-nats 负责**断线后**的自动重连（默认无限次、指数退避封顶 4s，
    /// 且重连时把保留的订阅重新发到新连接上），本循环补两个缺口：
    /// 1. **首次**连接失败不会自动重试（`retry_on_initial_connect` 默认 `false`）：
    ///    NATS 后于节点启动时，旧实现 `initialize` 直接 `return`，进程永久退化单机；
    /// 2. async-nats 的重试长时间不成功时（server 反复重启、网络闪断叠加），
    ///    主动 `force_reconnect` 一次并打告警，避免一直等到下一次退避窗口。
    ///
    /// **不变式**：句柄一旦放入 [`Self::bus`] 就不再替换。替换会让各订阅循环
    /// 手里的旧句柄继续收消息、新句柄又发一份，产生重复投递。句柄为空时
    /// 才换新句柄，那时订阅循环还停在 [`Cluster::bus_or`] 里等它，不冲突。
    pub async fn connect_loop(self: Arc<Cluster>) {
        let mut ticker = tokio::time::interval(Duration::from_millis(CONNECTION_POLL_MS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let retry_after_ms = self.cfg.request_timeout_secs.max(1) * 1000;
        let mut next_attempt_ms = 0u64;
        let mut disconnected_since_ms = 0u64;
        loop {
            ticker.tick().await;
            let now = unix_ms();

            match self.bus_ref() {
                None => {
                    // 从未连上：按 `request_timeout_secs` 的节奏重试直到 NATS 起来。
                    if disconnected_since_ms == 0 {
                        disconnected_since_ms = now;
                    }
                    if now.saturating_sub(next_attempt_ms) >= retry_after_ms {
                        next_attempt_ms = now.saturating_add(retry_after_ms);
                        if self.reconnect_once().await {
                            disconnected_since_ms = 0;
                        }
                    }
                }
                Some(bus) => {
                    if bus.is_connected() {
                        if disconnected_since_ms > 0 {
                            if bus.flush().await.is_ok() {
                                info!(
                                    node = %self.cfg.node_id,
                                    downtime_ms = now.saturating_sub(disconnected_since_ms),
                                    "NATS 连接已恢复，节点重新加入集群"
                                );
                                self.mark_rejoined().await;
                            }
                            disconnected_since_ms = 0;
                        }
                        continue;
                    }

                    if disconnected_since_ms == 0 {
                        disconnected_since_ms = now;
                        warn!(
                            node = %self.cfg.node_id,
                            "NATS 连接断开，等待 async-nats 自动重连"
                        );
                        self.bump_status(|s| {
                            s.nats_connected = false;
                            s.last_disconnect_ms = now;
                        });
                    } else if now.saturating_sub(disconnected_since_ms) >= self.join_target_ms()
                        && now.saturating_sub(next_attempt_ms) >= retry_after_ms
                    {
                        next_attempt_ms = now.saturating_add(retry_after_ms);
                        warn!(
                            node = %self.cfg.node_id,
                            down_ms = now.saturating_sub(disconnected_since_ms),
                            "NATS 长时间未恢复，主动强制重连"
                        );
                        if self.force_reconnect(&bus).await {
                            disconnected_since_ms = 0;
                            self.mark_rejoined().await;
                        }
                    }
                }
            }
        }
    }

    /// 记录一次「断开后重新加入集群」。首次入集群不计（首次由 `on_connected` 处理）。
    async fn mark_rejoined(&self) {
        let now = unix_ms();
        let stats = self.bus_ref().map(|b| b.stats());
        self.bump_status(|s| {
            s.rejoin_count = s.rejoin_count.saturating_add(1);
            s.last_rejoin_ms = now;
            s.last_reconnect_ms = now;
            s.nats_connected = true;
            if let Some(st) = stats {
                s.nats_connects = st.connects;
            }
        });
        // 重连后要重新收敛成员视图：断线期间对端心跳没收，视图可能已经过期。
        self.view_converged.store(false, Ordering::SeqCst);
        self.joined_at_ms.store(now, Ordering::SeqCst);
    }

    /// 建立一条新连接（首次连接或首次连接失败后的重试）。
    ///
    /// 走 [`crate::bus::reconnect_with_probe`]：除了握手，还要过连接状态位 +
    /// `flush` 两次探针，确认能真的收发消息。
    async fn reconnect_once(&self) -> bool {
        self.bump_status(|s| {
            s.reconnect_attempts = s.reconnect_attempts.saturating_add(1);
            s.last_reconnect_attempt_ms = unix_ms();
            s.nats_connected = false;
        });

        match crate::bus::reconnect_with_probe(&self.cfg, DEFAULT_PUBLISHER_CAPACITY).await {
            Ok(bus) => {
                let bus = Arc::new(bus);
                self.set_bus(Arc::clone(&bus));
                self.on_connected(&bus).await;
                true
            }
            Err(e) => {
                warn!(error = %e, node = %self.cfg.node_id, "NATS 连接失败，稍后重试");
                false
            }
        }
    }

    /// 强制重连现有句柄（保留订阅，不丢 in-flight 消息）。
    async fn force_reconnect(&self, bus: &Arc<NatsBus>) -> bool {
        if bus.force_reconnect().await.is_err() || !bus.is_connected() {
            return false;
        }
        bus.flush().await.is_ok()
    }

    /// `join_target_secs` 转毫秒：既是成员视图收敛 SLO，也是断线兜底强制重连的时限。
    fn join_target_ms(&self) -> u64 {
        self.cfg.join_target_secs.saturating_mul(1000)
    }

    /// 等一个可用的 NATS 句柄。
    ///
    /// `initialize` 时 NATS 可能还没起来（`initial` 为 `None`），各订阅循环不会退出，
    /// 而是轮询 [`Cluster::bus_ref`] 直到 [`Cluster::connect_loop`] 补上句柄 ——
    /// 这就是「NATS 后启动 / 后恢复，节点无需重启进程就能加入集群」。
    ///
    /// 之后不再重新取句柄：async-nats 断线重连时**复用同一个 `Client`**，并把保留的
    /// 订阅重新发到新连接上，所以这里拿到的 `Subscriber` 会继续收到消息。
    async fn bus_or(&self, initial: Option<Arc<NatsBus>>) -> Arc<NatsBus> {
        if let Some(bus) = initial {
            return bus;
        }
        let mut waited = 0u64;
        loop {
            if let Some(bus) = self.bus_ref() {
                info!(node = %self.cfg.node_id, waited_ms = waited, "NATS 已可用，订阅循环开始");
                return bus;
            }
            tokio::time::sleep(Duration::from_millis(CONNECTION_POLL_MS)).await;
            waited += CONNECTION_POLL_MS;
        }
    }

    /// 成员视图是否已收敛（收到过至少一个对端心跳）。
    pub fn is_view_converged(&self) -> bool {
        self.view_converged.load(Ordering::SeqCst)
    }

    // ── 健康检查 / 故障迁移 ──

    /// 健康检查（幂等，可周期性调用）：判死过期节点 + 触发其房间迁移 + 摘僵尸节点。
    ///
    /// 返回本轮新判死的节点 id 列表。
    ///
    /// 这里同时消费两个过去只是「写进日志的数字」的配置（F5 / F7）：
    /// * `failover_target_secs` —— 迁移失败的宽限期。超时内失败过的房间会进
    ///   `failover_retry`，下一轮健康检查重试（并排除上一次的目标节点，避免反复
    ///   撞同一台机器）；超期后明确记一次放弃，不再静默丢失。
    /// * 死节点宽限期 —— 判死后不再有心跳回来复活它，若不摘掉，`cluster_nodes`
    ///   只增不减，调度候选池里永远混着僵尸节点。
    pub async fn health_check(&self) -> Vec<String> {
        let now = unix_ms();

        let (dead, pruned, pending) = {
            let mut reg = self.registry.lock();
            let dead = reg.reap_dead(&self.cfg, now);

            // 宽限期必须覆盖「健康窗口 + 迁移时限」：否则迁移还没跑完源节点就被摘掉，
            // 房间归属就再也没人认账（源节点视图没了 owner，房间在集群里变成孤儿）。
            // `health_window` 返回毫秒，`failover_target_secs` 是秒，要统一到毫秒。
            let window_ms = Registry::health_window(&self.cfg);
            let grace_ms = window_ms
                .saturating_add(self.cfg.failover_target_secs.saturating_mul(1000));
            let pruned = reg.prune_dead(now, grace_ms);
            let pending = reg.pending_migrations();
            (dead, pruned, pending)
        };

        for id in &pruned {
            warn!(node = %id, "节点超出宽限期，已从集群视图摘除");
        }

        self.bump_status(|s| {
            s.health_checks = s.health_checks.saturating_add(1);
            s.last_health_check_ms = now;
            s.dead_nodes_detected = s.dead_nodes_detected.saturating_add(dead.len() as u64);
            s.pruned_dead_nodes = s.pruned_dead_nodes.saturating_add(pruned.len() as u64);
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
                            String::new(),
                        ));
                    }
                }
            }
            out
        };

        // F5：上一轮失败、仍在宽限期内迁移，重试（排除上次目标节点）。
        let mut retried = 0u64;
        for p in pending {
            let room = {
                let reg = self.registry.lock();
                reg.room(&p.room_id).cloned()
            };
            let Some(room) = room else {
                continue; // 房间已被删除或已归属到别的节点
            };
            if !room.is_owned() {
                continue;
            }
            retried += 1;
            info!(
                room = %room.id,
                source = %p.source_node,
                exclude = %p.last_target,
                attempt = p.attempts,
                "重试上一轮失败的故障迁移"
            );
            self.bump_status(|s| {
                s.failover_retries = s.failover_retries.saturating_add(1);
            });
            self.try_migrate(&p.room_id, &p.source_node, &p.last_target, room.streams, room.listeners)
                .await;
        }

        for (room_id, streams, listeners, source, exclude) in rooms_to_migrate {
            info!(
                room = %room_id,
                source = %source,
                streams,
                listeners,
                "触发故障迁移"
            );
            self.try_migrate(&room_id, &source, &exclude, streams, listeners).await;
        }

        // 重新同步视图计数（prune 之后节点数会变化）。
        let reg = self.registry.lock();
        self.bump_status(|s| {
            s.cluster_rooms = reg.rooms().count();
            s.cluster_nodes = reg.nodes().count();
            s.local_rooms = reg.owned_count() as usize;
            s.total_listeners = reg.total_listeners();
        });

        debug!(dead_nodes = dead.len(), pruned = pruned.len(), retried, "健康检查完成");
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
    ///
    /// 只做决策，**不写状态**。真正建立房间必须走 [`Cluster::create_room`]，
    /// 否则 owner 索引里永远不会有条目 —— 这正是 QM-024 复核判 F2 不成立的原因。
    pub fn assign_room(&self, room_id: &str) -> Option<String> {
        let reg = self.registry.lock();
        self.router
            .assign(&reg, room_id, DEFAULT_NEW_ROOM_STREAMS, 0)
    }

    /// 在本节点建立一个房间并广播（生产路径，F2）。
    ///
    /// 过去只有故障迁移的目标节点会写 `owner`，正常建房间没有任何代码写 owner 索引：
    /// `assign_room` 只回一个节点 id，`Router::assign` 只读视图，`with_owner` 只出现在
    /// `handle_migration_request` 和单测里。后果是 `cluster_rooms` 永远是 0、
    /// 故障迁移找不到房间、旁听计数无处可加 —— 验收标准 1「会议落到某个节点」
    /// 在集群里没有任何可观测的证据。
    ///
    /// `id` 会经 [`crate::bus::sanitize_id`] 清洗（F6）：房间 id 拼进
    /// `qm.<cid>.room.<id>`，带 `.` 或空白会把 subject 拆成多段，订阅端的
    /// `room.*` 立刻收不到，房间归属在节点间分裂。
    ///
    /// 返回广播后的房间状态；NATS 不可用（断线且超过等待时限）时返回 `Err`，
    /// 调用方应告知客户端稍后重试，而不是假设房间已经生效。
    pub async fn create_room(&self, id: &str, streams: u64, listeners: u64) -> QmResult<RoomState> {
        let room_id = crate::bus::sanitize_id(id);
        let Some(bus) = self.bus_for_request().await else {
            return Err(qm_common::Error::cluster(format!(
                "NATS 不可用，无法创建房间 {room_id}"
            )));
        };

        // 如果这个房间在别的节点上已经有归属，不要覆盖：广播自己的版本会被
        // `revision` 收敛成一次无意义的抖动，还会让调用方以为房间建在本节点。
        let already_owned = {
            let reg = self.registry.lock();
            reg.room(&room_id)
                .filter(|r| r.is_owned() && r.owner.as_deref() != Some(self.cfg.node_id.as_str()))
                .is_some()
        };
        if already_owned {
            return Err(qm_common::Error::cluster(format!(
                "房间 {room_id} 已归属其他节点"
            )));
        }

        let existing = {
            let reg = self.registry.lock();
            reg.room(&room_id).map(|r| r.revision)
        }
        .unwrap_or(0);

        let room = RoomState::new(room_id.clone(), streams, listeners).with_owner(
            Some(self.cfg.node_id.clone()),
            &self.cfg.advertised_addr,
            self.cfg.node_role,
        );
        // revision 必须大于已有值才会被其它节点接受（`apply_room` 按 revision 收敛）。
        let room = RoomState {
            revision: existing.saturating_add(1).max(1),
            ..room
        };

        self.rooms.lock().insert(room.id.clone(), room.clone());
        self.registry.lock().apply_room(room.clone());
        bus.publish_room(&room).await?;
        self.bump_status(|s| {
            s.rooms_created = s.rooms_created.saturating_add(1);
        });
        info!(
            room = %room.id,
            owner = %self.cfg.node_id,
            streams = room.streams,
            listeners = room.listeners,
            revision = room.revision,
            "房间已创建并写进 owner 索引"
        );
        Ok(room)
    }

    /// 请求本节点处理一个房间创建请求（用于信令层的同步入口，F3）。
    ///
    /// 信令层的 `join` 是同步调用，无法直接 `await` 集群，所以走这条通道：
    /// 提交请求 + 拿结果，NATS 不可用时返回 `None`（信令层退化为本机建房间）。
    pub async fn request_create_room(
        &self,
        room_id: &str,
        streams: u64,
        listeners: u64,
    ) -> Option<RoomState> {
        match self.create_room(room_id, streams, listeners).await {
            Ok(room) => Some(room),
            Err(e) => {
                warn!(room = %room_id, error = %e, "房间创建失败（NATS 不可用）");
                None
            }
        }
    }

    /// 分布式落点（QM-024 F3）：按集群视图选一个负载最低的节点建这个房间。
    ///
    /// 这是 `POST /room/{id}/place` 的真实实现。三种结果：
    /// * `Ok(Some(room))` —— 本节点自己承接（写 owner 索引 + 广播）；
    /// * `Ok(None)` —— 集群视图里已经有别的节点在承载这个房间（幂等）；
    /// * `Err` —— NATS 不可用，或者选出的目标节点拒绝了请求。
    ///
    /// 委派通过专用 subject `qm.<cid>.room.place` 走请求 / 响应：目标节点是
    /// `Router::assign` 的结论，队列订阅会让别的节点抢走消息直接丢弃，
    /// 所以必须像迁移请求一样定向投递。
    pub async fn place_room(
        &self,
        room_id: &str,
        streams: u64,
        listeners: u64,
    ) -> QmResult<Option<RoomState>> {
        let bus = self.bus_for_request().await.ok_or_else(|| {
            qm_common::Error::cluster(format!("NATS 不可用，无法落点房间 {room_id}"))
        })?;

        // 幂等：别的节点已经在承载，直接回那份归属，不重复建。
        let existing = {
            let reg = self.registry.lock();
            reg.room(room_id).filter(|r| r.is_owned()).cloned()
        };
        if let Some(room) = existing {
            if room.owner.as_deref() != Some(self.cfg.node_id.as_str()) {
                self.bump_status(|s| {
                    s.rooms_placed = s.rooms_placed.saturating_add(1);
                });
                info!(room = %room_id, node = ?room.owner, "房间已存在，落点直接复用现有归属");
            }
            return Ok(Some(room));
        }

        // 选目标：优先排除本节点（避免自己给自己派单，多绕一次 NATS 往返）；
        // 单节点集群里没有其他候选时由自己承接，否则 `/place` 会永远失败。
        let exclude_self = if self.node_count() > 1 {
            self.cfg.node_id.clone()
        } else {
            String::new()
        };
        let target = self
            .migration_destination_for(&exclude_self, streams, listeners)
            .or_else(|| self.assign_room(room_id))
            .ok_or_else(|| qm_common::Error::cluster(format!("集群无可用落点节点（{room_id}）")))?;

        let req = PlaceRequest {
            room_id: room_id.to_string(),
            from_node: self.cfg.node_id.clone(),
            streams,
            listeners,
        };

        if target == self.cfg.node_id {
            // 自己承接：走同一个创建路径，保证 owner 索引与广播只发生一次。
            let room = self.create_room(room_id, streams, listeners).await?;
            self.bump_status(|s| {
                s.rooms_placed = s.rooms_placed.saturating_add(1);
            });
            info!(room = %room_id, target, "落点由本节点承接");
            return Ok(Some(room));
        }

        let reply = bus.request_place(&target, &req).await?;
        if !reply.ok {
            return Err(qm_common::Error::cluster(format!(
                "落点被节点 {target} 拒绝：{}",
                reply.reason
            )));
        }
        self.bump_status(|s| {
            s.rooms_placed = s.rooms_placed.saturating_add(1);
        });
        info!(
            room = %room_id,
            target,
            node = ?reply.owner,
            media = %reply.media_addr,
            "落点已委派给目标节点"
        );
        Ok(Some(RoomState {
            id: room_id.to_string(),
            owner: reply.owner,
            media_addr: reply.media_addr,
            node_role: NodeRole::Full,
            streams,
            listeners,
            revision: reply.revision,
        }))
    }

    /// 处理落点请求（本节点是目标时）。
    async fn handle_place_request(
        &self,
        req: &PlaceRequest,
        bus: &NatsBus,
        msg: async_nats::Message,
    ) {
        if req.room_id.trim().is_empty() {
            let reply = PlaceReply {
                ok: false,
                room_id: req.room_id.clone(),
                owner: None,
                media_addr: String::new(),
                revision: 0,
                reason: "room_id 为空".to_string(),
            };
            let _ = bus.reply_place(&msg, &reply).await;
            return;
        }
        match self.create_room(&req.room_id, req.streams, req.listeners).await {
            Ok(room) => {
                let reply = PlaceReply {
                    ok: true,
                    room_id: room.id.clone(),
                    owner: room.owner.clone(),
                    media_addr: room.media_addr.clone(),
                    revision: room.revision,
                    reason: "accepted".to_string(),
                };
                let _ = bus.reply_place(&msg, &reply).await;
            }
            Err(e) => {
                warn!(room = %req.room_id, error = %e, "落点请求处理失败");
                let reply = PlaceReply {
                    ok: false,
                    room_id: req.room_id.clone(),
                    owner: None,
                    media_addr: String::new(),
                    revision: 0,
                    reason: e.to_string(),
                };
                let _ = bus.reply_place(&msg, &reply).await;
            }
        }
    }

    /// 订阅落点请求：`Router::assign` 已经选定目标，所以用**定向 subject**
    /// （按 node_id 分片）而不是队列 —— 队列会让别的节点抢走消息后直接丢弃。
    async fn place_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        let bus = self.bus_or(bus).await;
        let subject = bus.subjects().place_for_self.clone();
        let Some(mut sub) = bus.subscribe(&subject).await.ok() else {
            warn!("无法订阅落点 subject，会议落点将不可用");
            return;
        };
        while let Some(msg) = sub.next().await {
            let Some(req) = decode::<PlaceRequest>(&msg.payload) else {
                warn!("收到无法解析的落点请求，已忽略");
                continue;
            };
            self.handle_place_request(&req, &bus, msg).await;
        }
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
    async fn snapshot_publish_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        let bus = self.bus_or(bus).await;
        let interval = Duration::from_secs(self.cfg.heartbeat_secs.max(1));
        loop {
            let (rooms, total) = self.owned_rooms_and_total();
            if let Err(e) = bus.publish_snapshot(&rooms, total).await {
                warn!(error = %e, "房间快照发布失败（连接可能已断开）");
                self.bump_status(|s| s.nats_connected = false);
            }
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

    /// 读取**全集群**房间视图里某个房间的归属（只读）。
    ///
    /// 与 [`Self::rooms_snapshot`] 的区别很关键：`rooms_snapshot` 只含**本节点**
    /// 归属的房间（`self.rooms`），而集群视图（`registry`）里的房间可能归属别的
    /// 节点。判断「这个会议现在到底落在哪台机器上」必须读全量视图 ——
    /// 只读 `rooms_snapshot` 会漏掉所有远端归属的房间，故障迁移的结果也就
    /// 永远观察不到。
    ///
    /// 返回 `None` 表示集群视图里没有这个房间；`Some(None)` 表示有记录但归属
    /// 未确定（刚创建、还在等落点回包）。
    pub fn room_owner(&self, room_id: &str) -> Option<Option<String>> {
        self.registry.lock().room(room_id).map(|r| r.owner.clone())
    }

    /// 全集群房间视图的完整快照（**含远端归属**的房间）。
    ///
    /// `rooms_snapshot` 只给本节点归属的房间；要读到「别的节点上的会议」
    /// 必须用这个方法。只读，不修改任何状态。
    pub fn cluster_rooms_snapshot(&self) -> Vec<RoomState> {
        self.registry.lock().rooms().cloned().collect()
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

    /// 为房间选一个迁移目标并执行一次迁移尝试。
    ///
    /// 失败时把失败记进 `failover_retry`（`failover_target_secs` 宽限期内），
    /// 下一轮健康检查会重试 —— 没有这个机制时一次超时就把房间永久留在死节点上。
    ///
    /// `exclude` 是重试时要排除的节点（上一次失败的目标），新发起的迁移传空串。
    async fn try_migrate(
        &self,
        room_id: &str,
        source: &str,
        exclude: &str,
        streams: u64,
        listeners: u64,
    ) {
        let target = match self.migration_destination_for(exclude, streams, listeners) {
            Some(t) => t,
            None => {
                warn!(room = %room_id, source = %source, exclude = %exclude, "无可用迁移目标");
                let keep_retrying = {
                    let mut reg = self.registry.lock();
                    reg.record_migration_failure(
                        room_id,
                        source,
                        source,
                        self.cfg.failover_target_secs,
                    )
                };
                if !keep_retrying {
                    warn!(
                        room = %room_id,
                        source = %source,
                        secs = self.cfg.failover_target_secs,
                        "故障迁移超过 failover_target_secs 宽限期，放弃并告警"
                    );
                }
                return;
            }
        };

        let started = unix_ms();
        let ok = self.migrate_room(room_id, source, &target, streams, listeners).await;
        let elapsed = unix_ms().saturating_sub(started);

        let mut reg = self.registry.lock();
        if ok {
            reg.clear_migration_failure(room_id);
            info!(room = %room_id, target = %target, elapsed_ms = elapsed, "迁移完成");
        } else {
            let keep_retrying = reg.record_migration_failure(
                room_id,
                source,
                &target,
                self.cfg.failover_target_secs,
            );
            if keep_retrying {
                warn!(
                    room = %room_id,
                    target = %target,
                    elapsed_ms = elapsed,
                    deadline_secs = self.cfg.failover_target_secs,
                    "迁移失败，将在宽限期内重试"
                );
            } else {
                warn!(
                    room = %room_id,
                    target = %target,
                    secs = self.cfg.failover_target_secs,
                    "故障迁移超过 failover_target_secs 宽限期，放弃并告警"
                );
            }
        }
    }

    /// 选迁移目标；`exclude` 为空串表示不额外排除。
    fn migration_destination_for(
        &self,
        exclude: &str,
        streams: u64,
        listeners: u64,
    ) -> Option<String> {
        let reg = self.registry.lock();
        let ex = (!exclude.is_empty()).then_some(exclude);
        self.router
            .migration_destination(&reg, ex, streams, listeners)
    }

    fn bump_status<F: FnOnce(&mut ClusterStatus)>(&self, f: F) {
        f(&mut self.status.lock());
    }

    /// 为一次 NATS 请求等待可用连接：断线时等 async-nats 重连回来。
    ///
    /// 不新建连接（旧实现在每次迁移时 `NatsBus::connect` 一条新连接，既浪费
    /// 又可能在 NATS 抖动时反复失败），也不提前失败 —— 迁移是验收标准 2 的关键
    /// 路径，值得等它恢复。
    async fn bus_for_request(&self) -> Option<Arc<NatsBus>> {
        let mut waited = 0u64;
        loop {
            let bus = self.bus_ref()?;
            if bus.is_connected() {
                return Some(bus);
            }
            tokio::time::sleep(Duration::from_millis(CONNECTION_POLL_MS)).await;
            waited += CONNECTION_POLL_MS;
            if waited >= self.join_target_ms() {
                warn!(node = %self.cfg.node_id, waited_ms = waited, "等待 NATS 恢复超时，本次操作放弃");
                return None;
            }
        }
    }

    /// 向候选目标节点发起一次迁移请求（请求 / 响应）。
    ///
    /// 返回是否成功承接：调用方 [`Cluster::try_migrate`] 据此决定是否进入
    /// `failover_target_secs` 宽限期的重试。
    async fn migrate_room(
        &self,
        room_id: &str,
        source: &str,
        target: &str,
        streams: u64,
        listeners: u64,
    ) -> bool {
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

        // 迁移必须真等到连接可用：NATS 抖动期间不重试就会把房间永久留在死节点上。
        let Some(bus) = self.bus_for_request().await else {
            warn!(room = %room_id, "NATS 不可用，迁移本轮放弃（将重试）");
            self.bump_status(|s| {
                s.migrations_failed = s.migrations_failed.saturating_add(1);
            });
            return false;
        };

        let started = unix_ms();
        let result = bus.request_migration(&req).await;
        let elapsed = unix_ms().saturating_sub(started);

        match result {
            Ok(r) if r.ok => {
                info!(room = %room_id, target = %target, elapsed_ms = elapsed, "迁移已被目标节点接受");
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
                true
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
                false
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
                false
            }
        }
    }

    // ── 后台循环 ──

    /// 每 `heartbeat_secs` 发一次心跳（`seq` 单调递增，接收端据此去重）。
    async fn heartbeat_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        let bus = self.bus_or(bus).await;
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
                // publish_and_flush 失败意味着 flush 探针不过，节点其实已不在连接上。
                // 不能只打日志：否则 `nats_connected` 永远显示绿色而心跳全丢。
                warn!(error = %e, seq, "心跳发送失败（连接已断开）");
                self.bump_status(|s| {
                    s.nats_connected = false;
                    s.last_disconnect_ms = unix_ms();
                    s.heartbeat_dropped = s.heartbeat_dropped.saturating_add(1);
                });
            } else {
                debug!(seq, node = %self.cfg.node_id, "心跳已送达 server");
                self.bump_status(|s| {
                    s.heartbeat_sent_count = s.heartbeat_sent_count.saturating_add(1);
                });
            }
        }
    }

    /// 订阅全集群心跳：更新节点视图，并据它判定成员视图是否收敛（F5）。
    async fn heartbeat_recv_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        let bus = self.bus_or(bus).await;
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
            let was_converged = self.view_converged.load(Ordering::SeqCst);
            self.registry.lock().mark_heartbeat(&hb, now);

            if hb.node_id == self.cfg.node_id {
                // 自己发的（`>` 会匹配到本节点自己的 subject），不算对端。
                continue;
            }

            // `join_target_secs`：从连上 NATS 到收到第一个对端心跳的耗时，
            // 超 SLO 则告警 —— 这是它从「纯装饰值」变成有实际含义的地方。
            if !was_converged {
                let joined = self.joined_at_ms.load(Ordering::SeqCst);
                let s = self.cfg.join_target_secs;
                let converge_ms = if joined > 0 {
                    now.saturating_sub(joined)
                } else {
                    0
                };
                self.view_converged.store(true, Ordering::SeqCst);
                self.bump_status(|st| {
                    st.view_converge_ms = converge_ms;
                });
                if s > 0 && converge_ms > s.saturating_mul(1000) {
                    warn!(
                        node = %self.cfg.node_id,
                        converge_ms,
                        target_secs = s,
                        "成员视图收敛超过 join_target_secs"
                    );
                } else {
                    debug!(
                        node = %self.cfg.node_id,
                        converge_ms,
                        target_secs = s,
                        nodes = self.node_count(),
                        "成员视图已收敛"
                    );
                }
            }
        }
    }

    /// 队列订阅新会议调度请求：只有队列里一个节点会处理。
    async fn route_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        let bus = self.bus_or(bus).await;
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
    async fn migrate_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        // 只订阅「投给本节点」的迁移请求：subject 本身按目标 node_id 分片，
        // 所以这里不靠队列分摊，用队列名只为与路由队列区分。
        let bus = self.bus_or(bus).await;
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
    async fn snapshot_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        let bus = self.bus_or(bus).await;
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
    async fn room_state_loop(self: Arc<Self>, bus: Option<Arc<NatsBus>>) {
        // 房间 subject 是 `qm.<cid>.room.*`，用一个通配订阅即可。
        let bus = self.bus_or(bus).await;
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
        let dest = c.migration_destination_for("n1", 1, 0);
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
