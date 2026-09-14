//! 集群核心纯函数层：状态模型、最低负载调度、健康判定与故障迁移目标选择。
//!
//! 这一层**不触碰任何 IO / NATS**，因此可以在没有 NATS 服务器的情况下
//! 用单元测试逐条断言验收逻辑：
//! * 房间状态收敛（[`Registry::apply_room`]）—— 按 `revision` 收敛到全局一致；
//! * 最低负载调度（[`Router::assign`]）—— 新会议自动落到负载最低的媒体节点；
//! * 健康判定（[`Registry::mark_dead`]）—— 连续错过心跳即判死并自动下线；
//! * 迁移目标选择（[`Router::migration_destination`]）—— 只选健康且有余量的节点。
//!
//! 与 [`crate::bus`] / [`crate::cluster`] 的关系：这里只产出决策，
//! 上层负责把决策通过 NATS 变成跨节点一致的状态。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use qm_common::{ClusterConfig, NodeRole};

/// 心跳间隔默认值（秒）。
pub const DEFAULT_HEARTBEAT_SECS: u64 = 5;
/// 连续错过心跳次数默认值：2 × 5s = 10s，正好落在验收标准 2 的窗口内。
pub const DEFAULT_UNHEALTHY_MISSES: u64 = 2;
/// 迁移请求超时默认值（秒）。
pub const DEFAULT_MIGRATION_TIMEOUT_SECS: u64 = 5;
/// 新会议请求默认承载的上行媒体流条数。
pub const DEFAULT_NEW_ROOM_STREAMS: u64 = 1;
/// 迁移请求默认携带的旁听者数量（无数据时的保守默认）。
pub const DEFAULT_MIGRATION_LISTENERS: u64 = 0;

// ─────────────────────────── 房间状态 ───────────────────────────

/// 房间状态：跨节点同步的最小一致单元。
///
/// `revision` 是单调递增的**消息序号**（不是分布式版本号）：任何节点收到同一房间的
/// 更新时，只接受 `revision` 大于等于当前值的消息，并始终覆盖本节点视图 —— 于是
/// 所有节点对同一房间收敛到同一份状态，不需要额外协调。
///
/// `owner == None` 表示「房间已创建但归属未确定」，例如刚建好还在等调度回包。
/// 这类房间不参与调度，也不会被当成孤儿迁移 —— 这是两段式交接的安全锚点。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomState {
    pub id: String,
    /// 归属节点 id；`None` 表示归属未确定。
    pub owner: Option<String>,
    /// 归属节点的媒体回源地址（`host:port`，内网地址）。
    pub media_addr: String,
    /// 归属节点角色，随房间状态传播，旁听侧据此识别来源节点能力。
    pub node_role: NodeRole,
    /// 当前上行媒体流条数（推流端数量）。
    pub streams: u64,
    /// 旁听参会者数量（只收流不发流）。
    pub listeners: u64,
    /// 单调递增的消息序号。
    pub revision: u64,
}

impl RoomState {
    /// 新建房间的初始状态：`revision = 1`，尚无归属。
    pub fn new(id: impl Into<String>, streams: u64, listeners: u64) -> Self {
        Self {
            id: id.into(),
            owner: None,
            media_addr: String::new(),
            node_role: NodeRole::default(),
            streams,
            listeners,
            revision: 1,
        }
    }

    /// 是否已经有归属节点（只有有归属的房间才可能被判为孤儿）。
    pub fn is_owned(&self) -> bool {
        self.owner.is_some()
    }

    /// 迁移后的状态：刷新归属并递增 `revision`。
    ///
    /// 旁听计数不在这里清零 —— 迁的是媒体归属，旁听计数由迁移后新归属节点接管。
    pub fn with_owner(mut self, owner: Option<String>, media_addr: &str, role: NodeRole) -> Self {
        self.owner = owner;
        self.media_addr = media_addr.to_string();
        self.node_role = role;
        self.revision = self.revision.saturating_add(1);
        self
    }

    /// 用新的参与者计数刷新状态（递增 `revision`）。
    pub fn with_counts(mut self, streams: u64, listeners: u64) -> Self {
        self.streams = streams;
        self.listeners = listeners;
        self.revision = self.revision.saturating_add(1);
        self
    }
}

// ─────────────────────────── 节点状态 ───────────────────────────

/// 节点角色在跨节点消息中的呈现（避免直接耦合 [`NodeRole`] 的序列化细节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRoleTag {
    Full,
    Listener,
}

impl From<NodeRole> for NodeRoleTag {
    fn from(role: NodeRole) -> Self {
        match role {
            NodeRole::Full => NodeRoleTag::Full,
            NodeRole::Listener => NodeRoleTag::Listener,
        }
    }
}

impl NodeRoleTag {
    pub fn to_role(self) -> NodeRole {
        match self {
            NodeRoleTag::Full => NodeRole::Full,
            NodeRoleTag::Listener => NodeRole::Listener,
        }
    }
}

/// 节点健康状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
    /// 健康窗口内：可以承接新会议与迁移。
    Live,
    /// 已注册但超出健康窗口：自动下线。
    Dead,
}

/// 节点心跳载荷：集群成员关系的唯一事实来源。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMsg {
    pub node_id: String,
    /// 媒体回源地址（`host:port`）。
    pub advertised: String,
    pub role: NodeRoleTag,
    /// 单节点会议数上限（调度硬上限）。
    pub max_rooms: u64,
    /// 单节点旁听参会者上限。
    pub listener_capacity: u64,
    /// 旁听放大系数：一条上行流折算的旁听槽位。
    pub fanout: u64,
    /// 心跳序号（单调递增，去重用）。
    pub seq: u64,
    /// 心跳时间戳（Unix 毫秒）。
    pub ts: u64,
}

impl HeartbeatMsg {
    /// 由节点配置构造心跳载荷。
    pub fn from_node(cfg: &ClusterConfig, seq: u64) -> Self {
        Self {
            node_id: cfg.node_id.clone(),
            advertised: cfg.advertised_addr.clone(),
            role: NodeRoleTag::from(cfg.node_role),
            max_rooms: cfg.max_rooms_per_node as u64,
            listener_capacity: cfg.listener_capacity as u64,
            fanout: cfg.listener_fanout as u64,
            seq,
            ts: unix_ms(),
        }
    }
}

/// 集群节点视图（由心跳维护）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub node_id: String,
    pub advertised: String,
    pub role: NodeRoleTag,
    /// 最近一次收到心跳的 Unix 毫秒时间。
    pub last_seen_ms: u64,
    /// 最近一次心跳序号。
    pub last_seq: u64,
    pub status: NodeStatus,
    pub max_rooms: u64,
    /// 首次判死时刻，用于上报「故障下线时刻」。
    pub dead_since_ms: Option<u64>,
}

impl Node {
    /// 旁听容量权重：`fanout × listener_capacity`。
    pub fn weight(&self, fanout: u64, listener_capacity: u64) -> u64 {
        (fanout.max(1) * listener_capacity.max(1)).max(1)
    }

    /// 是否可以作为调度目标：健康 + 全功能角色。
    pub fn is_schedulable(&self) -> bool {
        self.status == NodeStatus::Live && self.role == NodeRoleTag::Full
    }
}

/// 迁移请求载荷：故障节点 -> 候选目标节点。
///
/// 源节点不直接改房间的归属 —— 它先发一条迁移请求，目标节点自己决定接不接；
/// 接了才发布新的房间状态。这样即使源节点在迁移途中再次崩溃，
/// 房间也不会同时拥有两个归属。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRequest {
    pub room_id: String,
    /// 原归属节点（已故障）。
    pub source_node: String,
    /// 请求目标节点。
    pub target_node: String,
    /// 当前上行媒体流条数。
    pub streams: u64,
    /// 当前旁听者数量。
    pub listeners: u64,
}

// ─────────────────────────── 注册表 ───────────────────────────

/// 集群注册表：节点健康视图 + 全集群房间视图。
///
/// 全部**纯内存**：权威状态在 NATS（发布 + 订阅），本表只是本节点收敛后的视图。
/// 所以节点动态增减不需要任何持久化，新节点启动后靠订阅 + 一次全量快照即可跟上。
#[derive(Debug, Default)]
pub struct Registry {
    node_id: String,
    nodes: HashMap<String, Node>,
    rooms: HashMap<String, RoomState>,
}

impl Registry {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            nodes: HashMap::new(),
            rooms: HashMap::new(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 健康窗口：`heartbeat_secs × unhealthy_misses`。
    pub fn health_window(cfg: &ClusterConfig) -> u64 {
        cfg.heartbeat_secs
            .saturating_mul(cfg.unhealthy_misses.max(1))
    }

    /// 节点是否在健康窗口内。
    pub fn is_healthy(&self, node_id: &str, cfg: &ClusterConfig, now_ms: u64) -> bool {
        self.nodes.get(node_id).is_some_and(|n| {
            n.status == NodeStatus::Live
                && n.last_seen_ms.saturating_add(Self::health_window(cfg)) >= now_ms
        })
    }

    /// 注册 / 更新一个节点（本地注册自身时直接写入，不走心跳）。
    ///
    /// 与 [`Registry::mark_heartbeat`] 的区别：这里是**直接注册**（首次上线、
    /// 快照恢复），不需要「先判死再等下一跳」的收敛过程。
    pub fn insert_node(&mut self, node: Node) -> bool {
        let exists = self.nodes.contains_key(&node.node_id);
        self.nodes.insert(node.node_id.clone(), node);
        !exists
    }

    /// 处理一条心跳：首次见到的节点标记为 `Unknown` 等待下一跳，
    /// 已存在的节点更新最近心跳时间并重置为 `Live`。
    /// 返回 `true` 表示本次心跳改变了节点状态。
    pub fn mark_heartbeat(&mut self, msg: &HeartbeatMsg, now_ms: u64) -> bool {
        let node = self
            .nodes
            .entry(msg.node_id.clone())
            .or_insert_with(|| Node {
                node_id: msg.node_id.clone(),
                advertised: msg.advertised.clone(),
                role: msg.role,
                last_seen_ms: 0,
                last_seq: 0,
                status: NodeStatus::Dead,
                max_rooms: 0,
                dead_since_ms: None,
            });
        let changed = node.status != NodeStatus::Live || node.last_seq < msg.seq;
        node.last_seen_ms = msg.ts.max(now_ms.max(1));
        node.last_seq = msg.seq.max(node.last_seq);
        node.advertised = msg.advertised.clone();
        node.role = msg.role;
        node.max_rooms = msg.max_rooms;
        node.status = NodeStatus::Live;
        node.dead_since_ms = None;
        changed
    }

    /// 检查所有节点是否应该判死；返回新判死的节点 id 列表（用于触发迁移）。
    pub fn reap_dead(&mut self, cfg: &ClusterConfig, now_ms: u64) -> Vec<String> {
        let window = Self::health_window(cfg);
        let mut dead = Vec::new();
        for (_, node) in self.nodes.iter_mut() {
            if node.status == NodeStatus::Dead {
                continue;
            }
            if node.last_seen_ms.saturating_add(window) < now_ms {
                node.dead_since_ms = Some(now_ms);
                node.status = NodeStatus::Dead;
                dead.push(node.node_id.clone());
            }
        }
        dead
    }

    /// 应用一条房间状态更新，按 `revision` 收敛。
    ///
    /// 只接受 `revision >=` 当前值的消息，并**始终覆盖**本节点视图 —— 因为 revision
    /// 是单调递增的消息序号，覆盖即收敛，不需要额外的分布式仲裁。
    pub fn apply_room(&mut self, room: RoomState) -> bool {
        match self.rooms.get(&room.id) {
            Some(cur) if room.revision < cur.revision => false,
            Some(cur) => {
                let changed = *cur != room;
                self.rooms.insert(room.id.clone(), room);
                changed
            }
            None => {
                self.rooms.insert(room.id.clone(), room);
                true
            }
        }
    }

    /// 更新本节点归属的房间（旁听计数等本地变化）。
    pub fn update_local_room(&mut self, room: RoomState) -> bool {
        let changed = match self.rooms.get(&room.id) {
            Some(cur) => cur.revision != room.revision || cur.listeners != room.listeners,
            None => true,
        };
        self.rooms.insert(room.id.clone(), room);
        changed
    }

    /// 移除一个房间（本地归零时调用）。
    pub fn remove_room(&mut self, room_id: &str) -> bool {
        self.rooms.remove(room_id).is_some()
    }

    pub fn room(&self, room_id: &str) -> Option<&RoomState> {
        self.rooms.get(room_id)
    }

    pub fn rooms(&self) -> impl Iterator<Item = &RoomState> {
        self.rooms.values()
    }

    /// 本节点归属的房间。
    pub fn owned_rooms(&self) -> impl Iterator<Item = &RoomState> {
        self.rooms
            .values()
            .filter(|r| r.owner.as_deref() == Some(&self.node_id))
    }

    pub fn node(&self, node_id: &str) -> Option<&Node> {
        self.nodes.get(node_id)
    }

    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.nodes.values()
    }

    /// 本节点会议数。
    pub fn owned_count(&self) -> u64 {
        self.owned_rooms().count() as u64
    }

    /// 全集群旁听总人数。
    pub fn total_listeners(&self) -> u64 {
        self.rooms.values().map(|r| r.listeners).sum()
    }

    /// 指定节点已承载的旁听人数。
    pub fn listeners_on(&self, node_id: &str) -> u64 {
        self.rooms
            .values()
            .filter(|r| r.owner.as_deref() == Some(node_id))
            .map(|r| r.listeners)
            .sum()
    }

    /// 指定节点会议数。
    pub fn rooms_on(&self, node_id: &str) -> u64 {
        self.rooms
            .values()
            .filter(|r| r.owner.as_deref() == Some(node_id))
            .count() as u64
    }

    /// 节点是否还有承接新会议的余量（会议数上限 + 旁听容量）。
    pub fn has_capacity_for_room(
        &self,
        node_id: &str,
        cfg: &ClusterConfig,
        streams: u64,
        listeners: u64,
    ) -> bool {
        let Some(node) = self.nodes.get(node_id) else {
            return false;
        };
        if node.max_rooms == 0 || self.rooms_on(node_id) >= node.max_rooms {
            return false;
        }
        // 旁听容量：按上行流折算的槽位数要能覆盖该房间的旁听者。
        let weight = cfg.listener_weight();
        let per_stream = weight / streams.max(1).max(1);
        listeners <= per_stream && self.listeners_on(node_id) <= weight
    }
}

// ─────────────────────────── 路由 / 迁移 ───────────────────────────

/// 调度与迁移的纯函数路由器。
#[derive(Debug, Default)]
pub struct Router {
    node_id: String,
}

impl Router {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// 节点负载分数（越低越空闲）。
    ///
    /// 归一到 0..=1000 的两个维度相加：
    /// * 会议数占比（`rooms / max_rooms`）；
    /// * 旁听人数占比（`listeners / listener_weight`）。
    ///
    /// 这样「多小房间」和「少大旁听房间」可以放在同一把尺子上比较。
    pub fn node_load(&self, reg: &Registry, cfg: &ClusterConfig, node_id: &str) -> u64 {
        let Some(node) = reg.node(node_id) else {
            return u64::MAX;
        };
        let rooms_ratio = if node.max_rooms > 0 {
            reg.rooms_on(node_id).saturating_mul(1000) / node.max_rooms
        } else {
            1000
        };
        let weight = cfg.listener_weight();
        let listeners_ratio = if weight > 0 {
            reg.listeners_on(node_id).saturating_mul(1000) / weight
        } else {
            1000
        };
        rooms_ratio.saturating_add(listeners_ratio).min(2000)
    }

    /// 新会议分配：返回负载最低的候选节点 id。
    ///
    /// 候选条件：健康、全功能角色、还有会议数余量、旁听容量能覆盖请求。
    /// 负载相同则按 `node_id` 字典序打破平局 —— 保证三个节点在同样配置下
    /// 对「哪个节点最闲」有完全一致的结论（分布式收敛的前提）。
    pub fn assign(
        &self,
        reg: &Registry,
        cfg: &ClusterConfig,
        room_id: &str,
        streams: u64,
        listeners: u64,
    ) -> Option<String> {
        let mut best: Option<(u64, String)> = None;
        for node in reg.nodes() {
            if !node.is_schedulable() {
                continue;
            }
            if !reg.has_capacity_for_room(&node.node_id, cfg, streams, listeners) {
                continue;
            }
            let load = self.node_load(reg, cfg, &node.node_id);
            let better = match &best {
                None => true,
                Some((l, id)) => load < *l || (load == *l && node.node_id < *id),
            };
            if better {
                best = Some((load, node.node_id.clone()));
            }
        }
        // `room_id` 目前只用于日志上下文，签名上保留以便后续做粘性分配（同房间复用节点）。
        let _ = room_id;
        best.map(|(_, id)| id)
    }

    /// 故障迁移目标：负载最低、健康、有余量、且**不是**本节点（避免自迁）。
    pub fn migration_destination(
        &self,
        reg: &Registry,
        cfg: &ClusterConfig,
        exclude: Option<&str>,
        streams: u64,
        listeners: u64,
    ) -> Option<String> {
        let mut best: Option<(u64, String)> = None;
        for node in reg.nodes() {
            if node.node_id == self.node_id || exclude.is_some_and(|e| node.node_id == e) {
                continue;
            }
            if !node.is_schedulable() {
                continue;
            }
            if !reg.has_capacity_for_room(&node.node_id, cfg, streams, listeners) {
                continue;
            }
            let load = self.node_load(reg, cfg, &node.node_id);
            let better = match &best {
                None => true,
                Some((l, id)) => load < *l || (load == *l && node.node_id < *id),
            };
            if better {
                best = Some((load, node.node_id.clone()));
            }
        }
        best.map(|(_, id)| id)
    }

    /// 本节点是否有余量承接一次迁移。
    pub fn can_accept(
        &self,
        reg: &Registry,
        cfg: &ClusterConfig,
        streams: u64,
        listeners: u64,
    ) -> bool {
        reg.has_capacity_for_room(&self.node_id, cfg, streams, listeners)
            && reg.node(&self.node_id).is_some_and(|n| n.is_schedulable())
    }
}

// ─────────────────────────── 时间工具 ───────────────────────────

/// 当前 Unix 毫秒时间戳。
pub fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
