//! 集群核心纯函数层：状态模型、最低负载调度、健康判定与故障迁移目标选择。
//!
//! 这一层**不触碰任何 IO / NATS**，因此可以在没有 NATS 服务器的情况下
//! 用单元测试逐条断言验收逻辑：
//! * 房间状态收敛（[`Registry::apply_room`]）—— 按 `revision` 收敛到全局一致；
//! * 最低负载调度（[`Router::assign`]）—— 新会议自动落到负载最低的媒体节点；
//! * 健康判定（[`Registry::reap_dead`]）—— 连续错过心跳即判死并自动下线；
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

    /// 刷新归属（递增 `revision`），**保留计数**。
    ///
    /// 迁的是媒体归属，旁听计数由新归属节点接管，不在这里清零。
    pub fn with_owner(mut self, owner: Option<String>, media_addr: &str, role: NodeRole) -> Self {
        self.owner = owner;
        self.media_addr = media_addr.to_string();
        self.node_role = role;
        self.revision = self.revision.saturating_add(1);
        self
    }

    /// 用新的参与者计数刷新状态（递增 `revision`）。
    ///
    /// 只改计数、**不动归属** —— 旁听者加减走的正是这个路径，它必须保持
    /// `owner` / `media_addr` / `node_role` 不变，否则整个集群会把房间当成
    /// 未归属的 pending 房间，`Router::assign` 直接跳过它（见
    /// `add_listener_keeps_ownership` 测试）。
    /// 要连归属一起改时用 [`with_owner_and_counts`](Self::with_owner_and_counts)。
    pub fn with_counts(mut self, streams: u64, listeners: u64) -> Self {
        self.streams = streams;
        self.listeners = listeners;
        self.revision = self.revision.saturating_add(1);
        self
    }

    /// 一次性更新归属与计数（递增 `revision`）。
    ///
    /// 承接迁移时用：目标节点接房间时既要把 `owner` 改成自己，
    /// 也要把旁听数改成新的值。注意 `streams` / `listeners` 是**必须传入**的
    /// 完整值，不是增量 —— 旁听加减请改用 [`with_counts`](Self::with_counts)。
    pub fn with_owner_and_counts(
        mut self,
        owner: Option<String>,
        media_addr: &str,
        role: NodeRole,
        streams: u64,
        listeners: u64,
    ) -> Self {
        self.owner = owner;
        self.media_addr = media_addr.to_string();
        self.node_role = role;
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
    /// 单节点旁听参会者上限（随心跳上报，调度时用它算容量）。
    pub listener_capacity: u64,
    /// 旁听放大系数：一条上行流折算的旁听槽位（随心跳上报）。
    pub listener_fanout: u64,
    /// 首次判死时刻，用于上报「故障下线时刻」。
    pub dead_since_ms: Option<u64>,
}

impl Node {
    /// 新建一个健康节点视图（`Live`，未见过心跳）。
    ///
    /// 主要给测试用：真实运行里的节点视图由
    /// [`Registry::insert_node`](Registry::insert_node) /
    /// [`Registry::mark_heartbeat`](Registry::mark_heartbeat) 维护。
    pub fn new(
        node_id: impl Into<String>,
        advertised: impl Into<String>,
        cfg: &ClusterConfig,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            advertised: advertised.into(),
            role: NodeRoleTag::from(cfg.node_role),
            last_seen_ms: unix_ms(),
            last_seq: 0,
            status: NodeStatus::Live,
            max_rooms: cfg.max_rooms_per_node as u64,
            listener_capacity: cfg.listener_capacity as u64,
            listener_fanout: cfg.listener_fanout as u64,
            dead_since_ms: None,
        }
    }

    /// 旁听容量权重：`fanout × listener_capacity`。
    ///
    /// 用**本节点自己声明的**值（随心跳上报），而不是调用方的本地配置 ——
    /// 这是异构集群（媒体节点 + 旁听节点）能算出不同权重的关键。
    pub fn weight(&self) -> u64 {
        (self.listener_fanout.max(1) * self.listener_capacity.max(1)).max(1)
    }

    /// 是否还能承接一个「`streams` 条上行流 + `listeners` 名旁听者」的房间。
    ///
    /// 放在 `Node` 而不是 `Registry`：容量上限是节点自己声明的（随心跳上报），
    /// 只有已承载量需要读全集群房间视图，所以房间表以参数传入。
    pub fn has_capacity_for_room(
        &self,
        rooms: &HashMap<String, RoomState>,
        streams: u64,
        listeners: u64,
    ) -> bool {
        let (mut current_rooms, mut current_listeners) = (0u64, 0u64);
        for room in rooms.values() {
            if room.owner.as_deref() == Some(self.node_id.as_str()) {
                current_rooms += 1;
                current_listeners = current_listeners.saturating_add(room.listeners);
            }
        }
        if self.max_rooms == 0 || current_rooms >= self.max_rooms {
            return false;
        }
        // 旁听容量：按上行流折算的槽位数要能覆盖该房间的旁听者。
        let weight = self.weight();
        let per_stream = weight / streams.max(1);
        listeners <= per_stream && current_listeners <= weight
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
/// 所以节点动态增减不需要任何持久化：新节点启动后靠订阅，然后在**一个心跳周期内**
/// 收到各节点周期广播的房间快照（见 `Cluster::snapshot_publish_loop`）即可跟上。
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

    /// 用一组已存在的节点视图初始化（主要是测试用）。
    pub fn with_nodes(node_id: impl Into<String>, nodes: impl IntoIterator<Item = Node>) -> Self {
        let mut reg = Self::new(node_id);
        for node in nodes {
            reg.insert_node(node);
        }
        reg
    }

    /// 合并另一份注册表里的**全部节点**（保留本注册表自己的 `node_id` 与房间）。
    ///
    /// 主要给测试用；运行时节点视图靠心跳逐个收敛，不做批量拷贝。
    pub fn insert_all(&mut self, other: &Registry) {
        for node in other.nodes() {
            self.insert_node(node.clone());
        }
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
                listener_capacity: msg.listener_capacity,
                listener_fanout: msg.fanout,
                dead_since_ms: None,
            });
        let changed = node.status != NodeStatus::Live || node.last_seq < msg.seq;
        node.last_seen_ms = msg.ts.max(now_ms.max(1));
        node.last_seq = msg.seq.max(node.last_seq);
        node.advertised = msg.advertised.clone();
        node.role = msg.role;
        node.max_rooms = msg.max_rooms;
        node.listener_capacity = msg.listener_capacity;
        node.listener_fanout = msg.fanout;
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
        _cfg: &ClusterConfig,
        streams: u64,
        listeners: u64,
    ) -> bool {
        self.nodes
            .get(node_id)
            .map(|node| node.has_capacity_for_room(&self.rooms, streams, listeners))
            .unwrap_or(false)
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
    /// * 旁听人数占比（`listeners / 该节点自声明的 weight`）。
    ///
    /// 这样「多小房间」和「少大旁听房间」可以放在同一把尺子上比较。
    pub fn node_load(&self, reg: &Registry, node_id: &str) -> u64 {
        let Some(node) = reg.node(node_id) else {
            return u64::MAX;
        };
        let rooms_ratio = if node.max_rooms > 0 {
            reg.rooms_on(node_id).saturating_mul(1000) / node.max_rooms
        } else {
            1000
        };
        // 权重用**该节点自己声明的** fanout × capacity（随心跳上报），
        // 而不是本地配置：否则所有节点都会用本机配置去给远端节点计分，
        // 旁听节点一上线就会和媒体节点打成平手（见 `Router::assign` 测试）。
        let weight = node.weight();
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
        room_id: &str,
        streams: u64,
        listeners: u64,
    ) -> Option<String> {
        let mut best: Option<(u64, String)> = None;
        for node in reg.nodes() {
            if !node.is_schedulable() {
                continue;
            }
            if !node.has_capacity_for_room(&reg.rooms, streams, listeners) {
                continue;
            }
            let load = self.node_load(reg, &node.node_id);
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
            if !node.has_capacity_for_room(&reg.rooms, streams, listeners) {
                continue;
            }
            let load = self.node_load(reg, &node.node_id);
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
    pub fn can_accept(&self, reg: &Registry, streams: u64, listeners: u64) -> bool {
        reg.node(&self.node_id)
            .map(|node| node.has_capacity_for_room(&reg.rooms, streams, listeners))
            .unwrap_or(false)
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

// ─────────────────────────── 单元测试 ───────────────────────────
//
// 这里全部不联网：`Registry` / `Router` 是纯函数，异构集群的调度结果、
// 房间归属的保持、迁移目标的选择都可以直接用断言钉死。

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// 测试用配置：只覆盖与容量相关的三个字段，其余走默认。
    fn cfg(listener_capacity: u64, listener_fanout: u64) -> ClusterConfig {
        ClusterConfig {
            listener_capacity: listener_capacity as usize,
            listener_fanout: listener_fanout as usize,
            ..ClusterConfig::default()
        }
    }

    /// 测试用节点视图：`id` 用完整前缀，便于阅读断言输出。
    fn node(id: &str, listener_capacity: u64, listener_fanout: u64) -> Node {
        Node::new(
            id,
            format!("10.0.0.1:{}", 8080 + id.len()),
            &cfg(listener_capacity, listener_fanout),
        )
    }

    /// 构造一个已归属的房间。
    fn owned_room(id: &str, owner: &str, streams: u64, listeners: u64) -> RoomState {
        RoomState::new(id, streams, listeners).with_owner(
            Some(owner.to_string()),
            "10.0.0.1:8080",
            NodeRole::Full,
        )
    }

    // ── 发现 1：权重必须用节点自己声明的值 ──

    #[test]
    fn node_load_uses_the_node_own_capacity_not_local_config() {
        // a-small：小旁听节点（weight = 5 × 200 = 1000）
        // z-big  ：大媒体节点（weight = 200 × 20000 = 4,000,000）
        // 两个节点的 max_rooms 相同、会议数都是 1，所以 rooms_ratio 相同；
        // 差别只在 listeners_ratio —— 它必须按**各自声明的** weight 折算。
        let reg = Registry::with_nodes("me", [node("a-small", 200, 5), node("z-big", 20_000, 200)]);
        let reg = insert_rooms(
            reg,
            [
                owned_room("r-a", "a-small", 1, 800),
                owned_room("r-z", "z-big", 1, 800),
            ],
        );

        let router = Router::new("me");
        // 同一次调度必须得到不同分数：1000 的节点 800 人 = 800‰，
        // 4,000,000 的节点 800 人 = 0‰。
        let load_small = router.node_load(&reg, "a-small");
        let load_big = router.node_load(&reg, "z-big");
        assert!(
            load_small > load_big,
            "小容量节点承载同样人数时负载应更高: small={load_small} big={load_big}"
        );
        // 大节点只剩会议数维度的开销（1 / 64 × 1000 = 15‰），旁听维度是 0‰。
        assert!(
            load_big <= 20,
            "大节点 800 人 / 4,000,000 槽位应≈0‰，实际 {load_big}"
        );
        assert!(
            load_small >= 800,
            "800 人 / 1000 槽位至少是 800‰，实际 {load_small}"
        );
    }

    #[test]
    fn assign_picks_the_big_node_over_a_saturated_small_node() {
        // 关键构造：被撑满的小节点 `a-small` 在字典序上**排在前面**。
        // 如果权重错用了本机配置（所有节点同一个 weight），两个节点会打成平手，
        // 平局靠 node_id 字典序打破 → 选中已经被撑满的 a-small，bug 无法被发现。
        // 这一条断言就是那个复现用例。
        let reg = Registry::with_nodes("me", [node("a-small", 200, 5), node("z-big", 20_000, 200)]);
        let reg = insert_rooms(reg, [owned_room("r-a", "a-small", 1, 800)]);

        let assign = Router::new("me").assign(&reg, "r-new", 1, 0);
        assert_eq!(
            assign.as_deref(),
            Some("z-big"),
            "应当选容量足够的 z-big，而不是已被 800 人撑到 800‰ 的 a-small"
        );
    }

    #[test]
    fn assign_tie_break_is_deterministic() {
        // 三个同配置节点、完全空载：分数全为 0，必须按 node_id 字典序确定唯一答案
        // —— 多节点各自计算时结论必须一致（分布式收敛的前提）。
        let reg = Registry::with_nodes(
            "me",
            [
                node("c-3", 20_000, 200),
                node("c-1", 20_000, 200),
                node("c-2", 20_000, 200),
            ],
        );
        let router = Router::new("me");
        assert_eq!(router.assign(&reg, "r", 1, 0).as_deref(), Some("c-1"));
        // 每个节点看到的全集群负载一致，无论调用方是谁结论都不变。
        let other = Router::new("c-2");
        assert_eq!(
            other.assign(&reg, "r", 1, 0),
            router.assign(&reg, "r", 1, 0)
        );
    }

    #[test]
    fn assign_skips_listener_role_and_dead_nodes() {
        let reg = Registry::with_nodes(
            "me",
            [
                node("media-a", 20_000, 200),
                node("listener-a", 500_000, 500),
            ],
        );
        // 旁听节点容量再大也不承接媒体归属；媒体节点判死。
        let mut listener_role = reg.node("listener-a").unwrap().clone();
        listener_role.role = NodeRoleTag::Listener;
        let mut dead = reg.node("media-a").unwrap().clone();
        dead.status = NodeStatus::Dead;

        let reg = Registry::with_nodes("me", [dead, listener_role]);
        assert!(
            Router::new("me").assign(&reg, "r", 1, 0).is_none(),
            "旁听节点与故障节点都不能成为调度目标"
        );
    }

    #[test]
    fn has_capacity_rejects_when_room_limit_or_listener_slots_exhausted() {
        let node = node("n", 10, 2); // weight = 20 槽位，max_rooms = 默认 64
        let empty = HashMap::new();
        assert!(node.has_capacity_for_room(&empty, 1, 20));
        assert!(!node.has_capacity_for_room(&empty, 1, 21), "超出折算槽位");

        // 会议数上限：max_rooms = 1 时再挂第二个就拒绝。
        let limited = Node {
            max_rooms: 1,
            ..node.clone()
        };
        let with_one = map_one(owned_room("r1", "n", 1, 0));
        assert!(!limited.has_capacity_for_room(&with_one, 1, 0), "已满会");
    }

    #[test]
    fn heartbeat_declared_capacity_drives_scheduling() {
        // 容量来自心跳上报，不是调用方的本地配置。
        let mut reg = Registry::new("me");
        let msg = HeartbeatMsg::from_node(&cfg(50, 5), 1); // weight = 250
        reg.mark_heartbeat(&msg, unix_ms());

        let node = reg.node("node-1").expect("心跳应注册节点");
        assert_eq!(node.listener_capacity, 50);
        assert_eq!(node.listener_fanout, 5);
        assert_eq!(node.weight(), 250);

        // 250 槽位 ÷ 1 条上行流 = 250；251 人就装不下。
        let one_room = map_one(owned_room("r", "node-1", 1, 0));
        assert!(node.has_capacity_for_room(&one_room, 1, 250));
        assert!(!node.has_capacity_for_room(&one_room, 1, 251));
    }

    // ── 发现 2：改计数不能冲掉归属 ──

    #[test]
    fn with_counts_preserves_ownership() {
        let before = owned_room("r1", "n2", 1, 3);
        let after = before.clone().with_counts(1, 4);
        assert_eq!(
            after.owner,
            Some("n2".to_string()),
            "旁听加减不得清空 owner"
        );
        assert_eq!(after.media_addr, before.media_addr);
        assert_eq!(after.node_role, before.node_role);
        assert_eq!(after.listeners, 4);
        assert_eq!(after.revision, before.revision + 1);
    }

    #[test]
    fn with_owner_preserves_counts() {
        let before = owned_room("r1", "n1", 1, 7);
        let after =
            before
                .clone()
                .with_owner(Some("n2".to_string()), "192.168.0.11:8080", NodeRole::Full);
        assert_eq!((after.streams, after.listeners), (1, 7), "迁移不清零旁听数");
        assert_eq!(after.owner.as_deref(), Some("n2"));
    }

    #[test]
    fn with_owner_and_counts_replaces_both() {
        let before = owned_room("r1", "n1", 1, 7);
        let after = before.clone().with_owner_and_counts(
            Some("n2".to_string()),
            "192.168.0.11:8080",
            NodeRole::Full,
            1,
            0,
        );
        assert_eq!(after.owner.as_deref(), Some("n2"));
        assert_eq!(after.listeners, 0);
    }

    #[test]
    fn listener_update_keeps_room_schedulable_and_migratable() {
        // 复现：旁听加减若把 owner 冲成 None，整个集群会把房间当成未归属的
        // pending 房间 —— Router::assign 会跳过它，故障节点上也就迁不走。
        // 注册表自身的 node_id 是 n1：owned_count 只统计本节点归属的房间。
        let mut reg = Registry::with_nodes("n1", [node("n1", 20_000, 200)]);
        let room = owned_room("r1", "n1", 1, 3);
        reg.apply_room(room.clone());

        // 加旁听 → 减旁听（cluster.rs 的 add/remove_listener 走的就是 with_counts）。
        let grown = room.with_counts(1, 4);
        let shrunk = grown.clone().with_counts(1, 3);
        reg.apply_room(grown);
        reg.apply_room(shrunk);

        let cur = reg.room("r1").expect("房间仍在").clone();
        assert!(cur.is_owned(), "加旁听后房间不能变成未归属");
        assert_eq!(cur.listeners, 3);
        assert_eq!(reg.owned_count(), 1, "归属节点视角的房间数应不变");

        // n1 故障后，这个房间仍要能被迁走（加第二个候选节点）。
        let mut dead = reg.node("n1").unwrap().clone();
        dead.status = NodeStatus::Dead;
        reg.insert_node(dead);
        reg.insert_node(node("n2", 20_000, 200));
        let dest =
            Router::new("me").migration_destination(&reg, Some("n1"), cur.streams, cur.listeners);
        assert_eq!(dest.as_deref(), Some("n2"), "加过旁听的房间必须仍然可迁移");
    }

    // ── 迁移目标 ──

    #[test]
    fn migration_destination_honours_exclude_and_capacity() {
        let reg = Registry::with_nodes("me", [node("n1", 20_000, 200), node("n2", 20_000, 200)]);
        let reg = insert_rooms(reg, [owned_room("r1", "n2", 1, 0)]);

        let router = Router::new("me");
        // 排除源节点后，n2 负载更高（1 个会议），所以应该回落到 n1。
        assert_eq!(
            router
                .migration_destination(&reg, Some("n2"), 1, 0)
                .as_deref(),
            Some("n1")
        );
        // 排除全部候选 → 无目标。
        let only = Registry::with_nodes("me", [node("n1", 20_000, 200)]);
        assert!(router
            .migration_destination(&only, Some("n1"), 1, 0)
            .is_none());
    }

    #[test]
    fn can_accept_respects_scheduling_role() {
        let reg = Registry::with_nodes("me", [node("me", 10, 2)]);
        assert!(Router::new("me").can_accept(&reg, 1, 0));

        // 判死后不再承接迁移。
        let dead = {
            let mut n = reg.node("me").unwrap().clone();
            n.status = NodeStatus::Dead;
            n
        };
        let reg = Registry::with_nodes("me", [dead]);
        assert!(!Router::new("me").can_accept(&reg, 1, 0));
    }

    #[test]
    fn apply_room_converges_by_revision() {
        let mut reg = Registry::new("me");
        let v_lo = owned_room("r1", "n1", 1, 3); // revision = 2
        let v_hi = v_lo.clone().with_counts(1, 5); // revision = 3

        assert!(reg.apply_room(v_hi.clone()), "首次插入");
        assert!(!reg.apply_room(v_lo), "低 revision 的旧消息不得覆盖新状态");
        assert_eq!(reg.room("r1").unwrap().listeners, 5);
        assert!(
            !reg.apply_room(v_hi.clone()),
            "相同 revision 重播视为无变化"
        );

        let v_newer = v_hi.clone().with_counts(1, 7); // revision = 4
        assert!(reg.apply_room(v_newer), "更高 revision 覆盖即收敛");
        assert_eq!(reg.room("r1").unwrap().listeners, 7);
    }

    #[test]
    fn health_window_math() {
        let cfg = ClusterConfig {
            heartbeat_secs: 5,
            unhealthy_misses: 2,
            ..ClusterConfig::default()
        };
        assert_eq!(Registry::health_window(&cfg), 10);
    }

    fn insert_rooms(reg: Registry, rooms: impl IntoIterator<Item = RoomState>) -> Registry {
        let mut reg = reg;
        for room in rooms {
            reg.apply_room(room);
        }
        reg
    }

    fn map_one(room: RoomState) -> HashMap<String, RoomState> {
        HashMap::from([(room.id.clone(), room)])
    }
}
