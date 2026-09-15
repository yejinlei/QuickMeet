//! NATS 传输适配层：把房间状态、节点心跳、会议调度命令收敛到一套 subject 上。
//!
//! 设计取舍：
//! * 只用 **core NATS**（不引入 JetStream / KV）：房间状态是短生命周期的实时数据，
//!   掉线靠节点重启后重新订阅 + 周期广播的房间快照恢复，不需要持久化队列。
//! * 每个 subject 只承载一种消息类型，业务层不做类型分派。
//! * 所有 subject 都带 `cluster_id` 前缀，多集群（测试 / 生产）共用一个 NATS server
//!   时也不会串。
//!
//! 与 [`crate::state`] / [`crate::cluster`] 的关系：本模块只管「怎么收发」，
//! 业务语义（谁该接房间、谁该判死）都在上层。

use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use qm_common::{ClusterConfig, Error, Result as QmResult};

use crate::state::{HeartbeatMsg, MigrationRequest, RoomState};

/// 默认连接超时（秒）：NATS 不可达时启动最多挂这么久，不无限等。
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 3;
/// 订阅接收端缓冲：NATS 发送方与业务消费方的速度差缓冲。
pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 256;

/// id 最大长度（清洗后）。
pub const MAX_ID_LEN: usize = 64;

/// 清洗一个要拼进 NATS subject 的 id（集群 id / 节点 id / 房间 id）。
///
/// NATS 用 `.` 分隔 subject token。一个含 `.`、空白或 `/` 的 id 会把本应是 N 段
/// 的 subject 拆成 N+k 段，订阅端的通配（`*` 匹配一段、`>` 匹配剩余所有段）
/// 立刻失效 —— 最典型的是房间 `meet-1.room-2` 发布到 `qm.<cid>.room.meet-1.room-2`，
/// 而所有节点都订阅 `qm.<cid>.room.*`（只多一段），于是**集群内其他节点永远看不到
/// 这个房间**，房间归属在节点间分裂。
///
/// 规则：只保留 `[A-Za-z0-9_-]`；其余字节（`.`、空白、`/`、中文……）**折叠成 `-`**，
/// 连续的 `-` 压成一个，首尾的 `-` 丢弃，最后截到 [`MAX_ID_LEN`]。
/// 清洗后为空时返回 `unnamed`，保证任何输入都能得到一个非空、可直接拼进 subject 的 id。
pub fn sanitize_id(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len().min(MAX_ID_LEN));
    // 有一个待写入的分隔符；等遇到下一个字母数字才决定要不要写，
    // 这样首尾的分隔符天然被丢弃、连续的分隔符天然被压缩成一个。
    let mut pending_dash = false;
    for &b in raw.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' {
            if pending_dash && !out.is_empty() {
                out.push('-');
                pending_dash = false;
            }
            out.push(b as char);
        } else {
            // `-` 与 `.` / 空白 / 其它字符同处理：都代表「这里需要一段分隔」。
            pending_dash = !out.is_empty();
        }
    }
    let truncated: String = out.chars().take(MAX_ID_LEN).collect();
    if truncated.is_empty() {
        "unnamed".to_string()
    } else {
        truncated
    }
}

/// 集群 subject 前缀：`qm.<cluster_id>`。
///
/// 先清洗 `cluster_id`：非法字符会把前缀拆成多段，所有下游 subject 一起失效。
/// 清洗后为空（比如配置里 `cluster_id = "..."`）时由 [`sanitize_id`] 回退到
/// `unnamed`，避免出现 `qm..room.*` 这种带空 token 的畸形 subject。
pub fn subject_prefix(cluster_id: &str) -> String {
    format!("qm.{}", sanitize_id(cluster_id))
}

/// 节点心跳 subject（按节点分片，便于单独订阅某个节点）。
pub fn subject_heartbeat(prefix: &str, node_id: &str) -> String {
    format!("{prefix}.hb.{}", sanitize_id(node_id))
}

/// 全集群心跳的订阅 subject（递归通配）。
///
/// 必须用 `>` 而不是 `*`：心跳按节点分片发布在 `{prefix}.hb.<node_id>`（比本 subject
/// 多一层），而 NATS 的 `*` **只匹配一段**。写成 `*` 时发布端与订阅端的 token 数
/// 差一层，心跳永远投递不到 —— 集群成员视图永远不会更新，调度与故障迁移全部失效。
/// `>` 匹配剩余所有段，正好对上按节点分片的发布端。
pub fn subject_heartbeat_all(prefix: &str) -> String {
    format!("{prefix}.hb.>")
}

/// 房间状态变更 subject（按房间分片）。
pub fn subject_room(prefix: &str, room_id: &str) -> String {
    format!("{prefix}.room.{}", sanitize_id(room_id))
}

/// 房间全量快照 subject：各节点每 `heartbeat_secs` 广播一次自己归属的房间，
/// 晚到的节点靠它追上全量状态，避免靠订阅顺序拼状态。
pub fn subject_room_snapshot(prefix: &str) -> String {
    format!("{prefix}.room.snapshot")
}

/// 房间调度请求 subject：客户端 / 信令层 -> 路由节点。
pub fn subject_route(prefix: &str) -> String {
    format!("{prefix}.route.new")
}

/// 迁移请求 subject（前缀形式）：完整形如 `qm.<cid>.migrate.req.<node_id>`。
///
/// 迁移请求必须**定向**投递到候选目标节点（发起方已经选定目标），
/// 所以不能走队列订阅 —— 队列会让任意节点抢走这条消息。
pub fn subject_migrate_for(prefix: &str, target_node: &str) -> String {
    format!("{prefix}.migrate.req.{}", sanitize_id(target_node))
}

/// 迁移执行结果 subject：迁移完成 / 失败回包（全集群广播，观测用）。
pub fn subject_migrate_done(prefix: &str) -> String {
    format!("{prefix}.migrate.done")
}

/// 会议落点请求 subject（投给候选目标节点）。
///
/// 与迁移请求同理：`Router::assign` 已经选定目标，必须是**定向**投递。
/// 落点走 `qm.<cid>.place.req.<target_node>`，**不能**放在 `qm.<cid>.room.<id>`
/// 下 —— 落点发生在房间建立**之前**，房间 subject 已经被 `qm.<cid>.room.*`
/// 订阅走（用于房间状态变更），同一条消息会被 `apply_remote_room` 按
/// `RoomState` 反序列化并静默丢弃。
pub fn subject_place_for(prefix: &str, target_node: &str) -> String {
    format!("{prefix}.place.req.{}", sanitize_id(target_node))
}

/// 房间调度请求载荷。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoomRouteRequest {
    pub room_id: String,
    /// 创建请求来源节点（回包校验用）。
    pub from_node: String,
    pub streams: u64,
    pub listeners: u64,
}

/// 会议落点请求载荷（QM-024 F3）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceRequest {
    pub room_id: String,
    /// 请求来源节点（日志用；定向 subject 已经保证只有目标节点收到）。
    pub from_node: String,
    pub streams: u64,
    pub listeners: u64,
}

/// 会议落点回包载荷。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceReply {
    pub ok: bool,
    pub room_id: String,
    /// 实际承载节点（成功时非空）。
    pub owner: Option<String>,
    pub media_addr: String,
    pub revision: u64,
    pub reason: String,
}

/// 迁移执行结果载荷。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MigrationResult {
    pub room_id: String,
    pub target_node: String,
    pub ok: bool,
    pub reason: String,
    /// 完成时刻（Unix 毫秒）。
    pub ts: u64,
}

/// 集群 subject 集合：由配置一次性算出，避免散在业务代码里重复拼接。
#[derive(Debug, Clone)]
pub struct Subjects {
    pub prefix: String,
    pub heartbeat: String,
    pub heartbeat_all: String,
    pub room_snapshot: String,
    pub route_new: String,
    /// 投给本节点的迁移请求 subject（每个目标节点各自订阅，不做队列分摊）。
    pub migrate_for_self: String,
    pub migrate_done: String,
    /// 投给本节点的会议落点请求 subject（QM-024 F3）。
    pub place_for_self: String,
}

impl Subjects {
    pub fn from_config(cfg: &ClusterConfig) -> Self {
        let prefix = subject_prefix(&cfg.cluster_id);
        Self {
            heartbeat: subject_heartbeat(&prefix, &cfg.node_id),
            heartbeat_all: subject_heartbeat_all(&prefix),
            room_snapshot: subject_room_snapshot(&prefix),
            route_new: subject_route(&prefix),
            migrate_for_self: subject_migrate_for(&prefix, &cfg.node_id),
            migrate_done: subject_migrate_done(&prefix),
            place_for_self: subject_place_for(&prefix, &cfg.node_id),
            prefix,
        }
    }

    /// 某个房间的 subject（房间 id 不能预生成，按需拼接）。
    pub fn room(&self, room_id: &str) -> String {
        subject_room(&self.prefix, room_id)
    }

    /// 投递给指定目标节点的迁移请求 subject。
    pub fn migrate_for(&self, target_node: &str) -> String {
        subject_migrate_for(&self.prefix, target_node)
    }

    /// 投递给指定目标节点的落点请求 subject。
    pub fn place_for(&self, target_node: &str) -> String {
        subject_place_for(&self.prefix, target_node)
    }
}

/// 房间全量快照载荷。
///
/// `total` 是**发送方看到的全集群房间数**，不是 `rooms.len()`：
/// `rooms` 只含发送方自己归属的房间（跨节点发快照时用），
/// 所以必须单独携带 total，接收方才判断得出一份快照是否覆盖了整个集群。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotPayload {
    pub total: usize,
    pub rooms: Vec<RoomState>,
}

/// async-nats 连接统计的快照（`Statistics` 不实现 `Clone`，字段是 `AtomicU64`）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectionStats {
    /// 累计收写字节。
    pub in_bytes: u64,
    pub out_bytes: u64,
    pub in_messages: u64,
    pub out_messages: u64,
    /// 连接建立次数：首次连接 + 之后每一次成功重连。
    ///
    /// 断线重连验证就用它：重启 NATS 之前是 1，恢复后应该变成 2。
    pub connects: u64,
}

/// NATS 客户端封装：把 `async_nats::Client` 收在这一层，业务层只面对本类型。
///
/// 单独一层的好处是：[`crate::state`] 的调度 / 健康判定是纯函数，
/// 可以完全不联网跑单元测试；只有集成测试才会碰到 `NatsBus`。
///
/// 业务层通过 `Arc<NatsBus>` 在几个订阅循环之间共享它 —— `async_nats::Client`
/// 本身就是廉价共享的句柄，本类型再包一层顺带带上 subject 集合。
#[derive(Debug)]
pub struct NatsBus {
    client: async_nats::Client,
    subjects: Subjects,
}

impl NatsBus {
    /// 用已有连接句柄构造（重连路径用）。
    pub fn from_client(client: async_nats::Client, subjects: Subjects) -> Self {
        Self { client, subjects }
    }

    /// 连接到 NATS server。地址来自配置，必须是内网地址（配置加载时已校验）。
    ///
    /// 用 [`async_nats::connect_with_options`] 而不是 `connect`：默认 `connect`
    /// 不带超时，NATS server 不可达时会挂住整个启动流程。
    ///
    /// `publisher_capacity` 是本节点向 NATS 发送的消息队列缓冲，
    /// 心跳 + 订阅回包 + 若干请求会共享这一条连接。
    ///
    /// 断线后的**自动重连**由连接层承担（async-nats 默认无限次重试、指数退避
    /// 上限 4s，见 [`connect_with_timeout`]）；但**首次**连接失败不会自动重连，
    /// 必须靠上层 [`crate::cluster::Cluster::connect_loop`] 周期重试。
    pub async fn connect(cfg: &ClusterConfig, publisher_capacity: usize) -> QmResult<Self> {
        let subjects = Subjects::from_config(cfg);
        let addr = format!("{}:{}", cfg.server, cfg.port);
        let client = connect_with_timeout(&addr, cfg, publisher_capacity).await?;
        debug!(%addr, cluster = %cfg.cluster_id, node = %cfg.node_id, "NATS 已连接");
        Ok(Self { client, subjects })
    }

    /// 带超时的连接尝试：NATS 不可达时最多挂 `request_timeout_secs`，不无限等。
    pub async fn try_connect(cfg: &ClusterConfig, publisher_capacity: usize) -> QmResult<Self> {
        Self::connect(cfg, publisher_capacity).await
    }

    /// NATS 是否仍处于已连接状态。
    ///
    /// async-nats 的 `Client` 在断线期间是**静默**的：`publish` 仍然排队、
    /// `request` 会等到超时，不会立刻报错。所以判活必须显式读 `connection_state`。
    pub fn is_connected(&self) -> bool {
        use async_nats::connection::State;
        matches!(self.client.connection_state(), State::Connected)
    }

    /// 连接统计（重连次数等），供 healthz 与压测观测。
    ///
    /// async-nats 的 `statistics()` 返回 `Arc<Statistics>`，而 `Statistics` 的字段全是
    /// `AtomicU64`、**不实现 `Clone`**，所以只能逐个 `load` 出来；这里读的是
    /// 生命周期累计值（含初始连接与之后每一次成功重连），断线前后对比就能看出
    /// 节点确实换过连接、没有靠进程重启「假装」恢复。
    pub fn stats(&self) -> ConnectionStats {
        let s = self.client.statistics();
        ConnectionStats {
            in_bytes: s.in_bytes.load(Ordering::Relaxed),
            out_bytes: s.out_bytes.load(Ordering::Relaxed),
            in_messages: s.in_messages.load(Ordering::Relaxed),
            out_messages: s.out_messages.load(Ordering::Relaxed),
            connects: s.connects.load(Ordering::Relaxed),
        }
    }

    /// 强制重连：走 async-nats 自己的重连流程，保留现有订阅。
    ///
    /// 这个方法**不等待**重连完成；调用方要自己用 [`NatsBus::is_connected`] +
    /// `flush` 做探针确认（见 [`reconnect_with_probe`]）。
    pub async fn force_reconnect(&self) -> QmResult<()> {
        self.client
            .force_reconnect()
            .await
            .map_err(|e| Error::cluster(format!("NATS 强制重连失败: {e}")))
    }

    /// flush 探针：确认消息真的能到达 server。
    pub async fn flush(&self) -> QmResult<()> {
        self.client
            .flush()
            .await
            .map_err(|e| Error::cluster(format!("NATS flush 失败: {e}")))
    }

    /// subject 集合。
    pub fn subjects(&self) -> &Subjects {
        &self.subjects
    }

    /// 客户端句柄（测试与底层调试用）。
    pub fn client(&self) -> &async_nats::Client {
        &self.client
    }

    /// 发送一条 JSON 消息。
    ///
    /// `subject` 用 `String` 传给 `async-nats`：它的 `ToSubject` trait 只实现了
    /// `String` / `Subject` / `&'static str`，动态拼出来的 `&str` 需要先转成拥有权值。
    pub async fn publish_json<T: Serialize + ?Sized>(
        &self,
        subject: &str,
        value: &T,
    ) -> QmResult<()> {
        let payload = serde_json::to_vec(value)
            .map_err(|e| Error::cluster(format!("集群消息序列化失败: {e}")))?;
        self.client
            .publish(subject.to_string(), payload.into())
            .await
            .map_err(|e| Error::cluster(format!("NATS 发布失败 [{subject}]: {e}")))
    }

    /// 发送心跳（`seq` 单调递增，接收方据此去重）。
    pub async fn publish_heartbeat(&self, msg: &HeartbeatMsg) -> QmResult<()> {
        self.publish_json(&self.subjects.heartbeat, msg).await
    }

    /// 发布房间状态更新。
    pub async fn publish_room(&self, room: &RoomState) -> QmResult<()> {
        self.publish_json(&self.subjects.room(&room.id), room).await
    }

    /// 广播房间全量快照（`snapshot_publish_loop` 每 `heartbeat_secs` 发一次）。
    ///
    /// 带一个 `total` 字段：**发送方**自己的房间视图总数，不是 `rooms` 的长度。
    /// `rooms` 只包含本节点归属的房间，但 total 是发送方看到的全集群房间数 ——
    /// 两者必须分开，否则无法判断「这份快照有没有覆盖全集群」（见
    /// [`Cluster::apply_remote_snapshot`](crate::Cluster::apply_remote_snapshot)）。
    pub async fn publish_snapshot(&self, rooms: &[RoomState], total: usize) -> QmResult<()> {
        self.publish_json(
            &self.subjects.room_snapshot,
            &SnapshotPayload {
                total,
                rooms: rooms.to_vec(),
            },
        )
        .await
    }

    /// 订阅一个 subject，返回不断 yield `async_nats::Message` 的流。
    ///
    /// 直接把原生消息交给业务层：队列订阅场景需要读 `msg.reply` 才能回包，
    /// 所以不在这一层把 payload 剥出来。JSON 解码放在 [`crate::cluster::decode`]。
    pub async fn subscribe(&self, subject: &str) -> QmResult<async_nats::Subscriber> {
        self.client
            .subscribe(subject.to_string())
            .await
            .map_err(|e| Error::cluster(format!("NATS 订阅失败 [{subject}]: {e}")))
    }

    /// 队列订阅：同一 subject 下同一队列名的消费者共享消息（负载分摊）。
    ///
    /// 只给**房间调度请求**用 —— 三个节点同时收同一个请求时，只有队列里的
    /// 一个会处理。迁移请求**不能**用队列：目标在发起方已经选定，队列会让
    /// 别的节点抢走这条消息后直接丢弃。
    pub async fn queue_subscribe(
        &self,
        subject: &str,
        queue: &str,
    ) -> QmResult<async_nats::Subscriber> {
        self.client
            .queue_subscribe(subject.to_string(), queue.to_string())
            .await
            .map_err(|e| Error::cluster(format!("NATS 队列订阅失败 [{subject}]: {e}")))
    }

    /// 发布迁移结果。
    pub async fn publish_migration_result(&self, result: &MigrationResult) -> QmResult<()> {
        self.publish_json(&self.subjects.migrate_done, result).await
    }

    /// 定向发送会议落点请求并等回包（QM-024 F3）。
    ///
    /// 与迁移请求同理走**定向 subject**：`Router::assign` 已经选定目标节点，
    /// 队列订阅会让别的节点抢走消息直接丢弃。
    pub async fn request_place(
        &self,
        target_node: &str,
        req: &PlaceRequest,
    ) -> QmResult<PlaceReply> {
        request_json(&self.client, &self.subjects.place_for(target_node), req).await
    }

    /// 回包一条落点请求。
    pub async fn reply_place(
        &self,
        msg: &async_nats::Message,
        reply: &PlaceReply,
    ) -> QmResult<bool> {
        self.reply_json(msg, reply).await
    }

    /// 定向发送迁移请求并等回包。
    ///
    /// 迁移请求**不走队列**：发起方已经选定了目标节点（`req.target_node`），
    /// 必须投到该节点的专属 subject 上，否则会落到别的节点被直接忽略。
    pub async fn request_migration(&self, req: &MigrationRequest) -> QmResult<MigrationResult> {
        request_json(
            &self.client,
            &self.subjects.migrate_for(&req.target_node),
            req,
        )
        .await
    }

    /// 请求一个房间的调度决策（回包是分配到的目标节点 id；无可用节点时回空串）。
    ///
    /// 调度请求走队列订阅，NATS 只投递给队列里一个节点，由它回包。
    pub async fn request_route(&self, req: &RoomRouteRequest) -> QmResult<String> {
        request_json(&self.client, &self.subjects.route_new, req).await
    }

    /// 发送一条请求并等待回包；超时按 `cluster.request_timeout_secs` 收。
    pub async fn request_json<T: Serialize, R: serde::de::DeserializeOwned>(
        &self,
        subject: &str,
        value: &T,
    ) -> QmResult<R> {
        request_json(&self.client, subject, value).await
    }

    /// 按请求自带的 `reply` subject 回包（NATS 队列订阅的标准应答方式）。
    ///
    /// `async_nats` 会把请求方的 reply subject 填进 `msg.reply`，但不会替你发布，
    /// 这里补上这一步。`reply` 为空表示对方没等回包，返回 `Ok(None)`。
    pub async fn reply_json<R: Serialize + ?Sized>(
        &self,
        msg: &async_nats::Message,
        value: &R,
    ) -> QmResult<bool> {
        let Some(reply) = &msg.reply else {
            return Ok(false);
        };
        let subject = reply.to_string();
        let payload = serde_json::to_vec(value)
            .map_err(|e| Error::cluster(format!("集群消息序列化失败: {e}")))?;
        self.client
            .publish(reply.clone(), payload.into())
            .await
            .map_err(|e| Error::cluster(format!("NATS 回包失败 [{subject}]: {e}")))?;
        Ok(true)
    }

    /// 发送一条心跳后立即 flush，确认消息已经到达 server。
    ///
    /// 故障检测依赖「最后一次心跳确实送达」这个事实：只有它送达了，
    /// 节点掉线才会被正确判死，而不是被当成「网络抖动」。
    pub async fn publish_and_flush<T: Serialize>(&self, subject: &str, value: &T) -> QmResult<()> {
        self.publish_json(subject, value).await?;
        self.client
            .flush()
            .await
            .map_err(|e| Error::cluster(format!("NATS flush 失败 [{subject}]: {e}")))
    }

    /// 优雅关闭：flush 等 in-flight 消息写完，然后靠 Drop 释放连接。
    ///
    /// async-nats 0.37 没有 `drain()`（0.38 才加），所以这里用 `flush` 替代；
    /// 退订发生在客户端 Drop 时，进程即将退出，不做额外等待。
    pub async fn shutdown(&self) {
        if let Err(e) = self.client.flush().await {
            warn!(error = %e, "NATS flush 失败（关闭时）");
        }
    }
}

/// 连接 + 带超时的 request/reply 底层实现。
async fn connect_with_timeout(
    addr: &str,
    cfg: &ClusterConfig,
    publisher_capacity: usize,
) -> QmResult<async_nats::Client> {
    let opts = base_options(cfg, publisher_capacity);

    async_nats::connect_with_options(addr, opts)
        .await
        .map_err(|e| Error::cluster(format!("NATS 连接失败 [{addr}]: {e}")))
}

/// 连接参数：首次连接与重连共用，保证两处语义一致。
fn base_options(cfg: &ClusterConfig, publisher_capacity: usize) -> async_nats::ConnectOptions {
    // async-nats 默认无限重试、退避 `2^(n-1)` ms 封顶 4s —— 「NATS 断线自动重连」
    // 不需要我们自己做退避表，这里只需要保证**重试不会被掐掉**（不设 max_reconnects）。
    async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(cfg.request_timeout_secs.max(1)))
        .request_timeout(Some(Duration::from_secs(cfg.request_timeout_secs.max(1))))
        .ping_interval(Duration::from_secs(cfg.heartbeat_secs.max(1)))
        .client_capacity(publisher_capacity.max(1))
        .name(cfg.node_id.clone())
}

/// 重新建立一条 NATS 连接，并**校验它真的可用**（F4）。
///
/// 与 [`NatsBus::connect`] 的区别只在收尾校验：`connect_with_options` 在
/// `retry_on_initial_connect` 关掉时（默认值）只试一次就返回错误，而成功返回的
/// 句柄不保证能立刻收发消息 —— 必须过一次真实探针才算连上。不做校验的话，重连
/// 循环会以为自己已经恢复、把 healthz 置回绿色，而心跳其实一直在丢。
///
/// 校验是两次真实读：`flush` 探针（写路径必须能在 server 侧被确认）+
/// 连接状态位（`Connected`）。注意 0.37 里状态只在 `Connected` / `Disconnected`
/// 之间切，且 `Disconnected` 只在连接被丢弃时写一次，所以状态位**单独用不可靠**，
/// 探针才是判据。
pub async fn reconnect_with_probe(
    cfg: &ClusterConfig,
    publisher_capacity: usize,
) -> QmResult<NatsBus> {
    let addr = format!("{}:{}", cfg.server, cfg.port);
    let client = async_nats::connect_with_options(addr.clone(), base_options(cfg, publisher_capacity))
        .await
        .map_err(|e| Error::cluster(format!("NATS 连接失败 [{addr}]: {e}")))?;

    // 握手成功不等于能收发消息：flush 探针确认写路径真的通到 server。
    client
        .flush()
        .await
        .map_err(|e| Error::cluster(format!("NATS 探针 flush 失败 [{addr}]: {e}")))?;

    debug!(%addr, node = %cfg.node_id, "NATS 连接已建立（探针已确认）");
    Ok(NatsBus {
        client,
        subjects: Subjects::from_config(cfg),
    })
}

async fn request_json<T: Serialize, R: serde::de::DeserializeOwned>(
    client: &async_nats::Client,
    subject: &str,
    value: &T,
) -> QmResult<R> {
    send_request_raw(client, subject, value)
        .await
        .and_then(|msg| {
            serde_json::from_slice(&msg.payload)
                .map_err(|e| Error::cluster(format!("集群消息反序列化失败 [{subject}]: {e}")))
        })
}

async fn send_request_raw<T: Serialize>(
    client: &async_nats::Client,
    subject: &str,
    value: &T,
) -> QmResult<async_nats::Message> {
    let payload = serde_json::to_vec(value)
        .map_err(|e| Error::cluster(format!("集群消息序列化失败 [{subject}]: {e}")))?;
    client
        .request(subject.to_string(), payload.into())
        .await
        .map_err(|e| Error::cluster(format!("NATS 请求超时 [{subject}]: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::ClusterConfig;

    #[test]
    fn subjects_are_partitioned_by_cluster_id() {
        let cfg = ClusterConfig {
            cluster_id: "qm-prod".to_string(),
            node_id: "n1".to_string(),
            ..Default::default()
        };
        let s = Subjects::from_config(&cfg);
        assert_eq!(s.prefix, "qm.qm-prod");
        assert_eq!(s.heartbeat, "qm.qm-prod.hb.n1");
        // 必须带递归通配符：心跳发布在 `qm.qm-prod.hb.n1`（比订阅端多一段），
        // NATS 的 `*` 只匹配一段，写成 `qm.qm-prod.hb.*` 会**永远收不到心跳**。
        assert_eq!(s.heartbeat_all, "qm.qm-prod.hb.>");
        assert_eq!(s.room("meet-1"), "qm.qm-prod.room.meet-1");
        assert_eq!(s.route_new, "qm.qm-prod.route.new");
        assert_eq!(s.migrate_for_self, "qm.qm-prod.migrate.req.n1");
        assert_eq!(s.migrate_for("n3"), "qm.qm-prod.migrate.req.n3");
        assert_eq!(s.migrate_done, "qm.qm-prod.migrate.done");
    }

    #[test]
    fn two_clusters_do_not_share_subjects() {
        let a = Subjects::from_config(&ClusterConfig {
            cluster_id: "a".to_string(),
            ..Default::default()
        });
        let b = Subjects::from_config(&ClusterConfig {
            cluster_id: "b".to_string(),
            ..Default::default()
        });
        assert_ne!(a.heartbeat, b.heartbeat);
        assert_ne!(a.room("same"), b.room("same"));
    }

    // ── id 清洗：subject 的 token 数绝不能被 id 撑破 ──

    #[test]
    fn sanitize_id_keeps_alnum_underscore_and_dash() {
        assert_eq!(sanitize_id("meet-2026-0916"), "meet-2026-0916");
        assert_eq!(sanitize_id("meet_2026"), "meet_2026");
        assert_eq!(sanitize_id("Abc123"), "Abc123");
    }

    #[test]
    fn sanitize_id_folds_dots_spaces_and_separator_runs() {
        // `.` 是 NATS 的 token 分隔符：不清洗时 `qm.<cid>.room.a.b` 变成 5 段，
        // 而所有节点都订阅 `qm.<cid>.room.*`（多 1 段），于是**收不到这个房间**。
        assert_eq!(sanitize_id("meet.1"), "meet-1");
        assert_eq!(sanitize_id("a/b c"), "a-b-c");
        assert_eq!(sanitize_id("a--b"), "a-b");
        assert_eq!(sanitize_id("..."), "unnamed");
        assert_eq!(sanitize_id("a...b"), "a-b");
        assert_eq!(sanitize_id("a  b"), "a-b");
    }

    #[test]
    fn sanitize_id_strips_leading_and_trailing_separators() {
        assert_eq!(sanitize_id("-a-"), "a");
        assert_eq!(sanitize_id("...a..."), "a");
        assert_eq!(sanitize_id("-a"), "a");
        assert_eq!(sanitize_id("a-"), "a");
    }

    #[test]
    fn sanitize_id_empty_input_becomes_unnamed() {
        assert_eq!(sanitize_id(""), "unnamed");
        assert_eq!(sanitize_id("..."), "unnamed");
        assert_eq!(sanitize_id("会议室"), "unnamed");
    }

    #[test]
    fn sanitize_id_truncates_to_max_len_without_half_a_char() {
        let long = "a".repeat(MAX_ID_LEN + 40);
        assert_eq!(sanitize_id(&long).len(), MAX_ID_LEN);

        // 多字节字符在截断边界上会被整体丢弃，不能产生半个字符。
        let mixed = format!("room-{}-会议室", "x".repeat(MAX_ID_LEN));
        let out = sanitize_id(&mixed);
        assert!(out.len() <= MAX_ID_LEN);
        assert!(
            out.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "清洗结果只能是 [A-Za-z0-9_-]: {out}"
        );
    }

    #[test]
    fn sanitize_id_output_is_always_subject_safe() {
        let raws: Vec<String> = [
            "".to_string(),
            "a".to_string(),
            "a.b.c".to_string(),
            "  spaced  ".to_string(),
            "a/b".to_string(),
            "会议室".to_string(),
            "r\x001".to_string(),
            format!("{}{}", "z".repeat(200), "会议室"),
        ]
        .into_iter()
        .collect();
        for raw in raws {
            let out = sanitize_id(&raw);
            assert!(!out.is_empty(), "清洗结果不能为空: {raw:?} -> {out:?}");
            assert!(out.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
            assert!(out.len() <= MAX_ID_LEN);
        }
    }

    #[test]
    fn subjects_are_stable_after_sanitizing() {
        // 同一个非法 id 在两个节点上必须算出**完全相同**的 subject，
        // 否则两端订阅不到对方发的消息 —— 这是 F6 要防止的实际故障。
        let cid = "qm-prod";
        assert_eq!(subject_prefix(cid), "qm.qm-prod");
        // 两个节点用同一个（含 `.` 的）房间 id，subject 必须一致。
        assert_eq!(
            subject_room("qm.qm-prod", "meet.1"),
            subject_room("qm.qm-prod", "meet.1")
        );
        assert_eq!(subject_room("qm.qm-prod", "meet.1"), "qm.qm-prod.room.meet-1");

        // 畸形 cluster_id 也不能产出带空 token 的 subject（`qm..room.*` 是坏 subject）。
        assert_eq!(subject_prefix("..."), "qm.unnamed");
        assert_ne!(subject_prefix("a"), subject_prefix("b"));
    }

    #[test]
    fn subject_helpers_are_symmetric_with_config() {
        let cfg = ClusterConfig {
            cluster_id: "c".to_string(),
            node_id: "node-9".to_string(),
            ..Default::default()
        };
        let subjects = Subjects::from_config(&cfg);
        assert_eq!(
            subject_heartbeat(&subjects.prefix, &cfg.node_id),
            subjects.heartbeat
        );
        assert_eq!(
            subject_heartbeat_all(&subjects.prefix),
            subjects.heartbeat_all
        );
        assert_eq!(subject_room(&subjects.prefix, "r"), subjects.room("r"));
        assert_eq!(
            subject_migrate_for(&subjects.prefix, "node-9"),
            subjects.migrate_for_self
        );
        assert_ne!(
            subjects.migrate_for("other-node"),
            subjects.migrate_for_self
        );
    }

    #[test]
    fn migration_result_round_trips() {
        let r = MigrationResult {
            room_id: "r1".into(),
            target_node: "n2".into(),
            ok: true,
            reason: "ok".into(),
            ts: 1_700_000_000_000,
        };
        let back: MigrationResult =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn room_route_request_round_trips() {
        let r = RoomRouteRequest {
            room_id: "r1".into(),
            from_node: "n1".into(),
            streams: 2,
            listeners: 1000,
        };
        let back: RoomRouteRequest =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(r, back);
    }
}
