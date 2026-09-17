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

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use qm_common::{ClusterConfig, Error, Result as QmResult};

use crate::state::{HeartbeatMsg, MigrationRequest, RoomState};

/// 默认连接超时（秒）：NATS 不可达时启动最多挂这么久，不无限等。
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 3;
/// 订阅接收端缓冲：NATS 发送方与业务消费方的速度差缓冲。
pub const DEFAULT_SUBSCRIBER_CAPACITY: usize = 256;

/// 集群 subject 前缀：`qm.<cluster_id>`。
pub fn subject_prefix(cluster_id: &str) -> String {
    format!("qm.{}", cluster_id)
}

/// 节点心跳 subject（按节点分片，便于单独订阅某个节点）。
pub fn subject_heartbeat(prefix: &str, node_id: &str) -> String {
    format!("{prefix}.hb.{}", node_id)
}

/// 全集群心跳的订阅 subject（通配）。
pub fn subject_heartbeat_all(prefix: &str) -> String {
    format!("{prefix}.hb.*")
}

/// 房间状态变更 subject（按房间分片）。
pub fn subject_room(prefix: &str, room_id: &str) -> String {
    format!("{prefix}.room.{}", room_id)
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
    format!("{prefix}.migrate.req.{}", target_node)
}

/// 迁移执行结果 subject：迁移完成 / 失败回包（全集群广播，观测用）。
pub fn subject_migrate_done(prefix: &str) -> String {
    format!("{prefix}.migrate.done")
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
    /// 连接到 NATS server。地址来自配置，必须是内网地址（配置加载时已校验）。
    ///
    /// 用 [`async_nats::connect_with_options`] 而不是 `connect`：默认 `connect`
    /// 不带超时，NATS server 不可达时会挂住整个启动流程。
    ///
    /// `publisher_capacity` 是本节点向 NATS 发送的消息队列缓冲，
    /// 心跳 + 订阅回包 + 若干请求会共享这一条连接。
    pub async fn connect(cfg: &ClusterConfig, publisher_capacity: usize) -> QmResult<Self> {
        let subjects = Subjects::from_config(cfg);
        let addr = format!("{}:{}", cfg.server, cfg.port);
        let client = connect_with_timeout(&addr, cfg, publisher_capacity).await?;
        debug!(%addr, cluster = %cfg.cluster_id, node = %cfg.node_id, "NATS 已连接");
        Ok(Self { client, subjects })
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
    let opts = async_nats::ConnectOptions::new()
        .connection_timeout(Duration::from_secs(cfg.request_timeout_secs.max(1)))
        .request_timeout(Some(Duration::from_secs(cfg.request_timeout_secs.max(1))))
        .ping_interval(Duration::from_secs(cfg.heartbeat_secs.max(1)))
        .client_capacity(publisher_capacity.max(1))
        .name(cfg.node_id.clone());

    async_nats::connect_with_options(addr, opts)
        .await
        .map_err(|e| Error::cluster(format!("NATS 连接失败 [{addr}]: {e}")))
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
        assert_eq!(s.heartbeat_all, "qm.qm-prod.hb.*");
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
