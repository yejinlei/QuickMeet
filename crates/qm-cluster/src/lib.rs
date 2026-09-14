//! # qm-cluster — QuickMeet 分布式集群层
//!
//! SFU 集群化：多节点部署的房间状态同步、会议调度、故障迁移与旁听容量扩展。
//!
//! 对应 Issue **YEJ-95**（QM-006），父 Epic **YEJ-89**。
//!
//! ## 模块划分
//!
//! | 模块 | 职责 | 是否有 IO |
//! | --- | --- | --- |
//! | [`state`] | 房间 / 节点状态模型、最低负载调度、健康判定、迁移目标选择 | 否（纯函数，可离线单测） |
//! | [`bus`] | NATS 传输适配：subject 规划 + pub/sub + request/reply | 是 |
//! | [`cluster`] | 集群门面：心跳、订阅循环、故障迁移编排、对外 API | 是 |
//!
//! ## 关键设计
//!
//! * **节点动态增减无需重启**：成员关系由心跳维护（`qm.<cid>.hb.*`）。
//!   房间视图由订阅维护，每个节点每 `heartbeat_secs` 广播一次自己归属的
//!   房间快照（`SnapshotPayload`），所以**晚到的节点**在一个心跳周期内就能
//!   追上全量状态 —— 不依赖请求/回复那种要求对端先在线的交互。
//!   快照携带发送方的 `total`（它看到的全集群房间数），接收方据此判断
//!   这份快照是否覆盖了整个集群；`rooms` 只含发送方归属的房间。
//! * **故障自动下线**：连续错过 `unhealthy_misses` 次心跳即判死，
//!   触发其归属房间的迁移。`failover_target_secs` 是目标时限（默认 10s）。
//! * **最低负载调度**：按「会议数占比 + 旁听人数占比」两个维度归一后相加，
//!   负载相同按 `node_id` 字典序打破平局 —— 保证多节点对「谁最闲」结论一致。
//! * **旁听只做计数**：加 / 减旁听者只更新 `listeners` 并发布，不搬运媒体载荷；
//!   旁听容量按 `listener_fanout × listener_capacity` 折算成媒体节点的可承接上限。
//!
//! ## 依赖约束（继承 Epic YEJ-89 全局强制约束）
//!
//! * MSRV 1.75：`async-nats` 固定 `=0.37.0` —— 它是最后一个不依赖
//!   `tokio-websockets` 的版本；0.38 起强制依赖 `tokio-websockets` 0.10
//!   （rust-version = 1.79），0.46/0.50 又各自声明 1.79/1.88，都编不过。
//! * 无私有依赖：全部来自 crates.io。
//! * 内网合规：NATS server 地址与媒体回源地址都必须是内网 IP，
//!   配置加载期由 `ClusterConfig::validate` 校验，越界直接拒绝启动。
//! * 数据本地化：本 crate 不持久化任何媒体载荷，只同步房间状态。

pub mod bus;
pub mod cluster;
pub mod server;
pub mod state;

pub use bus::{MigrationResult, NatsBus, RoomRouteRequest, SnapshotPayload, Subjects};
pub use cluster::{Cluster, ClusterStatus, MigrationRecord};
pub use server::run_cluster;
pub use state::{
    unix_ms, HeartbeatMsg, MigrationRequest, Node, NodeRoleTag, NodeStatus, Registry, RoomState,
    Router,
};

/// 集群层版本号（与 workspace 一致，单独导出便于运维面板显示）。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// MSRV 约束：Epic YEJ-89 全局强制约束 1。
pub const MSRV: &str = "1.75";

#[cfg(test)]
mod tests {
    use super::{MSRV, VERSION};

    #[test]
    fn version_is_semver() {
        let parts: Vec<&str> = VERSION.split('.').collect();
        assert_eq!(parts.len(), 3, "workspace 版本应为语义化版本: {VERSION}");
    }

    #[test]
    fn msrv_matches_epic_constraint() {
        assert_eq!(MSRV, "1.75");
    }
}
