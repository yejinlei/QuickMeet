//! 集群模式进程入口（QM-006）。
//!
//! 职责：加载并校验配置 → 构造 [`Cluster`] → 连接 NATS 加入集群 → 阻塞到 Ctrl+C。
//!
//! 拆成独立模块而不是塞进 `main.rs` 有两个原因：
//! * 配置加载 + 校验可以脱离 CLI 参数单独单测（`main.rs` 里的 `#[cfg(test)]`
//!   拿到的是测试进程的环境，不适合断言文件加载）；
//! * `docker-compose.yml` 的容器命令走 `qm-demo --cluster`，
//!   入口路径只有一处，不需要在 compose 里再写一遍参数拼装。

use std::sync::Arc;

use qm_common::{config, logging, Error, Result as QmResult};
use tracing::info;

use crate::Cluster;

/// 启动集群模式。
///
/// 流程：
/// 1. 加载配置（`default.toml` → `local.json` → `QM_*` 环境变量），加载期即校验，
///    配置非法直接返回错误，不进入集群；
/// 2. 构造 `Cluster` 并 `initialize()`：连接 NATS、注册自身、启动心跳/调度/迁移循环；
/// 3. 阻塞到 Ctrl+C，然后正常退出（容器收到 `docker stop` 的 SIGTERM 时同样退出）。
///
/// NATS 不可达时**不返回错误**：节点退化为单机模式继续运行，
/// 下一跳心跳重试连接。这是「节点动态增减、无需重启整体服务」的前提。
///
/// 只有配置非法、runtime 创建失败这类硬错误会返回 `Err` —— 配置问题必须
/// fail fast，不能带着非法配置进集群。
///
/// 函数内部自己建 runtime 并阻塞：不能在已经是 runtime 上下文的地方
/// 嵌套 `Runtime::build()`（会 panic），所以这里保持同步签名，
/// 调用方（`main`）直接调用即可。
pub fn run_cluster(config_dir: &str) -> QmResult<()> {
    // 加载期即校验（含集群合规校验：NATS 地址必须内网或回环）。
    // 校验失败在这里就返回错误，进程不会带着非法配置启动。
    let cfg = config::load_from(config_dir).map(|(cfg, _)| cfg)?;
    logging::init_subscriber(&cfg);
    info!(
        node = %cfg.cluster.node_id,
        cluster = %cfg.cluster.cluster_id,
        nats = %cfg.cluster.server,
        nats_port = cfg.cluster.port,
        advertised = %cfg.cluster.advertised_addr,
        role = cfg.cluster.node_role.as_str(),
        heartbeat_secs = cfg.cluster.heartbeat_secs,
        unhealthy_misses = cfg.cluster.unhealthy_misses,
        failover_target_secs = cfg.cluster.failover_target_secs,
        "集群节点启动（QM-006）"
    );

    let cluster = Arc::new(Cluster::from_app_config(&cfg));

    // 连接 NATS 需要在 runtime 里跑（initialize 会 spawn 心跳/调度/迁移循环），
    // 所以先建 runtime 再 block_on 整个启动流程。
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| Error::cluster(format!("tokio runtime 创建失败: {e}")))?;
    runtime.block_on(async move {
        cluster.initialize().await;
        let _ = tokio::signal::ctrl_c().await;
        Ok::<(), Error>(())
    })?;
    tracing::info!("收到 Ctrl+C，节点退出集群");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::AppConfig;

    /// `from_app_config` 只搬运 `cluster` 段，不引入额外状态。
    #[test]
    fn from_app_config_copies_cluster_section() {
        let mut app = AppConfig::default();
        app.cluster.node_id = "node-7".to_string();
        app.cluster.cluster_id = "qm-prod".to_string();
        app.cluster.advertised_addr = "192.168.0.7:8080".to_string();

        let cluster = Cluster::from_app_config(&app);
        let status = cluster.status_snapshot();

        assert_eq!(status.node_id, "node-7");
        assert_eq!(status.cluster_id, "qm-prod");
        assert_eq!(status.advertised_addr, "192.168.0.7:8080");
        assert_eq!(status.role, "full");
        // 尚未 initialize：NATS 未连接。
        assert!(!status.nats_connected);
    }

    /// 加载真实配置目录并通过校验：证明本仓库的 `config/default.toml`
    /// 里的 `cluster` 段是合法的（否则所有节点都起不来）。
    ///
    /// 路径必须从 `CARGO_MANIFEST_DIR` 推导：`cargo test -p qm-cluster` 的
    /// 工作目录是 crate 目录而不是仓库根，写死 `./config` 会找不到文件。
    #[test]
    fn repo_default_config_passes_cluster_validation() {
        let repo_config = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("config")
            .to_string_lossy()
            .to_string();
        let (cfg, src) =
            config::load_from(&repo_config).expect("config/default.toml 必须能通过校验");
        assert!(src.default_file, "应当读到 config/default.toml");
        assert!(cfg.validate().is_ok());

        // 验收标准 2 的判死窗口：heartbeat × misses = 10s。
        assert_eq!(
            cfg.cluster
                .heartbeat_secs
                .saturating_mul(cfg.cluster.unhealthy_misses),
            10
        );
        // 验收标准 3 的旁听容量口径。
        assert!(cfg.cluster.listener_weight() >= 10_000);
        // 验收标准 4 的接入时限。
        assert_eq!(cfg.cluster.join_target_secs, 30);
    }

    /// 默认配置必须包含一个可用的 cluster 段，且节点可调度。
    #[test]
    fn default_cluster_config_is_schedulable() {
        let cfg = AppConfig::default();
        assert!(cfg.cluster.is_schedulable(), "默认节点角色必须可调度");
        assert_eq!(cfg.cluster.node_role.as_str(), "full");
    }
}
