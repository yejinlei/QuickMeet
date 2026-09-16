//! # qm-common — QuickMeet 公共基础
//!
//! 统一错误模型、配置加载、日志初始化与本地持久化工具。
//! 本 crate 不依赖任何 WebRTC/网络 crate，供 qm-media / qm-signaling / demo 复用。

pub mod config;
pub mod error;
pub mod logging;
pub mod storage;

pub use config::{
    AppConfig, AuthConfig, ClusterConfig, ConfigSource, NodeRole, RoomConfig, TlsConfig,
};
pub use error::{Cidr, Error, ErrorKind, Result};

/// 运行时版本与约束快照，便于在日志与验收记录中固化构建参数。
pub const NAME: &str = "QuickMeet";
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// MSRV 约束：Epic YEJ-89 全局强制约束 1。
pub const MSRV: &str = "1.75";
/// 内网网段默认值（Epic 全局约束 3）。
pub const DEFAULT_CIDR: &str = "192.168.0.0/24";
/// 媒体服务默认端口（Epic 全局约束 3）。
pub const DEFAULT_MEDIA_PORT: u16 = 8080;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epic_constraints_are_code_documented() {
        assert_eq!(NAME, "QuickMeet");
        assert_eq!(MSRV, "1.75");
        assert_eq!(DEFAULT_CIDR, "192.168.0.0/24");
        assert_eq!(DEFAULT_MEDIA_PORT, 8080);
        // 默认配置与常量一致，防止常量与默认值漂移
        assert_eq!(AppConfig::default().media.port, DEFAULT_MEDIA_PORT);
        assert!(AppConfig::default()
            .network
            .cidrs
            .iter()
            .any(|c| c == DEFAULT_CIDR));
    }

    #[test]
    fn version_is_semver() {
        let v: Vec<&str> = VERSION.split('.').collect();
        assert_eq!(v.len(), 3, "workspace 版本应为语义化版本: {VERSION}");
    }
}
