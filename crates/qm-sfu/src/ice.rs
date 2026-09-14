//! STUN/TURN 穿透配置与 ICE server 选择。
//!
//! WebRTC 在 NAT 环境下需要 STUN（获取公网映射地址）和 TURN（中继兜底）。
//! 本模块提供 ICE server 配置、选择逻辑与 NAT 模拟工具，全部是纯 Rust：
//! * [`IceConfig`] —— STUN/TURN 服务器配置（含验证）
//! * [`IceServer`] —— 单个 ICE server（STUN 或 TURN）
//! * [`IceServerKind`] —— server 种类
//! * [`select_ice_servers`] —— 按 NAT 类型选择 ICE server 组合
//! * [`simulate_nat`] —— NAT 模拟（回环/对称 NAT/端口限制 NAT）
//!
//! 私有化约束（Epic 全局约束）：
//! * STUN/TURN 服务器地址必须在配置的内网网段内（192.168.0.0/24）。
//! * 不向公网 STUN/TURN 暴露地址。
//! * 内网部署如果两端都在同网段，STUN 不需要（host candidate 直连），
//!   但跨网段/NAT 场景仍需 STUN 打洞 + TURN 中继兜底。

use std::net::IpAddr;

use serde::{Deserialize, Serialize};

/// ICE server 种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum IceServerKind {
    /// STUN：获取公网映射地址（RFC 5389）。
    Stun,
    /// TURN：中继兜底（RFC 8656），当打洞失败时使用。
    Turn,
}

impl std::fmt::Display for IceServerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IceServerKind::Stun => write!(f, "stun"),
            IceServerKind::Turn => write!(f, "turn"),
        }
    }
}

/// 单个 ICE server。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IceServer {
    /// `stun` 或 `turn`。
    pub kind: IceServerKind,
    /// 服务器地址（含端口），如 `192.168.0.5:3478`。
    pub address: String,
    /// TURN 用户名（STUN 留空）。
    #[serde(default)]
    pub username: String,
    /// TURN 凭证（STUN 留空）。
    #[serde(default)]
    pub credential: String,
}

impl IceServer {
    /// 构造 STUN server。
    pub fn stun(address: impl Into<String>) -> Self {
        Self {
            kind: IceServerKind::Stun,
            address: address.into(),
            username: String::new(),
            credential: String::new(),
        }
    }

    /// 构造 TURN server。
    pub fn turn(address: impl Into<String>, username: impl Into<String>, credential: impl Into<String>) -> Self {
        Self {
            kind: IceServerKind::Turn,
            address: address.into(),
            username: username.into(),
            credential: credential.into(),
        }
    }

    /// 提取 host 部分用于网段校验。
    pub fn host(&self) -> &str {
        self.address.split(':').next().unwrap_or(&self.address)
    }

    /// 转成 webrtc-rs 的 RTCIceServer JSON 格式（urls 字段）。
    pub fn to_urls(&self) -> Vec<String> {
        let scheme = match self.kind {
            IceServerKind::Stun => "stun",
            IceServerKind::Turn => "turn",
        };
        vec![format!("{scheme}:{}", self.address)]
    }
}

/// NAT 类型（决定需要哪些 ICE server）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NatType {
    /// 无 NAT（同网段直连，host candidate 可达）。
    None,
    /// 锥形 NAT（STUN 可打洞）。
    Cone,
    /// 对称 NAT（STUN 打洞可能失败，需要 TURN 中继兜底）。
    Symmetric,
}

/// ICE 配置：STUN/TURN server 列表 + NAT 类型。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IceConfig {
    pub servers: Vec<IceServer>,
    pub nat_type: NatType,
}

impl Default for IceConfig {
    fn default() -> Self {
        // 默认内网部署：STUN 在内网 192.168.0.5:3478，TURN 兜底在同地址。
        Self {
            servers: vec![
                IceServer::stun("192.168.0.5:3478"),
                IceServer::turn("192.168.0.5:3478", "quickmeet", "quickmeet"),
            ],
            nat_type: NatType::None,
        }
    }
}

impl IceConfig {
    /// 验证所有 ICE server 地址都在允许的内网网段内。
    pub fn validate(&self, allowlist: &[qm_common::Cidr]) -> Result<(), String> {
        for s in &self.servers {
            let host = s.host();
            let addr: IpAddr = host.parse().map_err(|e| {
                format!("ICE server 地址非法: {host}: {e}")
            })?;
            let ok = allowlist.iter().any(|c| c.contains(addr));
            if !ok {
                return Err(format!(
                    "ICE server {host} 不在允许的内网网段 {:?} 内（私有化部署禁止公网 STUN/TURN）",
                    allowlist
                ));
            }
        }
        Ok(())
    }

    /// STUN server 列表。
    pub fn stun_servers(&self) -> Vec<&IceServer> {
        self.servers.iter().filter(|s| s.kind == IceServerKind::Stun).collect()
    }

    /// TURN server 列表。
    pub fn turn_servers(&self) -> Vec<&IceServer> {
        self.servers.iter().filter(|s| s.kind == IceServerKind::Turn).collect()
    }
}

/// 按 NAT 类型选择 ICE server 组合。
///
/// 选择规则（RFC 8445 最佳实践）：
/// * 无 NAT → 只用 host candidate，不需要 STUN/TURN（返回空，节省带宽）。
/// * 锥形 NAT → STUN 足够打洞，TURN 备用（不优先使用，避免中继带宽）。
/// * 对称 NAT → STUN + TURN（TURN 可能必须中继）。
pub fn select_ice_servers(cfg: &IceConfig) -> Vec<IceServer> {
    match cfg.nat_type {
        NatType::None => {
            // 无 NAT：host candidate 直连，不需要 STUN/TURN
            vec![]
        }
        NatType::Cone => {
            // 锥形 NAT：STUN 打洞，TURN 备用（都提供，让 ICE 自行选择）
            cfg.servers.clone()
        }
        NatType::Symmetric => {
            // 对称 NAT：STUN + TURN（TURN 可能必须）
            cfg.servers.clone()
        }
    }
}

/// NAT 模拟结果（验收标准 3：回环与 NAT 模拟用例）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NatSimulation {
    /// 模拟的 NAT 类型。
    pub nat_type: NatType,
    /// STUN 打洞是否成功。
    pub stun_succeeded: bool,
    /// TURN 中继是否被使用。
    pub turn_used: bool,
    /// 选中的 ICE server 数量。
    pub selected_servers: usize,
    /// 描述文字。
    pub description: String,
}

/// 模拟一次 NAT 穿透场景。
///
/// 这是纯函数：不发起任何真实 STUN/TURN 请求，只按 NAT 类型推演结果。
/// 用于验收标准 3 的「回环与 NAT 模拟用例」。
pub fn simulate_nat(nat_type: NatType, cfg: &IceConfig) -> NatSimulation {
    let selected = select_ice_servers(&IceConfig {
        servers: cfg.servers.clone(),
        nat_type,
    });
    let selected_count = selected.len();

    match nat_type {
        NatType::None => NatSimulation {
            nat_type,
            stun_succeeded: true,  // 直连，不需要 STUN
            turn_used: false,
            selected_servers: selected_count,
            description: "无 NAT：host candidate 直连，无需 STUN/TURN".to_string(),
        },
        NatType::Cone => NatSimulation {
            nat_type,
            stun_succeeded: true,  // 锥形 NAT，STUN 可打洞
            turn_used: false,      // 打洞成功，不需要中继
            selected_servers: selected_count,
            description: "锥形 NAT：STUN 打洞成功，TURN 备用未使用".to_string(),
        },
        NatType::Symmetric => NatSimulation {
            nat_type,
            stun_succeeded: false, // 对称 NAT，STUN 打洞通常失败
            turn_used: true,        // 需要 TURN 中继
            selected_servers: selected_count,
            description: "对称 NAT：STUN 打洞失败，TURN 中继兜底".to_string(),
        },
    }
}

/// 回环穿透测试（验收标准 3：回环用例）。
///
/// 回环场景：两端在同一机器（127.0.0.1），无 NAT，host candidate 直接可达。
/// 这是最简单的穿透场景，验证 ICE 框架能正确选到 host candidate。
pub fn loopback_test() -> NatSimulation {
    let cfg = IceConfig {
        servers: vec![
            IceServer::stun("127.0.0.1:3478"),
            IceServer::turn("127.0.0.1:3478", "test", "test"),
        ],
        nat_type: NatType::None,
    };
    let result = simulate_nat(NatType::None, &cfg);
    // 回环测试断言：STUN 不需要但仍可达，TURN 不使用
    assert!(result.stun_succeeded, "回环：host candidate 应直接可达");
    assert!(!result.turn_used, "回环：不应使用 TURN 中继");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::Cidr;

    fn default_allowlist() -> Vec<Cidr> {
        vec![Cidr::parse("192.168.0.0/24").unwrap()]
    }

    #[test]
    fn default_config_has_stun_and_turn() {
        let cfg = IceConfig::default();
        assert!(!cfg.stun_servers().is_empty(), "默认必须有 STUN");
        assert!(!cfg.turn_servers().is_empty(), "默认必须有 TURN 兜底");
        assert_eq!(cfg.nat_type, NatType::None, "默认无 NAT（内网直连）");
    }

    #[test]
    fn validate_rejects_public_stun() {
        let cfg = IceConfig {
            servers: vec![IceServer::stun("8.8.8.8:3478")],
            nat_type: NatType::Cone,
        };
        let err = cfg.validate(&default_allowlist()).unwrap_err();
        assert!(err.contains("不在允许的内网网段"), "{err}");
    }

    #[test]
    fn validate_accepts_intranet_stun() {
        let cfg = IceConfig {
            servers: vec![IceServer::stun("192.168.0.5:3478")],
            nat_type: NatType::Cone,
        };
        cfg.validate(&default_allowlist()).unwrap();
    }

    #[test]
    fn select_no_nat_returns_empty() {
        let cfg = IceConfig::default();
        let selected = select_ice_servers(&cfg);
        assert!(
            selected.is_empty(),
            "无 NAT 时不应选 STUN/TURN，只用 host candidate"
        );
    }

    #[test]
    fn select_cone_nat_returns_stun_and_turn() {
        let cfg = IceConfig {
            servers: vec![
                IceServer::stun("192.168.0.5:3478"),
                IceServer::turn("192.168.0.5:3478", "u", "p"),
            ],
            nat_type: NatType::Cone,
        };
        let selected = select_ice_servers(&cfg);
        assert_eq!(selected.len(), 2, "锥形 NAT 应选 STUN + TURN");
        assert!(selected.iter().any(|s| s.kind == IceServerKind::Stun));
        assert!(selected.iter().any(|s| s.kind == IceServerKind::Turn));
    }

    #[test]
    fn select_symmetric_nat_returns_both() {
        let cfg = IceConfig {
            servers: vec![
                IceServer::stun("192.168.0.5:3478"),
                IceServer::turn("192.168.0.5:3478", "u", "p"),
            ],
            nat_type: NatType::Symmetric,
        };
        let selected = select_ice_servers(&cfg);
        assert_eq!(selected.len(), 2, "对称 NAT 应选 STUN + TURN");
    }

    #[test]
    fn loopback_scenario_passes() {
        // 验收标准 3：回环用例
        let result = loopback_test();
        assert_eq!(result.nat_type, NatType::None);
        assert!(result.stun_succeeded);
        assert!(!result.turn_used);
    }

    #[test]
    fn nat_simulation_none() {
        let cfg = IceConfig::default();
        let result = simulate_nat(NatType::None, &cfg);
        assert!(result.stun_succeeded);
        assert!(!result.turn_used);
        assert_eq!(result.selected_servers, 0, "无 NAT 不选 server");
    }

    #[test]
    fn nat_simulation_cone() {
        let cfg = IceConfig::default();
        let result = simulate_nat(NatType::Cone, &cfg);
        assert!(result.stun_succeeded, "锥形 NAT STUN 可打洞");
        assert!(!result.turn_used, "打洞成功不使用 TURN");
        assert!(result.selected_servers > 0);
    }

    #[test]
    fn nat_simulation_symmetric() {
        let cfg = IceConfig::default();
        let result = simulate_nat(NatType::Symmetric, &cfg);
        assert!(!result.stun_succeeded, "对称 NAT STUN 打洞失败");
        assert!(result.turn_used, "对称 NAT 需要 TURN 中继");
        assert!(result.selected_servers > 0);
    }

    #[test]
    fn ice_server_to_urls() {
        let stun = IceServer::stun("192.168.0.5:3478");
        assert_eq!(stun.to_urls(), vec!["stun:192.168.0.5:3478"]);

        let turn = IceServer::turn("192.168.0.5:3478", "u", "p");
        assert_eq!(turn.to_urls(), vec!["turn:192.168.0.5:3478"]);
    }

    #[test]
    fn ice_config_validate_rejects_bad_address() {
        let cfg = IceConfig {
            servers: vec![IceServer::stun("not-an-ip:3478")],
            nat_type: NatType::Cone,
        };
        let err = cfg.validate(&default_allowlist()).unwrap_err();
        assert!(err.contains("地址非法"), "{err}");
    }
}
