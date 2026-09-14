//! 统一错误模型。
//!
//! QuickMeet 全栈统一使用 [`Error`] / [`Result`]。webrtc-rs、hyper、std::io、
//! serde_json 的错误统一归一到本类型，业务层只需匹配一次错误分类。

use std::io;
use std::net::IpAddr;

/// 错误分类：用于日志级别决策与对外呈现（不向外泄漏内部细节）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum ErrorKind {
    Config,
    ConfigLoad,
    Signaling,
    WebRtc,
    Codec,
    Storage,
    Io,
    InvalidArgument,
    Internal,
    /// 本地 AI 接口错误（仅允许对接本地部署的硅基流动接口）。
    Ai,
}

/// QuickMeet 统一错误类型。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("配置错误[{kind:?}]: {source}")]
    Config {
        #[source]
        source: Text,
        kind: ErrorKind,
    },

    #[error("信令错误: {0}")]
    Signaling(String),

    #[error("WebRTC 错误: {0}")]
    WebRtc(String),

    #[error("编解码错误[{codec}]: {message}")]
    Codec { codec: String, message: String },

    #[error("存储错误: {0}")]
    Storage(String),

    #[error("本地 AI 接口错误: {0}")]
    Ai(String),

    #[error("非法输入: {0}")]
    InvalidArgument(String),

    #[error("内部错误: {0}")]
    Internal(String),

    #[error("IO 错误: {0}")]
    Io(#[from] io::Error),

    #[error("序列化错误: {0}")]
    Serde(#[from] serde_json::Error),
}

impl Error {
    /// 错误分类，日志与指标聚合使用。
    pub fn kind(&self) -> ErrorKind {
        match self {
            Error::Config { kind, .. } => *kind,
            Error::Signaling(_) => ErrorKind::Signaling,
            Error::WebRtc(_) => ErrorKind::WebRtc,
            Error::Codec { .. } => ErrorKind::Codec,
            Error::Storage(_) => ErrorKind::Storage,
            Error::Ai(_) => ErrorKind::Ai,
            Error::InvalidArgument(_) => ErrorKind::InvalidArgument,
            Error::Internal(_) => ErrorKind::Internal,
            Error::Io(_) => ErrorKind::Io,
            Error::Serde(_) => ErrorKind::Internal,
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::Config {
            source: Text(message.into()),
            kind: ErrorKind::Config,
        }
    }

    pub fn config_load(message: impl Into<String>) -> Self {
        Self::Config {
            source: Text(message.into()),
            kind: ErrorKind::ConfigLoad,
        }
    }

    pub fn codec(codec: impl Into<String>, message: impl Into<String>) -> Self {
        Error::Codec {
            codec: codec.into(),
            message: message.into(),
        }
    }

    /// 信令层错误（SDP / ICE candidate 转发与准入）。
    pub fn signaling(message: impl Into<String>) -> Self {
        Self::Signaling(message.into())
    }

    /// 调用方传入的非法参数。
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    /// 校验地址是否落在允许的内网网段内。
    ///
    /// 私有化部署硬约束：所有会议/信令地址必须属于配置声明的内网 CIDR，
    /// 越界（含公网地址）直接拒绝，避免数据出域。
    pub fn ensure_private_host(host: IpAddr, allowlist: &[Cidr]) -> Result<()> {
        if allowlist.iter().any(|c| c.contains(host)) {
            Ok(())
        } else {
            Err(Error::InvalidArgument(format!(
                "地址 {host} 不在允许的内网网段 {:?} 内（私有化部署禁止公网地址）",
                allowlist
            )))
        }
    }
}

/// 只携带文本的 `Error` source，避免 `Box<dyn Error>` 的分配开销。
#[derive(Debug, Clone, thiserror::Error)]
#[error("{0}")]
pub struct Text(String);

/// 轻量 CIDR（IPv4）解析与包含判定，避免为一个小工具引入额外依赖。
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct Cidr {
    network: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// 解析 `192.168.0.0/24` 形式。
    pub fn parse(s: &str) -> Result<Self> {
        let (net, prefix) =
            s.split_once('/').ok_or_else(|| Error::InvalidArgument(format!("CIDR 缺少 '/': {s}")))?;
        let network: IpAddr = net.trim().parse().map_err(|e| {
            Error::InvalidArgument(format!("CIDR 网络地址非法: {net}: {e}"))
        })?;
        let prefix: u8 = prefix.trim().parse().map_err(|e| {
            Error::InvalidArgument(format!("CIDR 前缀非法: {prefix}: {e}"))
        })?;
        if prefix > 32 {
            return Err(Error::InvalidArgument(format!("CIDR 前缀 {prefix} 超过上限 32")));
        }
        Ok(Self { network, prefix })
    }

    /// 地址是否落在本 CIDR 内。
    pub fn contains(&self, addr: IpAddr) -> bool {
        match (self.network, addr) {
            (IpAddr::V4(net), IpAddr::V4(a)) => {
                let want = u32::from(net);
                let have = u32::from(a);
                let mask = if self.prefix == 0 { 0 } else { u32::MAX << (32 - self.prefix) };
                want & mask == have & mask
            }
            _ => false,
        }
    }
}

impl std::fmt::Display for Cidr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

/// QuickMeet 统一 `Result` 别名。
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    fn v4(a: u8, b: u8, c: u8, d: u8) -> IpAddr {
        IpAddr::V4(std::net::Ipv4Addr::new(a, b, c, d))
    }

    #[test]
    fn cidr_parse_and_contains() {
        let c = Cidr::parse("192.168.0.0/24").unwrap();
        assert!(c.contains(v4(192, 168, 0, 7)));
        assert!(!c.contains(v4(192, 168, 1, 7)));
        assert!(!c.contains(v4(10, 0, 0, 1)));
        assert_eq!(c.to_string(), "192.168.0.0/24");
        // /0 覆盖全部 IPv4
        let all = Cidr::parse("0.0.0.0/0").unwrap();
        assert!(all.contains(v4(1, 2, 3, 4)));
        // /32 精确匹配
        let exact = Cidr::parse("127.0.0.1/32").unwrap();
        assert!(exact.contains(v4(127, 0, 0, 1)));
        assert!(!exact.contains(v4(127, 0, 0, 2)));
    }

    #[test]
    fn cidr_rejects_bad_input() {
        assert!(Cidr::parse("192.168.0.0/33").is_err());
        assert!(Cidr::parse("192.168.0.0").is_err());
        assert!(Cidr::parse("nope/24").is_err());
    }

    #[test]
    fn private_host_guard() {
        let allow = vec![Cidr::parse("192.168.0.0/24").unwrap()];
        assert!(Error::ensure_private_host(v4(192, 168, 0, 10), &allow).is_ok());
        assert!(Error::ensure_private_host(v4(8, 8, 8, 8), &allow).is_err());
        assert!(Error::ensure_private_host(v4(8, 8, 8, 8), &[]).is_err());
    }

    #[test]
    fn error_kind_mapping() {
        assert_eq!(Error::WebRtc("x".into()).kind(), ErrorKind::WebRtc);
        assert_eq!(Error::codec("vp8", "boom").kind(), ErrorKind::Codec);
        assert_eq!(Error::config("bad").kind(), ErrorKind::Config);
        assert_eq!(
            Error::from(std::io::Error::new(std::io::ErrorKind::Other, "io")).kind(),
            ErrorKind::Io
        );
    }

    #[test]
    fn error_display_contains_category() {
        let e = Error::codec("opus", "解码失败");
        let s = e.to_string();
        assert!(s.contains("opus"), "{s}");
        assert!(s.contains("解码失败"), "{s}");
    }
}
