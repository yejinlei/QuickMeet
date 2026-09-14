//! 配置加载。
//!
// 加载顺序（后者覆盖前者）：
//   1. `config/default.toml`（内网 192.168.0.0/24、媒体端口 8080 默认值）
//   2. `config/local.json`（可选，存在才加载，用于本机/私有化现场覆盖）
//   3. 环境变量 `QM_XXX_YYY`（覆盖 `xxx.yyy`，现场应急覆盖，无需改文件）
// 
//// 所有字段都有默认值，加载失败只在"显式指定的文件不存在"时返回 [`ErrorKind::ConfigLoad`]。

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::error::{Cidr, Error, Result};

/// 应用级全局配置。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub media: MediaConfig,
    pub network: NetworkConfig,
    pub logging: LoggingConfig,
    pub storage: StorageConfig,
    pub ai: AiConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            media: MediaConfig::default(),
            network: NetworkConfig::default(),
            logging: LoggingConfig::default(),
            storage: StorageConfig::default(),
            ai: AiConfig::default(),
        }
    }
}

/// 媒体服务配置。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct MediaConfig {
    /// 媒体服务默认监听端口（Epic 全局约束：8080）。
    pub port: u16,
    /// 单会议最大参与者数，0 表示不限制（仅做上限保护）。
    pub max_participants: usize,
    /// 会话空闲超时（秒），超时后关闭 PeerConnection 释放资源。
    pub idle_timeout_secs: u64,
    /// 信令 HTTP 服务端口。
    pub signaling_port: u16,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            max_participants: 64,
            idle_timeout_secs: 600,
            signaling_port: 8081,
        }
    }
}

/// 内网网段配置（私有化合规：仅允许配置声明的内网地址）。
///
/// 与其余四个配置结构体一致带 `#[serde(default)]`：否则任何**部分声明**的
/// `network` 段（例如现场只设 `QM_NETWORK_CIDRS`）都会因缺 `bind_host`
/// 直接报 `missing field`。`cidrs` 为空的问题由 [`AppConfig::validate`] 拦。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct NetworkConfig {
    /// 允许的本机/对端地址网段，默认内网 192.168.0.0/24。
    pub cidrs: Vec<String>,
    /// 信令服务绑定地址，默认只绑定内网网卡而非 0.0.0.0。
    pub bind_host: String,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            cidrs: vec!["192.168.0.0/24".to_string()],
            bind_host: "192.168.0.10".to_string(),
        }
    }
}

impl NetworkConfig {
    /// 解析全部 CIDR，任一非法即返回配置错误。
    pub fn parsed_cidrs(&self) -> Result<Vec<Cidr>> {
        self.cidrs.iter().map(|s| Cidr::parse(s.as_str())).collect()
    }
}

/// 日志配置。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// 日志级别：`trace`/`debug`/`info`/`warn`/`error`，支持 `target=level` 组合。
    pub level: String,
    /// 日志文件目录；为空则不写文件。
    pub file_dir: String,
    /// 是否使用 JSON 结构化输出（容器化场景推荐）。
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            file_dir: String::new(),
            json: false,
        }
    }
}

/// 本地存储配置（音视频/会议数据必须本地留存，禁止出域）。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct StorageConfig {
    /// 会议数据根目录。
    pub data_dir: String,
    /// 录制文件是否加密（第一阶段保留开关，实现由后续 Issue 提供）。
    pub encrypted: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: "./data/meetings".to_string(),
            encrypted: false,
        }
    }
}

/// 本地 AI 接口配置。
///
/// Epic 全局约束：AI 能力只对接本地部署的硅基流动接口，禁止调用公网第三方 API。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct AiConfig {
    /// 是否启用 AI 能力（第一阶段默认关闭，仅保留接入点）。
    pub enabled: bool,
    /// 本地硅基流动接口地址，必须是内网地址。
    pub base_url: String,
    /// 单次请求超时（毫秒）。
    pub timeout_ms: u64,
}

impl Default for AiConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: "http://127.0.0.1:3000/v1".to_string(),
            timeout_ms: 30_000,
        }
    }
}

/// 配置来源，便于在日志/调试中追踪某个值从哪来。
#[derive(Debug, Default)]
pub struct ConfigSource {
    pub default_file: bool,
    pub local_file: bool,
    pub env_overrides: usize,
}

/// 加载配置。默认读取 `config/` 目录，环境变量前缀 `QM_`。
pub fn load() -> Result<(AppConfig, ConfigSource)> {
    load_from("./config")
}

/// 从指定目录加载配置。
pub fn load_from(config_dir: &str) -> Result<(AppConfig, ConfigSource)> {
    let mut source = ConfigSource::default();
    // figment 的链式接口是 `Figment::from(..).merge(..).extract()`；
    // 后加入的来源覆盖先加入的，所以顺序必须是 default -> local -> env。
    use figment::providers::Format;

    let mut figment = figment::Figment::default();

    let default_file = format!("{config_dir}/default.toml");
    let local_file = format!("{config_dir}/local.json");

    if std::path::Path::new(&default_file).exists() {
        figment = figment.merge(figment::providers::Toml::file(default_file));
        source.default_file = true;
    }
    if std::path::Path::new(&local_file).exists() {
        figment = figment.merge(figment::providers::Json::file(local_file));
        source.local_file = true;
    }
    // 环境变量最后覆盖。这里**不能**直接用 figment 的 `.split("_")`：
    // 它的实现是 `key.replace("_", ".")`，会把**所有**下划线换成点，
    // 于是 `QM_MEDIA_SIGNALING_PORT` 变成 `media.signaling.port`，而字段名是
    // `signaling_port` —— serde 静默丢掉不认识的键，配置看似生效实则没生效。
    // 实测：设 `QM_MEDIA_SIGNALING_PORT=18081` 后进程仍报 `signaling_port=8081`。
    //
    // 正确的语义是「只按第一个 `_` 分层」：`QM_NETWORK_BIND_HOST` -> `network.bind_host`。
    // 也不宜用 `.map()` 去修键名：`Env::prefixed` 内部留了一份大写前缀 `QM_`
    // 参与 Profile 解析，再叠 map 后顶层键会变成残留前缀（实测是 `ia` 而非
    // `media`），整份配置落到未知字段被静默丢弃。所以这里自己读环境变量、
    // 组好嵌套 map，再用 `Serialized::defaults` 合入 —— 顶层键名完全由我们控制。
    let env_overrides = env_overrides_map()?;
    if !env_overrides.is_empty() {
        figment = figment.merge(figment::providers::Serialized::defaults(
            serde_json::Value::Object(env_overrides),
        ));
    }

    let config: AppConfig = figment
        .extract()
        .map_err(|e| Error::config_load(format!("配置解析失败: {e}")))?;

    // 加载期校验：尽早暴露非法配置，避免运行到一半才炸。
    config.validate()?;
    source.env_overrides = env_override_count();
    Ok((config, source))
}

/// 当前生效配置（进程内缓存，供只读消费方使用）。
pub static CURRENT: Lazy<Mutex<AppConfig>> = Lazy::new(|| Mutex::new(AppConfig::default()));

/// 写入进程内缓存并初始化日志。服务启动时调用一次。
pub fn init(config_dir: &str) -> Result<AppConfig> {
    let (cfg, src) = load_from(config_dir)?;
    *CURRENT.lock() = cfg.clone();
    crate::logging::init_subscriber(&cfg);
    tracing::info!(
        ?src,
        media_port = cfg.media.port,
        cidrs = ?cfg.network.cidrs,
        "QuickMeet 配置已加载"
    );
    Ok(cfg)
}

impl AppConfig {
    /// 启动期配置校验。
    pub fn validate(&self) -> Result<()> {
        if self.media.port == 0 {
            return Err(Error::config("media.port 不能为 0"));
        }
        if self.media.signaling_port == 0 {
            return Err(Error::config("media.signaling_port 不能为 0"));
        }
        if self.network.cidrs.is_empty() {
            return Err(Error::config("network.cidrs 不能为空（私有化部署需声明允许的内网网段）"));
        }
        self.network.parsed_cidrs()?;
        if self.logging.level.is_empty() {
            return Err(Error::config("logging.level 不能为空"));
        }
        Ok(())
    }
}

fn env_override_count() -> usize {
    std::env::vars()
        .filter(|(k, _)| k.starts_with("QM_"))
        .count()
}

/// 把 `QM_SECTION_FIELD` 形式的环境变量整理成顶层配置 map：
/// `{"media": {"signaling_port": 18081}, "network": {"cidrs": [...]}}`，
/// 供 figment 以 [`figment::providers::Serialized`] 的形式合入配置。
///
/// 约定：只按**第一个** `_` 分层，保留字段名里的下划线。
/// `QM_MEDIA_SIGNALING_PORT` -> `media.signaling_port`，而不是 `media.signaling.port`；
/// `QM_MEDIA_PORT` -> `media.port`。
///
/// 遇到不认识的 section 或字段名**直接报错**而不是静默忽略 —— 配置看起来
/// 生效了其实没生效，比直接失败难查得多（`QM_NETWORK_CIDRS_0` 就是这类错误）。
fn env_overrides_map() -> Result<serde_json::Map<String, serde_json::Value>> {
    let mut out = serde_json::Map::new();
    for (k, v) in std::env::vars() {
        if !k.starts_with("QM_") || k.len() <= "QM_".len() {
            continue;
        }
        let rest = &k["QM_".len()..];
        let (section, field) = match rest.find('_') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            // 无第二段的键定位不到字段，属于配置错误而非忽略。
            None => {
                return Err(Error::config(format!(
                    "环境变量 {k} 缺少字段名（应为 QM_SECTION_FIELD）"
                )));
            }
        };
        let section = section.to_ascii_lowercase();
        let field = field.to_ascii_lowercase();
        if !known_section(&section, &field) {
            return Err(Error::config(format!(
                "环境变量 {k} 指向未知的配置项 {section}.{field}"
            )));
        }
        out.entry(section.clone())
            .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
            .as_object_mut()
            .expect("env_overrides_map 只写 Object 节点")
            .insert(field, value_from_env(&v));
    }
    Ok(out)
}

/// 已知的配置项集合，与 [`AppConfig`] 的字段一一对应。
/// 加字段时这里也要加，否则对应的 `QM_*` 环境变量会报未知配置项。
fn known_section(section: &str, field: &str) -> bool {
    matches!(
        (section, field),
        ("media", "port" | "max_participants" | "idle_timeout_secs" | "signaling_port")
            | ("network", "cidrs" | "bind_host")
            | ("logging", "level" | "file_dir" | "json")
            | ("storage", "data_dir" | "encrypted")
            | ("ai", "enabled" | "base_url" | "timeout_ms")
    )
}

/// 尽力把环境变量字符串解析成 JSON 值：数组/对象/数字/布尔按其字面含义，
/// 否则当作字符串。这样 `QM_NETWORK_CIDRS='["a","b"]'` 能落进 Vec 字段。
fn value_from_env(v: &str) -> serde_json::Value {
    serde_json::from_str(v).unwrap_or_else(|_| serde_json::Value::String(v.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 环境变量是**进程级全局状态**，测试并行时互相踩。
    /// 所有改 `QM_*` 的测试都必须先拿到这把锁，否则断言会偶发失败。
    static ENV_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn defaults_match_epic_constraints() {
        let cfg = AppConfig::default();
        assert_eq!(cfg.media.port, 8080, "媒体服务默认监听 8080");
        assert!(
            cfg.network
                .parsed_cidrs()
                .unwrap()
                .iter()
                .any(|c| c.contains(std::net::IpAddr::V4(
                    std::net::Ipv4Addr::new(192, 168, 0, 55)
                )))
        );
        assert!(cfg.ai.enabled == false, "AI 能力默认关闭");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_cidrs() {
        let mut cfg = AppConfig::default();
        cfg.network.cidrs.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn load_from_empty_dir_uses_defaults_then_env_overrides() {
        let _g = ENV_LOCK.lock();
        std::fs::create_dir_all("./target/config_empty").unwrap();
        let (cfg, src) = load_from("./target/config_empty").unwrap();
        assert_eq!(cfg.media.port, 8080, "目录内无配置文件时必须用默认 8080");
        assert!(!src.default_file, "目录内无 default.toml 时不声明该来源");

        // QM_ 前缀环境变量优先于默认值（便于多实例并排部署）
        std::env::set_var("QM_MEDIA_PORT", "8090");
        let (cfg2, src2) = load_from("./target/config_empty").unwrap();
        std::env::remove_var("QM_MEDIA_PORT");
        assert_eq!(cfg2.media.port, 8090, "QM_MEDIA_PORT 必须覆盖默认端口");
        assert!(src2.env_overrides > 0, "有环境变量时必须声明 env 来源");
    }

    #[test]
    fn env_override_keeps_field_name_underscores() {
        // 回归护栏：figment 的 `.split("_")` 会把**所有**下划线换成点，
        // `QM_MEDIA_SIGNALING_PORT` -> `media.signaling.port`，而字段名是
        // `signaling_port`，serde 静默丢弃未知键 —— 结果配置"看起来生效了"
        // 实际没生效。这里逐个断言多段字段名都必须能覆盖成功。
        let _g = ENV_LOCK.lock();
        let dir = "./target/config_env_nested";
        std::fs::create_dir_all(dir).unwrap();

        std::env::set_var("QM_MEDIA_PORT", "8100");
        std::env::set_var("QM_MEDIA_SIGNALING_PORT", "8101");
        std::env::set_var("QM_NETWORK_BIND_HOST", "10.10.10.10");
        std::env::set_var("QM_LOGGING_FILE_DIR", "./target/qm_env_log");
        std::env::set_var("QM_STORAGE_DATA_DIR", "./target/qm_env_data");
        std::env::set_var("QM_AI_BASE_URL", "http://192.168.0.20/v1");
        std::env::set_var("QM_AI_TIMEOUT_MS", "1234");
        let (cfg, _) = load_from(dir).unwrap();
        for k in [
            "QM_MEDIA_PORT",
            "QM_MEDIA_SIGNALING_PORT",
            "QM_NETWORK_BIND_HOST",
            "QM_LOGGING_FILE_DIR",
            "QM_STORAGE_DATA_DIR",
            "QM_AI_BASE_URL",
            "QM_AI_TIMEOUT_MS",
        ] {
            std::env::remove_var(k);
        }

        assert_eq!(cfg.media.port, 8100, "media.port 单段覆盖");
        assert_eq!(
            cfg.media.signaling_port, 8101,
            "media.signaling_port 必须覆盖（split(\"_\") 会把它打成 media.signaling.port 并静默丢弃）"
        );
        assert_eq!(cfg.network.bind_host, "10.10.10.10", "network.bind_host 必须覆盖");
        assert_eq!(
            cfg.logging.file_dir, "./target/qm_env_log",
            "logging.file_dir 必须覆盖"
        );
        assert_eq!(cfg.storage.data_dir, "./target/qm_env_data", "storage.data_dir 必须覆盖");
        assert_eq!(cfg.ai.base_url, "http://192.168.0.20/v1", "ai.base_url 必须覆盖");
        assert_eq!(cfg.ai.timeout_ms, 1234, "ai.timeout_ms 必须覆盖");

        // 未设置的环境变量必须保持默认值，不能被上面的键污染。
        assert_eq!(cfg.media.max_participants, 64);
        assert_eq!(cfg.storage.encrypted, false);
    }

    #[test]
    fn env_override_json_array_replaces_whole_vec() {
        // Vec 字段必须整段给 JSON 数组：`QM_NETWORK_CIDRS_0/_1` 这种索引写法
        // 在旧的 `.split("_")` 路径下会变成嵌套 map，而 cidrs 期望 sequence，
        // 结果是配置加载直接失败（`invalid type: found map, expected a sequence`）。
        let _g = ENV_LOCK.lock();
        std::fs::create_dir_all("./target/config_env_nested").unwrap();
        std::env::set_var(
            "QM_NETWORK_CIDRS",
            r#"["192.168.0.0/24","10.0.0.0/8"]"#,
        );
        let (cfg, _) = load_from("./target/config_env_nested").unwrap();
        std::env::remove_var("QM_NETWORK_CIDRS");
        assert_eq!(cfg.network.cidrs.len(), 2, "整段 JSON 数组必须替换默认网段");
        assert!(cfg.network.cidrs[0].starts_with("192.168.0.0/"));

        // 索引写法必须失败，而不是静默退化成默认值。
        std::env::set_var("QM_NETWORK_CIDRS_0", "1.2.3.4/32");
        assert!(
            load_from("./target/config_env_nested").is_err(),
            "QM_NETWORK_CIDRS_0 索引写法对 Vec 字段必须报错，不能静默忽略"
        );
        std::env::remove_var("QM_NETWORK_CIDRS_0");
    }

    #[test]
    fn inline_toml_parses() {
        use figment::providers::Format;
        let cfg: AppConfig = figment::Figment::from(figment::providers::Toml::string(
            r#"
            [media]
            port = 8085
            [network]
            cidrs = ["192.168.0.0/24", "10.0.0.0/8"]
            "#
            .into(),
        ))
        .join(figment::providers::Serialized::defaults(AppConfig::default()))
        .extract()
        .unwrap();
        assert_eq!(cfg.media.port, 8085, "内联 TOML 必须覆盖默认端口");
        assert_eq!(cfg.network.bind_host, "192.168.0.10", "未声明字段回落默认值");
        assert_eq!(cfg.network.cidrs.len(), 2);
        assert_eq!(cfg.network.parsed_cidrs().unwrap().len(), 2);
    }
}
