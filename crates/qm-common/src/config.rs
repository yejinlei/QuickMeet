//! 配置加载。
//!
// 加载顺序（后者覆盖前者）：
//   1. `config/default.toml`（内网 192.168.0.0/24、媒体端口 8080 默认值）
//   2. `config/local.json`（可选，存在才加载，用于本机/私有化现场覆盖）
//   3. 环境变量 `QM_XXX_YYY`（覆盖 `xxx.yyy`，现场应急覆盖，无需改文件）
//
// 所有字段都有默认值，加载失败只在"显式指定的文件不存在"时返回 [`ErrorKind::ConfigLoad`]。

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use crate::error::{Cidr, Error, Result};

/// 应用级全局配置。
#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct AppConfig {
    pub media: MediaConfig,
    pub network: NetworkConfig,
    pub logging: LoggingConfig,
    pub storage: StorageConfig,
    pub ai: AiConfig,
    pub cluster: ClusterConfig,
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

/// 节点角色：集群内一个节点可以是全功能媒体节点，也可以是只发流的旁听分发节点。
///
/// 旁听节点不建入站 PeerConnection，只把远端房间状态缓存下来并向下发单向流，
/// 因此可以承接远大于全功能节点的只收流参会者数量。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    /// 全功能媒体节点：既收流也转发，参与房间调度候选。
    #[default]
    Full,
    /// 旁听分发节点：只转发已存在的远端房间，媒体容量按旁听权重折算。
    Listener,
}

impl NodeRole {
    /// 容错解析：配置里写成 `full` / `listener` / `FULL` 都认，拼写错误才报错。
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "full" | "media" => Ok(NodeRole::Full),
            "listener" | "listeners" => Ok(NodeRole::Listener),
            other => Err(Error::config(format!(
                "cluster.node_role 取值非法: {other}（合法值：full / listener）"
            ))),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            NodeRole::Full => "full",
            NodeRole::Listener => "listener",
        }
    }
}

/// 分布式集群配置（QM-006）。
///
/// 所有地址都必须落在 [`NetworkConfig::cidrs`] 声明的内网网段内（全局约束 3/5），
/// NATS 集群用于房间状态跨节点同步、会议调度与健康检查心跳。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// NATS 服务端地址；必须是内网地址，不接受公网域名。
    pub server: String,
    /// NATS 客户端端口。
    pub port: u16,
    /// 集群标识：同名集群才共享房间状态与调度视图。
    pub cluster_id: String,
    /// 本节点唯一标识，写入心跳与房间归属。
    pub node_id: String,
    /// 对外通告的媒体地址，供旁听节点回源拉流（内网地址）。
    pub advertised_addr: String,
    /// 本节点角色：`full`（全功能）或 `listener`（旁听分发）。
    pub node_role: NodeRole,
    /// 健康检查心跳间隔（秒）。
    pub heartbeat_secs: u64,
    /// 连续错过该次数心跳即判定节点故障。
    pub unhealthy_misses: u64,
    /// 判定故障后，多久内必须完成会议迁移（秒）。
    pub failover_target_secs: u64,
    /// 新节点上线后，多久内必须完成加入并开始承接新会议（秒）。
    pub join_target_secs: u64,
    /// 单次 NATS 请求超时（秒）：用于房间状态查询与迁移命令回包。
    pub request_timeout_secs: u64,
    /// 单节点最多承载的会议数（调度上限）。
    pub max_rooms_per_node: usize,
    /// 单节点旁听参会者预估上限（10000 人旁听的容量口径）。
    pub listener_capacity: usize,
    /// 单条上行流按多少倍只收流观众折算媒体容量。
    pub listener_fanout: usize,
    /// 节点健康探针端口（QM-018）。
    ///
    /// 集群模式不监听 `media.signaling_port`，docker healthcheck 没有 HTTP 端点可探；
    /// 节点在这个端口起一个只读 `/healthz`，让 `docker-compose.yml` 的 `healthcheck:`
    /// 有确定性的目标。默认 8090，刻意避开 8080（媒体）与 8081（信令）。
    /// 必须是 1..=65535 之间的非冲突值：写 0 等于探针没有目标，容器永远 unhealthy。
    pub health_port: u16,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            server: "127.0.0.1".to_string(),
            port: 4222,
            cluster_id: "quickmeet".to_string(),
            node_id: "node-1".to_string(),
            advertised_addr: "192.168.0.10:8080".to_string(),
            node_role: NodeRole::default(),
            heartbeat_secs: 5,
            unhealthy_misses: 2,
            failover_target_secs: 10,
            join_target_secs: 30,
            request_timeout_secs: 3,
            max_rooms_per_node: 64,
            listener_capacity: 20_000,
            listener_fanout: 200,
            health_port: 8090,
        }
    }
}

impl ClusterConfig {
    /// 由配置推导的旁听容量权重：一条上行流按 `listener_fanout` 折算成观众槽位。
    pub fn listener_weight(&self) -> u64 {
        let fanout = self.listener_fanout.max(1) as u64;
        let cap = self.listener_capacity.max(1) as u64;
        (fanout * cap).max(1)
    }

    /// 本节点是否可被调度（旁听节点不可作为调度目标，只承接已分配房间的转发）。
    pub fn is_schedulable(&self) -> bool {
        self.node_role != NodeRole::Listener
    }

    /// 集群配置的启动期校验：所有集群地址必须落在允许的内网网段内。
    pub fn validate(&self, allowlist: &[Cidr]) -> Result<()> {
        if self.cluster_id.trim().is_empty() {
            return Err(Error::config("cluster.cluster_id 不能为空"));
        }
        if self.node_id.trim().is_empty() {
            return Err(Error::config("cluster.node_id 不能为空（集群内必须唯一）"));
        }
        if self.port == 0 {
            return Err(Error::config("cluster.port 不能为 0"));
        }
        if self.heartbeat_secs == 0 || self.unhealthy_misses == 0 {
            return Err(Error::config(
                "cluster.heartbeat_secs / unhealthy_misses 不能为 0",
            ));
        }
        if self.failover_target_secs == 0 || self.join_target_secs == 0 {
            return Err(Error::config(
                "cluster.failover_target_secs / join_target_secs 不能为 0",
            ));
        }
        if self.request_timeout_secs == 0 || self.request_timeout_secs >= self.heartbeat_secs {
            return Err(Error::config(
                "cluster.request_timeout_secs 必须大于 0 且小于 cluster.heartbeat_secs",
            ));
        }
        // 私有化硬约束：NATS 与媒体回源地址都只能是内网地址，越界直接拒绝。
        //
        // 唯一的例外是**回环地址**：`127.0.0.1` 是 docker-compose 把 NATS 端口映射到
        // 宿主后的典型写法（NATS sidecar 与本节点同机部署），回环流量不离开本机，
        // 因此不违反数据不出域；非回环地址一律要求落在 cidrs 允许网段内。
        let (server_ip, _) = parse_host_port(&self.server, self.port, "cluster.server")?;
        if !server_ip.is_loopback() {
            Error::ensure_private_host(server_ip, allowlist)?;
        }
        let advertised = self.advertised_addr.split_once(':').ok_or_else(|| {
            Error::config(format!(
                "cluster.advertised_addr 需要 host:port 形式: {}",
                self.advertised_addr
            ))
        })?;
        let advertised_ip: std::net::IpAddr = advertised.0.parse().map_err(|e| {
            Error::config(format!(
                "cluster.advertised_addr 主机名非法（需 IPv4）: {}: {e}",
                advertised.0
            ))
        })?;
        if !advertised_ip.is_loopback() {
            Error::ensure_private_host(advertised_ip, allowlist)?;
        }
        Ok(())
    }
}

/// 解析 `host:port` 形式的地址字符串，返回 (IPv4, port)。
///
/// 集群配置只接受 IPv4：内网 CIDR 判定（[`Cidr`]）只实现 v4，接受域名或 v6 会让
/// 校验形同虚设。
fn parse_host_port(host: &str, port: u16, label: &str) -> Result<(std::net::IpAddr, u16)> {
    let host = host.trim();
    if host.is_empty() {
        return Err(Error::config(format!("{label} 不能为空")));
    }
    if port == 0 {
        return Err(Error::config(format!("{label} 端口不能为 0")));
    }
    let ip: std::net::IpAddr = host.parse().map_err(|e| {
        Error::config(format!(
            "{label} 需 IPv4 地址（不接受域名或 IPv6）: {host}: {e}"
        ))
    })?;
    if ip.is_ipv6() {
        return Err(Error::config(format!(
            "{label} 需 IPv4 地址（不接受 IPv6）: {host}"
        )));
    }
    Ok((ip, port))
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
            return Err(Error::config(
                "network.cidrs 不能为空（私有化部署需声明允许的内网网段）",
            ));
        }
        self.network.parsed_cidrs()?;
        if self.logging.level.is_empty() {
            return Err(Error::config("logging.level 不能为空"));
        }
        let cidrs = self.network.parsed_cidrs()?;
        self.cluster.validate(&cidrs)?;
        // 探针端口校验：写 0 等于禁用探针，会让 compose healthcheck 没有目标可探
        // （容器永远 unhealthy），所以配置层直接挡住，不把 0 当合法值传下去。
        if self.cluster.health_port == 0 {
            return Err(Error::config(
                "cluster.health_port 不能为 0（写 0 会禁用探针，让 docker healthcheck 无端点可探）",
            ));
        }
        // 跨段校验：必须放在这里，因为涉及 media 段，
        // `ClusterConfig::validate` 拿不到媒体端口。冲突会让 docker healthcheck
        // 探到错的服务（或探不到），节点被判不健康却实际正常。
        if self.cluster.health_port == self.media.port
            || self.cluster.health_port == self.media.signaling_port
        {
            return Err(Error::config(format!(
                "cluster.health_port（{}）不能与 media.port（{}）/ media.signaling_port（{}）相同",
                self.cluster.health_port, self.media.port, self.media.signaling_port
            )));
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
        (
            "media",
            "port" | "max_participants" | "idle_timeout_secs" | "signaling_port"
        ) | ("network", "cidrs" | "bind_host")
            | ("logging", "level" | "file_dir" | "json")
            | ("storage", "data_dir" | "encrypted")
            | ("ai", "enabled" | "base_url" | "timeout_ms")
            | (
                "cluster",
                "server"
                    | "port"
                    | "cluster_id"
                    | "node_id"
                    | "advertised_addr"
                    | "node_role"
                    | "heartbeat_secs"
                    | "unhealthy_misses"
                    | "failover_target_secs"
                    | "join_target_secs"
                    | "request_timeout_secs"
                    | "max_rooms_per_node"
                    | "listener_capacity"
                    | "listener_fanout"
                    | "health_port"
            )
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
        assert!(cfg
            .network
            .parsed_cidrs()
            .unwrap()
            .iter()
            .any(|c| c.contains(std::net::IpAddr::V4(std::net::Ipv4Addr::new(
                192, 168, 0, 55
            )))));
        assert!(!cfg.ai.enabled, "AI 能力默认关闭");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_cidrs() {
        let mut cfg = AppConfig::default();
        cfg.network.cidrs.clear();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn cluster_defaults_satisfy_acceptance_windows() {
        let c = AppConfig::default().cluster;
        // 验收标准 2：节点宕机后 10s 内迁移 —— 判死窗口 = 心跳 × 错过次数。
        assert_eq!(c.heartbeat_secs.saturating_mul(c.unhealthy_misses), 10);
        assert_eq!(c.failover_target_secs, 10);
        // 验收标准 4：新节点 30s 内接入。
        assert_eq!(c.join_target_secs, 30);
        // 验收标准 3：旁听容量口径 = fanout × capacity，必须覆盖 10000 人。
        assert!(
            c.listener_weight() >= 10_000,
            "旁听容量 {} 必须覆盖 10000 人",
            c.listener_weight()
        );
        assert!(c.is_schedulable());
    }

    /// 健康探针端口默认 8090，且不撞 8080（媒体）/ 8081（信令）/ 4222（NATS）。
    #[test]
    fn cluster_health_port_default_avoids_known_ports() {
        let c = AppConfig::default().cluster;
        assert_eq!(c.health_port, 8090);
        assert_ne!(c.health_port, 8080, "不能占用媒体端口（Epic 全局约束）");
        assert_ne!(c.health_port, 8081, "不能占用信令端口");
        assert_ne!(c.health_port, c.port, "不能占用 NATS 端口");
        assert_ne!(c.health_port, 0, "0 表示禁用探针，不是合理默认值");
    }

    /// `QM_CLUSTER_HEALTH_PORT` 覆盖探针端口；`QM_*` 白名单拒绝未知字段。
    #[test]
    fn env_override_cluster_health_port_and_reject_unknown() {
        let _g = ENV_LOCK.lock();
        let dir = "./target/config_health_env";
        std::fs::create_dir_all(dir).unwrap();

        std::env::set_var("QM_CLUSTER_HEALTH_PORT", "9090");
        let (cfg, _) = load_from(dir).expect("cluster.health_port 必须在白名单里");
        assert_eq!(cfg.cluster.health_port, 9090);

        // 拼错字段名必须 fail fast，不能静默忽略（与 QM_NETWORK_CIDRS_0 同款问题）。
        std::env::set_var("QM_CLUSTER_HEALTHPRT", "9091");
        let err = load_from(dir).expect_err("未知字段必须报错");
        assert!(
            format!("{err}").contains("healthprt"),
            "报错信息要带上字段名: {err}"
        );
        std::env::remove_var("QM_CLUSTER_HEALTHPRT");
        std::env::remove_var("QM_CLUSTER_HEALTH_PORT");

        // 跨段校验：探针端口不能和媒体/信令端口撞，否则 healthcheck 会探到错的服务。
        std::env::set_var("QM_CLUSTER_HEALTH_PORT", "8080");
        let clash = load_from(dir).expect_err("health_port 与 media.port 冲突必须报错");
        assert!(
            format!("{clash}").contains("cluster.health_port"),
            "冲突报错要指明字段: {clash}"
        );
        std::env::remove_var("QM_CLUSTER_HEALTH_PORT");

        std::env::set_var("QM_CLUSTER_HEALTH_PORT", "8081");
        let clash2 = load_from(dir).expect_err("health_port 与 signaling_port 冲突必须报错");
        assert!(format!("{clash2}").contains("signaling_port"), "{clash2}");
        std::env::remove_var("QM_CLUSTER_HEALTH_PORT");

        // 写 0 必须报错：探针没有目标会让容器永远 unhealthy。
        std::env::set_var("QM_CLUSTER_HEALTH_PORT", "0");
        let zero = load_from(dir).expect_err("health_port = 0 必须报错");
        assert!(
            format!("{zero}").contains("不能为 0"),
            "写 0 的报错要说明原因: {zero}"
        );
        std::env::remove_var("QM_CLUSTER_HEALTH_PORT");
    }

    #[test]
    fn cluster_validate_accepts_loopback_nats_sidecar() {
        // docker-compose 把 NATS 映射到宿主后，节点侧看到的地址就是 127.0.0.1：
        // 回环流量不离开本机，属于合法的私有化部署形态。
        let cfg = AppConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn cluster_validate_rejects_public_addresses() {
        let cidrs = AppConfig::default().network.parsed_cidrs().unwrap();

        // NATS server 指向公网地址：必须拒绝（数据不得出域）。
        let mut cfg = AppConfig::default();
        cfg.cluster.server = "8.8.8.8".to_string();
        let err = cfg.cluster.validate(&cidrs).unwrap_err();
        assert!(err.to_string().contains("不在允许的内网网段"), "{err}");

        // 媒体回源地址指向公网地址：必须拒绝。
        let mut cfg = AppConfig::default();
        cfg.cluster.advertised_addr = "1.1.1.1:8080".to_string();
        assert!(cfg.cluster.validate(&cidrs).is_err());
    }

    #[test]
    fn cluster_validate_accepts_allowlisted_internal_address() {
        let mut cfg = AppConfig::default();
        cfg.cluster.server = "192.168.0.42".to_string();
        cfg.cluster.advertised_addr = "192.168.0.42:8080".to_string();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn cluster_validate_rejects_request_timeout_not_under_heartbeat() {
        // 请求超时必须小于心跳间隔：否则请求可能挂到下一次心跳之后，
        // 把「请求超时」和「节点故障」混成一件事。
        let mut cfg = AppConfig::default();
        cfg.cluster.request_timeout_secs = cfg.cluster.heartbeat_secs;
        let cidrs = AppConfig::default().network.parsed_cidrs().unwrap();
        let err = cfg.cluster.validate(&cidrs).unwrap_err();
        assert!(err.to_string().contains("request_timeout_secs"), "{err}");
    }

    #[test]
    fn node_role_parses_all_spellings() {
        assert_eq!(NodeRole::parse("full").unwrap(), NodeRole::Full);
        assert_eq!(NodeRole::parse("media").unwrap(), NodeRole::Full);
        assert_eq!(NodeRole::parse("listener").unwrap(), NodeRole::Listener);
        assert_eq!(NodeRole::parse("LISTENERS").unwrap(), NodeRole::Listener);
        assert!(NodeRole::parse("router").is_err());
    }

    #[test]
    fn load_from_empty_dir_uses_defaults_then_env_overrides() {
        let _g = ENV_LOCK.lock();
        std::fs::create_dir_all("./target/config_empty").unwrap();
        let (cfg, src) = load_from("./target/config_empty").unwrap();
        assert_eq!(cfg.media.port, 8080, "目录内无配置文件时必须用默认 8080");
        assert!(!src.default_file, "目录内无 default.toml 时不声明该来源");

        // QM_ 前缀环境变量优先于默认值（便于多实例并排部署）
        // 18080 刻意避开 8090：cluster.health_port 默认 8090，撞了会被跨段校验挡住。
        std::env::set_var("QM_MEDIA_PORT", "18080");
        let (cfg2, src2) = load_from("./target/config_empty").unwrap();
        std::env::remove_var("QM_MEDIA_PORT");
        assert_eq!(cfg2.media.port, 18080, "QM_MEDIA_PORT 必须覆盖默认端口");
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
        assert_eq!(
            cfg.network.bind_host, "10.10.10.10",
            "network.bind_host 必须覆盖"
        );
        assert_eq!(
            cfg.logging.file_dir, "./target/qm_env_log",
            "logging.file_dir 必须覆盖"
        );
        assert_eq!(
            cfg.storage.data_dir, "./target/qm_env_data",
            "storage.data_dir 必须覆盖"
        );
        assert_eq!(
            cfg.ai.base_url, "http://192.168.0.20/v1",
            "ai.base_url 必须覆盖"
        );
        assert_eq!(cfg.ai.timeout_ms, 1234, "ai.timeout_ms 必须覆盖");

        // 未设置的环境变量必须保持默认值，不能被上面的键污染。
        assert_eq!(cfg.media.max_participants, 64);
        assert!(!cfg.storage.encrypted);
    }

    #[test]
    fn env_override_json_array_replaces_whole_vec() {
        // Vec 字段必须整段给 JSON 数组：`QM_NETWORK_CIDRS_0/_1` 这种索引写法
        // 在旧的 `.split("_")` 路径下会变成嵌套 map，而 cidrs 期望 sequence，
        // 结果是配置加载直接失败（`invalid type: found map, expected a sequence`）。
        let _g = ENV_LOCK.lock();
        std::fs::create_dir_all("./target/config_env_nested").unwrap();
        std::env::set_var("QM_NETWORK_CIDRS", r#"["192.168.0.0/24","10.0.0.0/8"]"#);
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
            "#,
        ))
        .join(figment::providers::Serialized::defaults(
            AppConfig::default(),
        ))
        .extract()
        .unwrap();
        assert_eq!(cfg.media.port, 8085, "内联 TOML 必须覆盖默认端口");
        assert_eq!(
            cfg.network.bind_host, "192.168.0.10",
            "未声明字段回落默认值"
        );
        assert_eq!(cfg.network.cidrs.len(), 2);
        assert_eq!(cfg.network.parsed_cidrs().unwrap().len(), 2);
    }
}
