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
    pub cluster: ClusterConfig,
    pub auth: AuthConfig,
    pub room: RoomConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            media: MediaConfig::default(),
            network: NetworkConfig::default(),
            logging: LoggingConfig::default(),
            storage: StorageConfig::default(),
            ai: AiConfig::default(),
            cluster: ClusterConfig::default(),
            auth: AuthConfig::default(),
            room: RoomConfig::default(),
        }
    }
}

/// 会议房间配置（QM-005：房间生命周期与权限管控）。
///
/// 所有字段都有默认值，且都落在 Issue 约定的硬约束上：
/// 空房回收宽限期 300s（验收标准 4「最后一人离开 5 分钟自动释放」）。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct RoomConfig {
    /// 所有人离开后的资源回收宽限期（秒）。到期整体释放房间对象 / 成员表 / 事件队列。
    pub grace_secs: u64,
    /// 单房间事件队列上限（环形截断，防无限增长）。
    pub max_events: usize,
    /// 参会人数上限的默认值（创建房间时未显式指定则用它；0 表示不限制）。
    pub default_max_participants: usize,
    /// 等候室默认开关。开启后非主持人成员进入后先停留在等候室，需主持人批准。
    pub default_waiting_room: bool,
    /// 主持人批准 / 拒绝等候室请求的超时时间（秒），超时按拒绝处理。
    pub approval_timeout_secs: u64,
    /// 会议预约的会前提醒时机（分钟前）。0 表示不发提醒（Issue YEJ-111：时机可配置）。
    pub appt_reminder_mins: u64,
    /// 预约开始前多少秒自动创建房间。0 表示正好在开始时刻创建。
    pub appt_auto_create_lead_secs: u64,
    /// 通知通道开关（Issue YEJ-111：站内信 + 桌面/移动通知）。
    /// 三条通道都只面向内网，不做公网推送。
    pub notify_in_app: bool,
    pub notify_desktop: bool,
    pub notify_mobile: bool,
}

impl Default for RoomConfig {
    fn default() -> Self {
        Self {
            // Issue QM-005 验收标准 4：最后一人离开后 5 分钟释放。
            grace_secs: 300,
            max_events: 256,
            default_max_participants: 64,
            default_waiting_room: false,
            approval_timeout_secs: 60,
            appt_reminder_mins: 15,
            appt_auto_create_lead_secs: 60,
            notify_in_app: true,
            notify_desktop: true,
            notify_mobile: true,
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
    /// 信令 HTTP 服务端口（兼容旧客户端的 REST 面：`/room/{id}/...`）。
    pub signaling_port: u16,
    /// WebSocket 信令（QM-004，WSS）监听端口。
    ///
    /// 与 HTTP 信令错开两位：媒体 8080 → 信令 HTTP 8081 → WS 信令 8082。
    /// 信令面仍与媒体/SFU 进程解耦，只是端口号连续便于内网防火墙放通。
    pub signaling_ws_port: u16,
    /// 每个信令房间的参会者上限（0 = 不限制）。
    ///
    /// 与 [`Self::max_participants`] 分工：后者是 SFU 媒体侧的会议容量口径，
    /// 这个上限约束的是单个信令房间的成员数，防止一个房间无限增长把广播
    /// 变成线性放大。
    pub signaling_max_per_room: usize,
    /// 单条信令帧的字节上限。超过即断开该连接。
    ///
    /// SDP 通常 2–6 KB，ICE candidate 单行远小于 1 KB，1 MB 已留出两个数量级
    /// 余量；同时把"大帧内存耗尽"这条拒绝服务面在协议层掐掉。
    pub signaling_max_frame_bytes: usize,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            max_participants: 64,
            idle_timeout_secs: 600,
            signaling_port: 8081,
            signaling_ws_port: 8082,
            signaling_max_per_room: 100,
            signaling_max_frame_bytes: 1_048_576,
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

/// 参会者身份鉴权配置（QM-004）。
///
/// 强制约束：未携带有效 JWT 的信令连接**直接拒绝**，且在任何认证之前
/// 一律不得返回会议信息。`enabled = false` 是给本地联调（浏览器页面 / demo）
/// 留的逃生口，私有化交付时必须打开；关闭时在服务端日志里打 WARN 提示。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct AuthConfig {
    /// 是否开启 JWT 强制鉴权。生产必须为 `true`。
    pub enabled: bool,
    /// HS256 对称密钥。**空字符串视为未配置**：一旦 `enabled = true` 而
    /// 密钥为空，服务拒绝启动，避免"以为开了鉴权其实没开"。
    /// 长度 ≥ 32 字节，否则拒绝启动（防短密钥暴力破解）。
    pub jwt_secret: String,
    /// 签名算法。本期只实现 HS256（本地对称密钥），其余值拒绝启动 ——
    /// RS256 / 外部 IdP 归入 SSO/OAuth 预留入口，不在本期范围。
    pub jwt_algorithm: String,
    /// Token 有效期（秒）。默认 24 小时，参会者长会议不会被中途踢下线。
    pub jwt_exp_secs: u64,
    /// 允许的时钟偏移（秒）。NTP 不干净的私有化现场常见几分钟漂移，
    /// 留 60 秒窗口，超过则按过期处理。
    pub jwt_clock_skew_secs: u64,
    /// 校验 token 里的 `exp` 声明。设为 `false` 时 token 永不过期 ——
    /// 只做签发侧的演示，验收时应当为 `true`。
    pub jwt_verify_exp: bool,
    /// 允许的 `iss` 声明。为空则不校验签发方（本地自签 token）。
    pub jwt_issuer: String,
    /// 允许的 `aud` 声明。为空则不校验受众。
    pub jwt_audience: String,
    /// `sub` 必须包含的域后缀（如 `corp.example`）。为空则不校验。
    /// SSO/OAuth 预留入口：IdP 侧把用户映射到 `sub`，这里做域级准入。
    pub jwt_required_domain: String,
    /// WSS（信令 WebSocket 的 TLS）配置。
    pub tls: TlsConfig,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            // QM-004 强制约束：未携带有效 JWT 的连接直接拒绝。默认开启，
            // 配一份**仅开发联调用**的密钥 —— 真实部署必须替换为现场生成的
            // 高熵随机密钥，否则等同于把会议室大门挂在门上。
            enabled: true,
            jwt_secret: env_var_or(
                "QM_AUTH_JWT_SECRET",
                "quickmeet-dev-signaling-secret-change-me-32b",
            ),
            jwt_algorithm: "HS256".to_string(),
            jwt_exp_secs: 86_400,
            jwt_clock_skew_secs: 60,
            jwt_verify_exp: true,
            jwt_issuer: String::new(),
            jwt_audience: String::new(),
            jwt_required_domain: String::new(),
            // QM-004 强制约束：信令通道强制 WSS，禁止明文传输。默认启用，
            // 未配置证书时启动期自动自签（内网联调零配置），生产替换为受信任 CA。
            tls: TlsConfig {
                enabled: true,
                ..Default::default()
            },
        }
    }
}

/// 读取环境变量；未设置时返回给定默认值。
fn env_var_or(key: &str, fallback: &str) -> String {
    std::env::var(key)
        .map(|v| {
            let v = v.trim().to_string();
            if v.is_empty() {
                fallback.to_string()
            } else {
                v
            }
        })
        .unwrap_or_else(|_| fallback.to_string())
}

impl AuthConfig {
    /// 是否要求客户端走 WSS。`tls.enabled` 为 `false` 时服务端拒绝启动
    /// 信令服务（QM-004 强制约束：信令通道强制 WSS，禁止明文传输）。
    pub fn wss_required(&self) -> bool {
        self.tls.enabled
    }

    /// JWT 鉴权是否生效：总开关打开**且**密钥已配置。
    ///
    /// 两者必须同时满足才算"真的在鉴权" —— `enabled = true` 而密钥为空是
    /// 最典型的"以为开了其实没开"。[`AppConfig::validate`] 会直接拦住后者。
    pub fn jwt_active(&self) -> bool {
        self.enabled && !self.jwt_secret.trim().is_empty()
    }
}

/// WSS / TLS 配置（QM-004 强制约束：信令通道强制 WSS 加密）。
///
/// 证书来源三选一（互斥，按 `cert_file` > `cert_pem` > 自动自签 的优先级）：
/// * `cert_file` + `key_file`：私有化现场签发好的证书（推荐）。
/// * `cert_pem`：直接把 PEM 文本写进配置（调试用）。
/// * 都为空：启动时**自动生成**一张 90 天有效期的自签证书，SAN 含
///   `localhost` / `127.0.0.1` / `0.0.0.0`，方便本机联调 —— 私有化交付
///   时应当换成受信任 CA 签发的证书。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
#[serde(default)]
pub struct TlsConfig {
    /// 是否启用 TLS（即 WSS）。服务启动时强制校验为 `true`。
    pub enabled: bool,
    /// 证书文件路径（PEM）。
    pub cert_file: String,
    /// 私钥文件路径（PEM / PKCS8 / RSA）。
    pub key_file: String,
    /// 内联证书 PEM 文本（与 `cert_file` 二选一）。
    pub cert_pem: String,
    /// 内联私钥 PEM 文本（与 `key_file` 二选一）。
    pub key_pem: String,
    /// 自签证书的 CN（仅 `cert_file` / `cert_pem` 都为空时生效）。
    pub self_signed_cn: String,
    /// 自签证书有效期（天）。
    pub self_signed_days: u32,
    /// 绑定 TLS 监听的地址（默认与 `network.bind_host` 相同）。
    pub bind_host: String,
    /// 最小 TLS 版本。`1.2` 或 `1.3`，其他值拒绝启动。
    pub min_tls_version: String,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_file: String::new(),
            key_file: String::new(),
            cert_pem: String::new(),
            key_pem: String::new(),
            self_signed_cn: "QuickMeet Signaling".to_string(),
            self_signed_days: 90,
            bind_host: String::new(),
            min_tls_version: "1.2".to_string(),
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
        if self.media.signaling_ws_port == 0 {
            return Err(Error::config("media.signaling_ws_port 不能为 0"));
        }
        // 两个信令面必须错开端口：HTTP 信令（旧 REST 面）与 WSS 信令并存，
        // 配成同一端口时后启动的那个必然 bind 失败，但错误信息会指向内核。
        if self.media.signaling_ws_port == self.media.signaling_port {
            return Err(Error::config(format!(
                "media.signaling_ws_port ({}) 不能与 media.signaling_port 相同（两个信令面并存，必须错开）",
                self.media.signaling_ws_port
            )));
        }
        if self.media.signaling_max_frame_bytes < 1024 {
            return Err(Error::config(
                "media.signaling_max_frame_bytes 不能小于 1024（一条 SDP 就远超该值）",
            ));
        }
        if self.media.signaling_max_frame_bytes > 8 * 1024 * 1024 {
            return Err(Error::config(
                "media.signaling_max_frame_bytes 不能超过 8 MiB（信令帧没有这么大的合法负载）",
            ));
        }
        if self.media.signaling_max_per_room > 512 {
            return Err(Error::config(
                "media.signaling_max_per_room 不能超过 512（房间广播的内存与 CPU 都随成员数线性增长）",
            ));
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
        self.auth.validate()?;
        // 房间层（QM-005）：宽限期写 0 等于「空房立即回收」，会让重连的参会者直接掉线，
        // 与验收标准 4（5 分钟宽限后释放）语义相反，所以在配置层挡住。
        if self.room.grace_secs == 0 {
            return Err(Error::config(
                "room.grace_secs 不能为 0（空房回收需要宽限期判定，Issue QM-005 约定 300s）",
            ));
        }
        if self.room.max_events == 0 {
            return Err(Error::config("room.max_events 不能为 0"));
        }
        if self.room.default_max_participants > 512 {
            return Err(Error::config(
                "room.default_max_participants 不能超过 512（单房间调度上限）",
            ));
        }
        if self.room.approval_timeout_secs == 0 {
            return Err(Error::config(
                "room.approval_timeout_secs 不能为 0（等候室批准必须有超时兜底）",
            ));
        }
        // 预约层（YEJ-111）：提醒时机可配置，但不能配置成"永远提前"。
        // 提前时间超过会议本身的时长时，提醒永远无法在会前触发，属于配置错误。
        if self.room.appt_reminder_mins > 24 * 60 {
            return Err(Error::config(
                "room.appt_reminder_mins 不能超过 1440（24 小时，提前提醒窗口上限）",
            ));
        }
        if self.room.appt_auto_create_lead_secs > 3600 {
            return Err(Error::config(
                "room.appt_auto_create_lead_secs 不能超过 3600（提前建房窗口上限 1 小时）",
            ));
        }
        Ok(())
    }
}

impl AuthConfig {
    /// 鉴权 / WSS 配置的内部一致性校验（QM-004）。
    ///
    /// `tls.enabled` 是否必须为 `true` 不放在这里 —— 媒体 / 集群服务不需要 WSS，
    /// 那条强制约束由信令服务启动时把关（`cfg.auth.wss_required()`），否则
    /// `tls.enabled = false` 会让整个 workspace 的配置校验都无法通过。
    /// 这里校验的是"填了就必须填对"：
    ///
    /// * 证书 / 私钥来源必须成对；
    /// * `min_tls_version` 只能是 1.2 / 1.3；
    /// * `jwt_algorithm` 本期只认 HS256 —— RS256 / 外部 IdP 归入 SSO/OAuth
    ///   预留入口，配了也跑不起来，因此拒绝启动；
    /// * `enabled = true` 时密钥必须非空且 ≥ 32 字节，避免"以为开了其实没开"；
    /// * `self_signed_days` 必须大于 0。
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.tls.min_tls_version.as_str(),
            "1.2" | "1.3" | "TLS1.2" | "TLS1.3"
        ) {
            return Err(Error::config(format!(
                "auth.tls.min_tls_version 取值非法: {}（合法值：1.2 / 1.3）",
                self.tls.min_tls_version
            )));
        }
        // 证书 / 私钥来源必须成对：文件与内联文本要么都填要么都留空。
        if self.tls.cert_file.trim().is_empty() != self.tls.key_file.trim().is_empty() {
            return Err(Error::config(
                "auth.tls.cert_file 与 auth.tls.key_file 必须成对填写（或都留空走自签证书）",
            ));
        }
        if self.tls.cert_pem.trim().is_empty() != self.tls.key_pem.trim().is_empty() {
            return Err(Error::config(
                "auth.tls.cert_pem 与 auth.tls.key_pem 必须成对填写（或都留空走证书文件 / 自签）",
            ));
        }
        if self.tls.self_signed_days == 0 {
            return Err(Error::config("auth.tls.self_signed_days 必须大于 0"));
        }
        if self.enabled && self.jwt_secret.trim().is_empty() {
            return Err(Error::config(
                "auth.enabled = true 但 auth.jwt_secret 为空：JWT 鉴权无法工作，请配置密钥（≥32 字节）",
            ));
        }
        if self.jwt_active() && self.jwt_secret.len() < 32 {
            return Err(Error::config(format!(
                "auth.jwt_secret 长度不足（{} 字节，至少 32）：短密钥会被暴力破解",
                self.jwt_secret.len()
            )));
        }
        if self.enabled && self.jwt_algorithm.trim().to_ascii_uppercase() != "HS256" {
            return Err(Error::config(format!(
                "auth.jwt_algorithm 不支持: {}（本期只实现 HS256；RS256 / 外部 IdP 走 SSO/OAuth 预留入口）",
                self.jwt_algorithm
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
        // `QM_AUTH_TLS_ENABLED` -> `auth.tls.enabled`：TlsConfig 嵌在 auth 下，
        // 环境变量只按第一个 `_` 分层，这里手动下钻一层。
        if section == "auth" && field.starts_with("tls_") {
            let inner = &field["tls_".len()..];
            let tls = out
                .entry("auth".to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                .as_object_mut()
                .expect("env_overrides_map 只写 Object 节点")
                .entry("tls".to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                .as_object_mut()
                .expect("env_overrides_map 只写 Object 节点");
            tls.insert(inner.to_string(), value_from_env(&v));
            continue;
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
            "port"
                | "max_participants"
                | "idle_timeout_secs"
                | "signaling_port"
                | "signaling_ws_port"
                | "signaling_max_per_room"
                | "signaling_max_frame_bytes"
        ) | ("network", "cidrs" | "bind_host")
            | ("logging", "level" | "file_dir" | "json")
            | ("storage", "data_dir" | "encrypted")
            | ("ai", "enabled" | "base_url" | "timeout_ms")
            | (
                "auth",
                "enabled"
                    | "jwt_secret"
                    | "jwt_algorithm"
                    | "jwt_exp_secs"
                    | "jwt_clock_skew_secs"
                    | "jwt_verify_exp"
                    | "jwt_issuer"
                    | "jwt_audience"
                    | "jwt_required_domain"
            ) | (
                "auth",
                "tls_enabled"
                    | "tls_cert_file"
                    | "tls_key_file"
                    | "tls_cert_pem"
                    | "tls_key_pem"
                    | "tls_self_signed_cn"
                    | "tls_self_signed_days"
                    | "tls_bind_host"
                    | "tls_min_tls_version"
            )
            | (
                "room",
                "grace_secs"
                    | "max_events"
                    | "default_max_participants"
                    | "default_waiting_room"
                    | "approval_timeout_secs"
                    | "appt_reminder_mins"
                    | "appt_auto_create_lead_secs"
                    | "notify_in_app"
                    | "notify_desktop"
                    | "notify_mobile"
            )
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
            )
    )
}

/// 尽力把环境变量字符串解析成 JSON 值：数组/对象/整数/布尔按其字面含义，
/// 其余一律当字符串。这样 `QM_NETWORK_CIDRS='["a","b"]'` 能落进 Vec 字段。
///
/// 特别注意**小数不能当数字**：`QM_AUTH_TLS_MIN_TLS_VERSION=1.3` 是 TLS 版本
/// 字符串（`"1.3"`），不是浮点数；被解析成 `1.3` 之后 figment 反序列化到
/// `String` 字段会静默失败，配置看起来生效其实没生效。
fn value_from_env(v: &str) -> serde_json::Value {
    let parsed = serde_json::from_str::<serde_json::Value>(v);
    if let Ok(serde_json::Value::Number(n)) = &parsed {
        if n.is_i64() || n.is_u64() {
            return parsed.unwrap();
        }
    } else if matches!(parsed.as_ref(), Ok(serde_json::Value::Bool(_) | serde_json::Value::Array(_) | serde_json::Value::Object(_))) {
        return parsed.unwrap();
    }
    serde_json::Value::String(v.to_string())
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
    fn auth_defaults_enforce_qm004_and_are_valid() {
        // QM-004 两条强制约束在默认配置里就要生效：
        //   * JWT 强制鉴权（默认开启 + 带开发联调用密钥）；
        //   * 信令通道强制 WSS（`tls.enabled = true`）。
        // 逃生口是给**本地联调**用的显式动作（`auth.enabled = false`），
        // 不是默认值；否则默认部署等于把会议室大门挂在门上。
        //
        // 注意：`auth.validate()` 只校验"填了就必须填对"，WSS 是否必须开启由
        // 信令服务启动时把关（`server::start_ws`），媒体 / 集群不需要 WSS。
        let cfg = AppConfig::default();
        assert!(cfg.auth.enabled, "QM-004：JWT 鉴权默认开启");
        assert!(cfg.auth.jwt_active(), "QM-004：默认带联调密钥，鉴权实际生效");
        assert!(cfg.auth.jwt_secret.len() >= 32, "默认密钥必须 ≥ 32 字节");
        assert!(cfg.auth.wss_required(), "QM-004：信令通道默认强制 WSS");
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn auth_validate_rejects_enabled_without_secret() {
        // 最典型的"以为开了其实没开"：开关打开但密钥是空的。
        // 默认配置自带一张联调密钥（本地零配置起得来），这里必须显式清空才能命中这条校验。
        let mut cfg = AppConfig::default();
        cfg.auth.enabled = true;
        cfg.auth.jwt_secret = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("jwt_secret"), "{err}");
    }

    #[test]
    fn auth_validate_rejects_short_secret() {
        let mut cfg = AppConfig::default();
        cfg.auth.enabled = true;
        cfg.auth.jwt_secret = "tooshort".to_string();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("长度不足"), "{err}");
    }

    #[test]
    fn auth_accepts_a_long_secret_and_reports_active() {
        let mut cfg = AppConfig::default();
        cfg.auth.enabled = true;
        cfg.auth.jwt_secret = "0123456789abcdef0123456789abcdef".to_string(); // 32 字节
        assert!(cfg.validate().is_ok());
        assert!(cfg.auth.jwt_active());
        assert!(cfg.auth.wss_required() == cfg.auth.tls.enabled);
    }

    #[test]
    fn auth_validate_rejects_unsupported_algorithm() {
        // 本期只实现 HS256；RS256 归 SSO/OAuth 预留入口，配了拒绝启动。
        let mut cfg = AppConfig::default();
        cfg.auth.enabled = true;
        cfg.auth.jwt_secret = "0123456789abcdef0123456789abcdef".to_string();
        cfg.auth.jwt_algorithm = "RS256".to_string();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("HS256"), "{err}");
    }

    #[test]
    fn auth_validate_rejects_bad_tls_version_and_mismatched_certs() {
        let mut cfg = AppConfig::default();
        cfg.auth.tls.min_tls_version = "1.1".to_string();
        let err = cfg.validate().unwrap_err();
        assert!(err.to_string().contains("min_tls_version"), "{err}");

        cfg.auth.tls.min_tls_version = "1.2".to_string();
        cfg.auth.tls.cert_file = "cert.pem".to_string(); // 私钥缺失，不成对
        assert!(cfg.validate().is_err());
        cfg.auth.tls.cert_file.clear();
        cfg.auth.tls.key_pem = "-----BEGIN PRIVATE KEY-----".to_string();
        assert!(cfg.validate().is_err(), "内联私钥没有配套证书也必须拒绝");
    }

    #[test]
    fn env_override_auth_tls_nested_into_auth_section() {
        let _g = ENV_LOCK.lock();
        std::env::set_var("QM_AUTH_TLS_ENABLED", "true");
        std::env::set_var("QM_AUTH_TLS_MIN_TLS_VERSION", "1.3");
        std::env::set_var("QM_AUTH_JWT_ALGORITHM", "HS256");
        let out = env_overrides_map().unwrap();
        std::env::remove_var("QM_AUTH_TLS_ENABLED");
        std::env::remove_var("QM_AUTH_TLS_MIN_TLS_VERSION");
        std::env::remove_var("QM_AUTH_JWT_ALGORITHM");

        let auth = out.get("auth").unwrap().as_object().unwrap();
        assert_eq!(
            auth.get("jwt_algorithm").and_then(|v| v.as_str()),
            Some("HS256")
        );
        let tls = auth.get("tls").unwrap().as_object().unwrap();
        assert_eq!(tls.get("enabled").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(tls.get("min_tls_version").and_then(|v| v.as_str()), Some("1.3"));
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
        assert_eq!(cfg.storage.encrypted, false);
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
            "#
            .into(),
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
