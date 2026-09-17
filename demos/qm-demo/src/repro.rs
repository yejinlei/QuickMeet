//! QM-024 复现工具：真 NATS + 3 节点，逐项验证 F1 / F3 / F4 / F7 与验收标准 1-3。
//!
//! 为什么必须真跑一个 NATS 进程：纯单测只能断言「subject 字符串长什么样」，
//! 断言不了「NATS 到底把消息投没投给订阅端」。F1 的结论尤其需要实测——
//! 修复前订阅 `qm.<cid>.hb.*`，4 段，而心跳发布在 `qm.<cid>.hb.<node_id>`
//! 也是 4 段，所以 `*` 到底匹不匹配，只有让 server 裁决才知道。
//!
//! 两种模式：
//! * `--repro-mode legacy` —— 复刻修复前的 subject 规划：心跳订阅用 `hb.*`。
//!   结论是实测出来的（不预设它一定坏）；如果 `hb.*` 本来就匹配，报告里会明确
//!   写着「F1 在默认 cluster_id 下并未构成实际故障，`>` 属加固」。
//! * `--repro-mode post`（默认）—— 修复后的完整行为。
//!
//! 用法（在仓库根目录）：
//!   cargo run --release --manifest-path demos/qm-demo/Cargo.toml \
//!       -- --repro --repro-nats F:/tmp/nats-src/nats-server.exe
//!
//! 末尾的 `REPRO-RESULT:` 行是完整 JSON，便于验收脚本逐字段断言。

use std::io::Write as _;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use qm_cluster::{bus, Cluster, NatsBus};
use qm_common::{Cidr, ClusterConfig, NodeRole};
use serde::Serialize;

/// 复现模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// 修复前：心跳订阅 subject 用 `hb.*`。
    Legacy,
    /// 修复后：心跳订阅 subject 用 `hb.>`。
    Post,
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Legacy => "legacy",
            Mode::Post => "post",
        }
    }

    /// 该模式下的「全集群心跳」订阅 subject。
    fn heartbeat_all(&self, prefix: &str) -> String {
        match self {
            Mode::Legacy => format!("{prefix}.hb.*"),
            Mode::Post => bus::subject_heartbeat_all(prefix),
        }
    }
}

/// 复现用的窗口参数档位。
///
/// 为什么要有两档：`failover_target_secs` 约束的是「判死 → 归属变更」，而判死
/// 之前集群无法知道节点已经没了。短档（默认）能把判死窗口压到 6s，让迁移与
/// 僵尸摘除落在同一个短窗口内被观察到；`production` 档则与 `config/default.toml`
/// 完全一致（10s 判死窗口 / 10s 迁移时限），用来回答「生产默认参数下这个数字
/// 到底是多少」—— 而不是留一个只在调过参数的配置下才成立的指标。
#[derive(Debug, Clone, Copy)]
pub enum Profile {
    /// 6s 判死窗口（2s × 3）/ 1s 迁移时限 / 7s 摘除宽限。
    Fast,
    /// 与 `config/default.toml` 一致：10s 判死窗口（5s × 2）/ 10s 迁移时限 / 20s 宽限。
    Production,
}

impl Profile {
    fn as_str(self) -> &'static str {
        match self {
            Profile::Fast => "fast",
            Profile::Production => "production(default.toml)",
        }
    }

    fn heartbeat_secs(self) -> u64 {
        match self {
            Profile::Fast => 2,
            Profile::Production => 5,
        }
    }

    fn unhealthy_misses(self) -> u64 {
        match self {
            Profile::Fast => 3,
            Profile::Production => 2,
        }
    }

    fn failover_target_secs(self) -> u64 {
        match self {
            Profile::Fast => 1,
            Profile::Production => 10,
        }
    }

    /// 判死窗口（毫秒）。
    fn window_ms(self) -> u64 {
        self.heartbeat_secs()
            .saturating_mul(self.unhealthy_misses().max(1))
            .saturating_mul(1000)
    }

    /// 判死后必须完成的迁移时限（毫秒）。
    fn deadline_ms(self) -> u64 {
        self.failover_target_secs().saturating_mul(1000)
    }

    /// 故障阶段的总预算：判死窗口 + 迁移时限 + 一次轮次余量。
    fn failover_budget(self) -> Duration {
        Duration::from_millis(
            self.window_ms()
                .saturating_add(self.deadline_ms())
                .saturating_add(5000),
        )
    }
}

/// 复现报告（`REPRO-RESULT:` 那行 JSON 的形状）。
#[derive(Debug, Default, Serialize)]
struct Report {
    mode: String,
    cluster_id: String,
    /// 发布端实际用的 subject（`qm.<cid>.hb.<node_id>`）。
    subject_heartbeat: String,
    /// 订阅端实际用的 subject。
    subject_heartbeat_all: String,
    /// 两个 subject 的 token 数（NATS 通配符匹配的前提是段数一致）。
    subject_tokens_pub: usize,
    subject_tokens_sub: usize,
    /// 修复前 subject 规划下，心跳到底投没投到订阅端（F1 的实测结论）。
    legacy_subscription_delivered: Option<bool>,
    /// legacy 模式的成员发现结论（`true` 表示修复前的 subject 规划**能**发现成员）。
    legacy_discovery_ok: Option<bool>,
    /// legacy 模式的结论说明。
    legacy_verdict: String,
    /// 窗口参数档位名（`fast` / `production(default.toml)`）。
    profile_name: String,
    /// 本次运行的窗口口径：`判死窗口 Ns / 宽限 Ms`。
    profile: String,
    /// 复现用的临时配置是否通过产品自己的启动期校验（B1 的防线）。
    cfg_validation_passed: Option<bool>,
    /// 该配置被 `validate()` 拒绝的原因；空数组表示通过。
    cfg_validation_errors: Vec<String>,
    /// 该配置能否经 `config/default.toml` + `QM_*` 环境变量到达（B1 的关键问题）。
    config_reachable_via_defaults_and_env: Option<bool>,
    /// 该配置能否经真实加载路径 `load_from`（含加载期校验）读出。
    config_loadable: Option<bool>,
    /// 反向对照的结论说明：旧配置必须被拒绝，否则防线是空转的。
    config_validation_note: String,
    /// 成员发现：三个节点互相看见的节点数（验收标准 1 的前提）。
    discovered_nodes: usize,
    /// 各节点各自的成员视图（用来判断是「没发现」还是「视图不一致」）。
    node_views: Vec<usize>,
    /// 成员视图收敛耗时（毫秒）。
    converge_ms: u64,
    /// 每次落点的耗时（毫秒）。
    place_ms: Vec<u64>,
    /// 落点返回的承载节点 id（验收标准 1：会议落到某个节点）。
    placed_on: Vec<String>,
    /// 落点失败次数。
    place_failed: usize,
    /// 落点是否真的分散到多个节点（调度确实生效）。
    placement_spread: bool,
    /// NATS 重启后是否自动重连（验收标准 2）。
    reconnect_ok: Option<bool>,
    /// NATS 重启到恢复的耗时（毫秒）。
    reconnect_ms: u64,
    /// 恢复后 async-nats 报告的连接建立次数（首次 1 → 恢复后应为 2）。
    nats_connects_after: u64,
    /// 恢复后落点是否仍可用（不只是「连接活了」）。
    place_after_reconnect_ok: bool,
    /// 恢复后成员视图是否重新收敛。
    view_reconverged: bool,
    /// 故障迁移是否成功（验收标准 3）。
    failover_ok: Option<bool>,
    /// 判死窗口（毫秒）= `heartbeat_secs × unhealthy_misses`。
    failover_window_ms: u64,
    /// 迁移时限（毫秒）= `failover_target_secs`，从**判死时刻**起算。
    failover_deadline_ms: u64,
    /// 从杀进程到判死（Dead 第一次出现在视图里）的耗时（毫秒）。
    failover_detect_ms: u64,
    /// **SLO 口径**：从判死时刻到房间归属变更的耗时（毫秒）。
    /// `failover_target_secs` 约束的就是这个区间，而不是 `failover_ms_from_kill`。
    failover_ms: u64,
    /// **端到端口径**：从杀进程到房间归属变更的耗时（毫秒），
    /// 天然包含判死窗口本身。两个口径都报，避免「10s」被读成杀进程后 10s。
    failover_ms_from_kill: u64,
    /// SLO 是否达标（`failover_ms <= failover_deadline_ms`）。
    failover_within_target: bool,
    /// 本档配置的**端到端最坏值** = 判死窗口 + 迁移时限。
    /// 「节点宕机 10s 内迁移」按生产默认参数就是这个 20s，不是判死后的 10s。
    failover_end_to_end_worst_ms: u64,
    /// 迁移目标节点。
    failover_target: String,
    /// 故障节点摘除后集群视图的节点数（F7：节点数必须能回落）。
    nodes_after_prune: usize,
    /// 判死后进入 Dead 状态的节点数（宽限期内仍在视图里，摘除要等 grace）。
    dead_nodes_detected: usize,
    /// 判死节点是否已经打上 `dead_since_ms`（摘除计时的起点）。
    has_dead_since_ms: bool,
    /// 累计摘除的僵尸节点数。
    pruned_dead_nodes: u64,
    /// 故障节点是否被判定为 Dead（`Registry::reap_dead` 的产出）。
    victim_reaped: bool,
    /// 故障节点是否已从集群视图摘除（F7：`prune_dead` 的产出）。
    victim_pruned: bool,
    /// 故障迁移重试次数（F5：`failover_target_secs` 是否真的驱动重试）。
    failover_retries: u64,
    /// 迁移尝试 / 完成次数（F7：迁移路径真的走过）。
    migrations_attempted: u64,
    migrations_completed: u64,
    /// 压测：批量落点总数 / 成功数。
    load_total: usize,
    load_ok: usize,
    /// 压测：批量落点总耗时（毫秒）。
    load_ms: u64,
    /// 压测期间集群是否出现非预期错误。
    load_errors: Vec<String>,
    /// 非断言性的观察记录（耗时统计、配置口径说明等），不表示失败。
    load_notes: Vec<String>,
}

/// 启动一个 NATS server 子进程，端口用 `-p 0` 让 server 自选空闲端口。
///
/// 返回 `(child, port)`。**日志读 stderr 而不是 stdout**：NATS 2.10.21
/// 把 `Listening for client connections on 127.0.0.1:<port>` 打在 **stderr**
/// 上（stdout 永远是空的）。第一次实现读 stdout 读不到端口，导致 `spawn_nats`
/// 在 15s 后返回 Err，进程带着「解析不到端口」的错误退出，同时把刚起的
/// NATS 留成孤儿 —— 现场看起来就像「卡住 24 分钟没有输出」。
///
/// 解析成功就立刻退出线程（端口确定），日志尾巴交给 1MB `stderr_cap`，
/// 避免 128KB 缓冲区填满把 server 卡住。解析失败的完整日志作为错误信息带回来。
fn spawn_nats(binary: &str) -> Result<(std::process::Child, u16), String> {
    use std::io::Read;
    let mut child = Command::new(binary)
        .args(["-a", "127.0.0.1", "-p", "0", "-m", "0"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("无法启动 NATS server [{binary}]: {e}"))?;

    // stderr 要先 `take` 出来再交给子线程：否则闭包借用 `child`，
    // 函数末尾又要把 `child` move 出去，所有权冲突。
    let stderr = child.stderr.take();
    let cap = 1u64 << 20;
    let port = std::thread::spawn(move || -> (Option<u16>, String) {
        let Some(mut pipe) = stderr else { return (None, String::new()); };
        let mut out = String::new();
        let mut buf = [0u8; 4096];
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            match pipe.read(&mut buf) {
                Ok(0) => break,
                Ok(k) => {
                    out.push_str(&String::from_utf8_lossy(&buf[..k]));
                    if let Some(p) = parse_port(&out) {
                        return (Some(p), out);
                    }
                    if (out.len() as u64) >= cap {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        (None, out)
    })
    .join()
    .unwrap_or((None, String::new()));

    match port.0 {
        Some(p) => Ok((child, p)),
        None => Err(format!(
            "20s 内没从 NATS 日志解析到监听端口；日志开头：{}",
            port.1.lines().take(12).collect::<Vec<_>>().join(" / ")
        )),
    }
}

/// 从 NATS 启动日志里解析监听端口。
fn parse_port(log: &str) -> Option<u16> {
    for line in log.lines() {
        let l = line.trim();
        // Windows 上的原生格式：`Listening for client connections on 127.0.0.1:4222`。
        if let Some(rest) = l.split_once("on ") {
            let addr = rest.1.trim();
            if let Some(port) = addr.rsplit_once(':') {
                if let Ok(p) = port.1.trim().parse::<u16>() {
                    return Some(p);
                }
            }
        }
        if let Some(rest) = l.split_once("Listening:") {
            if let Some(port) = rest.1.trim().rsplit_once(':') {
                if let Ok(p) = port.1.trim().parse::<u16>() {
                    return Some(p);
                }
            }
        }
        if let Some(rest) = l.split_once("port: ") {
            if let Ok(p) = rest.1.trim().parse::<u16>() {
                return Some(p);
            }
        }
    }
    None
}

/// 阶段日志：`println!` 后立刻 flush，避免 stdout 被管道/文件吞掉导致
/// 卡死时看不到停在哪一步（stdout 非 tty 时 Rust 会整块缓冲）。
/// 同时打 Unix 秒，方便把日志与 kill/重连时刻对齐。
fn stage(msg: &str) {
    use std::io::Write;
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("[repro t={ts}s] {msg}");
    let _ = std::io::stdout().flush();
}

/// 构造一个节点配置（同机 127.0.0.1 部署，node_id 各不相同）。
///
/// 窗口参数由 [`Profile`] 决定；`request_timeout_secs` 恒为 1，
/// 因为校验契约是**严格小于** `heartbeat_secs`。
pub fn node_cfg(
    i: usize,
    cluster_id: &str,
    port: u16,
    nats_port: u16,
    profile: Profile,
) -> ClusterConfig {
    ClusterConfig {
        server: "127.0.0.1".to_string(),
        port: nats_port,
        cluster_id: cluster_id.to_string(),
        node_id: format!("n{i}"),
        advertised_addr: format!("127.0.0.1:{port}"),
        node_role: NodeRole::Full,
        heartbeat_secs: profile.heartbeat_secs(),
        // 判死窗口 = `heartbeat_secs × unhealthy_misses`。
        //
        // 为什么必须明显大于一个心跳周期：健康检查是**异步轮次**（每 `heartbeat_secs`
        // 一次），而心跳也是每 `heartbeat_secs` 一次。两者周期接近时，一条刚发的心跳
        // 可能还没被接收端处理，健康检查就先跑了 —— 于是「刚发过心跳的节点」看起来
        // 像「一次都没心跳」。窗口取 2s 配 1s 心跳时，这种竞态在真实运行里会周期性
        // 地把健康节点误判为 Dead（三次实测都出现了 `n0/n1/n2 同时 Dead、2s 后自己
        // 复活` 的抖动）。`Fast` 取 6s、`Production` 取 10s，心跳始终新鲜，误判消失。
        // 这同时说明：窗口必须明显大于「心跳周期 + 一次轮次延迟」，否则判死是竞态的。
        unhealthy_misses: profile.unhealthy_misses(),
        // 摘除宽限 = 判死窗口 + failover_target_secs。`Fast` 取 1s 让宽限 = 7s，
        // 迁移完成与僵尸摘除落在同一个短窗口内；`Production` 与 default.toml 一致。
        failover_target_secs: profile.failover_target_secs(),
        join_target_secs: 30,
        request_timeout_secs: 1,
        max_rooms_per_node: 64,
        listener_capacity: 20_000,
        listener_fanout: 200,
        // 探针端口不为 0：`AppConfig::validate` 明确拒绝 0（写 0 会让 docker
        // healthcheck 没有端点可探）。复现不监听这个端口，但配置必须合法；
        // 18100 + i 避开默认的 8080（媒体）/ 8081（信令）/ 8090（默认探针）。
        health_port: 18_100 + i as u16,
    }
}

/// 临时配置目录：`load_from` 要读到的那份 `default.toml` 写在仓库根，
/// 避免污染工作树（`repo_default_config_passes_cluster_validation` 读的就是它）。
fn config_dir_for_repro() -> String {
    let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .to_string_lossy()
        .to_string();
    std::path::Path::new(&repo_root)
        .join("target")
        .join("qm024-repro-config")
        .to_string_lossy()
        .to_string()
}

/// 把一份节点配置渲染成合法的 `AppConfig`：补上校验层要看的其余段，
/// 再把 `cluster` 段整体换成这份配置。
fn app_cfg_for(cluster: &ClusterConfig) -> qm_common::AppConfig {
    qm_common::AppConfig {
        media: qm_common::config::MediaConfig::default(),
        network: qm_common::config::NetworkConfig::default(),
        logging: qm_common::config::LoggingConfig::default(),
        storage: qm_common::config::StorageConfig::default(),
        ai: qm_common::config::AiConfig::default(),
        cluster: cluster.clone(),
    }
}

/// 把一份 `AppConfig` 渲染成可直接被 `load_from` 读到的 `default.toml` 内容。
/// 用于两条路径的对照：新配置应当读得出来，旧配置应当被拒。
fn render_toml(app: &qm_common::AppConfig) -> String {
    let c = &app.cluster;
    format!(
        "[media]\nport = {port}\nsignaling_port = {signal}\nmax_participants = {mp}\nidle_timeout_secs = {idle}\n\n\
         [network]\ncidrs = {cidrs}\nbind_host = \"{bind}\"\n\n\
         [logging]\nlevel = \"{lvl}\"\nfile_dir = \"\"\njson = {json}\n\n\
         [storage]\ndata_dir = \"{dd}\"\nencrypted = {enc}\n\n\
         [ai]\nenabled = {on}\nbase_url = \"{url}\"\ntimeout_ms = {tm}\n\n\
         [cluster]\nserver = \"{srv}\"\nport = {nport}\ncluster_id = \"{cid}\"\nnode_id = \"{nid}\"\n\
         advertised_addr = \"{adv}\"\nnode_role = \"{role}\"\nheartbeat_secs = {hb}\nunhealthy_misses = {um}\n\
         failover_target_secs = {fo}\njoin_target_secs = {jt}\nrequest_timeout_secs = {rt}\n\
         max_rooms_per_node = {mr}\nlistener_capacity = {lc}\nlistener_fanout = {lf}\nhealth_port = {hp}\n",
        port = app.media.port,
        signal = app.media.signaling_port,
        mp = app.media.max_participants,
        idle = app.media.idle_timeout_secs,
        cidrs = serde_json::to_string(&app.network.cidrs).unwrap_or_else(|_| "[]".to_string()),
        bind = app.network.bind_host,
        lvl = app.logging.level,
        json = app.logging.json,
        dd = app.storage.data_dir,
        enc = app.storage.encrypted,
        on = app.ai.enabled,
        url = app.ai.base_url,
        tm = app.ai.timeout_ms,
        srv = c.server,
        nport = c.port,
        cid = c.cluster_id,
        nid = c.node_id,
        adv = c.advertised_addr,
        role = c.node_role.as_str(),
        hb = c.heartbeat_secs,
        um = c.unhealthy_misses,
        fo = c.failover_target_secs,
        jt = c.join_target_secs,
        rt = c.request_timeout_secs,
        mr = c.max_rooms_per_node,
        lc = c.listener_capacity,
        lf = c.listener_fanout,
        hp = c.health_port,
    )
}

/// 写一份 `default.toml` 并用**真实加载路径**读回（figment 解析 + 加载期校验）。
fn load_rendered(toml: &str) -> Result<qm_common::AppConfig, String> {
    let dir = config_dir_for_repro();
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    std::fs::write(
        std::path::Path::new(&dir).join("default.toml"),
        toml.as_bytes(),
    )
    .map_err(|e| e.to_string())?;
    qm_common::config::load_from(&dir).map(|(cfg, _)| cfg).map_err(|e| e.to_string())
}

/// B1 的防线：复现用的配置必须能通过产品**自己**的校验层，
/// 并且必须能经 `default.toml` 这条真实加载路径到达。
///
/// 上一版把这份配置直接写成结构体字面量，绕过了整个校验层 —— 于是验收证据
/// 是在一个「按产品自身规范非法、生产环境到不了」的配置下产生的。
/// 这里同时做**反向对照**：旧配置必须被拒，否则这条防线是空转的。
fn verify_config_reachability(cluster: &ClusterConfig) -> (Vec<String>, bool, bool, String) {
    let app = app_cfg_for(cluster);
    let mut errors = match app.validate() {
        Ok(()) => Vec::new(),
        Err(e) => vec![e.to_string()],
    };
    let validate_ok = errors.is_empty();
    let loaded = match load_rendered(&render_toml(&app)) {
        Ok(cfg) => Some(cfg),
        Err(e) => {
            errors.push(format!("load_from 拒绝：{e}"));
            None
        }
    };
    // 反证：同一份配置把 `request_timeout_secs` 抬到等于 `heartbeat_secs`、
    // `health_port` 写 0，必须被拒。
    let mut legacy = app.clone();
    legacy.cluster.request_timeout_secs = legacy.cluster.heartbeat_secs;
    legacy.cluster.health_port = 0;
    let legacy_err = match load_rendered(&render_toml(&legacy)) {
        Err(e) => e,
        Ok(_) => "未被拒绝（防线失效）".to_string(),
    };
    (errors, validate_ok, loaded.is_some(), legacy_err)
}

/// 等端口可连（TCP 探测）。
async fn wait_tcp(addr: std::net::SocketAddr, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// 成员发现：等三个节点的视图一致且都 >= 期望成员数，返回 (节点数, 耗时)。
///
/// 断言里可能还挂着一台**已死**的节点（比如受害者被摘除前），所以判定用
/// 「不小于期望数且各节点一致」，不用「等于」。
async fn wait_discovery(
    nodes: &[Arc<Cluster>],
    expected: usize,
    budget: Duration,
) -> (usize, Vec<usize>, u64) {
    let start = Instant::now();
    let check = |views: &[usize]| {
        views.iter().all(|v| *v >= expected)
            && views.iter().copied().max() == views.iter().copied().min()
    };
    loop {
        let views: Vec<usize> = nodes.iter().map(|n| n.node_count()).collect();
        if check(&views) {
            return (
                *views.iter().max().unwrap_or(&0),
                views,
                start.elapsed().as_millis() as u64,
            );
        }
        if start.elapsed() >= budget {
            return (
                *views.iter().max().unwrap_or(&0),
                views,
                start.elapsed().as_millis() as u64,
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 批量落点，记录耗时与承载节点。
async fn run_placements(node: &Cluster, count: usize) -> (Vec<u64>, Vec<String>, usize) {
    let mut ms = Vec::new();
    let mut on = Vec::new();
    let mut failed = 0usize;
    for k in 0..count {
        let id = format!("meeting-{k:03}");
        let t = Instant::now();
        match node.place_room(&id, 1, 0).await {
            Ok(Some(room)) => {
                ms.push(t.elapsed().as_millis() as u64);
                on.push(room.owner.clone().unwrap_or_default());
            }
            _ => failed += 1,
        }
    }
    (ms, on, failed)
}

/// NATS 重启后验证自动重连（验收标准 2）。
async fn verify_reconnect(nodes: &[Arc<Cluster>], report: &mut Report, budget: Duration) {
    let target = nodes[0].clone();
    let before = target.status_snapshot().nats_connects;
    let start = Instant::now();
    while start.elapsed() < budget {
        let st = target.status_snapshot();
        if st.nats_connected && st.nats_connects != before && st.rejoin_count >= 1 {
            report.reconnect_ok = Some(true);
            report.reconnect_ms = start.elapsed().as_millis() as u64;
            report.nats_connects_after = st.nats_connects;
            // 恢复后再落一个房间：确认不只是「连接活了」，而是「能干活」。
            report.place_after_reconnect_ok =
                target.place_room("after-reconnect", 1, 0).await.is_ok();
            report.failover_retries = st.failover_retries;
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    report.reconnect_ok = Some(false);
    report.reconnect_ms = start.elapsed().as_millis() as u64;
    let st = target.status_snapshot();
    report.nats_connects_after = st.nats_connects;
    report.failover_retries = st.failover_retries;
}

/// 等成员视图在重连后重新收敛（`view_converged` 在重连时被清零，收到第一条
/// 对端心跳才置回 true）。不能只看一个瞬间：检查点若落在两条心跳之间就是假阴性。
async fn wait_view_reconverge(nodes: &[Arc<Cluster>], report: &mut Report) {
    let deadline = Instant::now() + Duration::from_secs(6);
    while Instant::now() < deadline {
        let views: Vec<usize> = nodes.iter().map(|n| n.node_count()).collect();
        let all_converged = nodes.iter().all(|n| n.is_view_converged());
        let all_full = views.iter().all(|v| *v >= nodes.len());
        if all_converged && all_full {
            report.view_reconverged = true;
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 用「伪造心跳发布」判断一个订阅 subject 到底能不能收到心跳。
///
/// 这是 F1 的裁决依据：不靠推理 subject 段数，而是让 NATS server 自己投一次。
/// 用探针节点自己的 bus 订阅，用另一个临时 bus 从 `hb.<node_id>` 发布，
/// 两个连接互不干扰。
async fn probe_subscription(
    prefix: &str,
    probe_node: &Cluster,
    subject_all: &str,
) -> Option<bool> {
    let cfg = probe_node.cfg();
    let probe_bus = NatsBus::connect(cfg, 128).await.ok()?;
    let mut sub = probe_bus.subscribe(subject_all).await.ok()?;
    // 从受害者 subject 上发一条真实心跳，看订阅端收不收得到。
    // 发送方身份与被发布 subject 的尾段保持一致（`hb.n0` ↔ node_id n0）。
    let probe_sender_cfg = ClusterConfig {
        node_id: "n0".to_string(),
        ..cfg.clone()
    };
    let hb = qm_cluster::HeartbeatMsg::from_node(&probe_sender_cfg, 1);
    let hb_subject = bus::subject_heartbeat(prefix, "n0");
    let sender = NatsBus::connect(&probe_sender_cfg, 128).await.ok()?;
    sender.publish_json(&hb_subject, &hb).await.ok()?;
    tokio::time::timeout(Duration::from_secs(3), sub.next()).await.is_ok().then_some(true)
}

/// 10s 故障迁移（验收标准 3）。
///
/// 用**独立的子进程**扮演故障节点，然后真的把它杀干净。
///
/// 为什么必须是独立进程：NATS 会一直保活空闲的 TCP 连接，所以「不发心跳」
/// 不等于「节点死了」—— 但 `Registry::reap_dead` 判死的依据恰恰是心跳停更，
/// 因此必须让那条发心跳的心跳循环真的消失。受害者做成同一进程里的一个连接时，
/// 杀连接杀不掉进程，async-nats 会自动重连并在判死窗口（1s × 2 = 2s）内把
/// 节点复活成 Live，`reap_dead` 永远等不到时机。真实故障是「那台机器没了」，
/// 所以这里起一个单独的可执行文件当受害者：它自己连 NATS、自己发心跳、
/// 自己在集群里建房间；本工具要验证故障时直接 `kill` 掉它。
///
/// 判死之后的迁移路径完全是生产代码：`health_check` → `reap_dead` →
/// `try_migrate` → 定向 request/reply → 目标节点 `handle_migration_request`
/// → 改归属 + 广播。本工具只负责造出「节点消失」这个故障本身。
///
/// 受害者进程通过 `QM_REPRO_ROOM` 环境变量拿到唯一房间 id（同一 cluster_id 跨 run
/// 复用，房间 id 必须每次不同，否则会被上一轮迁走之后的归属卡住）。
async fn verify_failover(
    nodes: &[Arc<Cluster>],
    reporter: Arc<Cluster>,
    report: &mut Report,
    budget: Duration,
    victim_binary: &str,
    nats_port: u16,
) {
    let victim_id = "victim";
    let room_id = format!("victim-room-{}-{}", std::process::id(), unix_ms());

    // 清掉本进程遗留的受害者房间：`remove_room` 只删本节点归属的房间，所以
    // 遍历「本节点归属且 id 前缀匹配」的条目是安全的。
    if let Some(bus) = reporter.bus_ref() {
        for r in reporter.rooms_snapshot() {
            if r.id.starts_with("victim-room-") {
                let _ = reporter.remove_room(&r.id, &bus).await;
            }
        }
    }

    // 起受害者子进程：同机、同一个 NATS、独立 node_id。
    let victim_cfg_snapshot = reporter.cfg().clone();
    let mut child = match Command::new(victim_binary)
        .args(["--repro-victim"])
        .env("QM_REPRO_CLUSTER", &victim_cfg_snapshot.cluster_id)
        .env("QM_REPRO_NODE_ID", victim_id)
        .env("QM_REPRO_NATS_PORT", nats_port.to_string())
        .env("QM_REPRO_ROOM", &room_id)
        // 与对端节点同一份窗口口径：受害者与存活节点必须同参数，
        // 否则「多久判死」不是对端视图的口径，迁移数字就失去意义。
        // 直接沿用对端节点已加载的配置值，不重复声明，避免两档漂移。
        .env("QM_REPRO_HEARTBEAT_SECS", victim_cfg_snapshot.heartbeat_secs.to_string())
        .env(
            "QM_REPRO_UNHEALTHY_MISSES",
            victim_cfg_snapshot.unhealthy_misses.to_string(),
        )
        .env(
            "QM_REPRO_FAILOVER_TARGET_SECS",
            victim_cfg_snapshot.failover_target_secs.to_string(),
        )
        .env(
            "QM_REPRO_REQUEST_TIMEOUT_SECS",
            victim_cfg_snapshot.request_timeout_secs.to_string(),
        )
        .env("QM_REPRO_HEALTH_PORT", "18100")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            report.failover_ok = Some(false);
            report.load_errors.push(format!("无法启动故障节点子进程：{e}"));
            return;
        }
    };

    // 1) 等受害者成为合法成员（它自己会发心跳）。
    let mut seen = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline && !seen {
        seen = reporter.node_count() >= nodes.len() + 1;
        if !seen {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    if !seen {
        report.failover_ok = Some(false);
        report.load_errors.push("10s 内集群未发现故障节点（受害者无法成为成员）".to_string());
        child.kill().ok();
        child.wait().ok();
        return;
    }

    // 2) 等受害者把它的房间广播进集群视图。
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if reporter.room_owner(&room_id) == Some(Some(victim_id.to_string())) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if reporter.room_owner(&room_id) != Some(Some(victim_id.to_string())) {
        report.failover_ok = Some(false);
        report.load_errors.push("10s 内集群视图没收敛到故障节点的房间归属".to_string());
        child.kill().ok();
        child.wait().ok();
        return;
    }
    stage(&format!(
        "故障节点（独立进程 pid={}）已持有房间 {}，节点视图={} 个",
        child.id(),
        room_id,
        reporter.node_count()
    ));

    // 3) 真正杀掉故障节点：进程消失，心跳与所有请求都不会再回来。
    let kill_ms = unix_ms();
    child.kill().ok();
    // 判死窗口（毫秒）：`heartbeat_secs × unhealthy_misses`，与对端节点同口径。
    let window_ms = reporter
        .cfg()
        .heartbeat_secs
        .saturating_mul(reporter.cfg().unhealthy_misses.max(1))
        .saturating_mul(1000);
    stage(&format!("已杀掉故障节点子进程（t0），判死窗口 {}ms", window_ms));

    // 判死时刻（t_dead）：本节点视图里受害者第一次进入 `Dead`。
    // 这是「迁移时限」的正确起点 —— 杀进程到判死之间还有整整一个判死窗口，
    // 把那段算进 `failover_ms` 会把 SLO 起点说成杀进程，而不是判死。
    let mut dead_ms: Option<u64> = None;
    let detect_budget_ms = window_ms.saturating_mul(2).max(4000);
    let detect_end = kill_ms.saturating_add(detect_budget_ms);
    while unix_ms() <= detect_end {
        for n in nodes {
            let _ = n.health_check().await;
        }
        let dead = reporter
            .nodes()
            .iter()
            .any(|n| n.node_id == victim_id && n.is_dead());
        if dead {
            dead_ms = Some(unix_ms());
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    if dead_ms.is_none() {
        report.failover_ok = Some(false);
        report.load_errors.push(format!(
            "{}ms 内受害者未被判定为 Dead（判死窗口 {}ms）",
            unix_ms().saturating_sub(kill_ms),
            window_ms
        ));
    }

    // 迁移时限：从**判死时刻**起算 `failover_target_secs`，而不是从杀进程起算。
    let failover_ms = reporter
        .cfg()
        .failover_target_secs
        .saturating_mul(1000)
        .max(1);
    let deadline_ms = dead_ms.unwrap_or(kill_ms).saturating_add(failover_ms);
    stage(&format!(
        "迁移 SLO：判死后 {}ms 内完成（判死发生在 t0+{}ms）",
        failover_ms,
        dead_ms.map(|d| d.saturating_sub(kill_ms)).unwrap_or(0)
    ));

    let mut ok = false;
    let mut diag_at = Instant::now();
    while unix_ms() <= deadline_ms {
        // 显式驱动判死轮次：`health_loop` 也会跑，但这里保证计时从 kill 那一刻起算。
        for n in nodes {
            let _ = n.health_check().await;
        }
        // 判读依据是**集群视图**里的归属。`rooms_snapshot` 只含本节点归属的房间，
        // 受害者归属的房间在它里面永远不存在 —— 用它会得到恒 false 的假阴性。
        if reporter.room_owner(&room_id) != Some(Some(victim_id.to_string())) {
            ok = true;
            break;
        }
        // 故障诊断：受害者到底是 Dead 了又被心跳复活，还是根本没被判死。
        // 迁移失败必须能区分这两种情况 —— 前者是判死逻辑的竞态，后者是心跳还在流。
        if diag_at.elapsed() >= Duration::from_millis(700) {
            diag_at = Instant::now();
            let now = unix_ms();
            let room_now = reporter
                .room_owner(&room_id)
                .flatten()
                .unwrap_or_default();
            // 故障诊断：受害者到底是 Dead 了又被心跳复活，还是根本没被判死。
            // 迁移失败必须能区分这两种情况 —— 前者是判死逻辑的竞态，后者是心跳还在流。
            let detail = reporter
                .nodes()
                .iter()
                .map(|n| {
                    format!(
                        "{}:{:?}/seen={}ms",
                        n.node_id,
                        n.status,
                        now.saturating_sub(n.last_seen_ms)
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            println!(
                "  [diag +{}ms] 归属={room_now}；{detail}",
                now.saturating_sub(kill_ms)
            );
            let _ = std::io::stdout().flush();
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    let now_ms = unix_ms();
    report.failover_ok = Some(ok);
    // 口径 A（**SLO 口径**）：从判死时刻起算。这才是 `failover_target_secs` 约束的区间。
    report.failover_ms = now_ms.saturating_sub(dead_ms.unwrap_or(kill_ms));
    // 口径 B（端到端口径）：从杀进程起算，包含判死窗口本身。
    report.failover_ms_from_kill = now_ms.saturating_sub(kill_ms);
    report.failover_detect_ms = dead_ms.map(|d| d.saturating_sub(kill_ms)).unwrap_or(0);
    report.failover_window_ms = window_ms;
    report.failover_deadline_ms = failover_ms;
    report.failover_within_target = ok && report.failover_ms <= failover_ms;
    report.failover_target = reporter.room_owner(&room_id).flatten().unwrap_or_default();

    // 跨节点一致性：三个节点的视图都必须收敛到「不再是受害者归属」。
    if ok {
        let reconverge_deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < reconverge_deadline {
            let all = nodes
                .iter()
                .all(|n| n.room_owner(&room_id) != Some(Some(victim_id.to_string())));
            if all {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // 4) F7 摘除阶段：判死后节点进入 Dead，grace（判死窗口 + failover_target_secs）
    //    到期后 `prune_dead` 才把它从视图里拿掉。这一步是独立的结果，
    //    单独计时，避免把「迁移成功」误读成「僵尸节点已摘除」。
    //
    // 时限取 `grace + 判死窗口` 的 2 倍余量：摘除起点是判死时刻（`dead_since_ms`），
    // 但 `prune_dead` 只在本轮 `health_check` 里跑，最长还要等一个轮次。
    let grace_ms = window_ms.saturating_add(failover_ms);
    let prune_deadline = Instant::now() + Duration::from_millis(grace_ms.saturating_mul(2).max(10_000));
    let mut reaped = false;
    while Instant::now() < prune_deadline {
        for n in nodes {
            let _ = n.health_check().await;
        }
        let dead = reporter
            .nodes()
            .iter()
            .filter(|n| n.node_id == victim_id && n.is_dead())
            .count();
        let gone = reporter.nodes().iter().all(|n| n.node_id != victim_id);
        if dead > 0 {
            reaped = true;
        }
        if gone {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    report.dead_nodes_detected = reporter
        .nodes()
        .iter()
        .filter(|n| n.is_dead())
        .count();
    report.has_dead_since_ms = reporter
        .nodes()
        .iter()
        .any(|n| n.node_id == victim_id && n.dead_since_ms.is_some());
    report.nodes_after_prune = reporter.node_count();
    report.victim_reaped = reaped;
    report.victim_pruned = reporter.nodes().iter().all(|n| n.node_id != victim_id);
    let st = reporter.status_snapshot();
    report.pruned_dead_nodes = st.pruned_dead_nodes;
    report.failover_retries = st.failover_retries;
    report.migrations_attempted = st.migrations_attempted;
    report.migrations_completed = st.migrations_completed;
    report.load_notes.push(format!(
        "迁移 SLO（判死→归属变更）={}ms / 时限 {}ms，达标={}",
        report.failover_ms,
        report.failover_deadline_ms,
        report.failover_within_target
    ));
    report.load_notes.push(format!(
        "端到端（杀进程→归属变更）={}ms，其中判死 {}ms + 迁移 {}ms",
        report.failover_ms_from_kill,
        report.failover_detect_ms,
        report.failover_ms
    ));
    report.load_notes.push(format!(
        "迁移前归属={victim_id}，迁移后归属={}，端到端 {}ms（{}ms 预算内={}）",
        report.failover_target,
        report.failover_ms_from_kill,
        budget.as_millis() as u64,
        report.failover_ms_from_kill <= (budget.as_millis() as u64)
    ));
    report.load_notes.push(format!(
        "受害者判死={reaped}，已从集群视图摘除={}（剩余 {} 节点，宽限 {}ms）",
        report.victim_pruned,
        report.nodes_after_prune,
        grace_ms
    ));
}

/// 当前 Unix 毫秒（避免引 qm_cluster 内部时间函数）。
fn unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// 受害者子进程入口：自己连 NATS、自己发心跳、自己在集群里建一个房间，
/// 然后挂住直到被 `kill`。
///
/// 单独进程才能真实模拟「那台机器没了」：把受害者做成同一个进程里的一个
/// 连接时，杀连接杀不掉进程，async-nats 会自动重连并在判死窗口内把节点
/// 复活成 Live。
pub async fn run_victim() -> Result<(), String> {
    let env = |k: &str| -> Result<String, String> {
        std::env::var(k).map_err(|_| format!("缺少环境变量 {k}"))
    };
    let env_u64 = |k: &str, default: u64| -> u64 {
        std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
    };

    let cluster_id = env("QM_REPRO_CLUSTER")?;
    let node_id = env("QM_REPRO_NODE_ID")?;
    let nats_port: u16 = match std::env::var("QM_REPRO_NATS_PORT") {
        Ok(v) => match v.parse::<u16>() {
            Ok(p) => p,
            Err(e) => return Err(format!("QM_REPRO_NATS_PORT 解析失败：{e}")),
        },
        Err(e) => return Err(format!("QM_REPRO_NATS_PORT 缺失：{e}")),
    };
    let room_id = env("QM_REPRO_ROOM")?;

    let cfg = ClusterConfig {
        server: "127.0.0.1".to_string(),
        port: nats_port,
        cluster_id,
        node_id,
        advertised_addr: "127.0.0.1:8199".to_string(),
        node_role: NodeRole::Full,
        // 与对端节点同窗口：受害者必须用同样的判死参数，否则「多久判死」
        // 不是对端节点的视图口径，验收数字就失去意义。
        heartbeat_secs: env_u64("QM_REPRO_HEARTBEAT_SECS", 2).max(1),
        unhealthy_misses: env_u64("QM_REPRO_UNHEALTHY_MISSES", 3).max(1),
        failover_target_secs: env_u64("QM_REPRO_FAILOVER_TARGET_SECS", 10),
        join_target_secs: 30,
        request_timeout_secs: env_u64("QM_REPRO_REQUEST_TIMEOUT_SECS", 1).max(1),
        max_rooms_per_node: 64,
        listener_capacity: 20_000,
        listener_fanout: 200,
        // 探针端口不为 0（`AppConfig::validate` 明确拒绝 0）。受害者不监听它，
        // 但配置必须与对端一样合法，不能靠绕过校验层来跑。
        health_port: env_u64("QM_REPRO_HEALTH_PORT", 18_100).clamp(1, 65535) as u16,
    };
    // 显式走产品自己的校验：受害者如果对端节点用不了这份配置，就不该被用来
    // 当「真实故障节点」。`127.0.0.0/8` 是本机回环组网的允许段。
    cfg.validate(&[Cidr::parse("127.0.0.0/8").expect("127.0.0.0/8 是合法 CIDR")])
        .map_err(|e| format!("受害者配置未通过 validate()：{e}"))?;

    let victim = Arc::new(Cluster::new(cfg));
    victim.clone().initialize().await;

    // 等 NATS 连上再建房间（`create_room` 需要一条活的 NATS 句柄）。
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && !victim.status_snapshot().nats_connected {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    if !victim.status_snapshot().nats_connected {
        return Err("30s 内没有连上 NATS".to_string());
    }
    if let Err(e) = victim.create_room(&room_id, 1, 0).await {
        return Err(format!("受害者无法建立自己的房间：{e}"));
    }
    eprintln!("repro-victim 已加入集群并持有房间 {room_id}");

    // 挂住：心跳循环会一直把本节点标记为 Live，直到进程被 kill。
    loop {
        tokio::time::sleep(Duration::from_secs(3600)).await;
    }
}

/// 入口：跑完整套复现并打印 `REPRO-RESULT:` JSON。
pub async fn run(_config_dir: &str, nats_binary: &str, mode: Mode, profile: Profile) -> Result<(), String> {
    let cluster_id = format!("qm024-{}", std::process::id());
    let prefix = bus::subject_prefix(&cluster_id);

    stage(&format!("启动 NATS：{nats_binary}"));
    let (mut nats, nats_port) = spawn_nats(nats_binary)?;
    let nats_addr: std::net::SocketAddr = format!("127.0.0.1:{nats_port}").parse().unwrap();
    stage(&format!("NATS 监听 {nats_addr}"));
    if !wait_tcp(nats_addr, Duration::from_secs(15)).await {
        nats.kill().ok();
        return Err(format!("NATS 端口 {nats_port} 在 15s 内不可连"));
    }

    let mut report = Report {
        mode: mode.as_str().to_string(),
        cluster_id: cluster_id.clone(),
        subject_heartbeat: bus::subject_heartbeat(&prefix, "n0"),
        subject_heartbeat_all: mode.heartbeat_all(&prefix),
        subject_tokens_pub: bus::subject_heartbeat(&prefix, "n0").split('.').count() as usize,
        subject_tokens_sub: mode.heartbeat_all(&prefix).split('.').count() as usize,
        ..Default::default()
    };

    // 先建三个节点但**还没** initialize —— 让 connect_loop 有机会在 NATS 未就绪时重试。
    let nodes: Vec<Arc<Cluster>> = (0..3u16)
        .map(|i| Arc::new(Cluster::new(node_cfg(i as usize, &cluster_id, 8100 + i, nats_port, profile))))
        .collect();
    let reporter = nodes[0].clone();

    // ── 前置断言 0：配置可达性（B1）──
    //
    // `Cluster::new` 不跑校验层，所以这里必须显式跑一次：复现用的配置若被
    // `AppConfig::validate` 拒绝，它就不是生产能到达的配置，验收数据也就失去
    // 意义。同时做一次反向对照，证明这条防线不是空转的。
    let profile_name = profile.as_str().to_string();
    let profile_deadline_ms = profile.deadline_ms();

    stage(&format!(
        "前置断言 0：复现配置必须通过产品自己的 validate() 且能被真实加载（档位 {profile_name}）"
    ));
    let base_cfg = nodes[0].cfg().clone();
    let window_ms = base_cfg
        .heartbeat_secs
        .saturating_mul(base_cfg.unhealthy_misses.max(1))
        .saturating_mul(1000);
    let grace_ms = window_ms.saturating_add(base_cfg.failover_target_secs.saturating_mul(1000));
    report.failover_window_ms = window_ms;
    report.failover_deadline_ms = base_cfg.failover_target_secs.saturating_mul(1000);
    report.profile_name = profile_name.clone();
    // 本档配置的**端到端最坏值**（判死窗口 + 迁移时限）：验收表里「10s」
    // 必须按这个口径解释，而不是按判死之后的 10s。
    report.failover_end_to_end_worst_ms = window_ms.saturating_add(report.failover_deadline_ms);
    report.profile = format!(
        "判死窗口 {}ms（{}s×{}）/ 迁移时限 {}ms / 摘除宽限 {}ms",
        window_ms,
        base_cfg.heartbeat_secs,
        base_cfg.unhealthy_misses,
        report.failover_deadline_ms,
        grace_ms
    );
    let (cfg_errors, cfg_ok, cfg_loaded, cfg_legacy_rejected) =
        verify_config_reachability(&base_cfg);
    report.cfg_validation_passed = Some(cfg_ok);
    report.cfg_validation_errors = cfg_errors.clone();
    report.config_reachable_via_defaults_and_env = Some(cfg_ok && cfg_loaded);
    report.config_loadable = Some(cfg_loaded);
    report.config_validation_note = format!(
        "旧配置（request_timeout_secs == heartbeat_secs, health_port = 0）经 load_from 被拒：{cfg_legacy_rejected}"
    );
    if !cfg_ok || !cfg_loaded {
        nats.kill().ok();
        nats.wait().ok();
        println!(
            "REPRO-FAIL:{}",
            serde_json::to_string(&report).unwrap()
        );
        let _ = std::io::stdout().flush();
        return Err(format!("复现配置未通过校验：{:?}", cfg_errors));
    }
    stage(&format!("配置校验通过：{}", report.profile));

    // F1 裁决：legacy 模式下实测心跳订阅能不能收到心跳。
    if mode == Mode::Legacy {
        stage("legacy 模式：起探针节点，实测 `hb.*` 的投递");
        // 起一个探针节点，让 subject 规划生效，然后测 `hb.*` 的投递行为。
        let probe_node = Arc::new(Cluster::new(node_cfg(7, &cluster_id, 8170, nats_port, profile)));
        probe_node.clone().initialize().await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        if let Some(ok) = probe_subscription(&prefix, &probe_node, &mode.heartbeat_all(&prefix))
            .await
        {
            report.legacy_subscription_delivered = Some(ok);
        }
        if let Some(bus) = probe_node.bus_ref() {
            tokio::spawn(async move { bus.shutdown().await });
        }
    }

    for n in &nodes {
        // `initialize` 按值消费 `Arc<Self>`（各后台循环各持一份），所以留一份。
        n.clone().initialize().await;
    }

    // ── 断言 1：成员发现（验收标准 1 的前提）──
    let (found, views, converge_ms) = wait_discovery(&nodes, nodes.len(), Duration::from_secs(20)).await;
    report.converge_ms = converge_ms;
    if let Some(n) = nodes.iter().map(|n| n.status_snapshot().view_converge_ms).max() {
        report.converge_ms = report.converge_ms.max(n);
    }
    stage(&format!(
        "成员发现：{found} 节点，各节点视图={views:?}，收敛 {}ms",
        report.converge_ms
    ));
    report.discovered_nodes = found;
    report.node_views = views;

    // F1 的实测结论：修复前的 subject 规划到底有没有让成员发现失效。
    if mode == Mode::Legacy {
        report.legacy_discovery_ok = Some(found >= 3);
        match (report.legacy_subscription_delivered, found >= 3) {
            (Some(true), true) => report.legacy_verdict = format!(
                "F1 未成立：`hb.*` 与 `hb.<node_id>` 都是 {tokens} 段，`*` 匹配成功，\
                 成员发现正常（{found} 节点）。`>` 属加固（对未来加段免疫），不是修 bug。",
                tokens = report.subject_tokens_sub,
            ),
            (Some(true), false) => report.legacy_verdict = format!(
                "`hb.*` 能收到心跳（投递={:?}",
                report.legacy_subscription_delivered
            ),
            (Some(false), _) => report.legacy_verdict = format!(
                "F1 成立：`hb.*` 投递不到 `{}` 上的心跳，成员发现失败（只发现 {found} 节点）。",
                report.subject_heartbeat
            ),
            (None, true) => report.legacy_verdict =
                "探针未取到结果，但成员发现成功（{found} 节点），F1 未成立。".to_string(),
            (None, false) => report.legacy_verdict =
                "探针未取到结果，成员发现失败（{found} 节点）。".to_string(),
        }
    } else {
        report.legacy_verdict =
            "post 模式：使用 `hb.>` 递归通配符，成员发现实测成立。".to_string();
    }

    // legacy 模式下成员发现不成立就到此为止：后面的断言都依赖集群视图。
    if mode == Mode::Legacy && found < 3 {
        nats.kill().ok();
        println!("REPRO-RESULT:{}", serde_json::to_string(&report).unwrap());
        let _ = std::io::stdout().flush();
        return Ok(());
    }

    // ── 断言 2：会议落点（验收标准 1）──
    stage("断言 2：6 次会议落点");
    let (ms, on, failed) = run_placements(&nodes[0], 6).await;
    report.place_ms = ms;
    report.placed_on = on;
    report.place_failed = failed;
    report.placement_spread = {
        let s: std::collections::HashSet<_> = report.placed_on.iter().collect();
        s.len() > 1
    };
    stage(&format!(
        "落点：成功 {}，失败 {}，承载节点={:?}",
        report.placed_on.len(),
        failed,
        report.placed_on
    ));

    // ── 断言 3：NATS 重启后自动重连（验收标准 2）──
    stage("断言 3：杀掉 NATS 再重启，验证自动重连");
    nats.kill().ok();
    nats.wait().ok();
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (mut nats2, port2) = spawn_nats(nats_binary)?;
    let _ = port2;
    verify_reconnect(&nodes, &mut report, Duration::from_secs(45)).await;
    wait_view_reconverge(&nodes, &mut report).await;
    stage(&format!(
        "重连结果 ok={:?} 耗时 {}ms connects={} 视图重收敛={}",
        report.reconnect_ok, report.reconnect_ms, report.nats_connects_after, report.view_reconverged
    ));

    // ── 断言 4：批量落点压测（验收标准 3 的「不能只是没崩」）──
    stage("断言 4：50 次批量落点压测");
    let load_total = 50usize;
    let t = Instant::now();
    let (lms, lon, lfailed) = run_placements(&nodes[1], load_total).await;
    report.load_total = load_total;
    report.load_ok = lon.len();
    report.load_ms = t.elapsed().as_millis() as u64;
    if lfailed > 0 {
        report.load_errors.push(format!("压测有 {lfailed} 个落点失败"));
    }
    if !lms.is_empty() {
        let total: u64 = lms.iter().sum();
        let avg = total.saturating_mul(1000) / lms.len() as u64;
        let max = *lms.iter().max().unwrap_or(&0);
        report.load_notes.push(format!(
            "单次落点平均 {avg}us，最慢 {max}ms（{}/{} 成功）",
            lms.len(),
            load_total
        ));
    }

    stage(&format!("压测：{}/{} 成功，总耗时 {}ms", report.load_ok, report.load_total, report.load_ms));

    // ── 断言 5：10s 故障迁移（验收标准 3）──
    stage(&format!(
        "断言 5：故障迁移（档位 {}，SLO = 判死后 {}ms）",
        profile_name, profile_deadline_ms
    ));
    // 受害者就是**本程序自己**的子进程（`--repro-victim`）：受害者必须能真正被
    // 杀掉，所以不能是 NATS 二进制，也不能是同一进程里的另一个连接。
    let victim_exe = std::env::current_exe()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    verify_failover(
        &nodes,
        reporter,
        &mut report,
        profile.failover_budget(),
        &victim_exe,
        nats_port,
    )
    .await;

    stage(&format!(
        "迁移结果 ok={:?} 耗时 {}ms 归属={} 判死={} 已摘除={}",
        report.failover_ok,
        report.failover_ms,
        report.failover_target,
        report.victim_reaped,
        report.victim_pruned
    ));

    nats2.kill().ok();
    nats2.wait().ok();
    let json = serde_json::to_string(&report).unwrap();
    println!("REPRO-RESULT:{}", json);
    let _ = std::io::stdout().flush();
    Ok(())
}
