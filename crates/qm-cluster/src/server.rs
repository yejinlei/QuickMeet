//! 集群模式进程入口（QM-006）+ 节点健康探针（QM-018）。
//!
//! 职责：加载并校验配置 → 构造 [`Cluster`] → 连接 NATS 加入集群 →
//! 起健康探针 → 阻塞到 Ctrl+C。
//!
//! 拆成独立模块而不是塞进 `main.rs` 有两个原因：
//! * 配置加载 + 校验可以脱离 CLI 参数单独单测（`main.rs` 里的 `#[cfg(test)]`
//!   拿到的是测试进程的环境，不适合断言文件加载）；
//! * `docker-compose.yml` 的容器命令走 `qm-demo --cluster`，
//!   入口路径只有一处，不需要在 compose 里再写一遍参数拼装。

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use http::{Method, Request, Response, StatusCode};
use qm_common::{config, logging, Error, Result as QmResult};
use tracing::{info, warn};

use crate::Cluster;

/// 集群节点健康探针默认监听端口（`QM_CLUSTER_HEALTH_PORT` 可覆盖）。
///
/// 8090 刻意避开 8080（媒体，全局约束）与 8081（信令）；三个集群节点各自
/// 占用它，docker-compose 把节点对外暴露到 8091/8092/8093。
pub const DEFAULT_HEALTH_PORT: u16 = 8090;

/// 启动集群模式。
///
/// 流程：
/// 1. 加载配置（`default.toml` → `local.json` → `QM_*` 环境变量），加载期即校验，
///    配置非法直接返回错误，不进入集群；
/// 2. 构造 [`Cluster`] 并 `initialize()`：连接 NATS、注册自身、启动心跳/调度/迁移循环；
/// 3. 起一个**独立**的健康探针 HTTP 服务（[`serve_health`]），让 `docker-compose`
///    的 `healthcheck` 有 HTTP 端点可探 —— 集群模式不监听 8081，没有别的
///    探针目标可用，这正是 QM-018 补的洞；
/// 4. 阻塞到 Ctrl+C，然后正常退出（容器收到 `docker stop` 的 SIGTERM 时同样退出）。
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
        // `initialize` 接收 `Arc<Self>` 并按值移走，所以要先把探针用的 Arc 克隆出来；
        // 两个 Arc 指向同一份集群状态，探针读到的始终是最新快照。
        let health_target = cluster.clone();
        cluster.initialize().await;
        // 探针任务在后台跑；绑定失败返回 None，节点照常入集群（见 serve_health 文档）。
        let _health_probe = serve_health(health_target, health_addr(&cfg)).await;
        let _ = tokio::signal::ctrl_c().await;
        Ok::<(), Error>(())
    })?;
    tracing::info!("收到 Ctrl+C，节点退出集群");
    Ok(())
}

/// 从配置推导探针监听地址：端口取 `cluster.health_port`（`QM_CLUSTER_HEALTH_PORT`
/// 可覆盖），绑 `network.bind_host`。
///
/// 端口为 0 不在这里兜底：`AppConfig::validate` 已经保证 `health_port != 0`，
/// 如果这里再写一个回退默认值，就出现两套「禁用探针」的语义（文档说「0 表示禁用」，
/// 代码实际绑 8090）。让配置层单一负责，`health_addr` 只做地址拼接。
pub fn health_addr(cfg: &qm_common::AppConfig) -> SocketAddr {
    let port = cfg.cluster.health_port;
    let s = format!("{}:{}", cfg.network.bind_host, port);
    SocketAddr::from_str(&s).unwrap_or_else(|_| {
        SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::UNSPECIFIED,
            port,
        ))
    })
}

/// 健康探针 HTTP 服务（只读状态，不做任何写操作）。
///
/// 把服务放到**后台任务**里跑并返回 `JoinHandle`，而不是阻塞到 Ctrl+C：
/// 集群主循环自己等 Ctrl+C，这样不会注册两个 `ctrl_c` 接收端 ——
/// 第一个消费掉信号后，第二个永远不会再触发，进程就退不出去了。
///
/// * `GET /healthz` → `200` + JSON（`status` / `node_id` / `nats_connected` / 集群规模等）；
/// * `POST /healthz` → `202`（让 `curl -X POST -s -o /dev/null` 形式的探针也能判活）；
/// * 其它方法 → `405`，其它路径 → `404`。
///
/// 绑定失败返回 `None` 并 `warn!`：探针起不来不应阻止节点入集群
/// （节点仍能调度与迁移），只是容器健康检查会报不健康。
/// 丢掉 `JoinHandle` 不会中断任务（`tokio::spawn` 是 detached），
/// 需要停止时显式 `abort()`。
pub async fn serve_health(
    cluster: Arc<Cluster>,
    addr: SocketAddr,
) -> Option<tokio::task::JoinHandle<()>> {
    // 用 `try_bind` 而不是 `bind`：`bind` 绑定失败会 panic，探针失败不该让节点起不来。
    // 绑定动作放在这里（而不是 spawn 之后）才能把「绑定失败」报告为 `None`。
    let server = match hyper::Server::try_bind(&addr) {
        Ok(builder) => builder.serve(hyper::service::make_service_fn(move |_| {
            let cluster = cluster.clone();
            async move {
                Ok::<_, Error>(hyper::service::service_fn(move |req| {
                    handle_health_req(cluster.clone(), req)
                }))
            }
        })),
        Err(e) => {
            warn!(
                addr = %addr,
                error = %e,
                "健康探针未能监听，跳过（不影响集群成员关系）"
            );
            return None;
        }
    };
    info!(addr = %addr, "集群健康探针已启动（docker healthcheck 目标）");

    Some(tokio::spawn(async move {
        if let Err(e) = server.await {
            warn!(error = %e, "健康探针服务退出");
        }
    }))
}

/// 处理单个探针请求。所有分支都返回 `Ok(Response)`：探针侧只需要状态码。
pub async fn handle_health_req(
    cluster: Arc<Cluster>,
    req: Request<hyper::Body>,
) -> Result<Response<hyper::Body>, Error> {
    use hyper::body::to_bytes;

    if req.uri().path() != "/healthz" {
        return Ok(json_resp(StatusCode::NOT_FOUND, "no such route"));
    }
    // `method()` 返回借用；探针只关心两种方法，解引用匹配即可。
    match *req.method() {
        Method::GET => {
            let s = cluster.status_snapshot();
            let body = serde_json::json!({
                "status": "ok",
                "service": "qm-cluster",
                "node_id": s.node_id,
                "role": s.role,
                "advertised_addr": s.advertised_addr,
                "cluster_id": s.cluster_id,
                "nats_connected": s.nats_connected,
                "heartbeat_seq": s.heartbeat_seq,
                "health_checks": s.health_checks,
                "local_rooms": s.local_rooms,
                "cluster_rooms": s.cluster_rooms,
                "cluster_nodes": s.cluster_nodes,
                "total_listeners": s.total_listeners,
                "migrations_completed": s.migrations_completed,
                "migrations_failed": s.migrations_failed,
                "version": crate::VERSION,
            });
            Ok(json_resp(StatusCode::OK, body.to_string()))
        }
        Method::POST => {
            // 消费掉请求体，避免探活侧连接未正常结束而报 broken pipe。
            // `to_bytes` 没有长度上限参数，只负责把 body 读到底并返回 Bytes。
            let _ = to_bytes(req.into_body()).await;
            let body = serde_json::json!({"status": "ok", "service": "qm-cluster"});
            Ok(json_resp(StatusCode::ACCEPTED, body.to_string()))
        }
        _ => Ok(json_resp(
            StatusCode::METHOD_NOT_ALLOWED,
            "healthz 只支持 GET / POST",
        )),
    }
}

/// 构造文本/JSON 响应。`body` 同时接受字面量与 `String`，
/// 避免调用点为了类型匹配而 `.leak()` 制造内存泄漏。
fn json_resp(status: StatusCode, body: impl Into<String>) -> Response<hyper::Body> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json; charset=utf-8")
        .body(hyper::Body::from(body.into()))
        .expect("status + content-type 是合法响应")
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::AppConfig;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

    /// 默认探针端口是 8090（避开 8080 媒体 / 8081 信令）。
    #[test]
    fn default_health_port_is_not_media_or_signal() {
        assert_eq!(DEFAULT_HEALTH_PORT, 8090);
        assert_ne!(DEFAULT_HEALTH_PORT, 8080);
        assert_ne!(DEFAULT_HEALTH_PORT, 8081);
    }

    /// 无覆盖时探针地址 = `bind_host:8090`。
    #[test]
    fn health_addr_defaults_to_8090() {
        let mut cfg = AppConfig::default();
        cfg.network.bind_host = "192.168.0.10".to_string();
        let addr = health_addr(&cfg);
        assert_eq!(addr.port(), DEFAULT_HEALTH_PORT);
        assert_eq!(addr.ip().to_string(), "192.168.0.10");
    }

    /// `QM_CLUSTER_HEALTH_PORT` 覆盖端口（0 不在这里兜底，由配置层校验挡住）。
    #[test]
    fn health_addr_honors_config_override() {
        let mut cfg = AppConfig::default();
        cfg.cluster.health_port = 9090;
        assert_eq!(health_addr(&cfg).port(), 9090);
    }

    /// GET /healthz → 200 + JSON，且带 nats_connected / version 字段。
    #[tokio::test]
    async fn healthz_get_returns_ok_json() {
        let cluster = Arc::new(Cluster::default());
        let req = Request::builder()
            .method(http::Method::GET)
            .uri("/healthz")
            .body(hyper::Body::empty())
            .unwrap();
        let resp = handle_health_req(cluster, req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        let body = String::from_utf8_lossy(&bytes).into_owned();
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert!(v["nats_connected"].is_boolean());
        assert_eq!(v["version"], crate::VERSION);
    }

    /// POST /healthz → 202（compose `--fail-with-health` 语义：探针已收到）。
    #[tokio::test]
    async fn healthz_post_is_accepted() {
        let cluster = Arc::new(Cluster::default());
        let req = Request::builder()
            .method(http::Method::POST)
            .uri("/healthz")
            .body(hyper::Body::from("probe"))
            .unwrap();
        let resp = handle_health_req(cluster, req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    /// 非 GET/POST 的 /healthz → 405；未知路径 → 404。
    #[tokio::test]
    async fn healthz_unknown_method_or_path_rejected() {
        let cluster = Arc::new(Cluster::default());
        let put = Request::builder()
            .method(http::Method::PUT)
            .uri("/healthz")
            .body(hyper::Body::empty())
            .unwrap();
        assert_eq!(
            handle_health_req(cluster.clone(), put)
                .await
                .unwrap()
                .status(),
            StatusCode::METHOD_NOT_ALLOWED
        );

        let missing = Request::builder()
            .method(http::Method::GET)
            .uri("/nope")
            .body(hyper::Body::empty())
            .unwrap();
        assert_eq!(
            handle_health_req(cluster, missing).await.unwrap().status(),
            StatusCode::NOT_FOUND
        );
    }

    /// 探针地址解析失败时回退到默认端口（不 panic，节点仍可入集群）。
    #[test]
    fn health_addr_survives_bad_bind_host() {
        let mut cfg = AppConfig::default();
        cfg.network.bind_host = "not an ip".to_string();
        let addr = health_addr(&cfg);
        assert_eq!(addr.port(), DEFAULT_HEALTH_PORT);
    }

    /// 发一个原始 HTTP/1.1 GET，返回 (状态码, 响应体)。
    /// 不用第三方 HTTP client 库：探针只需要状态码和 JSON 文本，
    /// 直接用 `tokio` 的 TcpStream 最省事，也不给锁文件添新依赖。
    async fn http_get(addr: SocketAddr) -> (u16, String) {
        request(addr, "GET /healthz HTTP/1.1", "", b"").await
    }

    /// 发一个原始 HTTP/1.1 POST。
    async fn http_post(addr: SocketAddr, body: &str) -> (u16, String) {
        request(
            addr,
            "POST /healthz HTTP/1.1",
            &format!("Content-Length: {}\r\n", body.len()),
            body.as_bytes(),
        )
        .await
    }

    /// 构造并发送一个原始 HTTP 请求，读回响应。
    /// `extra_headers` 是完整的额外头（含行尾），没有就传空串。
    async fn request(
        addr: SocketAddr,
        start_line: &str,
        extra_headers: &str,
        body: &[u8],
    ) -> (u16, String) {
        let mut stream = tokio::net::TcpStream::connect(addr)
            .await
            .expect("应能连上探针端口");
        let mut head = format!("{start_line}\r\nHost: {addr}\r\nConnection: close\r\n");
        if !extra_headers.is_empty() {
            head.push_str(extra_headers);
        }
        head.push_str("\r\n");
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();

        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let n = match stream.read(&mut tmp).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => panic!("读取响应失败: {e}"),
            };
            buf.extend_from_slice(&tmp[..n]);
            if buf.len() > 64 * 1024 {
                break;
            }
        }
        let raw = String::from_utf8_lossy(&buf).into_owned();
        let (resp_head, resp_body) = raw.split_once("\r\n\r\n").unwrap_or((raw.as_str(), ""));
        let status: u16 = resp_head
            .lines()
            .next()
            .expect("响应必须有状态行")
            .split_whitespace()
            .nth(1)
            .expect("状态行应包含数字状态码")
            .parse()
            .expect("状态码必须是数字");
        (status, resp_body.to_string())
    }

    /// 轮询等端口可连，集成测试用的最小等待工具。
    async fn wait_tcp(addr: SocketAddr, budget: Duration) -> bool {
        let start = tokio::time::Instant::now();
        while start.elapsed() < budget {
            if tokio::net::TcpStream::connect(addr).await.is_ok() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    /// 集成测试：`serve_health` 真的把监听器绑起来，真实 HTTP GET 拿到 200 + JSON。
    /// 只有单测覆盖 `handle_health_req` 不够 —— 端口绑定与 hyper server 是另一条路径，
    /// 而 compose healthcheck 探的就是这条路径。
    #[tokio::test]
    async fn serve_health_binds_and_serves_real_http() {
        std::env::remove_var("QM_CLUSTER_HEALTH_PORT");
        let addr: SocketAddr = "127.0.0.1:48090".parse().unwrap();
        let cluster = Arc::new(Cluster::default());
        let h = serve_health(cluster.clone(), addr)
            .await
            .expect("测试端口必须可绑定");

        assert!(
            wait_tcp(addr, Duration::from_secs(10)).await,
            "健康探针在 10s 内没有监听 {addr}"
        );

        let resp = http_get(addr).await;
        assert_eq!(resp.0, 200, "GET /healthz 必须返回 200");
        let v: serde_json::Value = serde_json::from_str(&resp.1).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["service"], "qm-cluster");
        assert!(
            v["nats_connected"].is_boolean(),
            "探活响应要带 NATS 连接状态，便于现场判断是否退化单机"
        );

        // abort 任务后监听器必须真的关闭 —— 对应「容器重启不会端口冲突」。
        h.abort();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            !wait_tcp(addr, Duration::from_millis(500)).await,
            "abort 后监听器必须已经关闭"
        );
    }

    /// 集成测试：POST /healthz 返回 202（`curl -X POST -s -o /dev/null` 形式探针也可用）。
    #[tokio::test]
    async fn serve_health_accepts_post() {
        std::env::remove_var("QM_CLUSTER_HEALTH_PORT");
        let addr: SocketAddr = "127.0.0.1:48091".parse().unwrap();

        let h = serve_health(Arc::new(Cluster::default()), addr)
            .await
            .expect("测试端口必须可绑定");
        assert!(
            wait_tcp(addr, Duration::from_secs(10)).await,
            "健康探针在 10s 内没有监听 {addr}"
        );

        let resp = http_post(addr, "probe").await;
        assert_eq!(resp.0, 202, "POST /healthz 必须返回 202");
        assert!(resp.1.contains("status") && resp.1.contains("ok"));

        h.abort();
    }
}
