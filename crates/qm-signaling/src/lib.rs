//! # qm-signaling — QuickMeet 信令层
//!
//! 信令是 WebRTC 里唯一跨进程的一步：SDP 与 ICE candidate 必须由某处搬运。
//! 这里刻意把"路由 + 准入 + 转发记录"做成**不绑定任何 HTTP 库**的纯函数
//! （[`SignalRouter::route`]），HTTP 适配（[`SignalHttp`] / [`start`]）只是薄壳，
//! 这样路由逻辑可以在 `cargo test --workspace` 里离线逐条断言。
//!
//! 接口（全部 JSON）：
//! * `GET  /healthz`                 → 存活探针（端口 / 房间数 / 转发计数）
//! * `GET  /room/{id}/peers`         → 房间内 peer 列表
//! * `POST /room/{id}/join`          → 加入房间
//! * `POST /room/{id}/offer`         → 投递 SDP（`kind` = `offer` / `answer`）
//! * `POST /room/{id}/candidate`     → 投递 ICE candidate
//! * `POST /room/{id}/leave`         → 离开房间
//!
//! 关键约束的落点：
//! * **Epic 约束 3（内网 192.168.0.0/24）** —— 每个信令请求的 `peer` 地址都必须
//!   落在配置的私有网段内（[`Error::ensure_private_host`]），否则拒绝；
//!   `start()` 也只在配置的 `network.bind_host`（IPv4）上监听。
//! * **Epic 约束 5（数据本地化）** —— 信令侧不持久化任何媒体载荷，状态只存在进程内存，
//!   进程退出即清空；房间为空会自动回收。
//!
//! hyper / tokio 只被 [`SignalHttp`] / [`start`] 用到；路由核心（[`SignalRouter::route`]）
//! 不触碰它们，因此单测可以完全离线断言路由逻辑。

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use http::{Method, Request, Response, StatusCode};
use hyper::service::Service;
use parking_lot::Mutex;
use qm_common::error::Error;
use qm_common::error::Result as QmResult;
use serde::{Deserialize, Serialize};
use tracing::debug;

/// 信令服务默认端口（媒体端口 8080，信令错开一位）。
pub const DEFAULT_SIGNALING_PORT: u16 = 8081;

/// 一个已加入房间的 peer。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    /// peer 标识（客户端自填或 uuid）。
    pub id: String,
    /// peer 声称的来源地址，格式 `ip:port`。
    pub address: String,
}

/// 房间（私有化：不存媒体，只存成员名单）。
#[derive(Debug, Default)]
struct Room {
    peers: Vec<Peer>,
}

/// 信令内存状态。
#[derive(Debug, Default)]
struct State {
    rooms: std::collections::HashMap<String, Room>,
    /// 已转发 SDP 总数。
    sdp_exchanges: u64,
    /// 已转发 ICE candidate 总数。
    candidates_exchanged: u64,
}

/// 信令路由：状态 + 配置 + 纯函数分发（状态在 `Arc` 里，可自由克隆）。
#[derive(Clone)]
pub struct SignalRouter {
    cfg: Arc<qm_common::AppConfig>,
    state: Arc<Mutex<State>>,
}

impl SignalRouter {
    /// 按配置构造路由器。
    pub fn new(cfg: Arc<qm_common::AppConfig>) -> Self {
        Self {
            cfg,
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// 信令监听端口。
    pub fn listen_port(&self) -> u16 {
        self.cfg.media.signaling_port
    }

    /// 媒体监听端口。
    pub fn media_port(&self) -> u16 {
        self.cfg.media.port
    }

    /// 私有网段允许列表（来自配置；配置非法时返回参数错误）。
    pub fn allowlist(&self) -> QmResult<Vec<qm_common::Cidr>> {
        self.cfg.network.parsed_cidrs()
    }

    /// 准入校验：`peer` 地址必须落在配置的私有网段内。
    pub fn check_peer(&self, peer: &str) -> QmResult<()> {
        let host = peer.split(':').next().unwrap_or(peer);
        let addr = match host.parse::<IpAddr>() {
            Ok(a) => a,
            Err(_) => return Err(Error::invalid_argument(format!("peer 地址非法：{peer}"))),
        };
        let cidrs = self.allowlist()?;
        Error::ensure_private_host(addr, &cidrs).map_err(|_| {
            Error::signaling(format!(
                "拒绝非内网 peer {peer}：信令只接受 {}",
                self.cfg.network.cidrs.join(", ")
            ))
        })
    }

    /// 分发一个请求（HTTP 无关，可在单测里直接调用）。
    pub fn route(&self, method: Method, path: &str, body: &[u8]) -> Response<String> {
        if path == "/healthz" {
            return match method {
                m if m == Method::GET => {
                    let st = self.state.lock();
                    json_ok(&Health {
                        name: qm_common::NAME,
                        version: qm_common::VERSION,
                        signaling_port: self.listen_port(),
                        media_port: self.media_port(),
                        rooms: st.rooms.len(),
                        sdp_exchanges: st.sdp_exchanges,
                        candidates_exchanged: st.candidates_exchanged,
                    })
                }
                _ => json_err(StatusCode::METHOD_NOT_ALLOWED, "healthz 只支持 GET"),
            };
        }

        let method_display = method.to_string();
        let mut parts = path.split('/').filter(|s| !s.is_empty());
        if parts.next() != Some("room") {
            return json_err(StatusCode::NOT_FOUND, format!("未知路径：{path}"));
        }
        let room = parts.next().unwrap_or("").to_string();
        let tail = parts.next().unwrap_or("").to_string();
        if room.is_empty() {
            return json_err(StatusCode::NOT_FOUND, format!("缺少 room：{path}"));
        }

        if tail == "peers" {
            return match method {
                m if m == Method::GET => {
                    let peers = self
                        .state
                        .lock()
                        .rooms
                        .get(&room)
                        .map(|r| r.peers.clone())
                        .unwrap_or_default();
                    json_ok(&PeerList { room, peers })
                }
                _ => json_err(StatusCode::METHOD_NOT_ALLOWED, "peers 只支持 GET"),
            };
        }

        match (method, tail.as_str()) {
            (m, "join") if m == Method::POST => self.join(&room, body),
            (m, "offer") if m == Method::POST => self.offer(&room, body),
            (m, "candidate") if m == Method::POST => self.candidate(&room, body),
            (m, "leave") if m == Method::POST => self.leave(&room, body),
            _ => json_err(
                StatusCode::NOT_FOUND,
                format!("不支持的路由：{method_display} /room/{room}/{tail}"),
            ),
        }
    }
}

#[derive(Debug, Serialize)]
struct Health {
    name: &'static str,
    version: &'static str,
    signaling_port: u16,
    media_port: u16,
    rooms: usize,
    sdp_exchanges: u64,
    candidates_exchanged: u64,
}

#[derive(Debug, Serialize)]
struct PeerList {
    room: String,
    peers: Vec<Peer>,
}

#[derive(Debug, Serialize, Deserialize)]
struct JoinBody {
    pub peer: String,
    #[serde(default)]
    pub peer_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OfferBody {
    pub peer: String,
    #[serde(default)]
    pub peer_id: Option<String>,
    /// SDP 文本。
    pub sdp: String,
    /// `offer` 或 `answer`。
    pub kind: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CandidateBody {
    pub peer: String,
    #[serde(default)]
    pub peer_id: Option<String>,
    /// 一行 ICE candidate。
    pub candidate: String,
}

#[derive(Debug, Serialize)]
struct Ack {
    ok: bool,
    room: String,
    peer: String,
    note: String,
}

fn ack(room: &str, peer: &str, note: impl Into<String>) -> Ack {
    Ack {
        ok: true,
        room: room.to_string(),
        peer: peer.to_string(),
        note: note.into(),
    }
}

/// 成功响应（JSON）。命名为 `json_ok` 以避开 `Result::Ok` 遮蔽。
fn json_ok<T: Serialize>(v: &T) -> Response<String> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string()))
        .unwrap()
}

/// 错误响应（JSON）。命名为 `json_err` 以避开 `Result::Err` 遮蔽。
fn json_err(status: StatusCode, message: impl Into<String>) -> Response<String> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            serde_json::to_string(&Ack {
                ok: false,
                room: String::new(),
                peer: String::new(),
                note: message.into(),
            })
            .unwrap(),
        )
        .unwrap()
}

impl SignalRouter {
    fn join(&self, room: &str, body: &[u8]) -> Response<String> {
        let m: JoinBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return json_err(StatusCode::BAD_REQUEST, format!("join 请求体解析失败：{e}"))
            }
        };
        if let Err(e) = self.check_peer(&m.peer) {
            return json_err(StatusCode::FORBIDDEN, e.to_string());
        }
        let id = m
            .peer_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let address = m.peer.clone();
        let mut st = self.state.lock();
        let peers = &mut st.rooms.entry(room.to_string()).or_default().peers;
        if !peers.iter().any(|p| p.id == id) {
            peers.push(Peer { id, address });
        }
        let now = peers.len();
        drop(st);
        json_ok(&ack(room, &m.peer, format!("已加入房间，当前 {now} 人")))
    }

    fn offer(&self, room: &str, body: &[u8]) -> Response<String> {
        let m: OfferBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return json_err(
                    StatusCode::BAD_REQUEST,
                    format!("offer 请求体解析失败：{e}"),
                )
            }
        };
        if let Err(e) = self.check_peer(&m.peer) {
            return json_err(StatusCode::FORBIDDEN, e.to_string());
        }
        if m.sdp.trim().is_empty() {
            return json_err(StatusCode::BAD_REQUEST, "SDP 不能为空");
        }
        let sdp_len = m.sdp.len();
        let kind = m.kind.clone();
        self.state.lock().sdp_exchanges += 1;
        debug!(room, kind, sdp_bytes = sdp_len, "信令转发 SDP");
        json_ok(&ack(
            room,
            &m.peer,
            format!("已转发 {kind}（{sdp_len} 字节）"),
        ))
    }

    fn candidate(&self, room: &str, body: &[u8]) -> Response<String> {
        let m: CandidateBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return json_err(
                    StatusCode::BAD_REQUEST,
                    format!("candidate 请求体解析失败：{e}"),
                )
            }
        };
        if let Err(e) = self.check_peer(&m.peer) {
            return json_err(StatusCode::FORBIDDEN, e.to_string());
        }
        if m.candidate.trim().is_empty() {
            return json_err(StatusCode::BAD_REQUEST, "ICE candidate 不能为空");
        }
        let c = m.candidate.len();
        self.state.lock().candidates_exchanged += 1;
        debug!(room, chars = c, "信令转发 ICE candidate");
        json_ok(&ack(
            room,
            &m.peer,
            format!("已转发 ICE candidate（{c} 字符）"),
        ))
    }

    fn leave(&self, room: &str, body: &[u8]) -> Response<String> {
        let m: JoinBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return json_err(
                    StatusCode::BAD_REQUEST,
                    format!("leave 请求体解析失败：{e}"),
                )
            }
        };
        if let Err(e) = self.check_peer(&m.peer) {
            return json_err(StatusCode::FORBIDDEN, e.to_string());
        }
        let id = m
            .peer_id
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let mut st = self.state.lock();
        let room_left = st
            .rooms
            .get_mut(room)
            .map(|r| {
                r.peers.retain(|p| p.id != id);
                r.peers.len()
            })
            .unwrap_or(0);
        if room_left == 0 {
            st.rooms.remove(room);
        }
        drop(st);
        json_ok(&ack(room, &m.peer, "已离开房间"))
    }
}

/// 服务超时时间（信令是短连接，30 秒足够）。
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// HTTP 适配：把 [`SignalRouter`] 包装成 hyper 的 `Service`。
///
/// hyper 的 `Service` 每请求克隆一次 handler，所以路由器状态必须包在 `Arc`
/// 里，否则克隆副本各自一份状态、房间计数会分裂。
#[derive(Clone)]
pub struct SignalHttp {
    inner: Arc<SignalRouter>,
}

impl SignalHttp {
    /// 用路由器构造 HTTP 服务。
    pub fn new(router: SignalRouter) -> Self {
        Self {
            inner: Arc::new(router),
        }
    }
}

#[allow(clippy::unused_async)]
impl Service<Request<hyper::Body>> for SignalHttp {
    type Response = Response<hyper::Body>;
    type Error = Error;
    type Future =
        std::pin::Pin<Box<dyn std::future::Future<Output = QmResult<Self::Response>> + Send>>;

    fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<QmResult<()>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<hyper::Body>) -> Self::Future {
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let inner = self.inner.clone();
        Box::pin(async move {
            let bytes = match hyper::body::to_bytes(req.into_body()).await {
                Ok(b) => b,
                Err(e) => {
                    let resp = json_err(StatusCode::BAD_REQUEST, format!("请求体读取失败：{e}"));
                    return Ok(Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .header("content-type", "application/json")
                        .body(hyper::Body::from(resp.into_body()))
                        .map_err(|e| Error::signaling(e.to_string()))?);
                }
            };
            let resp = inner.route(method, &path, bytes.as_ref());
            Ok(Response::builder()
                .status(resp.status())
                .header("content-type", "application/json")
                .body(hyper::Body::from(resp.into_body()))
                .map_err(|e| Error::signaling(e.to_string()))?)
        })
    }
}

/// 请求处理：读取 body → 路由 → 转成 hyper 响应。
///
/// 抽成自由函数是为了让连接层闭包可以 `move` 一个 `SignalRouter` 而不必再实现
/// `Service`（hyper 0.14 连接层与请求层是两层不同的 `Service`）。
async fn handle_request(
    router: SignalRouter,
    req: Request<hyper::Body>,
) -> QmResult<Response<hyper::Body>> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let bytes = match hyper::body::to_bytes(req.into_body()).await {
        Ok(b) => b,
        Err(e) => {
            let resp = json_err(StatusCode::BAD_REQUEST, format!("请求体读取失败：{e}"));
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .header("content-type", "application/json")
                .body(hyper::Body::from(resp.into_body()))
                .map_err(|e| Error::signaling(e.to_string()))?);
        }
    };
    let resp = router.route(method, &path, bytes.as_ref());
    Ok(Response::builder()
        .status(resp.status())
        .header("content-type", "application/json")
        .body(hyper::Body::from(resp.into_body()))
        .map_err(|e| Error::signaling(e.to_string()))?)
}

/// 启动信令 HTTP 服务（只绑定配置的内网 IPv4 地址，Ctrl+C 优雅退出）。
///
/// hyper 0.14 要求 handler 先实现 `Service<AddrStream>`（每连接一个），
/// 内层才是 `Service<Request<Body>>`。这里用 `service_fn` 做两层适配：
/// 连接层闭包只负责克隆路由器，请求层调用 [`handle_request`]。
pub async fn start(cfg: Arc<qm_common::AppConfig>) -> QmResult<()> {
    let host = cfg.network.bind_host.clone();
    let port = cfg.media.signaling_port;
    let addr = SocketAddr::new(
        host.parse::<IpAddr>()
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    );
    let router = SignalRouter::new(cfg.clone());
    tracing::info!(%host, %port, "信令服务启动（仅监听内网地址）");
    let server = hyper::Server::bind(&addr).serve(hyper::service::make_service_fn(move |_| {
        let router = router.clone();
        async move {
            Ok::<_, Error>(hyper::service::service_fn(
                move |req: Request<hyper::Body>| handle_request(router.clone(), req),
            ))
        }
    }));
    tokio::select! {
        r = server => r.map_err(|e| Error::signaling(e.to_string()))?,
        _ = tokio::signal::ctrl_c() => { tracing::info!("收到 Ctrl+C，信令服务退出"); }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use qm_common::error::ErrorKind;

    fn router() -> SignalRouter {
        SignalRouter::new(Arc::new(qm_common::AppConfig::default()))
    }

    fn get(r: &SignalRouter, path: &str) -> Response<String> {
        r.route(Method::GET, path, &[])
    }

    fn post(r: &SignalRouter, path: &str, body: &[u8]) -> Response<String> {
        r.route(Method::POST, path, body)
    }

    #[test]
    fn default_ports_match_epic_constraints() {
        let r = router();
        assert_eq!(r.media_port(), 8080, "媒体端口必须是 8080（Epic 约束 3）");
        assert_eq!(r.listen_port(), 8081, "信令端口与媒体端口错开");
        assert!(
            r.allowlist()
                .unwrap()
                .iter()
                .any(|c| c.contains("192.168.0.1".parse().unwrap())),
            "允许列表必须覆盖 192.168.0.0/24（Epic 约束 3）"
        );
    }

    #[test]
    fn healthz_reports_ports_and_state() {
        let r = router();
        let resp = get(&r, "/healthz");
        assert_eq!(resp.status(), StatusCode::OK);
        let v: serde_json::Value = serde_json::from_str(&resp.into_body()).unwrap();
        assert_eq!(v["signaling_port"], 8081);
        assert_eq!(v["media_port"], 8080);
        assert_eq!(v["rooms"], 0);
        assert_eq!(v["name"], "QuickMeet");
    }

    #[test]
    fn public_peer_is_forbidden() {
        let r = router();
        let body = br#"{"peer":"8.8.8.8:5060","peer_id":"p1"}"#;
        assert_eq!(
            post(&r, "/room/a/join", body).status(),
            StatusCode::FORBIDDEN
        );
        // SDP / candidate 通道同样要挡住公网来源
        assert_eq!(
            post(
                &r,
                "/room/a/offer",
                br#"{"peer":"1.2.3.4:1","sdp":"v=0","kind":"offer"}"#
            )
            .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            post(
                &r,
                "/room/a/candidate",
                br#"{"peer":"1.2.3.4:1","candidate":"c"}"#
            )
            .status(),
            StatusCode::FORBIDDEN
        );
        // 直连校验器：公网 IP 报 Signaling 类错误
        assert_eq!(
            r.check_peer("8.8.8.8:5060").unwrap_err().kind(),
            ErrorKind::Signaling
        );
        // 非法地址报参数错误
        assert_eq!(
            r.check_peer("not-an-ip").unwrap_err().kind(),
            ErrorKind::InvalidArgument
        );
    }

    #[test]
    fn intranet_peer_can_join_and_leave() {
        let r = router();
        let join = br#"{"peer":"192.168.0.42:5060","peer_id":"p1"}"#;
        assert_eq!(
            post(&r, "/room/meeting/join", join).status(),
            StatusCode::OK
        );

        let peers: serde_json::Value =
            serde_json::from_str(&get(&r, "/room/meeting/peers").into_body()).unwrap();
        assert_eq!(peers["peers"].as_array().unwrap().len(), 1);
        assert_eq!(peers["peers"][0]["address"], "192.168.0.42:5060");

        assert_eq!(
            post(&r, "/room/meeting/leave", join).status(),
            StatusCode::OK
        );
        let peers: serde_json::Value =
            serde_json::from_str(&get(&r, "/room/meeting/peers").into_body()).unwrap();
        assert_eq!(peers["peers"].as_array().unwrap().len(), 0);
        // 空房间自动回收
        let health: serde_json::Value =
            serde_json::from_str(&get(&r, "/healthz").into_body()).unwrap();
        assert_eq!(health["rooms"], 0);
    }

    #[test]
    fn offer_and_candidate_get_recorded() {
        let r = router();
        let offer = br#"{"peer":"192.168.0.10:1","sdp":"v=0\r\no=- 0 0 IN IP4 192.168.0.10\r\n","kind":"offer"}"#;
        assert_eq!(post(&r, "/room/m/offer", offer).status(), StatusCode::OK);
        let cand = br#"{"peer":"192.168.0.10:1","candidate":"candidate:1 1 udp 2130706431 192.168.0.10 5060 typ host"}"#;
        assert_eq!(post(&r, "/room/m/candidate", cand).status(), StatusCode::OK);

        let health: serde_json::Value =
            serde_json::from_str(&get(&r, "/healthz").into_body()).unwrap();
        assert_eq!(health["sdp_exchanges"], 1);
        assert_eq!(health["candidates_exchanged"], 1);
    }

    #[test]
    fn empty_sdp_and_candidate_are_rejected() {
        let r = router();
        assert_eq!(
            post(
                &r,
                "/room/m/offer",
                br#"{"peer":"192.168.0.10:1","sdp":"","kind":"answer"}"#
            )
            .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            post(
                &r,
                "/room/m/candidate",
                br#"{"peer":"192.168.0.10:1","candidate":"  "}"#
            )
            .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn malformed_body_reports_bad_request() {
        let r = router();
        assert_eq!(
            post(&r, "/room/m/join", b"not json").status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn unknown_routes_and_methods_are_rejected() {
        let r = router();
        assert_eq!(get(&r, "/nope").status(), StatusCode::NOT_FOUND);
        assert_eq!(get(&r, "/room/m/join").status(), StatusCode::NOT_FOUND);
        assert_eq!(get(&r, "/healthz").status(), StatusCode::OK);
        assert_eq!(get(&r, "/room/m/peers").status(), StatusCode::OK);
        assert!(get(&r, "/x/y").status() != StatusCode::OK);
        // peers 只支持 GET
        assert_eq!(
            post(&r, "/room/m/peers", b"{}").status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
    }
}
