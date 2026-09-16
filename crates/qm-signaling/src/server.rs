//! WebSocket / WSS 传输层（QM-004）。
//!
//! 这一层只做四件事：TLS 握手、JWT 门禁、把 JSON 帧喂给 [`WsRouter`]、
//! 把 [`WsResult::broadcasts`] 送进广播注册表。**所有协议判断都不在这里** ——
//! 协议是纯函数 [`WsRouter::dispatch`]，可以在 `cargo test` 里离线逐条断言。
//!
//! ## 两道强制门禁（Issue 特有约束）
//!
//! * **JWT 强制**：身份在**握手阶段**校验（[`auth_callback`]），token 缺失或
//!   无效 → HTTP 401，`101 Switching Protocols` 不会发出，连接根本不会成为
//!   WebSocket —— 未携带有效 JWT 的连接拿不到任何会议信息（验收标准 3）。
//! * **WSS 强制**：[`start_ws`] 启动时若 `auth.tls.enabled = false` 直接拒绝
//!   启动，端口上**只有 TLS 监听器**，因此不存在"明文 `ws://` 也能连"的配置
//!   路径；明文客户端走不到 TLS 握手，在 TCP 层就被拒绝。
//!
//! ## token 来源
//!
//! 浏览器 `WebSocket` API 无法设置自定义 header，所以**主**用 `?token=` query，
//! **次**用 `Authorization` 头（curl / 自写客户端）。两者都由
//! [`crate::auth::extract_bearer`] 解析，缺哪个都不影响另一条路。
//!
//! ## 与 SFU 解耦
//!
//! WSS 信令独占 `media.signaling_ws_port`（默认 8082），与 HTTP 信令（8081）、
//! ICE 网关（8083）、SFU 媒体（8080）都不共用端口。信令服务不接触媒体端口。

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::future::FusedFuture;
use futures::{Sink, SinkExt, StreamExt};
use parking_lot::Mutex;
use qm_common::error::{Error as QmError, Result as QmResult};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::server::{
    Callback, ErrorResponse, Request, Response,
};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, WebSocketConfig};

// tungstenite 依赖的是 `http 1.x`（经 `tungstenite::http` 导出），本 crate 的
// `http` 是 `0.2.12`（`hyper 0.14` 固定）。两者是不同的 crate 版本、类型不
// 兼容，握手相关的构造与断言一律走 tungstenite 那份。
use tokio_tungstenite::tungstenite::http as http1;

use crate::auth::{self, JwtVerifier};
use crate::ws::{Identity, WsRouter};

/// 服务端主动 ping 的间隔（秒）。
///
/// 必须小于浏览器默认的 30s close timeout（`websocket.frameInterval`），
/// 否则浏览器会在服务端回 pong 之前掐断连接。
const PING_INTERVAL_SECS: u64 = 20;

/// 单帧发送超时（秒）：写超时说明对端已死，立刻断开，不无限重试。
const SEND_TIMEOUT_SECS: u64 = 15;

/// TLS / WebSocket 握手超时（秒）：慢握手直接拒，避免半开连接占住 accept 循环。
const ACCEPT_TIMEOUT_SECS: u64 = 15;

/// 广播通道容量（帧数）。广播帧很小（JSON + SDP，通常 2–6 KB），队列满说明
/// 接收方已经卡死或断开 —— 立刻清掉，不排队。
const BROADCAST_CAPACITY: usize = 64;

/// 关闭原因标签（日志用）。
#[derive(Debug, Clone, Copy)]
enum CloseReason {
    Normal,
    ReadError,
    FrameTooLarge,
    Dead,
}

impl CloseReason {
    fn as_str(self) -> &'static str {
        match self {
            CloseReason::Normal => "normal",
            CloseReason::ReadError => "read_error",
            CloseReason::FrameTooLarge => "frame_too_large",
            CloseReason::Dead => "dead",
        }
    }
}

/// 握手阶段的身份槽：每条连接一个，握手回调写、连接任务读。
///
/// 用 slot 而不是全局表：握手回调和连接任务生命周期相同，`tokio::spawn` 时把
/// 同一个 `Arc` 传过去，不需要跨连接共享状态，也没有竞态窗口。
type HandshakeSlot = Arc<Mutex<Option<(String, u64, Identity)>>>;

/// 握手回调工厂：`Ok` = 101 放行；`Err` = 401 拒绝（**不发** 101）。
fn auth_callback(verifier: JwtVerifier, slot: HandshakeSlot) -> impl Callback {
    move |request: &Request, response: Response| -> Result<Response, ErrorResponse> {
        // `request.uri` 含 query（`/?token=...&peer=...`），`extract_bearer`
        // 同时认 `?token=` 与 `Authorization: Bearer`，拼成一段多行输入即可。
        let input = bearer_input(request);
        let Some(peer) = query_get(request.uri().query(), "peer") else {
            // 缺少 peer 也要走 401 分支：不返回成功状态，否则 tungstenite 会
            // 报 `CustomResponseSuccessful` 并仍然把 101 写给客户端。
            tracing::warn!("拒绝连接：URL 缺少 ?peer= 参数");
            return Err(error_response(
                request,
                "缺少 ?peer= 参数（形如 wss://host:8082/?peer=alice&token=***）",
            ));
        };
        let generation = query_get(request.uri().query(), "gen")
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(0);
        match verifier.verify(&input) {
            Ok(identity) => {
                tracing::debug!(%peer, subject = %identity.subject, "WSS 握手通过（JWT 有效）");
                slot.lock().replace((peer, generation, identity));
                Ok(response)
            }
            Err(e) => {
                tracing::warn!(%peer, reason = %e, "WSS 握手拒绝（无效 / 缺失 JWT），不发 101");
                let body = if e.to_string().contains("缺少") {
                    "missing token"
                } else {
                    "invalid token"
                };
                Err(error_response(request, body))
            }
        }
    }
}

/// 构造握手拒绝响应。
///
/// tungstenite 只接受**非 2xx** 的自定义响应作为拒绝（2xx 会被当成放行并报
/// `CustomResponseSuccessful`），所以这里必须返回 4xx。同时保留握手请求的
/// HTTP 版本，避免 `HTTP/2.0` 请求被降级成 `HTTP/1.0`。
fn error_response(request: &Request, body: &str) -> ErrorResponse {
    http1::Response::builder()
        .status(http1::StatusCode::UNAUTHORIZED)
        .version(request.version())
        .header("content-type", "text/plain; charset=utf-8")
        .header("connection", "close")
        .body(Some(body.to_string()))
        .expect("静态 401 响应一定能构造")
}

/// 把握手请求里的 token 材料拼成 [`auth::extract_bearer`] 的输入。
///
/// 注意：`tungstenite` 依赖的是 `http 1.x`（经 `tokio_tungstenite::tungstenite::http`
/// 导出），本 crate 的 `http` 是 `0.2.12`（`hyper 0.14` 固定），两者是不同的
/// crate 版本、类型不兼容。这里统一用 tungstenite 那一份。
fn bearer_input(request: &Request) -> String {
    let authz = request
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| format!("Authorization: {s}"));
    match authz {
        Some(h) => format!("{}\n{h}", request.uri().clone()),
        None => request.uri().to_string(),
    }
}

/// 极简 query 解析：带百分号解码，只做两个标量的提取。
///
/// 不用完整 URL 解析器 —— 只需要两个标量，手写比引入依赖更省 MSRV 风险。
fn query_get(query: Option<&str>, key: &str) -> Option<String> {
    let q = query?;
    for kv in q.split('&') {
        // 没有 `=` 的分片直接跳过，不能因为一个残缺片段就丢掉整个 query。
        let Some((k, v)) = kv.split_once('=') else {
            continue;
        };
        if k != key {
            continue;
        }
        let s = percent_decode(v);
        let s = String::from_utf8(s).unwrap_or_default();
        let s = s.trim().to_string();
        if s.is_empty() {
            return None;
        }
        return Some(s);
    }
    None
}

/// 百分号解码。
///
/// 必须逐字符**消费** `%XX` 三个字节：用 `map` 边迭代边解码的话，`%3A1` 会被
/// 解成 `:` 再追加原始的 `3`、`A`、`1`，peer 名就成了 `192.168.0.10:3A1` ——
/// 参会者身份被静默改写，房间成员表随即对不上。
fn percent_decode(v: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len());
    let bytes = v.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            out.push(hex_two(&v[i + 1..i + 3]));
            i += 3;
        } else if bytes[i] == b'+' {
            out.push(b' ');
            i += 1;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    out
}

/// 两个 hex 字符 → 字节（非法返回 0 占位，不影响安全性）。
fn hex_two(s: &str) -> u8 {
    fn nib(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if bytes.len() == 2 {
        if let (Some(h), Some(l)) = (nib(bytes[0]), nib(bytes[1])) {
            return h * 16 + l;
        }
    }
    0
}

/// 把握手错误折成 HTTP 状态码（仅 `Error::Http` 携带）。
///
/// `accept_hdr_async_*` 在拒绝回调返回**非 2xx** 时以 `Err(Error::Http(response))`
/// 返回 —— 这就是"握手阶段就被拒"的传输层证据；2xx 会被 tungstenite 当成放行并报
/// `CustomResponseSuccessful`，所以拒绝必须走 `Err` + 4xx。
fn handshake_reject_status(e: &tokio_tungstenite::tungstenite::Error) -> Option<http1::StatusCode> {
    match e {
        tokio_tungstenite::tungstenite::Error::Http(resp) => Some(resp.status()),
        _ => None,
    }
}

/// 构造 WebSocket 关闭帧（`reason` 要求 `Utf8Bytes`，`String` 可直接 `.into()`）。
fn ws_close(code: CloseCode, reason: String) -> CloseFrame {
    CloseFrame {
        code,
        reason: reason.into(),
    }
}

/// 控制中断退出信号（`Send`，`select!` 的 `&mut` 分支要求可熔断）。
///
/// 自带一份 `ctrl_c()` 未来，每次 `poll` 都委托它 —— 不能就地调用
/// `tokio::signal::ctrl_c().poll_unpin`，因为 `Future::poll` 只给 `Pin<&mut Self>`。
struct CtrlCEnd {
    signal: std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>,
}

impl CtrlCEnd {
    fn new() -> Self {
        Self {
            signal: Box::pin(tokio::signal::ctrl_c()),
        }
    }
}

impl Future for CtrlCEnd {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match self.signal.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => Poll::Ready(()),
            // 忽略 Ctrl+C 路径上的 IO 错误：退出就是退出，不需要区分原因。
            Poll::Ready(Err(_)) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl FusedFuture for CtrlCEnd {
    fn is_terminated(&self) -> bool {
        false
    }
}


/// 永不就绪（测试用）：`run_server` 起在单线程/多线程 runtime 里常驻等待连接。
struct NeverEnd;

impl Future for NeverEnd {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl FusedFuture for NeverEnd {
    fn is_terminated(&self) -> bool {
        false
    }
}

/// 带超时的发送（`tokio-tungstenite 0.29` 没有 `send_with_timeout`）。
/// 超时要设：一个已死但还没被探测出来的连接会让广播帧的写入永远挂着，
/// 进而卡住整个广播出口 —— 验收标准 4（1000 并发无泄漏）的关键一环。
/// 只返回成败，具体错误只写日志（写失败一律按连接已死处理）。
async fn send_in_time<S>(ws: &mut S, msg: Message, timeout: Duration) -> bool
where
    S: SinkExt<Message>
        + Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin,
{
    match tokio::time::timeout(timeout, ws.send(msg)).await {
        Ok(Ok(())) => true,
        Ok(Err(e)) => {
            tracing::debug!(error = %e, "WS 写失败");
            false
        }
        Err(_) => false,
    }
}



/// 连接级限额。
#[derive(Debug, Clone)]
pub struct Limits {
    /// 单房间成员上限（0 = 不限制）。
    pub max_per_room: usize,
    /// 单条信令帧字节上限。
    pub max_frame_bytes: usize,
    /// 进程级已接受连接数上限（0 = 不限制）。
    pub max_connections: usize,
}

impl Limits {
    /// 从全局配置派生；连接数上限读环境变量 `QM_SIGNAL_MAX_CONNECTIONS`
    /// （默认 4000，验收标准 4 的口径是 1000 个并发）。
    pub fn from_config(cfg: &qm_common::AppConfig) -> Self {
        Self {
            max_per_room: cfg.media.signaling_max_per_room,
            max_frame_bytes: cfg.media.signaling_max_frame_bytes,
            max_connections: std::env::var("QM_SIGNAL_MAX_CONNECTIONS")
                .ok()
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(4000),
        }
    }

    /// 连接数上限门禁：`max_connections = 0` 表示不限制。
    fn reject_by_capacity(max_connections: usize, accepted: u64) -> bool {
        max_connections > 0 && accepted as usize > max_connections
    }
}

/// 一条已鉴权的信令连接。
#[derive(Debug)]
pub struct Session {
    /// 房间内的 peer 名（来自 URL `?peer=`）。
    pub peer: String,
    /// 握手阶段 JWT 校验出来的身份。
    pub identity: Identity,
    /// 同一 peer 的并发连接序号（重连递增）。
    pub generation: u64,
    /// 广播出口（clone 出来发）。
    pub tx: tokio::sync::broadcast::Sender<String>,
}

/// 连接注册表 + 广播通道。
///
/// 以 **peer** 为键（不是连接 id）：协议层的 fan-out 只认 peer 名。同一 peer
/// 可以有并发连接（多标签页 / 多房间），全部收到同一帧。
#[derive(Debug, Default)]
pub struct WsRegistry {
    sessions: Mutex<HashMap<String, Vec<Session>>>,
    /// 已接受的连接总数（含已断开），与协议层 `WsState::connections` 口径不同。
    connections: Mutex<u64>,
}

impl WsRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册一条会话。
    pub fn register(&self, session: Session) {
        self.sessions
            .lock()
            .entry(session.peer.clone())
            .or_default()
            .push(session);
    }

    /// 按 generation 精确移除一条会话，返回移除后该 peer 是否已无连接。
    ///
    /// 用 `remove_if` 语义而不是"找到就删"：重连后旧连接的清理不能把新连接
    /// 误删。
    pub fn remove(&self, peer: &str, generation: u64) -> bool {
        let mut map = self.sessions.lock();
        let Some(list) = map.get_mut(peer) else {
            return true;
        };
        if let Some(pos) = list.iter().position(|s| s.generation == generation) {
            list.remove(pos);
        }
        if list.is_empty() {
            map.remove(peer);
            true
        } else {
            false
        }
    }

    /// 把广播帧送给 `peer` 的所有连接。返回 `Err` 表示至少有一个接收方已死。
    pub fn send(&self, peer: &str, frame: &str) -> Result<(), String> {
        let guard = self.sessions.lock();
        let Some(list) = guard.get(peer) else {
            // 目标不存在：正常情况（对方已 leave / 已断），不是错误。
            return Ok(());
        };
        let mut lagged = false;
        for s in list {
            if s.tx.send(frame.to_string()).is_err() {
                lagged = true;
            }
        }
        if lagged {
            Err("广播队列已满（接收方可能已断开）".to_string())
        } else {
            Ok(())
        }
    }

    /// 当前活跃连接数。
    pub fn count(&self) -> usize {
        self.sessions.lock().values().map(|v| v.len()).sum()
    }

    /// 已接受的连接总数（含已断开）。
    pub fn connections_seen(&self) -> u64 {
        *self.connections.lock()
    }

    /// 记一次已接受的连接。
    pub fn note_connection(&self) {
        *self.connections.lock() += 1;
    }
}

/// 启动 WebSocket（WSS）信令服务。
///
/// 独立于 HTTP 信令（8081）、ICE 网关（8083）与 SFU 媒体（8080）监听端口，
/// 满足"信令服务独立部署，与 SFU 服务解耦"。`Ctrl+C` 优雅退出。
pub async fn start_ws(cfg: Arc<qm_common::AppConfig>, router: WsRouter) -> QmResult<()> {
    run_server(cfg, router, true).await
}

/// [`start_ws`] 的实现，`accept_ctrl_c = false` 时在测试里跑，不受终端信号影响。
pub async fn run_server(
    cfg: Arc<qm_common::AppConfig>,
    router: WsRouter,
    accept_ctrl_c: bool,
) -> QmResult<()> {
    // QM-004 强制约束：信令通道必须走 WSS。明文启动路径直接拒绝 —— 配置层
    // 不拦这条，因为媒体 / 集群服务不需要 WSS，拦在这里才不会误伤别的进程。
    if !cfg.auth.wss_required() {
        return Err(QmError::config(
            "auth.tls.enabled = false：QM-004 强制信令通道走 WSS，禁止明文传输 \
             （请设 auth.tls.enabled = true）",
        ));
    }
    let bind_host = if cfg.auth.tls.bind_host.trim().is_empty() {
        cfg.network.bind_host.clone()
    } else {
        cfg.auth.tls.bind_host.clone()
    };
    let addr: SocketAddr = format!("{}:{}", bind_host, cfg.media.signaling_ws_port)
        .parse()
        .map_err(|e| QmError::config(format!("WS 信令监听地址非法：{e}")))?;

    let tls = auth::load_tls_config(&cfg.auth.tls)?;
    let verifier = JwtVerifier::new(&cfg.auth);
    if !cfg.auth.jwt_active() {
        tracing::warn!(
            "auth.enabled = false：JWT 鉴权未生效（QM-004 要求私有化部署必须打开，仅本地联调可用）"
        );
    }
    let limits = Limits::from_config(&cfg);
    let registry = Arc::new(WsRegistry::new());
    let acceptor = TlsAcceptor::from(tls.config);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| QmError::signaling(format!("WS 信令绑定 {addr} 失败：{e}")))?;

    if tls.self_signed {
        tracing::warn!(
            "WS 信令使用自动生成的自签证书（SAN 含 localhost / 127.0.0.1）；浏览器会提示不受信任，私有化交付请替换为受信任 CA 证书"
        );
    }
    tracing::info!(
        %addr,
        tls = %tls.min_tls_version,
        jwt = verifier.enabled(),
        max_connections = limits.max_connections,
        max_per_room = limits.max_per_room,
        "WS 信令（WSS）服务启动：无有效 JWT 的连接在握手阶段拒绝，不会收到任何会议信息"
    );

    // 握手参数每条连接都一样，提到循环外只构造一次。
    let ws_cfg = WebSocketConfig::default()
        .max_message_size(Some(limits.max_frame_bytes))
        .max_frame_size(Some(limits.max_frame_bytes));
    let handshake_timeout = Duration::from_secs(ACCEPT_TIMEOUT_SECS);
    let send_timeout = Duration::from_secs(SEND_TIMEOUT_SECS);

    // 优雅退出监听器：生产进程跟 Ctrl+C；单测里传 `false` 让它永不就绪，
    // 这样 `run_server` 可以被 spawn 后直接驱动。两个分支的类型不同（`Pending`
    // 与 `Fuse<…>`），统一成一个 `Send + FusedFuture` 的 trait 对象，让
    // `select!` 的 `&mut` 分支能同时接受两者。
    let mut shutdown: Pin<Box<dyn Send + FusedFuture<Output = ()>>> = if accept_ctrl_c {
        Box::pin(CtrlCEnd::new())
    } else {
        Box::pin(NeverEnd)
    };

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("收到 Ctrl+C，WS 信令服务退出");
                break;
            }
            accepted = listener.accept() => {
                let (tcp, peer_addr) = match accepted {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "WS 信令 accept 失败");
                        continue;
                    }
                };
                // 半开连接：TLS 握手必须有界，否则 accept 循环会被慢客户端占住。
                let tls_stream = match tokio::time::timeout(handshake_timeout, acceptor.accept(tcp))
                    .await
                {
                    Err(_) => {
                        tracing::warn!(%peer_addr, "TLS 握手超时（明文 ws 客户端在此被拒绝）");
                        continue;
                    }
                    Ok(Err(e)) => {
                        // 明文 ws:// 客户端走不到 TLS 握手 —— 这正是"强制 WSS"的效果。
                        tracing::warn!(%peer_addr, reason = %e, "TLS 握手失败（可能是明文 ws 客户端）");
                        continue;
                    }
                    Ok(Ok(s)) => s,
                };

                let slot: HandshakeSlot = Arc::new(Mutex::new(None));
                let handshake = tokio::time::timeout(
                    handshake_timeout,
                    tokio_tungstenite::accept_hdr_async_with_config(
                        tls_stream,
                        auth_callback(verifier.clone(), slot.clone()),
                        Some(ws_cfg),
                    ),
                )
                .await;
                let mut ws = match handshake {
                    Err(_) => {
                        tracing::warn!(%peer_addr, "WebSocket 握手超时");
                        continue;
                    }
                    Ok(Err(e)) => {
                        // 401 分支：拒绝响应已经写出去了，不需要再写任何东西。
                        tracing::warn!(
                            %peer_addr,
                            status = ?handshake_reject_status(&e),
                            "WebSocket 握手被拒（无效 / 缺失 JWT，101 未发出）"
                        );
                        continue;
                    }
                    Ok(Ok(ws)) => ws,
                };
                let Some((peer, generation, identity)) = slot.lock().take() else {
                    // 理论上到不了：回调成功就会写 slot。防御性分支，避免 unwrap panic。
                    tracing::error!(%peer_addr, "握手通过但未取到身份，断开连接");
                    let _ = ws
                        .close(Some(ws_close(CloseCode::Protocol, "握手状态异常".to_string())))
                        .await;
                    continue;
                };

                registry.note_connection();
                let total = registry.connections_seen();
                if Limits::reject_by_capacity(limits.max_connections, total) {
                    tracing::warn!(
                        %peer_addr,
                        %peer,
                        total,
                        max = limits.max_connections,
                        "信令连接数超限，拒绝"
                    );
                    let _ = ws
                        .close(Some(ws_close(
                            CloseCode::Again,
                            format!("连接数已达上限 {}", limits.max_connections),
                        )))
                        .await;
                    continue;
                }

                // 身份来自握手阶段（JWT 校验结果），**不信任**客户端在帧里
                // 自填的 `peer` —— 后者只是房间内的显示名。鉴权关闭时回落到
                // peer 名，行为与旧客户端一致。
                let identity = if cfg.auth.jwt_active() {
                    identity
                } else {
                    Identity {
                        subject: peer.clone(),
                        display: peer.clone(),
                    }
                };
                router.set_identity(&peer, identity.clone());
                let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
                registry.register(Session {
                    peer: peer.clone(),
                    identity,
                    generation,
                    tx,
                });
                let stats = router.stats();
                tracing::info!(
                    %peer_addr,
                    %peer,
                    generation,
                    active = registry.count(),
                    rooms = stats.get("rooms").and_then(serde_json::Value::as_u64).unwrap_or(0),
                    "WS 信令连接建立"
                );
                tokio::spawn(dispatch_session(
                    ws,
                    router.clone(),
                    registry.clone(),
                    limits.clone(),
                    peer,
                    generation,
                    send_timeout,
                ));
            }
        }
    }
    Ok(())
}

/// 单条已建立连接：收帧 → [`WsRouter::dispatch`] → 回包 + 广播；同时跑 ping 保活
/// 与发送超时，避免死连接占住内存（验收标准 4：1000 并发无泄漏）。
async fn dispatch_session(
    mut ws: tokio_tungstenite::WebSocketStream<tokio_rustls::server::TlsStream<TcpStream>>,
    router: WsRouter,
    registry: Arc<WsRegistry>,
    limits: Limits,
    peer: String,
    generation: u64,
    send_timeout: Duration,
) {
    let start = std::time::Instant::now();
    let mut reason = CloseReason::Normal;
    let mut ping_ticker = tokio::time::interval(Duration::from_secs(PING_INTERVAL_SECS));
    ping_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            frame = ws.next() => {
                let Some(frame) = frame else { break; };
                match frame {
                    Err(e) => {
                        tracing::debug!(peer = %peer, error = %e, "WS 读失败");
                        reason = CloseReason::ReadError;
                        break;
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(Message::Ping(p)) => {
                        if !send_in_time(
                            &mut ws,
                            Message::Pong(p),
                            send_timeout,
                        )
                        .await
                        {
                            tracing::debug!(peer = %peer, "WS pong 发送失败");
                            reason = CloseReason::Dead;
                            break;
                        }
                    }
                    Ok(Message::Pong(_)) => continue,
                    Ok(Message::Text(text)) => {
                        let raw = text.as_str().to_string();
                        if raw.len() > limits.max_frame_bytes {
                            tracing::warn!(
                                peer = %peer,
                                bytes = raw.len(),
                                limit = limits.max_frame_bytes,
                                "WS 帧过大，断开"
                            );
                            reason = CloseReason::FrameTooLarge;
                            let _ = ws
                                .close(Some(ws_close(
                                    CloseCode::Size,
                                    format!(
                                        "信令帧超过 {} 字节上限",
                                        limits.max_frame_bytes
                                    ),
                                )))
                                .await;
                            break;
                        }
                        // 房间上限在协议层（`WsRouter::join`）返回 ROOM_FULL；
                        // 这里只负责把错误应答发回去。
                        let res = router.dispatch(&raw);
                        if let Some(reply) = res.reply {
                            let Ok(body) = serde_json::to_string(&reply) else {
                                tracing::warn!(peer = %peer, "应答序列化失败");
                                continue;
                            };
                            if !send_in_time(
                                &mut ws,
                                Message::Text(body.into()),
                                send_timeout,
                            )
                            .await
                            {
                                tracing::debug!(peer = %peer, "WS 发送失败");
                                reason = CloseReason::Dead;
                                break;
                            }
                        }
                        for b in res.broadcasts {
                            let Ok(body) = serde_json::to_string(&b) else {
                                tracing::warn!(peer = %peer, "广播帧序列化失败");
                                continue;
                            };
                            let Some(target) = b.get("to").and_then(serde_json::Value::as_str) else {
                                continue;
                            };
                            if let Err(e) = registry.send(target, &body) {
                                tracing::debug!(peer = %peer, target = %target, error = %e, "广播投递失败（发送方可能已断开）");
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        // 信令只收 JSON 文本帧；二进制帧按协议错误回，不断开（兼容探针）。
                        tracing::warn!(peer = %peer, "收到二进制帧，信令只接受 JSON 文本帧");
                        let reply = serde_json::json!({
                            "ok": false,
                            "code": "BAD_REQUEST",
                            "type": "?",
                            "error": "信令只接受 JSON 文本帧（WebSocket text frame）",
                        });
                        if !send_in_time(
                            &mut ws,
                            Message::Text(
                                serde_json::to_string(&reply).unwrap_or_default().into()
                            ),
                            send_timeout,
                        )
                        .await
                        {
                            tracing::debug!(peer = %peer, "WS 发送失败");
                            reason = CloseReason::Dead;
                            break;
                        }
                        continue;
                    }
                    Ok(Message::Frame(f)) => {
                        tracing::debug!(peer = %peer, ?f, "收到原始帧，忽略");
                        continue;
                    }
                }
            }
            _ = ping_ticker.tick() => {
                if !send_in_time(
                    &mut ws,
                    Message::Ping(Vec::new().into()),
                    send_timeout,
                )
                .await
                {
                    tracing::warn!(peer = %peer, "WS 心跳失败，连接已死");
                    reason = CloseReason::Dead;
                    break;
                }
            }
        }
    }

    // 断开清理：按 generation 精确移除，避免旧连接的清理误删重连后的新连接。
    let all_gone = registry.remove(&peer, generation);
    if all_gone {
        // 该 peer 没有别的连接了，从成员表里摘掉并广播 peer_left ——
        // 否则其他人会一直等它的 SDP / candidate，表现为"人在但没声音"。
        let rooms = router.forget_peer(&peer);
        for room in &rooms {
            let frame = serde_json::json!({
                "ok": true,
                "type": "peer_left",
                "room": room,
                "peer": peer,
            });
            for t in router.fan_out(room, &peer, &frame) {
                if let Err(e) = registry.send(&t, &frame.to_string()) {
                    tracing::debug!(peer = %peer, target = %t, error = %e, "peer_left 广播失败");
                }
            }
        }
    }

    let stats = router.stats();
    tracing::info!(
        %peer,
        generation,
        reason = reason.as_str(),
        duration_ms = start.elapsed().as_millis(),
        active = registry.count(),
        connections = registry.connections_seen(),
        rooms = stats.get("rooms").and_then(serde_json::Value::as_u64).unwrap_or(0),
        sdp = stats.get("sdpRelayed").and_then(serde_json::Value::as_u64).unwrap_or(0),
        ice = stats.get("iceRelayed").and_then(serde_json::Value::as_u64).unwrap_or(0),
        "WS 信令连接关闭"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::JwtVerifier;
    use crate::ws::{Identity, WsRouter};
    use http1::{StatusCode, Uri};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    /// 测试专用高端口：只被本 crate 的单测占用，避开生产 8082。
    const TEST_PORT: u16 = 18083;

    /// 一个最小化但通过校验的配置：回环绑定 + 测试端口。
    fn test_config() -> qm_common::AppConfig {
        let mut cfg = qm_common::AppConfig::default();
        cfg.network.bind_host = "127.0.0.1".to_string();
        cfg.media.signaling_ws_port = TEST_PORT;
        cfg.validate().expect("测试配置必须通过校验");
        cfg
    }

    fn cfg_arc() -> Arc<qm_common::AppConfig> {
        Arc::new(test_config())
    }

    fn ws_router(cfg: Arc<qm_common::AppConfig>) -> WsRouter {
        WsRouter::new(crate::SignalRouter::new(cfg))
    }

    /// 手工构造握手请求（tungstenite 没有公开的测试构造器）。
    fn server_request(path_query: &str) -> Request {
        let mut r = http1::Request::new(());
        *r.uri_mut() = path_query.parse().expect("测试 URI 合法");
        r
    }

    /// 一次带超时的 WSS 连接尝试：`Ok(response)` 表示拿到了 101，
    /// `Err(String)` 表示被拒（含超时），断言只看 `is_err()`。
    async fn try_connect(
        path_query: &str,
        auth_header: Option<&str>,
        wait: Duration,
    ) -> Result<http1::Response<()>, String> {
        tokio::time::timeout(wait, wss_client(path_query, auth_header)).await.map_err(|_| {
            "等待超时：连接未在期限内被接受或被拒绝".to_string()
        })?
    }

    /// 建一个走真实 TLS 的 WSS 客户端（测试用：跳过证书校验）。
    ///
    /// `auth_header` 非空时把 token 放在 `Authorization: Bearer` 头里 ——
    /// 浏览器 `WebSocket` API 设不了自定义头，这条路只供 curl / 自写客户端。
    async fn wss_client(
        path_query: &str,
        auth_header: Option<&str>,
    ) -> Result<http1::Response<()>, String> {
        // tungstenite 0.29 没有 `client::Builder`（`connect` 只在开了 `*-tls` 特性时才导出），
        // 所以手工拼握手请求：`wss` URI 的 authority 给 `Host`，端口给 `ServerName`。
        let url = format!("wss://127.0.0.1:{TEST_PORT}{path_query}");
        let uri: Uri = url.parse().expect("测试 URL 合法");
        let host = uri
            .authority()
            .and_then(|a| a.host().split_once(':'))
            .map(|(h, _)| h.to_string())
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let mut req = tungstenite::client::ClientRequestBuilder::new(uri)
            .with_header("Host", format!("{host}:{TEST_PORT}"));
        if let Some(h) = auth_header {
            req = req.with_header("Authorization", format!("Bearer {h}"));
        }
        let req = req
            .into_client_request()
            .map_err(|e| format!("构造请求失败：{e}"))?;

        let socket = TcpStream::connect(("127.0.0.1", TEST_PORT))
            .await
            .map_err(|e| format!("TCP 连接失败：{e}"))?;
        let connector = tokio_rustls::TlsConnector::from(Arc::new(
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(TestCertVerifier))
                .with_no_client_auth(),
        ));
        let tls = connector
            .connect(rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap(), socket)
            .await
            .map_err(|e| format!("TLS 握手失败：{e}"))?;
        let (_ws, response) = tokio_tungstenite::client_async_with_config(req, tls, None)
            .await
            .map_err(|e| format!("WSS 握手失败：{e}"))?;
        // 客户端握手返回的 body 是 `Option<Vec<u8>>`（升级请求多出来的字节），
        // 本测试只关心状态码：是否真的拿到了 `101 Switching Protocols`。
        Ok(http1::Response::builder()
            .status(response.status())
            .body(())
            .expect("静态响应一定能构造"))
    }

    #[test]
    fn query_get_parses_peer_and_gen() {
        assert_eq!(
            query_get(Some("peer=alice&gen=2"), "peer").as_deref(),
            Some("alice")
        );
        assert_eq!(query_get(Some("peer=alice&gen=2"), "gen"), Some("2".to_string()));
        // 百分号解码
        assert_eq!(
            query_get(Some("peer=192.168.0.10%3A1"), "peer").as_deref(),
            Some("192.168.0.10:1")
        );
        // 空白值不算给了 peer
        assert_eq!(query_get(Some("peer=%20&gen=1"), "peer"), None);
        assert_eq!(query_get(None, "peer"), None);
        assert_eq!(query_get(Some("room=m"), "peer"), None);
        // 同一 key 取第一个
        assert_eq!(query_get(Some("peer=a&peer=b"), "peer").as_deref(), Some("a"));
    }

    #[test]
    fn error_response_is_401_and_keeps_request_version() {
        let request = server_request("/?peer=alice");
        let r = error_response(&request, "missing token");
        assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
        // tungstenite 把 2xx 的自定义响应当成放行，这里必须是 4xx。
        assert!(!r.status().is_success());
        assert_eq!(r.version(), http1::Version::HTTP_11);
        assert!(
            r.headers().get("connection").is_some(),
            "拒绝响应要显式 connection: close"
        );
    }

    #[test]
    fn registry_send_is_noop_for_unknown_peer() {
        let reg = WsRegistry::new();
        assert!(reg.send("nobody", "{}").is_ok(), "目标不存在不应算错误");
        assert_eq!(reg.count(), 0);
        assert_eq!(reg.connections_seen(), 0);

        reg.note_connection();
        reg.note_connection();
        assert_eq!(reg.connections_seen(), 2);
    }

    #[test]
    fn registry_send_reaches_all_connections_of_a_peer() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let reg = Arc::new(WsRegistry::new());
            let mut sessions = Vec::new();
            for gen in [0u64, 1u64, 2u64] {
                let (tx, rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
                reg.register(Session {
                    peer: "alice".to_string(),
                    identity: Identity {
                        subject: "alice".to_string(),
                        display: "alice".to_string(),
                    },
                    generation: gen,
                    tx,
                });
                sessions.push(rx);
            }
            assert_eq!(reg.count(), 3);
            assert!(reg.send("alice", "{\"ok\":true}").is_ok());
            // 三条连接都收到
            assert!(sessions[0].recv().await.is_ok());
            assert!(sessions[1].recv().await.is_ok());
            assert!(sessions[2].recv().await.is_ok());
        });
    }

    #[test]
    fn registry_remove_is_generation_scoped() {
        let reg = WsRegistry::new();
        let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
        let (tx2, _rx2) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
        reg.register(Session {
            peer: "alice".to_string(),
            identity: Identity {
                subject: "alice".to_string(),
                display: "alice".to_string(),
            },
            generation: 0,
            tx,
        });
        reg.register(Session {
            peer: "alice".to_string(),
            identity: Identity {
                subject: "alice".to_string(),
                display: "alice".to_string(),
            },
            generation: 1,
            tx: tx2,
        });
        // 只删掉 gen=0：gen=1 的新连接必须还在。
        assert!(!reg.remove("alice", 0));
        assert_eq!(reg.count(), 1);
        assert!(reg.send("alice", "{}").is_ok());
        assert!(reg.remove("alice", 1));
        assert_eq!(reg.count(), 0);
    }

    #[test]
    fn registry_remove_reports_last_connection() {
        let reg = WsRegistry::new();
        let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
        reg.register(Session {
            peer: "bob".to_string(),
            identity: Identity {
                subject: "bob".to_string(),
                display: "bob".to_string(),
            },
            generation: 0,
            tx,
        });
        // 唯一一条连接断开 → 该 peer 已无连接。
        assert!(reg.remove("bob", 0));
        assert!(reg.send("bob", "{}").is_ok());
        assert_eq!(reg.count(), 0);
    }

    #[test]
    fn capacity_gate_allows_up_to_limit_and_blocks_over() {
        // 验收标准 4 的连接数上限口径：超过上限的连接必须被拒。
        assert!(!Limits::reject_by_capacity(0, 1 << 40), "0 = 不限制");
        assert!(!Limits::reject_by_capacity(3, 3));
        assert!(Limits::reject_by_capacity(3, 4));
        assert!(
            !Limits::reject_by_capacity(4000, 1000),
            "默认上限必须容纳验收的 1000 并发"
        );
    }

    #[test]
    fn limits_are_derived_from_config() {
        let cfg = qm_common::AppConfig::default();
        let l = Limits::from_config(&cfg);
        assert_eq!(l.max_per_room, cfg.media.signaling_max_per_room);
        assert_eq!(l.max_frame_bytes, cfg.media.signaling_max_frame_bytes);
        assert!(
            l.max_connections > 1000,
            "默认连接上限必须覆盖验收口径的 1000 并发"
        );
    }

    #[test]
    fn close_reason_labels_are_stable() {
        assert_eq!(CloseReason::Normal.as_str(), "normal");
        assert_eq!(CloseReason::ReadError.as_str(), "read_error");
        assert_eq!(CloseReason::FrameTooLarge.as_str(), "frame_too_large");
        assert_eq!(CloseReason::Dead.as_str(), "dead");
    }

    /// 明文启动路径必须被拒绝：QM-004 禁止信令明文传输。
    #[test]
    fn plaintext_signaling_is_refused_at_startup() {
        let mut cfg = test_config();
        cfg.auth.tls.enabled = false;
        let cfg = Arc::new(cfg);
        let router = ws_router(cfg.clone());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let res = rt.block_on(run_server(cfg, router, false));
        assert!(res.is_err(), "auth.tls.enabled = false 必须拒绝启动");
        assert!(
            res.unwrap_err().to_string().contains("WSS"),
            "错误信息要说明是 WSS 强制要求"
        );
    }

    /// 地址选择：`auth.tls.bind_host` 为空时回落 `network.bind_host`。
    #[test]
    fn binding_address_falls_back_to_network_bind_host() {
        let cfg = cfg_arc();
        let chosen = if cfg.auth.tls.bind_host.trim().is_empty() {
            cfg.network.bind_host.clone()
        } else {
            cfg.auth.tls.bind_host.clone()
        };
        assert_eq!(chosen, "127.0.0.1");
        let addr: SocketAddr = format!("{chosen}:{}", cfg.media.signaling_ws_port)
            .parse()
            .expect("监听地址必须可解析");
        assert_eq!(addr.port(), TEST_PORT);
    }

    /// 握手门禁的纯函数部分：有 peer + 合法 token 才放行，slot 只在放行时写。
    ///
    /// `sign` 只在 `dev-sign` / `test` 下存在，因此本测试与整个 `tests` 模块
    /// 都受这个 cfg 约束。
    #[cfg(any(test, feature = "dev-sign"))]
    #[test]
    fn handshake_gate_accepts_valid_and_rejects_missing_and_invalid() {
        let cfg = cfg_arc();
        let verifier = JwtVerifier::new(&cfg.auth);
        let token = verifier.sign("alice", "Alice").expect("测试配置应可签发 token");
        let pass = || Response::builder().status(StatusCode::SWITCHING_PROTOCOLS).body(())
            .expect("放行响应一定能构造");

        // 无 token：拒绝，slot 不写。
        let slot = Arc::new(Mutex::new(None));
        let r = auth_callback(verifier.clone(), slot.clone())
            .on_request(&server_request("/?peer=alice"), pass());
        assert_eq!(r.unwrap_err().status(), StatusCode::UNAUTHORIZED);
        assert!(slot.lock().is_none(), "拒绝时不能写入身份");

        // 无效 token：拒绝。
        let slot = Arc::new(Mutex::new(None));
        let r = auth_callback(verifier.clone(), slot.clone()).on_request(
            &server_request("/?peer=alice&token=not-a-jwt"),
            pass(),
        );
        assert_eq!(r.unwrap_err().status(), StatusCode::UNAUTHORIZED);
        assert!(slot.lock().is_none());

        // 缺 peer：拒绝（浏览器一定会带）。
        let r = auth_callback(
            verifier.clone(),
            Arc::new(Mutex::new(None)),
        )
        .on_request(&server_request(&format!("/?token={token}")), pass());
        assert_eq!(r.unwrap_err().status(), StatusCode::UNAUTHORIZED);

        // 合法 token：放行，slot 写入 peer / gen / identity。
        let slot = Arc::new(Mutex::new(None));
        let r = auth_callback(verifier.clone(), slot.clone()).on_request(
            &server_request(&format!("/?peer=alice&gen=7&token={token}")),
            pass(),
        );
        assert_eq!(r.unwrap().status(), StatusCode::SWITCHING_PROTOCOLS);
        let Some((peer, gen, identity)) = slot.lock().clone() else {
            panic!("放行后必须写入身份");
        };
        assert_eq!(peer, "alice");
        assert_eq!(gen, 7);
        assert_eq!(identity.subject, "alice");

        // Authorization 头这条路也要通（curl / 自写客户端）。
        let slot = Arc::new(Mutex::new(None));
        let mut req = server_request("/?peer=alice");
        req.headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse().unwrap());
        let r = auth_callback(verifier, slot.clone()).on_request(&req, pass());
        assert_eq!(r.unwrap().status(), StatusCode::SWITCHING_PROTOCOLS);
        assert!(slot.lock().is_some());
    }

    /// 测试用 TLS 校验器：**不校验证书**。
    ///
    /// 测试里跑的是自签证书 + 回环地址 + 单进程，跳过校验不影响任何生产路径；
    /// 保留真实 TLS 握手（ServerHello 协商 + 后续帧加密）才是本测试要覆盖的。
    /// 这个类型只存在于 `#[cfg(test)]`，不会进入产物。
    #[derive(Debug)]
    struct TestCertVerifier;

    impl rustls::client::danger::ServerCertVerifier for TestCertVerifier {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp_response: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<
            rustls::client::danger::HandshakeSignatureValid,
            rustls::Error,
        > {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &rustls::pki_types::CertificateDer<'_>,
            _dss: &rustls::DigitallySignedStruct,
        ) -> std::result::Result<
            rustls::client::danger::HandshakeSignatureValid,
            rustls::Error,
        > {
            Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            vec![
                rustls::SignatureScheme::RSA_PKCS1_SHA256,
                rustls::SignatureScheme::RSA_PSS_SHA256,
                rustls::SignatureScheme::RSA_PSS_SHA384,
                rustls::SignatureScheme::RSA_PSS_SHA512,
                rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
                rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
                rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
                rustls::SignatureScheme::ED25519,
            ]
        }
    }

    /// 验收标准 3 的端到端证明：没有有效 JWT 的连接在**握手阶段**被拒，
    /// `101 Switching Protocols` 不会发出，连接根本不会成为 WebSocket。
    #[cfg(any(test, feature = "dev-sign"))]
    #[tokio::test]
    async fn wss_handshake_rejects_missing_and_invalid_token() {
        let cfg = cfg_arc();
        let server = tokio::spawn(run_server(cfg.clone(), ws_router(cfg.clone()), false));

        // 等服务真正开始 accept：连得上说明 TLS 监听已就绪。
        let mut up = false;
        for _ in 0..30 {
            if TcpStream::connect(("127.0.0.1", TEST_PORT))
                .await
                .is_ok()
            {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(up, "WSS 信令服务 3s 内未就绪");

        let wait = Duration::from_secs(10);

        // 无 token：握手阶段被拒，拿不到 101。
        let missing = try_connect("/?peer=alice", None, wait).await;
        assert!(
            missing.is_err(),
            "无 token 的连接必须被拒，实际：{:?}",
            missing
        );

        // 无效 token：同样被拒。
        let invalid = try_connect("/?peer=alice&token=not-a-jwt", None, wait).await;
        assert!(
            invalid.is_err(),
            "无效 token 的连接必须被拒，实际：{:?}",
            invalid
        );

        let verifier = JwtVerifier::new(&cfg.auth);
        let token = verifier.sign("alice", "Alice").expect("测试配置应可签发 token");

        // 缺 peer：拒绝（不接受无名字的连接）。
        let no_peer = try_connect(&format!("/?token={token}"), None, wait).await;
        assert!(
            no_peer.is_err(),
            "缺 peer 的连接必须被拒，实际：{:?}",
            no_peer
        );

        // 只带 peer 不带任何 token：被拒。
        let bare = try_connect("/?peer=bob", None, wait).await;
        assert!(
            bare.is_err(),
            "只带 peer 不带任何 token 的连接必须被拒，实际：{:?}",
            bare
        );

        // 有效 token（query 这条路，浏览器用）：必须能完成 101。
        let ok = try_connect(&format!("/?peer=alice&token={token}"), None, wait).await;
        assert!(
            ok.is_ok(),
            "有效 token 的连接必须通过 WSS 握手：{:?}",
            ok
        );
        assert_eq!(ok.unwrap().status(), StatusCode::SWITCHING_PROTOCOLS);

        // 有效 token 放在 Authorization 头（curl / 自写客户端这条路）：必须通过。
        let hdr_ok = try_connect("/?peer=bob", Some(&token), wait).await;
        assert!(
            hdr_ok.is_ok(),
            "Authorization: Bearer 这条路的连接必须通过 WSS 握手：{:?}",
            hdr_ok
        );
        assert_eq!(hdr_ok.unwrap().status(), StatusCode::SWITCHING_PROTOCOLS);

        server.abort();
    }

    /// 明文客户端连 WSS 端口必然失败：服务端只有 TLS 监听器，
    /// 明文字节在 TLS 握手阶段就被拒绝（客户端收不到任何应用层帧）。
    #[cfg(any(test, feature = "dev-sign"))]
    #[tokio::test]
    async fn plaintext_client_cannot_speak_to_the_wss_listener() {
        let cfg = cfg_arc();
        let router = ws_router(cfg.clone());
        let server = tokio::spawn(run_server(cfg, router, false));
        let mut up = false;
        for _ in 0..30 {
            if TcpStream::connect(("127.0.0.1", TEST_PORT))
                .await
                .is_ok()
            {
                up = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(up, "WSS 信令服务 3s 内未就绪");

        // 直接把明文 HTTP/1.1 upgrade 请求喂给 TLS 监听器：rustls 会在
        // ClientHello 解析处失败，连接被关闭，读端拿到 EOF 或错误。
        let mut socket = TcpStream::connect(("127.0.0.1", TEST_PORT))
            .await
            .expect("TCP 层应可连上（TLS 握手在下一层拒绝）");
        let plain = b"GET /?peer=alice HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n";
        socket.write_all(plain).await.unwrap();
        let mut buf = [0u8; 64];
        let n = tokio::time::timeout(Duration::from_secs(5), socket.read(&mut buf))
            .await
            .expect("5s 内应有应答或连接关闭");
        // EOF（对端关闭）或明确错误都算通过；长时间挂住（超时会 panic）才是失败。
        let got = match n {
            Ok(n) => n,
            Err(_) => 0,
        };
        assert!(
            got < 100,
            "明文客户端最多只能拿到 TLS 告警字节，拿不到任何 HTTP / WS 应用层应答，实际读回 {got} 字节"
        );
        server.abort();
    }
}

