//! ICE 网关：浏览器 ↔ QM-SFU 的 SDP/ICE 互换端点。
//!
//! 角色：信令只做「谁把 SDP 交给谁」，媒体仍走 ICE 直连。一次互通会话里正好两方
//! （一个浏览器 tab + 一个 SFU 节点），网关按房间把它们配对，互为对端：
//! ```text
//!   POST /session/register               → { sid, peers: [...] }
//!   POST /session/{sid}/sdp   (提交)      → { sdp: <对端 SDP 或 null> }
//!   GET  /session/{sid}/sdp   (拉取)      → { sdp, kind }        ← 轮询用
//!   POST /session/{sid}/ice   (提交)      → { candidates: [对端] }
//!   GET  /session/{sid}/ice   (自查)      → { candidates: [自己] }
//!   GET  /session/{sid}/peer-ice          → { candidates: [对端] }  ← offerer 轮询用
//!   GET  /session/{sid}/summary           → { session, peer }
//!   GET  /sessions                          → 全部会话（按房间）
//!   GET  / /interop/index.html             → 本地验证页
//!   GET  /healthz                           → 存活
//!   *   /room/*                            → 转发给 SignalRouter（同一套准入）
//! ```
//
//! 私有化约束（Epic 全局约束 3/5）：
//! * 只监听配置的 `network.bind_host`（内网 IPv4），端口 = 信令端口 + 2；
//! * `iceServers` 恒为空 —— 内网 host candidate 直连，不接触公网 STUN/TURN；
//! * 只存 SDP 文本与 candidate 字符串，不落地任何媒体字节，会话在进程内存里
//!   按 [`SESSION_TTL`] 回收。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use http::{Method, Request, Response, StatusCode};
use parking_lot::Mutex;
use qm_common::error::{Error, Result as QmResult};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::SignalRouter;

/// 网关路径前缀（与信令的 `/room` 隔离，避免路由冲突）。
pub const SESSION_PREFIX: &str = "/session";
/// 会话空闲超时（无请求即回收，防止长跑内存累积）。
pub const SESSION_TTL: Duration = Duration::from_secs(300);
/// 本地静态页根目录（相对 workspace 根）。
pub const PAGE_ROOT: &str = "web/interop/index.html";

/// 一次互通会话的状态。
#[derive(Debug)]
struct Session {
    sid: String,
    role: String,
    room: String,
    /// 本方已提交的 SDP（`kind` 为 `offer` 或 `answer`）。
    ///
    /// 只有一个桶：对端直接读它。`POST /session/{sid}/sdp` 返回对端桶的副本，
    /// `GET /session/{sid}/sdp` 返回对端桶的副本 —— 两者语义一致，不需要再维护
    /// 一个 `peer_sdp` 镜像。
    local_sdp: Option<(String, String)>,
    /// 本方产生的 ICE candidate（完整 init 对象：`candidate` + `sdpMid` +
    /// `sdpMLineIndex`），待对端拉取或从 `POST /ice` 的返回值里收到。
    ///
    /// 必须是对象而不是裸字符串：`pc.addIceCandidate()` 规范要求同时给
    /// `sdpMid` 与 `sdpMLineIndex`，只带 `candidate` 文本会被浏览器拒绝。
    local_ice: Vec<serde_json::Value>,
    /// 本方发布的键值元数据（如 SFU 侧的媒体起始墙钟时间戳）。
    ///
    /// 网关只搬 JSON，不解析语义；对端通过 `GET /session/{sid}/summary` 的
    /// `meta` 字段读到，用于跨端对齐时间（端到端延迟测量的锚点）。
    meta: serde_json::Map<String, serde_json::Value>,
    /// 本方连接的 TCP 对端地址（网关从 HTTP LocalPeerAddr 记录）。
    ///
    /// 浏览器侧的 ICE candidate 地址被 mDNS 遮蔽成 <uuid>.local，SFU 无法
    /// 反查；这个字段让 SFU 拿到浏览器的真实字面 IP，从而把 .local 候选改写回
    /// 可直接使用的地址（见 demos/qm-demo/src/interop.rs）。
    remote_addr: Option<SocketAddr>,
    /// 最后一次活动（TTL 回收用）。
    last_active: Instant,
}

impl Session {
    /// 新建会话（`last_active` 取当前时刻；`Instant` 没有 `Default`）。
    fn new() -> Self {
        Self {
            sid: String::new(),
            role: String::new(),
            room: String::new(),
            local_sdp: None,
            local_ice: Vec::new(),
            meta: serde_json::Map::new(),
            remote_addr: None,
            last_active: Instant::now(),
        }
    }

    fn touch(&mut self) {
        self.last_active = Instant::now();
    }
}

/// 会话摘要（报告与断言用）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SessionSummary {
    pub sid: String,
    pub role: String,
    pub room: String,
    pub has_sdp: bool,
    pub sdp_kind: Option<String>,
    pub sdp_bytes: usize,
    pub ice_candidates: u32,
}

impl Session {
    fn summary(&self) -> SessionSummary {
        SessionSummary {
            sid: self.sid.clone(),
            role: self.role.clone(),
            room: self.room.clone(),
            has_sdp: self.local_sdp.is_some(),
            sdp_kind: self.local_sdp.as_ref().map(|(k, _)| k.clone()),
            sdp_bytes: self.local_sdp.as_ref().map(|(_, v)| v.len()).unwrap_or(0),
            ice_candidates: self.local_ice.len() as u32,
        }
    }
}

/// 网关内存状态（`Arc` 包裹以便在 hyper 的 handler 闭包里克隆）。
#[derive(Default)]
pub struct IceGatewayState {
    sessions: Mutex<HashMap<String, Session>>,
    /// sid -> 本方连接的 TCP 对端地址。
    ///
    /// remote_addr 是 TCP socket 层面的事实，不依赖信令消息：浏览器自己报的
    /// 地址被 mDNS 遮蔽时，这里仍能拿到它真实的字面 IP。
    remote_addrs: Mutex<HashMap<String, SocketAddr>>,
    /// 房间 → 会话（按登记顺序，用于确定对端）。
    rooms: Mutex<HashMap<String, Vec<String>>>,
    /// 只监听这个内网地址（Epic 约束 3）。
    bind_host: String,
    /// 监听端口（信令端口 + 2）。
    listen_port: u16,
    /// 可选信令路由器：有则把 `/room/*` 转发给 [`SignalRouter`]（同一套准入校验），
    /// 没有则 `/room/*` 返回 502 —— 网关本身只负责 SDP 互换。
    router: Option<Arc<SignalRouter>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RegisterBody {
    room: String,
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "browser".to_string()
}

#[derive(Debug, Serialize, Deserialize)]
struct SdpBody {
    sdp: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct IceBody {
    /// 完整 init 对象（推荐，含 `sdpMid` / `sdpMLineIndex`），或兼容的裸
    /// `candidate` 字符串（旧客户端；会被包成 `{"candidate": ...}` 再存）。
    candidate: serde_json::Value,
}

/// 把 `POST /ice` 的 `candidate` 字段规整成完整 init 对象。
/// 把 candidate 归一成 JSON 对象（字符串形式会尽力解析，解析不了就包一层）。
/// WebSocket 信令（QM-004）复用同一份归一逻辑，保证两条信令通道语义一致。
pub fn normalize_candidate(v: &serde_json::Value) -> Result<serde_json::Value, String> {
    match v {
        serde_json::Value::Object(o) => Ok(serde_json::Value::Object(o.clone())),
        serde_json::Value::String(t) => {
            if t.trim().is_empty() {
                return Err("candidate 不能为空".to_string());
            }
            // 已经是完整 JSON 字符串也认（直接解析回对象）
            Ok(
                serde_json::from_str::<serde_json::Value>(t)
                    .map(|p| match p {
                        serde_json::Value::Object(o) => serde_json::Value::Object(o),
                        _ => serde_json::json!({ "candidate": t }),
                    })
                    .unwrap_or_else(|_| serde_json::json!({ "candidate": t })),
            )
        }
        _ => Err("candidate 必须是字符串或对象".to_string()),
    }
}

/// 网关里取出的 candidate 是否带 `sdpMid` / `sdpMLineIndex`（报告与断言用）。
pub fn candidate_is_complete(v: &serde_json::Value) -> bool {
    v.get("sdpMid").is_some() && v.get("sdpMLineIndex").is_some()
}

/// 把 ICE candidate 文本里的 mDNS 主机名（`<uuid>.local`）换成字面 IP。
///
/// `candidate:3228112346 1 udp 2113937151 <uuid>.local 59128 typ host ...`：
/// `split_whitespace` 后第 0 个是 `candidate:<foundation>`（注意 foundation 直接
/// 拼在冒号后面，不是空格分隔），第 4 个是主机名、第 5 个是端口。
/// 非 `.local` 候选原样返回 —— 只对 mDNS 遮蔽的地址做还原。
pub fn normalize_mdns_candidate(text: &str, ip: IpAddr) -> String {
    let toks: Vec<&str> = text.split_whitespace().collect();
    if toks.len() < 7 || !toks[0].starts_with("candidate:") {
        return text.to_string();
    }
    let host = toks[4];
    if !host.ends_with(".local") {
        return text.to_string();
    }
    // 只在被空白包围的位置替换主机名：mDNS 主机名由连字符与小写字母组成，
    // 朴素 `find` 可能命中优先级、端口等恰好子串相同的片段。
    // IPv6 地址不走这条路径——`host.ends_with(".local")` 已是前置条件。
    let mut rest = text;
    loop {
        let Some(pos) = rest.find(host) else {
            return text.to_string();
        };
        let before = pos == 0 || rest.as_bytes()[pos - 1].is_ascii_whitespace();
        let after = pos + host.len() >= rest.len()
            || rest.as_bytes()[pos + host.len()].is_ascii_whitespace();
        if before && after {
            let mut out = String::with_capacity(text.len() + 16);
            out.push_str(&rest[..pos]);
            out.push_str(&ip.to_string());
            out.push_str(&rest[pos + host.len()..]);
            return out;
        }
        rest = &rest[pos + 1..];
    }
}

impl IceGatewayState {
    pub fn new(router: Option<Arc<SignalRouter>>) -> Self {
        Self {
            bind_host: "127.0.0.1".to_string(),
            listen_port: crate::DEFAULT_SIGNALING_PORT + crate::ICE_GATEWAY_OFFSET,
            router,
            ..Default::default()
        }
    }

    /// 按配置构造：端口 = 信令端口 + 2，与信令 8081 / 媒体 8080 错开。
    pub fn from_config(
        cfg: &qm_common::AppConfig,
        router: Option<Arc<SignalRouter>>,
    ) -> Self {
        Self {
            bind_host: cfg.network.bind_host.clone(),
            listen_port: cfg.media.signaling_port.saturating_add(crate::ICE_GATEWAY_OFFSET),
            ..Self::new(router)
        }
    }

    /// 监听端口。
    pub fn listen_port(&self) -> u16 {
        self.listen_port
    }

    /// 本机互通验证专用构造：只监听回环、端口显式指定、内网准入白名单齐全。
    pub fn for_local(bind_host: String, port: u16) -> Arc<Self> {
        let cfg = qm_common::AppConfig {
            network: qm_common::config::NetworkConfig {
                cidrs: vec![
                    "127.0.0.0/8".to_string(),
                    "192.168.0.0/24".to_string(),
                    "10.0.0.0/8".to_string(),
                    "172.16.0.0/12".to_string(),
                ],
                bind_host: bind_host.clone(),
            },
            ..Default::default()
        };
        let router = Arc::new(SignalRouter::new(Arc::new(cfg)));
        Arc::new(Self {
            bind_host,
            listen_port: port,
            router: Some(router),
            ..Default::default()
        })
    }

    /// 设置监听端口（默认 信令端口 + 2）。
    ///
    /// `&self`：`sessions` 是 `Mutex<HashMap>`，`lock()` 在 `&self` 上可用，
    /// 调用方拿到的是 `Arc<IceGatewayState>` 的共享引用。
    pub fn set_port(&mut self, port: u16) {
        self.listen_port = port;
    }

    /// 登记新会话，返回 sid（SFU 侧与浏览器侧各自登记一次）。
    pub fn register(&self, room: &str, role: &str) -> String {
        let sid = uuid::Uuid::new_v4().to_string();
        let mut s = Session::new();
        s.sid = sid.clone();
        s.room = room.to_string();
        s.role = role.to_string();
        self.sessions
            .lock()
            .insert(sid.clone(), s);
        self.rooms
            .lock()
            .entry(room.to_string())
            .or_default()
            .push(sid.clone());
        sid
    }

    /// 同房间里的对端（登记顺序上最近的一个非本人会话）。
    ///
    /// 锁顺序固定为 `rooms → sessions`（见 [`counterpart_by_room`]）；两个锁都在
    /// 同一函数内获取并释放，不会跨函数边界持锁。
    fn counterpart(&self, sid: &str) -> Option<String> {
        let room = {
            let sessions = self.sessions.lock();
            sessions.get(sid)?.room.clone()
        };
        self.counterpart_by_room(&room, sid)
    }

    /// 同房间里的对端（已知房间号；只拿一次锁，供已持 rooms 视图的调用方使用）。
    fn counterpart_by_room(&self, room: &str, sid: &str) -> Option<String> {
        let id = self.rooms.lock().get(room)?.iter().find(|s| s.as_str() != sid)?.clone();
        let sessions = self.sessions.lock();
        sessions.contains_key(&id).then_some(id)
    }

    /// 会话总数。
    pub fn live_sessions(&self) -> usize {
        self.sessions.lock().len()
    }

    /// 回收超过 [`SESSION_TTL`] 未活动的会话，返回回收数量。
    pub fn reap_expired(&self) -> usize {
        let now = Instant::now();
        let stale: Vec<String> = {
            let sessions = self.sessions.lock();
            sessions
                .iter()
                .filter(|(_, s)| now.duration_since(s.last_active) > SESSION_TTL)
                .map(|(sid, _)| sid.clone())
                .collect()
        };
        let mut removed = 0usize;
        if !stale.is_empty() {
            let mut sessions = self.sessions.lock();
            let mut rooms = self.rooms.lock();
            for sid in &stale {
                if sessions.remove(sid).is_some() {
                    removed += 1;
                    for list in rooms.values_mut() {
                        list.retain(|x| x.as_str() != sid);
                    }
                }
            }
            rooms.retain(|_, v| !v.is_empty());
        }
        if removed > 0 {
            debug!(removed, "已回收过期互通会话");
        }
        removed
    }

    /// 全部会话摘要（供 SFU 节点发现浏览器会话）。
    pub fn all_sessions(&self) -> Vec<SessionSummary> {
        self.sessions.lock().values().map(Session::summary).collect()
    }

    /// 读取本会话当前持有的 SDP（`kind` + 文本）。
    ///
    /// SFU 侧进程与网关共享同一个 [`IceGatewayState`]，所以它直接读内存即可，
    /// 不必给自己发 HTTP 请求（网关是单进程内存态，跨进程只有 HTTP 这一条路）。
    pub fn get_sdp(&self, sid: &str) -> Option<(String, String)> {
        self.sessions.lock().get(sid).and_then(|s| s.local_sdp.clone())
    }

    /// 写入本会话持有的 SDP（SFU 侧把 answer 交回网关后，浏览器靠
    /// `GET /session/{sid}/sdp` 或 `POST /session/{sid}/sdp` 的返回值取到它）。
    pub fn set_sdp(&self, sid: &str, kind: &str, sdp: String) -> bool {
        match self.sessions.lock().get_mut(sid) {
            Some(s) => {
                s.local_sdp = Some((kind.to_string(), sdp));
                s.touch();
                true
            }
            None => false,
        }
    }

    /// 登记本会话的新 ICE candidate（SFU 侧自己的 candidate，浏览器拉取）。
    pub fn add_local_ice(&self, sid: &str, candidate: serde_json::Value) {
        if let Some(s) = self.sessions.lock().get_mut(sid) {
            s.local_ice.push(candidate);
            s.touch();
        }
    }

    /// 从请求路径解析 sid 并登记本方 TCP 对端地址（只认 `/session/{sid}/...`）。
    ///
    /// 在每次请求进入路由前调用，`/session/register` 与后续消息都会被记录。
    pub fn note_peer_addr(&self, path: &str, addr: SocketAddr) {
        let seg: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();
        // `/session/register` 还没分配 sid，跳过（否则会给 key "register" 记一个
        // 永无人读的地址，掩盖"这个 sid 真的没连过"）。
        if seg.len() >= 2 && seg[0] == "session" && seg[1] != "register" {
            self.record_remote_addr(seg[1], addr);
        }
    }

    /// 记录某会话的 TCP 对端地址（幂等；后写覆盖，对端重连时取最新）。
    pub fn record_remote_addr(&self, sid: &str, addr: SocketAddr) {
        self.remote_addrs.lock().insert(sid.to_string(), addr);
        if let Some(sess) = self.sessions.lock().get_mut(sid) {
            sess.remote_addr = Some(addr);
        }
    }

    /// 对端会话的 TCP 对端地址。
    ///
    /// SFU 用它把浏览器的 `.local` 候选改写成语义正确的字面 IP：浏览器侧的
    /// mDNS 遮蔽让 ICE 无从解析（Chrome 的应答用了 DNS 压缩指针，webrtc-mdns
    /// 0.17.2 按字面名匹配不到），而 TCP 对端地址是不变的物理事实。
    pub fn peer_remote_addr(&self, sid: &str) -> Option<SocketAddr> {
        let peer = self.counterpart(sid)?;
        let from_sessions = self.sessions.lock().get(&peer)?.remote_addr;
        if from_sessions.is_some() {
            return from_sessions;
        }
        self.remote_addrs.lock().get(&peer).copied()
    }

    /// 直接登记本方地址（SFU 侧没有 TCP 对端，用它填绑定的真实 IP）。
    pub fn set_remote_addr(&self, sid: &str, addr: SocketAddr) {
        self.record_remote_addr(sid, addr);
    }

    /// 本会话产生的 ICE candidate 里 `.local` 主机名对应的真实字面 IP。
    ///
    /// 浏览器与 SFU 的候选都可能是 mDNS 遮蔽的 `<uuid>.local`：
    /// - 浏览器侧（Chrome）恒为 `<uuid>.local`；
    /// - SFU 侧（webrtc-rs `QueryAndGather`）同样是 `.local`。
    ///
    /// 浏览器侧换成它自己的真实 IP（TCP 对端地址，`note_peer_addr` 记下的）；
    /// SFU 侧换成登记时填的绑定地址。返回 `None` 时调用方原样透传。
    pub fn own_candidate_addr(&self, sid: &str) -> Option<IpAddr> {
        let sess_addr = self.sessions.lock().get(sid).and_then(|s| s.remote_addr).map(|a| a.ip());
        if sess_addr.is_some() {
            return sess_addr;
        }
        self.remote_addrs.lock().get(sid).map(|a| a.ip())
    }

    /// 取走（清空）本会话未投递的 ICE candidate —— 投递完就别再重发。
    pub fn take_local_ice(&self, sid: &str) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        if let Some(s) = self.sessions.lock().get_mut(sid) {
            s.local_ice.append(&mut out);
        }
        out
    }

    /// 本会话已收到的对端 ICE candidate 数（报告用）。
    pub fn local_ice_count(&self, sid: &str) -> u32 {
        self.sessions
            .lock()
            .get(sid)
            .map(|s| s.local_ice.len() as u32)
            .unwrap_or(0)
    }

    /// 同房间对端（浏览器）持有的 SDP —— SFU 侧读内存，不用给自己发 HTTP。
    ///
    /// 房间里只有 SFU 自己（浏览器还没接入）时返回 `None`，不把自己当成对端。
    pub fn peer_sdp(&self, sid: &str) -> Option<(String, String)> {
        let room = self.sessions.lock().get(sid)?.room.clone();
        let peer = self.counterpart_by_room(&room, &sid)?;
        self.sessions.lock().get(&peer).and_then(|s| s.local_sdp.clone())
    }

    /// 同房间对端（浏览器）已提交的 ICE candidate（SDP 原文）。
    pub fn peer_ice(&self, sid: &str) -> Vec<serde_json::Value> {
        let Some(room) = self.sessions.lock().get(sid).map(|s| s.room.clone()) else {
            return Vec::new();
        };
        let Some(peer) = self.counterpart_by_room(&room, &sid) else {
            return Vec::new();
        };
        self.sessions
            .lock()
            .get(&peer)
            .map(|s| s.local_ice.clone())
            .unwrap_or_default()
    }

    /// 发布本会话的键值元数据（`PUT /session/{sid}/meta`）。
    ///
    /// SFU 侧用它把「媒体开始写样本的墙钟时间」写进去；浏览器读完就能算出
    /// 端到端延迟 = 本机收到第一个远端帧的时刻 −（该时间 + 本机到 SFU 的
    /// HTTP 往返偏移）。网关只搬 JSON，不理解语义。
    pub fn set_meta(&self, sid: &str, meta: serde_json::Value) -> bool {
        match self.sessions.lock().get_mut(sid) {
            Some(s) if meta.is_object() => {
                s.meta = meta.as_object().unwrap().clone();
                s.touch();
                true
            }
            _ => false,
        }
    }

    /// 读本会话元数据。
    pub fn get_meta(&self, sid: &str) -> serde_json::Map<String, serde_json::Value> {
        self.sessions
            .lock()
            .get(sid)
            .map(|s| s.meta.clone())
            .unwrap_or_default()
    }

    /// 读同房间对端的元数据。
    pub fn peer_meta(&self, sid: &str) -> serde_json::Map<String, serde_json::Value> {
        let room = match self.sessions.lock().get(sid) {
            Some(s) => s.room.clone(),
            None => return serde_json::Map::new(),
        };
        match self.counterpart_by_room(&room, &sid) {
            Some(peer) => self
                .sessions
                .lock()
                .get(&peer)
                .map(|s| s.meta.clone())
                .unwrap_or_default(),
            None => serde_json::Map::new(),
        }
    }
}

/// 处理一次请求：`/session/*`、`GET /`（本地静态页）、`GET /healthz`、`/room/*`。
pub fn handle(state: &IceGatewayState, method: Method, path: &str, body: &[u8]) -> Response<String> {
    if path == "/healthz" {
        return match method {
            m if m == Method::GET => {
                let v = serde_json::json!({
                    "ok": true,
                    "service": "ice-gateway",
                    "sessions": state.live_sessions(),
                    "page": PAGE_ROOT,
                    "note": "SDP/ICE 互换端点；媒体走 ICE 直连，本服务不经手任何媒体字节"
                });
                json_ok(v.to_string())
            }
            _ => json_err(StatusCode::METHOD_NOT_ALLOWED, "healthz 只支持 GET"),
        };
    }

    // 静态页：GET / 或 GET /interop/index.html
    if method == Method::GET && (path == "/" || path == "/interop/index.html") {
        return serve_page();
    }

    let seg: Vec<&str> = path
        .split_terminator('/')
        .filter(|s| !s.is_empty())
        .collect();
    // `/room/*`：转发给信令路由器（同一套内网准入校验），无路由器则 502。
    if seg.first().copied() == Some("room") {
        return match &state.router {
            Some(r) => r.route(method.clone(), path, body),
            None => json_err(
                StatusCode::SERVICE_UNAVAILABLE,
                "本实例未挂载信令路由器，`/room/*` 不可用",
            ),
        };
    }

    // GET /sessions：全部会话（SFU 节点发现对端用）。
    if method == Method::GET && path == "/sessions" {
        return json_ok(serde_json::json!({
            "ok": true,
            "sessions": state.all_sessions(),
        }).to_string());
    }

    // 只有 `/session/...` 前缀属于本服务，其余一律 404。
    if seg.first().copied() != Some("session") || seg.len() < 2 {
        return json_err(StatusCode::NOT_FOUND, format!("不支持的路由：{method} {path}"));
    }
    let rest = &seg.as_slice()[1..];
    match rest {
        ["register"] if method == Method::POST => register(state, body),
        [sid, "sdp"] if method == Method::POST => sdp_submit(state, sid, body),
        [sid, "sdp"] if method == Method::GET => sdp_fetch(state, sid),
        [sid, "ice"] if method == Method::POST => ice_submit(state, sid, body),
        [sid, "ice"] if method == Method::GET => ice_fetch(state, sid),
        [sid, "peer-ice"] if method == Method::GET => ice_peer_fetch(state, sid),
        [sid, "summary"] if method == Method::GET => summary(state, sid),
        [sid, "meta"] if method == Method::PUT => meta_set(state, sid, body),
        [sid, "meta"] if method == Method::GET => meta_get(state, sid),
        _ => json_err(StatusCode::NOT_FOUND, format!("不支持的路由：{method} {path}")),
    }
}

fn register(state: &IceGatewayState, body: &[u8]) -> Response<String> {
    let m: RegisterBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, format!("register 请求体解析失败：{e}")),
    };
    if m.room.trim().is_empty() {
        return json_err(StatusCode::BAD_REQUEST, "room 不能为空");
    }
    let sid = state.register(&m.room, &m.role);
    let peers = state.counterpart(&sid).map(|p| vec![p]).unwrap_or_default();
    debug!(sid = %sid, room = %m.room, role = %m.role, "互通会话已登记");
    json_ok(
        serde_json::json!({
            "ok": true,
            "sid": sid,
            "room": m.room,
            "role": m.role,
            "peers": peers,
            "note": "会话已建立，请提交 SDP 与 ICE candidate",
        })
        .to_string(),
    )
}

/// 提交 SDP，返回对端已提交的 SDP（没有则为 null）。
fn sdp_submit(state: &IceGatewayState, sid: &str, body: &[u8]) -> Response<String> {
    let m: SdpBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, format!("sdp 请求体解析失败：{e}")),
    };
    let sdp = if let Some(t) = m.sdp.as_str() {
        t.to_string()
    } else if let Some(t) = m.sdp.get("sdp").and_then(serde_json::Value::as_str) {
        t.to_string()
    } else {
        return json_err(StatusCode::BAD_REQUEST, "sdp 必须是字符串或含 sdp 字段的对象");
    };
    if sdp.trim().is_empty() {
        return json_err(StatusCode::BAD_REQUEST, "SDP 不能为空");
    }
    let kind = m
        .sdp
        .get("type")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| "offer".to_string());

    // 先单独查一次会话（拿 session 里的 room，再查对端），避免跨锁嵌套。
    let room = {
        let sessions = state.sessions.lock();
        sessions.get(sid).map(|s| s.room.clone())
    };
    let peer = room.as_deref().and_then(|r| state.counterpart_by_room(r, sid));
    let peer_sdp = {
        let mut sessions = state.sessions.lock();
        let Some(s) = sessions.get_mut(sid) else {
            return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
        };
        s.touch();
        s.local_sdp = Some((kind.clone(), sdp.clone()));
        peer.and_then(|p| sessions.get(&p).and_then(|ps| ps.local_sdp.clone()))
    };

    json_ok(
        serde_json::json!({
            "ok": true,
            "sid": sid,
            "own_kind": kind,
            "sdp": peer_sdp.as_ref().map(|(_, v)| v.clone()),
            "peer_kind": peer_sdp.as_ref().map(|(k, _)| k.clone()),
            "note": if peer_sdp.is_some() {
                "已提交，并对端 SDP 已返回"
            } else {
                "已提交，对端尚未提交 SDP（可用 GET /session/{sid}/sdp 轮询）"
            },
        })
        .to_string(),
    )
}

/// 拉取对端 SDP（轮询）：对端没提交时返回 200 + ok:false，方便循环重试。
fn sdp_fetch(state: &IceGatewayState, sid: &str) -> Response<String> {
    let room = {
        let sessions = state.sessions.lock();
        sessions.get(sid).map(|s| s.room.clone())
    };
    if room.is_none() {
        return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
    }
    let peer = room
        .as_deref()
        .and_then(|r| state.counterpart_by_room(r, sid));
    let peer_sdp = {
        let sessions = state.sessions.lock();
        peer.and_then(|p| sessions.get(&p).and_then(|ps| ps.local_sdp.clone()))
    };
    match peer_sdp {
        Some((kind, sdp)) => json_ok(
            serde_json::json!({ "ok": true, "sid": sid, "sdp": sdp, "kind": kind })
                .to_string(),
        ),
        None => json_ok(
            serde_json::json!({ "ok": false, "sid": sid, "note": "对端尚未提交 SDP" })
                .to_string(),
        ),
    }
}

/// 提交 ICE candidate，返回对端已提交的 candidate 列表。
fn ice_submit(state: &IceGatewayState, sid: &str, body: &[u8]) -> Response<String> {
    let m: IceBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, format!("ice 请求体解析失败：{e}")),
    };
    let candidate = match normalize_candidate(&m.candidate) {
        Ok(v) => v,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, e),
    };
    // 先单独查一次会话（拿 session 里的 room，再查对端），避免跨锁嵌套。
    let room = {
        let sessions = state.sessions.lock();
        sessions.get(sid).map(|s| s.room.clone())
    };
    let peer = room.as_deref().and_then(|r| state.counterpart_by_room(r, sid));
    // 改写本方（`sid` 持有者）的 mDNS 主机名。浏览器侧的候选恒是
    // `<uuid>.local`，按它自己的 TCP 对端地址还原成字面 IP；SFU 侧的候选
    // 同样在 `ice_fetch` / `peer-ice` 出网时被 `rewrite_mdns_for` 改写。
    //
    // 顺序要求：`own_candidate_addr(sid)` 依赖 `handle_request` →
    // `note_peer_addr` 在本次请求路由前写下的 TCP 对端地址。
    let candidate = rewrite_ice_submit(state, sid, candidate);
    let (n, peer_candidates) = {
        let mut sessions = state.sessions.lock();
        let Some(s) = sessions.get_mut(sid) else {
            return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
        };
        s.touch();
        s.local_ice.push(candidate);
        let n = s.local_ice.len() as u32;
        // 取对端已提交的 candidate（本方是浏览器时拿到 SFU 的；反之亦然）。
        let peer_local: Vec<serde_json::Value> = match peer.as_deref() {
            Some(p) => sessions.get(p).map(|ps| ps.local_ice.clone()).unwrap_or_default(),
            None => Vec::new(),
        };
        (n, peer_local)
    };
    debug!(sid = %sid, total = n, peer_candidates = peer_candidates.len(), "已收到 ICE candidate");
    // 回传的是对端的 candidate，改写要用**对端**的地址（不是本方 sid）。
    let peer_out = match peer.as_deref() {
        Some(p) => peer_candidates
            .iter()
            .map(|c| rewrite_mdns_for(state, p, c))
            .collect::<Vec<_>>(),
        None => Vec::new(),
    };
    json_ok(
        serde_json::json!({
            "ok": true,
            "sid": sid,
            "ice_candidates": n,
            "candidates": peer_out,
            "note": format!("已收下第 {n} 个 candidate"),
        })
        .to_string(),
    )
}

/// `ice_submit` 里的「按持有者自己的 TCP 对端地址改写 mDNS 主机名」一步。
///
/// 抽成函数是因为这一步有副作用要观测：改写成功与否决定了 SFU 能不能把
/// Chrome 的 `<uuid>.local` 候选对到真实 IP，是整个互通成败的分水岭。
/// 单元测试走这条函数，才能不依赖真实的 TCP 连接把「改写」和「原样透传」
/// 两种分支都覆盖到。
fn rewrite_ice_submit(
    state: &IceGatewayState,
    sid: &str,
    candidate: serde_json::Value,
) -> serde_json::Value {
    let c = candidate;
    let owner = state.sessions.lock().get(sid).map(|s| s.role.clone()).unwrap_or_default();
    if owner != "browser" {
        return c;
    }
    // 只对 `.local` 候选做地址还原；已经是字面 IP 的原样保留。
    if !c.get("candidate").and_then(|t| t.as_str()).map(|t| t.contains(".local")).unwrap_or(false) {
        return c;
    }
    if state.own_candidate_addr(sid).is_none() {
        tracing::warn!(sid = %sid, "浏览器候选是 .local，但没有记录到 TCP 对端地址，原样透传");
        return c;
    }
    let out = rewrite_mdns_for(state, sid, &c);
    if out != c {
        tracing::info!(
            sid = %sid,
            before = %c.get("candidate").and_then(|t| t.as_str()).unwrap_or("?"),
            after = %out.get("candidate").and_then(|t| t.as_str()).unwrap_or("?"),
            "已把浏览器 mDNS 候选改写为真实字面 IP（webrtc-rs 无法解析 Chrome 的 mDNS 应答）"
        );
    }
    out
}

/// 把一条 ICE candidate 里的 mDNS 主机名（`<uuid>.local`）换成它的真实字面 IP。
///
/// `owner_sid` 是**该 candidate 的持有者**（不是调用方）：改写依据是对它自己
/// 的 TCP 对端地址，与谁在读这条 candidate 无关。原样返回非 `.local` 候选。
fn rewrite_mdns_for(
    state: &IceGatewayState,
    owner_sid: &str,
    candidate: &serde_json::Value,
) -> serde_json::Value {
    let Some(text) = candidate.get("candidate").and_then(|t| t.as_str()) else {
        return candidate.clone();
    };
    if !text.contains(".local") {
        return candidate.clone();
    }
    let Some(addr) = state.own_candidate_addr(owner_sid) else {
        return candidate.clone();
    };
    let rewritten = normalize_mdns_candidate(text, addr);
    if rewritten == text {
        return candidate.clone();
    }
    let mut out = candidate.clone();
    out["candidate"] = serde_json::Value::String(rewritten);
    out
}

/// 拉取**本会话自己**已提交的 ICE candidate（`GET /session/{sid}/ice`）。
///
/// 注意：语义与 `GET /session/{sid}/sdp` **相反**（后者返回对端的 SDP）。这里
/// 返回调用方自己的桶，用途是调试与交叉核对——「我到底提交了几条、内容是什么」，
/// 以及确认网关已经把自己的 mDNS 主机名改写成字面 IP。
/// 要拿对端的 candidate：offerer 用 `GET /session/{sid}/peer-ice` 轮询，
/// answerer 用 `POST /session/{sid}/ice` 的返回值；SFU 与网关同进程时直接
/// 读 `IceGatewayState::peer_ice`。
///
/// 返回 200 + `ok:true`（列表可能为空），让前端可以放心轮询。
fn ice_fetch(state: &IceGatewayState, sid: &str) -> Response<String> {
    let (n, ice, role) = {
        let mut sessions = state.sessions.lock();
        let Some(s) = sessions.get_mut(sid) else {
            return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
        };
        s.touch();
        (s.local_ice.len() as u32, s.local_ice.clone(), s.role.clone())
    };
    json_ok(
        serde_json::json!({
            "ok": true,
            "sid": sid,
            "role": role,
            "ice_candidates": n,
            "candidates": ice.into_iter()
                .map(|c| rewrite_mdns_for(state, sid, &c))
                .collect::<Vec<_>>(),
            "note": "本方自己已提交的 candidate（改写后的字面 IP 形式）",
        })
        .to_string(),
    )
}

/// 拉取**同房间对端**已提交的 ICE candidate（`GET /session/{sid}/peer-ice`）。
///
/// 语义与 `GET /session/{sid}/sdp` 完全一致：返回的是对端桶的副本。加这一条
/// 而不是把 `ice_fetch` 改成"返回对端"，是因为 `POST /ice` 的返回值本来就
/// 带对端 candidate；浏览器作为 offerer 时它的 `onicecandidate` 触发得比
/// SFU 产生 answer candidate 早得多，靠 POST 的返回值带不回来，必须自己轮询
/// —— 这就是这个端点存在的理由。返回 200 + `ok:true`（列表可能为空），
/// 对端尚未登记时 `note` 说明原因，前端可以放心轮询。
fn ice_peer_fetch(state: &IceGatewayState, sid: &str) -> Response<String> {
    let room = {
        let sessions = state.sessions.lock();
        sessions.get(sid).map(|s| s.room.clone())
    };
    if room.is_none() {
        return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
    }
    let peer = room.as_deref().and_then(|r| state.counterpart_by_room(r, sid));
    let (n, ice, peer_role) = {
        let sessions = state.sessions.lock();
        match peer.as_deref().and_then(|p| sessions.get(p)) {
            Some(ps) => {
                (ps.local_ice.len() as u32, ps.local_ice.clone(), Some(ps.role.clone()))
            }
            None => (0u32, Vec::new(), None),
        }
    };
    let peer_ref = peer.as_deref();
    json_ok(
        serde_json::json!({
            "ok": true,
            "sid": sid,
            "peer_sid": peer,
            "peer_role": peer_role,
            "ice_candidates": n,
            "candidates": ice.into_iter()
                .map(|c| rewrite_mdns_for(state, peer_ref.unwrap_or(sid), &c))
                .collect::<Vec<_>>(),
            "note": match peer_ref {
                Some(_) if n > 0 => format!("对端已提交 {n} 条 candidate"),
                Some(_) => "对端已登记但还没提交 candidate".to_string(),
                None => "对端尚未登记".to_string(),
            },
        })
        .to_string(),
    )
}

#[derive(Debug, Deserialize)]
struct MetaBody {
    meta: serde_json::Value,
}

/// 提交本方元数据（键值 JSON），返回对端已发布的元数据。
fn meta_set(state: &IceGatewayState, sid: &str, body: &[u8]) -> Response<String> {
    let m: MetaBody = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => return json_err(StatusCode::BAD_REQUEST, format!("meta 请求体解析失败：{e}")),
    };
    if state.set_meta(sid, m.meta) {
        json_ok(
            serde_json::json!({
                "ok": true,
                "sid": sid,
                "peer": state.peer_meta(sid),
                "note": "已发布本方元数据",
            })
            .to_string(),
        )
    } else {
        json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"))
    }
}

/// 读取本会话的元数据（含对端的）。
fn meta_get(state: &IceGatewayState, sid: &str) -> Response<String> {
    if state.sessions.lock().get(sid).is_none() {
        return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
    }
    json_ok(
        serde_json::json!({
            "ok": true,
            "sid": sid,
            "own": state.get_meta(sid),
            "peer": state.peer_meta(sid),
        })
        .to_string(),
    )
}

/// 会话快照（本方 + 对端）。
fn summary(state: &IceGatewayState, sid: &str) -> Response<String> {
    // `counterpart_by_room` 会拿 rooms + sessions 两把锁，所以这里先取快照再放锁。
    let snapshot: Option<(SessionSummary, String)> = {
        let sessions = state.sessions.lock();
        sessions.get(sid).map(|s| (s.summary(), s.room.clone()))
    };
    let Some((session, room)) = snapshot else {
        return json_err(StatusCode::NOT_FOUND, format!("未知会话：{sid}"));
    };
    let peer = state
        .counterpart_by_room(&room, sid)
        .and_then(|p| state.sessions.lock().get(&p).map(Session::summary));
    json_ok(
        serde_json::json!({
            "ok": true,
            "session": session,
            "peer": peer,
        })
        .to_string(),
    )
}

/// 托管本地静态验证页；文件缺失时返回明确的 500，不假装成功。
fn serve_page() -> Response<String> {
    match std::fs::read_to_string(PAGE_ROOT) {
        Ok(html) => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "text/html; charset=utf-8")
            .body(html)
            .unwrap(),
        Err(e) => json_err(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("无法读取本地验证页 {PAGE_ROOT}：{e}（请在仓库根目录启动 ICE 网关）"),
        ),
    }
}

fn json_ok(body: String) -> Response<String> {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

fn json_err(status: StatusCode, message: impl AsRef<str>) -> Response<String> {
    let note = message.as_ref().to_string();
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(serde_json::json!({ "ok": false, "note": note }).to_string())
        .unwrap()
}

/// 启动 ICE 网关（只绑定配置的内网 IPv4 地址，Ctrl+C 优雅退出）。
///
/// 端口：`信令端口 + 2`，保证与信令 8081、媒体 8080 错开（Epic 约束 3）。
/// 验证页默认地址：`http://127.0.0.1:8083/`。
pub async fn start_gateway(state: Arc<IceGatewayState>) -> QmResult<()> {
    let host = state.bind_host.clone();
    let port = state.listen_port();
    let addr = SocketAddr::new(
        host.parse::<IpAddr>()
            .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        port,
    );
    tracing::info!(%host, %port, "ICE 网关启动（SDP/ICE 互换 + 本地静态页）");

    // 会话回收：30s 扫一次，300s 空闲即回收（避免长跑内存累积）。
    let reaper = state.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let _ = reaper.reap_expired();
        }
    });

    let server = hyper::Server::bind(&addr).serve(hyper::service::make_service_fn({
        let state = state.clone();
        // `make_service_fn` 把 `&AddrStream` 交给闭包，`remote_addr()` 就是 TCP
        // 对端地址 —— 这是 hyper 官方文档给的写法，比从 Request extensions 里挖
        // `LocalPeerAddr`（那个类型藏在 `client` 特性后面）可靠得多。
        move |conn: &hyper::server::conn::AddrStream| {
            let state = state.clone();
            let peer_addr = conn.remote_addr();
            async move {
                Ok::<_, Error>(hyper::service::service_fn(
                    move |req: Request<hyper::Body>| {
                        handle_request(state.clone(), peer_addr, req)
                    },
                ))
            }
        }
    }));
    tokio::select! {
        r = server => r.map_err(|e| Error::signaling(e.to_string()))?,
        _ = tokio::signal::ctrl_c() => { tracing::info!("收到 Ctrl+C，ICE 网关退出"); }
    }
    Ok(())
}

/// 请求处理：读取 body → 路由 → 转成 hyper 响应（与 `SignalHttp` 同构）。
async fn handle_request(
    state: Arc<IceGatewayState>,
    peer_addr: SocketAddr,
    req: Request<hyper::Body>,
) -> QmResult<Response<hyper::Body>> {
    // `peer_addr` 由 `make_service_fn` 从 `AddrStream::remote_addr()` 取来，
    // 是 TCP 对端的真实地址 —— 在路由前登记，供 mDNS 候选改写使用。
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    state.note_peer_addr(&path, peer_addr);
    let bytes = hyper::body::to_bytes(req.into_body())
        .await
        .map_err(|e| Error::signaling(format!("请求体读取失败：{e}")))?;
    let resp = handle(&state, method, &path, bytes.as_ref());
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap_or("application/json").to_string())
        .unwrap_or_else(|| "application/json".to_string());
    Ok(Response::builder()
        .status(resp.status())
        .header("content-type", content_type)
        .body(hyper::Body::from(resp.into_body()))
        .map_err(|e| Error::signaling(e.to_string()))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> IceGatewayState {
        IceGatewayState::new(None)
    }

    fn post(state: &IceGatewayState, path: &str, body: &[u8]) -> Response<String> {
        handle(state, Method::POST, path, body)
    }

    fn get(state: &IceGatewayState, path: &str) -> Response<String> {
        handle(state, Method::GET, path, &[])
    }

    fn j(body: &str) -> serde_json::Value {
        serde_json::from_str(body).unwrap()
    }

    fn register(state: &IceGatewayState, room: &str, role: &str) -> String {
        let r = post(state, "/session/register", format!("{{\"room\":\"{room}\",\"role\":\"{role}\"}}").as_bytes());
        assert_eq!(r.status(), StatusCode::OK, "{r:?}");
        j(&r.into_body())["sid"].as_str().unwrap().to_string()
    }

    #[test]
    fn session_handshake_exchanges_sdp_and_candidates() {
        let state = state();
        let sid_a = register(&state, "interop-1", "browser");
        let sid_b = register(&state, "interop-1", "sfu");
        assert_eq!(state.live_sessions(), 2);

        // 缺字段 / 空值都明确 400，未知会话 404。
        let r = post(&state, &format!("/session/{sid_a}/sdp"), b"{\"sdp\":null}");
        assert_eq!(r.status(), StatusCode::BAD_REQUEST, "缺少 sdp 应报 400");
        assert_eq!(
            post(&state, "/session/nope/sdp", b"{\"sdp\":\"x\"}").status(),
            StatusCode::NOT_FOUND
        );

        // A 先发 offer：此时对端 B 还没发，返回 null（轮询模式）。
        let sdp_a = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\n";
        let body = serde_json::json!({"sdp": sdp_a, "type": "offer"});
        let r = post(&state, &format!("/session/{sid_a}/sdp"), body.to_string().as_bytes());
        assert_eq!(r.status(), StatusCode::OK);
        let v = j(&r.into_body());
        assert_eq!(v["ok"], true);
        assert_eq!(v["own_kind"], "offer");
        assert!(v["sdp"].is_null(), "对端未提交时应为 null");

        // GET 轮询同样能拿到（这里 A 查自己的对端 B，还没有）。
        assert_eq!(j(&get(&state, &format!("/session/{sid_a}/sdp")).into_body())["ok"], false);

        // B 回 answer：A 立刻能通过轮询拿到。
        let ans = serde_json::json!({"sdp": "v=0\r\no=- answer"});
        post(&state, &format!("/session/{sid_b}/sdp"), ans.to_string().as_bytes());
        let v = j(&get(&state, &format!("/session/{sid_a}/sdp")).into_body());
        assert_eq!(v["ok"], true);
        assert_eq!(v["kind"], "offer");
        assert!(v["sdp"].as_str().unwrap().contains("answer"));

        // candidate 互发：A 收到 B 的第一条。
        let c1 = serde_json::json!({"candidate": "candidate:1 1 udp 2130706431 127.0.0.1 1 typ host"});
        post(&state, &format!("/session/{sid_b}/ice"), c1.to_string().as_bytes());
        let c2 = serde_json::json!({"candidate": "candidate:2 1 udp 2130706431 127.0.0.1 2 typ host"});
        let r = post(&state, &format!("/session/{sid_a}/ice"), c2.to_string().as_bytes());
        let v = j(&r.into_body());
        assert_eq!(v["ice_candidates"], 1);
        assert_eq!(v["candidates"].as_array().unwrap().len(), 1, "应回传对端 candidate");

        // 汇总视图。
        let s = j(&get(&state, &format!("/session/{sid_a}/summary")).into_body());
        assert_eq!(s["session"]["ice_candidates"], 1);
        assert_eq!(s["peer"]["ice_candidates"], 1);
        assert_eq!(s["session"]["sdp_bytes"], sdp_a.len());

        // 边界：空 candidate / 空 room 都拒绝。
        assert_eq!(
            post(&state, &format!("/session/{sid_a}/ice"), b"{\"candidate\":\"  \"}").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            post(&state, "/session/register", b"{}").status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn healthz_reports_session_count() {
        let state = state();
        let v: serde_json::Value = serde_json::from_str(&get(&state, "/healthz").into_body()).unwrap();
        assert_eq!(v["sessions"], 0);
        assert_eq!(v["service"], "ice-gateway");
        register(&state, "r1", "browser");
        register(&state, "r2", "browser");
        let v: serde_json::Value = serde_json::from_str(&get(&state, "/healthz").into_body()).unwrap();
        assert_eq!(v["sessions"], 2);
        let sessions = j(&get(&state, "/sessions").into_body());
        assert_eq!(sessions["sessions"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn unknown_paths_return_404_or_502() {
        let state = state();
        assert_eq!(get(&state, "/nope").status(), StatusCode::NOT_FOUND);
        assert_eq!(
            post(&state, "/session/register", b"{}").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            post(&state, "/session/register", b"{}").status(),
            StatusCode::BAD_REQUEST
        );
        // 未挂路由器时 `/room/*` 明确报 502，不静默 404。
        assert_eq!(
            get(&state, "/room/x/join").status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[test]
    fn room_routes_are_forwarded_to_signaling_router() {
        // 准入白名单加上本机回环，才能接受 127.0.0.1 的 join。
        let cfg = qm_common::AppConfig {
            network: qm_common::config::NetworkConfig {
                cidrs: vec!["127.0.0.0/8".to_string(), "192.168.0.0/24".to_string()],
                bind_host: "127.0.0.1".to_string(),
            },
            ..Default::default()
        };
        let state = IceGatewayState::new(Some(Arc::new(SignalRouter::new(Arc::new(cfg)))));
        let join = br#"{"peer":"127.0.0.1:54321","peer_id":"b1"}"#;
        assert_eq!(post(&state, "/room/m/join", join).status(), StatusCode::OK);
        // 内网准入校验被继承：公网 peer 被拒。
        assert_eq!(
            post(&state, "/room/m/join", br#"{"peer":"8.8.8.8:1"}"#).status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(get(&state, "/room/m/peers").status(), StatusCode::OK);
    }

    #[test]
    fn counterpart_is_the_other_session_in_the_same_room() {
        let state = state();
        let a = register(&state, "room-a", "browser");
        let other_room = register(&state, "room-b", "browser");
        // 不同房间的会话互不配对。
        assert!(state.counterpart(&a).is_none(), "独自在房间的会话没有对端");
        assert!(state.counterpart(&other_room).is_none());
        let c = register(&state, "room-a", "sfu");
        assert_eq!(state.counterpart(&c).as_deref(), Some(a.as_str()));
        assert_eq!(state.counterpart(&a).as_deref(), Some(c.as_str()));
        assert!(state.counterpart("no-such-sid").is_none());
        assert!(state.counterpart_by_room("room-zzz", "x").is_none());
    }

    #[test]
    fn expired_sessions_are_reaped() {
        let state = state();
        let sid = register(&state, "r", "browser");
        assert_eq!(state.reap_expired(), 0, "刚登记的会话不应被回收");
        // 手工把活动时间拨回超时之外。
        {
            let mut g = state.sessions.lock();
            let s = g.get_mut(&sid).unwrap();
            s.last_active = Instant::now() - (SESSION_TTL + Duration::from_secs(1));
        }
        assert_eq!(state.reap_expired(), 1);
        assert_eq!(state.live_sessions(), 0);
        assert!(state.rooms.lock().is_empty(), "空房间应一并清理");
    }

    #[test]
    fn peer_ice_returns_the_counterpart_bucket_not_the_callers() {
        // `GET /session/{sid}/ice` 返回本方自己的桶；`GET /session/{sid}/peer-ice`
        // 返回对端桶。两个端点必须分开——浏览器是 offerer，SFU 的 candidate
        // 比它的 `onicecandidate` 来得晚，只能靠这个轮询端点拿回来。
        let state = state();
        let br = register(&state, "room-p", "browser");
        let sf = register(&state, "room-p", "sfu");

        let post_ice = |sid: &str, cand: &str| -> serde_json::Value {
            let body = format!(
                r#"{{"candidate":{{"candidate":"{cand}","sdpMid":"0","sdpMLineIndex":0}}}}"#
            );
            let r = post(&state, &format!("/session/{sid}/ice"), body.as_bytes());
            assert_eq!(r.status(), StatusCode::OK, "{cand}");
            j(&r.into_body())
        };
        post_ice(&br, "candidate:1 1 udp 2113937151 192.168.0.7 59001 typ host");
        post_ice(&sf, "candidate:2 1 udp 2130706431 192.168.0.7 60001 typ host");

        // SFU 视角：自己的桶里是 60001，对端桶里是 59001。
        let own = j(&get(&state, &format!("/session/{sf}/ice")).into_body());
        assert_eq!(
            own["candidates"][0]["candidate"].as_str().unwrap(),
            "candidate:2 1 udp 2130706431 192.168.0.7 60001 typ host"
        );
        let peer = j(&get(&state, &format!("/session/{sf}/peer-ice")).into_body());
        assert_eq!(peer["peer_sid"].as_str().unwrap(), br.as_str());
        assert_eq!(peer["peer_role"].as_str().unwrap(), "browser");
        assert_eq!(peer["ice_candidates"].as_u64().unwrap(), 1);
        assert_eq!(
            peer["candidates"][0]["candidate"].as_str().unwrap(),
            "candidate:1 1 udp 2113937151 192.168.0.7 59001 typ host"
        );

        // 浏览器视角对称。
        let bpeer = j(&get(&state, &format!("/session/{br}/peer-ice")).into_body());
        assert_eq!(
            bpeer["candidates"][0]["candidate"].as_str().unwrap(),
            "candidate:2 1 udp 2130706431 192.168.0.7 60001 typ host"
        );

        // 对方还没提交 candidate 时是 200 + 空列表（前端轮询友好），不报 404。
        let lonely = IceGatewayState::new(None);
        let lone = register(&lonely, "room-lone", "browser");
        let r = get(&lonely, &format!("/session/{lone}/peer-ice"));
        assert_eq!(r.status(), StatusCode::OK);
        let v = j(&r.into_body());
        assert_eq!(v["ok"].as_bool().unwrap(), true);
        assert_eq!(v["ice_candidates"].as_u64().unwrap(), 0);
        assert_eq!(v["candidates"].as_array().unwrap().len(), 0);
        assert!(v["note"].as_str().unwrap().contains("尚未登记"));

        // 未知会话 404，不伪装成空列表。
        assert_eq!(
            get(&state, "/session/nope/peer-ice").status(),
            StatusCode::NOT_FOUND
        );
    }

    #[test]
    fn browser_mdns_candidate_is_rewritten_to_the_tcp_peer_addr() {
        // 互通成败的分水岭：Chrome 的候选是 `<uuid>.local`，网关必须按它自己的
        // TCP 对端地址换成字面 IP。webrtc-ice 0.17.2 的 `find_remote_candidate`
        // 只读 `address()`，而 `set_ip()` 从不更新 `address`，所以 `.local`
        // 候选永远对不上 STUN success 的真实 IP。
        let state = state();
        let br = register(&state, "room-mdns", "browser");
        // 模拟 `handle_request` → `note_peer_addr`：浏览器连进来时记下真实地址。
        state.record_remote_addr(&br, "192.168.0.7:54321".parse().unwrap());

        let cand = "candidate:9 1 udp 2113937151 aa-11-22-33.local 59128 typ host generation 0 ufrag aB network-cost 999";
        let before = serde_json::json!({ "candidate": cand, "sdpMid": "0", "sdpMLineIndex": 0 });
        let out = rewrite_ice_submit(&state, &br, before);
        let text = out["candidate"].as_str().unwrap();
        assert!(text.contains("192.168.0.7"), "应替换成 TCP 对端地址，实际：{text}");
        assert!(!text.contains(".local"), "不应残留 mDNS 主机名，实际：{text}");
        assert!(text.contains("59128"), "端口不动，实际：{text}");
        assert_eq!(out["sdpMid"].as_str().unwrap(), "0", "非 candidate 字段原样保留");
        assert_eq!(out["sdpMLineIndex"].as_u64().unwrap(), 0);
    }

    #[test]
    fn rewrite_is_a_noop_for_non_mdns_and_non_browser_candidates() {
        let state = state();
        let br = register(&state, "room-norewrite", "browser");
        state.record_remote_addr(&br, "127.0.0.1:1".parse().unwrap());

        // 已经是字面 IP：原样返回。
        let lit = serde_json::json!({ "candidate": "candidate:1 1 udp 2130706431 10.0.0.5 5000 typ host", "sdpMid": "1", "sdpMLineIndex": 1 });
        assert_eq!(rewrite_ice_submit(&state, &br, lit.clone()), lit);

        // SFU 侧候选不由这条路径改写（走出网时的 `rewrite_mdns_for`）。
        let sf = register(&state, "room-norewrite", "sfu");
        let mdns = serde_json::json!({ "candidate": "candidate:1 1 udp 2130706431 zz.local 5000 typ host" });
        assert_eq!(rewrite_ice_submit(&state, &sf, mdns.clone()), mdns);

        // 没有 TCP 对端地址时 `.local` 候选原样透传（不猜地址）。
        let lonely = register(&state, "room-norewrite2", "browser");
        let out = rewrite_ice_submit(&state, &lonely, mdns);
        assert!(out["candidate"].as_str().unwrap().contains(".local"));

        // 非 `candidate:` 前缀的脏数据原样返回，不 panic。
        let garbage = serde_json::json!({ "candidate": "not-a-candidate" });
        assert_eq!(rewrite_ice_submit(&state, &br, garbage.clone()), garbage);
    }

}
