//! WebSocket 轻量信令协议（QM-004）。
//!
//! 与 [`crate::SignalRouter`] 的 HTTP 路由同源：把「解析 → 准入 → 分发 → 广播」
//! 做成**不绑定任何 WebSocket 库**的纯函数（[`WsRouter::dispatch`]/[`WsRouter::fan_out`]），
//! 传输层（[`crate::server`]）只做 TLS 握手、帧收发与关闭语义，
//! 协议行为可以在 `cargo test --workspace` 里离线逐条断言。
//!
//! ## 协议（一条 JSON 帧一个动作）
//!
//! 上行（客户端 → 服务端）：
//! ```json
//! {"t":"join",     "room":"qm-1024", "peer":"client-a", "seq":1}
//! {"t":"offer",    "room":"qm-1024", "peer":"client-a", "seq":2,
//!  "to":"client-b", "sdp":"v=0\no=...\n", "kind":"offer"}
//! {"t":"answer",   "room":"qm-1024", "peer":"client-a", "seq":3,
//!  "to":"client-b", "sdp":"v=0\no=...\n"}
//! {"t":"candidate","room":"qm-1024", "peer":"client-a", "seq":4,
//!  "to":"client-b", "candidate":"candidate:1 1 udp ... typ host",
//!  "sdpMid":"0", "sdpMLineIndex":0}
//! {"t":"leave",    "room":"qm-1024", "peer":"client-a", "seq":5}
//! ```
//!
//! 下行（服务端 → 客户端）：
//! ```json
//! {"ok":true, "to":"client-b", "from":"client-a", "type":"sdp",   "sdp":"...", "seq":2}
//! {"ok":true, "to":"client-b", "from":"client-a", "type":"ice",   "candidate":"...", "seq":4}
//! {"ok":true, "to":"client-a", "type":"peer_joined", "peer":"client-b", "room":"qm-1024"}
//! {"ok":true, "to":"client-a", "type":"peer_left",   "peer":"client-b", "room":"qm-1024"}
//! {"ok":false, "code":"ICE_PENDING", "error":"...", "seq":4}
//! ```
//!
//! ## 首次连接黑屏（ICE 乱序）
//!
//! 浏览器 `RTCPeerConnection.onicecandidate` 的事件顺序**不保证**先于 SDP
//! 协商完成到达：本地 host candidate 常常在 ICE 收集还在进行时就被发出去，
//! 服务端此时若已把对方 answer 发出而本端 candidate 还没到，就直接建连失败
//! 表现为黑屏。这里的处理是**确定性重排**，不是重试：
//!
//! 1. [`WsPeer::queue_ice_pending`] —— 本端 answer 未就位时先暂存 candidate；
//! 2. [`WsPeer::drain_pending_ice`] —— 收到 SDP 时把暂存的 candidate 一并下发；
//! 3. [`WsPeer::complete_ice`] 由 [`crate::ice_gateway::candidate_is_complete`] 判定，
//!    不完整（缺 `sdpMid` / `sdpMLineIndex`）的 candidate 一律不转发；
//! 4. [`ice_gateway::normalize_mdns_candidate`] —— Chrome 默认把 candidate 地址
//!    遮蔽成 `<uuid>.local`，不还原就无法连通（黑屏的另一大来源）。

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

/// 单个连接暂存的 ICE candidate 上限。收集超时或被新 offer 重置时清空，
/// 因此不会随会议时长线性增长。
const MAX_PENDING_ICE: usize = 64;

/// ICE 收集就绪判定窗口（秒）。超过该时间仍只有本地 candidate 时，
/// 提示客户端用 `restart` 触发新一轮 ICE。
pub const ICE_READY_TIMEOUT_SECS: u64 = 15;

/// 参会者身份（JWT 校验通过后从 claims 里取出，或鉴权关闭时的 fallback）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// 稳定的参会者标识，来自 JWT `sub`。
    pub subject: String,
    /// 显示名，来自 JWT `name`；缺省回落到 `subject`。
    pub display: String,
}

/// 服务端回给客户端的应答信封。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WsReply {
    /// 该动作是否被接受。
    pub ok: bool,
    /// 房间号（有房间上下文时填）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    /// 目标 peer（信令转发类应答）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    /// 发起方 peer。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<String>,
    /// 动作名（`join` / `offer` / `answer` / `candidate` / `leave`）或
    /// 事件名（`peer_joined` / `peer_left`）。
    pub type_: String,
    /// 错误码（`ok = false` 时），如 `BAD_REQUEST` / `FORBIDDEN` /
    /// `ICE_PENDING` / `ALREADY_JOINED`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// 面向人读的错误说明。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl WsReply {
    fn ok_(action: &str) -> Self {
        Self {
            ok: true,
            room: None,
            to: None,
            from: None,
            type_: action.to_string(),
            code: None,
            error: None,
        }
    }

    fn fail(action: &str, code: &str, error: impl Into<String>) -> Self {
        Self {
            ok: false,
            room: None,
            to: None,
            from: None,
            type_: action.to_string(),
            code: Some(code.to_string()),
            error: Some(error.into()),
        }
    }
}

/// 一个上行消息（客户端 → 服务端）。
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WsRequest {
    /// 动作名：`join` / `offer` / `answer` / `candidate` / `leave`。
    pub t: String,
    /// 房间号。
    #[serde(default)]
    pub room: String,
    /// 发起方 peer 标识（客户端自填）。
    #[serde(default)]
    pub peer: String,
    /// 目标 peer（`offer` / `answer` / `candidate` 必填）。
    #[serde(default)]
    pub to: Option<String>,
    /// SDP 文本（`offer` / `answer`）。
    #[serde(default)]
    pub sdp: Option<String>,
    /// SDP 类型：`offer` 或 `answer`（`t = offer` 时必填；`t = answer` 默认 answer）。
    #[serde(default)]
    pub kind: Option<String>,
    /// 一行 ICE candidate。
    #[serde(default)]
    pub candidate: Option<String>,
    /// `sdpMid`（ICE 完整性判定的必需字段之一）。
    #[serde(default)]
    pub sdp_mid: Option<String>,
    /// `sdpMLineIndex`（ICE 完整性判定的必需字段之一）。
    #[serde(default)]
    pub sdp_m_line_index: Option<Value>,
    /// 客户端序号，回显用于对账（不参与任何服务端排序逻辑）。
    #[serde(default)]
    pub seq: Option<u64>,
}

/// 房间内一个已鉴权的 peer。
#[derive(Debug, Clone)]
pub struct WsPeer {
    /// peer 标识。
    pub id: String,
    /// 参会者身份（鉴权关闭时是合成身份，仍然保留 subject/display 语义）。
    pub identity: Identity,
    /// 连接序号：同一 peer 重连递增，用于丢弃旧连接的滞留消息。
    pub generation: u64,
    /// 已收到本端 SDP（offer 或 answer 都算协商已推进）。
    pub sdp_in: bool,
    /// 已把 answer 发给本 peer —— 之后本端 candidate 才能直接下发。
    pub answer_out: bool,
    /// 已收到对端 answer（answerer 侧视角）。
    pub peer_answer: bool,
    /// 对端 answer 未就位时暂存的 candidate。
    pub ice_pending: Vec<Value>,
    /// 本 peer 已转发的 candidate 数。
    pub ice_sent: u64,
}

impl WsPeer {
    /// 构造一个刚加入房间的 peer（`identity` 必须先经过 JWT 校验）。
    pub fn new(id: String, identity: Identity, generation: u64) -> Self {
        Self {
            id,
            identity,
            generation,
            sdp_in: false,
            answer_out: false,
            peer_answer: false,
            ice_pending: Vec::new(),
            ice_sent: 0,
        }
    }

    /// 本 peer 的协商是否已推进到可以交换媒体。
    pub fn negotiated(&self) -> bool {
        self.sdp_in || self.peer_answer
    }

    /// 本端 candidate 是否可以直接下发。
    ///
    /// ICE 乱序的第一层防线：answer 已经发给本 peer 后，candidate 才允许走；
    /// 否则候选先于 SDP 到达，对端还没有 m-line 可挂，直接丢弃表现为黑屏。
    pub fn ice_ready(&self) -> bool {
        self.answer_out || self.peer_answer
    }

    /// 暂存一条 candidate。超过上限丢最旧的（收集超时后会重启 ICE）。
    pub fn queue_ice_pending(&mut self, c: Value) -> bool {
        if self.ice_pending.len() >= MAX_PENDING_ICE {
            self.ice_pending.remove(0);
        }
        self.ice_pending.push(c);
        true
    }

    /// 取出并清空暂存的 candidate（收到 SDP 时调用）。
    pub fn drain_pending_ice(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.ice_pending)
    }

    /// 标记协商完成并清空暂存队列（新一轮 ICE 重启时调用）。
    pub fn complete_ice(&mut self) {
        self.answer_out = true;
        self.ice_pending.clear();
    }
}

/// 一个房间的成员视图。
#[derive(Debug, Default)]
pub struct WsRoom {
    peers: HashMap<String, WsPeer>,
}

impl WsRoom {
    fn insert(&mut self, peer: WsPeer) -> bool {
        // 已有不同 generation 的同名 peer（重连）时覆盖，旧连接的滞留消息
        // 由 generation 比对丢弃。
        let had = self.peers.insert(peer.id.clone(), peer).is_some();
        !had
    }

    fn get(&self, id: &str) -> Option<&WsPeer> {
        self.peers.get(id)
    }

    fn get_mut(&mut self, id: &str) -> Option<&mut WsPeer> {
        self.peers.get_mut(id)
    }

    fn remove(&mut self, id: &str) -> Option<WsPeer> {
        self.peers.remove(id)
    }

    fn others(&self, id: &str) -> Vec<String> {
        self.peers
            .keys()
            .filter(|p| *p != id)
            .cloned()
            .collect()
    }

    fn peers(&self) -> Vec<WsPeer> {
        self.peers.values().cloned().collect()
    }

    fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }
}

/// 协议级内存状态。
#[derive(Debug, Default)]
pub struct WsState {
    rooms: HashMap<String, WsRoom>,
    /// 已转发的 SDP 总数。
    pub sdp_relayed: u64,
    /// 已转发的 ICE candidate 总数。
    pub ice_relayed: u64,
    /// 因协商未就绪而暂存的 candidate 数（排障指标：持续上涨说明 ICE 乱序严重）。
    pub ice_pending_total: u64,
    /// 因不完整（缺 sdpMid / sdpMLineIndex）被拒绝的 candidate 数。
    pub ice_incomplete_rejected: u64,
    /// 连接被接受的总数（含鉴权关闭时的 fallback 连接）。
    pub connections: u64,
}

impl WsState {
    fn room_mut(&mut self, room: &str) -> &mut WsRoom {
        self.rooms.entry(room.to_string()).or_default()
    }

    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// 当前已鉴权的连接数（= 所有房间的 peer 总数）。
    pub fn connection_count(&self) -> u64 {
        self.rooms.values().map(|r| r.peers().len() as u64).sum()
    }

    /// 房间里除 `id` 以外所有成员的 peer 标识（fan-out 目标列表）。
    fn others(&self, room: &str, id: &str) -> Vec<String> {
        self.rooms.get(room).map(|r| r.others(id)).unwrap_or_default()
    }

    /// 每个房间的成员视图。
    pub fn room_peers(&self, room: &str) -> Vec<WsPeer> {
        self.rooms.get(room).map(|r| r.peers()).unwrap_or_default()
    }
}

/// 协议路由结果：需要回给发起方的应答 + 需要广播给房间其他人的帧。
#[derive(Debug, Default)]
pub struct WsResult {
    /// 回给发起方（None 表示不回）。
    pub reply: Option<WsReply>,
    /// 广播给房间其他 peer 的 JSON 帧。
    pub broadcasts: Vec<Value>,
}

impl WsResult {
    fn reply_only(r: WsReply) -> Self {
        Self {
            reply: Some(r),
            broadcasts: Vec::new(),
        }
    }
}

/// WebSocket 协议路由：纯函数分发，不依赖 tokio / tungstenite。
#[derive(Clone)]
pub struct WsRouter {
    /// 内网准入（复用 HTTP 信令的私有网段校验）。
    router: crate::SignalRouter,
    state: std::sync::Arc<Mutex<WsState>>,
}

impl WsRouter {
    pub fn new(router: crate::SignalRouter) -> Self {
        Self {
            state: std::sync::Arc::new(Mutex::new(WsState::default())),
            router,
        }
    }

    /// 当前房间数（健康探针）。
    pub fn room_count(&self) -> usize {
        self.state.lock().room_count()
    }

    /// 当前已鉴权连接数。
    pub fn connection_count(&self) -> u64 {
        self.state.lock().connection_count()
    }

    /// 信令统计快照。
    pub fn stats(&self) -> serde_json::Value {
        let st = self.state.lock();
        json!({
            "rooms": st.room_count(),
            "connections": st.connection_count(),
            "sdpRelayed": st.sdp_relayed,
            "iceRelayed": st.ice_relayed,
            "icePending": st.ice_pending_total,
            "iceIncompleteRejected": st.ice_incomplete_rejected,
        })
    }

    /// 把一条 JSON 帧分发给对应动作。
    ///
    /// 返回的 [`WsResult::broadcasts`] 由传输层负责逐个发送到房间里其他
    /// 已建立连接 —— 协议层只产出帧，不接触 socket。
    pub fn dispatch(&self, raw: &str) -> WsResult {
        let req: WsRequest = match serde_json::from_str(raw) {
            Ok(r) => r,
            Err(e) => {
                return WsResult::reply_only(WsReply::fail(
                    "?",
                    "BAD_REQUEST",
                    format!("信令帧不是合法 JSON：{e}"),
                ))
            }
        };
        let t = req.t.trim().to_ascii_lowercase();
        if req.room.trim().is_empty() {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "缺少 room 字段",
            ));
        }
        if req.peer.trim().is_empty() {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "缺少 peer 字段",
            ));
        }
        match t.as_str() {
            "join" => self.join(&req),
            "offer" => self.sdp(&req, "offer"),
            "answer" => self.sdp(&req, "answer"),
            "candidate" | "ice" => self.ice(&req),
            "leave" => self.leave(&req),
            other => WsResult::reply_only(WsReply::fail(
                other,
                "BAD_REQUEST",
                format!("未知动作 t={other}（支持 join / offer / answer / candidate / leave）"),
            )),
        }
    }

    /// 把一帧发给房间里除 `skip` 以外的所有 peer。
    /// 传输层拿到返回的地址列表后逐条发送；发不出的连接会被传输层清理。
    pub fn fan_out(&self, room: &str, skip: &str, _frame: &Value) -> Vec<String> {
        self.state.lock().others(room, skip)
    }

    fn join(&self, req: &WsRequest) -> WsResult {
        let identity = Identity {
            subject: req.peer.trim().to_string(),
            display: req.peer.trim().to_string(),
        };
        let mut st = self.state.lock();
        let was_new = st
            .room_mut(&req.room)
            .insert(WsPeer::new(req.peer.trim().to_string(), identity, 1));
        if was_new {
            st.connections += 1;
        }
        drop(st);

        let reply = WsReply {
            ok: true,
            room: Some(req.room.clone()),
            to: None,
            from: None,
            type_: "join".to_string(),
            code: None,
            error: None,
        };
        let mut out = WsResult::reply_only(reply);
        if was_new {
            for target in self.fan_out(&req.room, &req.peer, &json!({
                "ok": true,
                "type": "peer_joined",
                "room": req.room,
                "peer": req.peer,
            })) {
                out.broadcasts.push(json!({
                    "ok": true,
                    "type": "peer_joined",
                    "room": req.room,
                    "peer": req.peer,
                    "to": target,
                }));
            }
        }
        out
    }

    fn sdp(&self, req: &WsRequest, kind: &str) -> WsResult {
        let Some(target) = req.to.as_deref() else {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "SDP 需要 to 字段指定接收方",
            ));
        };
        let Some(sdp) = req.sdp.as_deref() else {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "SDP 不能为空",
            ));
        };
        if sdp.trim().is_empty() {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "SDP 不能为空",
            ));
        }
        if let Err(e) = self.router.check_peer(&req.peer) {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "FORBIDDEN",
                e.to_string(),
            ));
        }

        let frame = json!({
            "ok": true,
            "to": target,
            "from": req.peer,
            "type": "sdp",
            "sdp": sdp,
            "kind": kind,
            "room": req.room,
            "seq": req.seq,
        });
        let mut out = WsResult {
            reply: Some(WsReply {
                ok: true,
                room: Some(req.room.clone()),
                to: Some(target.to_string()),
                from: Some(req.peer.trim().to_string()),
                type_: req.t.clone(),
                code: None,
                error: None,
            }),
            broadcasts: Vec::new(),
        };

        let mut st = self.state.lock();
        st.sdp_relayed += 1;
        let me = req.peer.trim();
        // 用块把 `&mut WsRoom` 的生命周期收窄到"取出暂存 candidate"这一步：
        // `pending` 是 `std::mem::take` 出来的拥有权数据，离开块之后就不再
        // 借用 `st.rooms`，后面的 `st.ice_relayed` 记账才合法。
        let pending = {
            let Some(room) = st.rooms.get_mut(&req.room) else {
                drop(st);
                return out;
            };
            if let Some(p) = room.get_mut(me) {
                p.sdp_in = true;
                if kind == "answer" {
                    p.complete_ice();
                }
                p.drain_pending_ice()
            } else {
                Vec::new()
            }
        };
        st.ice_relayed += pending.len() as u64;
        for c in pending {
            out.broadcasts.push(json!({
                "ok": true,
                "to": target,
                "from": me,
                "type": "ice",
                "room": req.room,
                "candidate": c["candidate"],
                "sdpMid": c["sdpMid"],
                "sdpMLineIndex": c["sdpMLineIndex"],
            }));
        }
        // 对端视角：对方给了 answer 时标记（offerer 侧），此时本端已无滞留借用。
        if kind == "answer" {
            if let Some(p) = st.rooms.get_mut(&req.room).and_then(|r| r.get_mut(target)) {
                p.peer_answer = true;
                p.complete_ice();
            }
        }
        drop(st);

        for t in self.fan_out(&req.room, me, &frame) {
            out.broadcasts.push(json!({
                "ok": true,
                "to": t,
                "from": me,
                "type": "sdp",
                "sdp": sdp,
                "kind": kind,
                "room": req.room,
                "seq": req.seq,
            }));
        }
        out
    }

    fn ice(&self, req: &WsRequest) -> WsResult {
        let Some(target) = req.to.as_deref() else {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "ICE candidate 需要 to 字段指定接收方",
            ));
        };
        let Some(candidate) = req.candidate.as_deref() else {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "ICE candidate 不能为空",
            ));
        };
        if candidate.trim().is_empty() {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "ICE candidate 不能为空",
            ));
        }

        // 归一 + mDNS 还原 + 完整性校验（黑屏根因之一：缺 sdpMid / sdpMLineIndex
        // 的 candidate 无法挂载到 m-line，一律拒绝而不是猜）。
        let mut normalized = match crate::ice_gateway::normalize_candidate(&Value::String(candidate.to_string())) {
            Ok(v) => v,
            Err(e) => {
                return WsResult::reply_only(WsReply::fail(
                    &req.t,
                    "BAD_REQUEST",
                    format!("ICE candidate 格式非法：{e}"),
                ))
            }
        };
        if let Some(obj) = normalized.as_object_mut() {
            if let Some(m) = req.sdp_mid.as_deref() {
                obj.insert("sdpMid".to_string(), Value::String(m.to_string()));
            }
            if let Some(i) = req.sdp_m_line_index.as_ref() {
                obj.insert("sdpMLineIndex".to_string(), i.clone());
            }
        }
        if !crate::ice_gateway::candidate_is_complete(&normalized) {
            self.state.lock().ice_incomplete_rejected += 1;
            tracing::warn!(room = %req.room, from = %req.peer, "拒绝不完整的 ICE candidate（缺 sdpMid / sdpMLineIndex）");
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "BAD_REQUEST",
                "ICE candidate 缺少 sdpMid 或 sdpMLineIndex，无法挂载到 m-line",
            ));
        }
        // mDNS 遮蔽还原（Chrome 默认开启）：`<uuid>.local` -> 字面 IP。
        let candidate = crate::ice_gateway::normalize_mdns_candidate(
            normalized
                .get("candidate")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
        );

        let mut st = self.state.lock();

        // 第四层防线：本端 answer 未就位时暂存而不是丢弃。
        let me = req.peer.trim();
        let queued = match st.rooms.get_mut(&req.room) {
            Some(room) => {
                if let Some(p) = room.get_mut(me) {
                    if p.ice_ready() {
                        p.ice_sent += 1;
                        st.ice_relayed += 1;
                        false
                    } else {
                        p.queue_ice_pending(json!({
                            "candidate": candidate,
                            "sdpMid": normalized.get("sdpMid"),
                            "sdpMLineIndex": normalized.get("sdpMLineIndex"),
                            "to": target,
                            "room": req.room,
                            "seq": req.seq,
                        }));
                        st.ice_pending_total += 1;
                        true
                    }
                } else {
                    false
                }
            }
            None => false,
        };
        drop(st);

        if queued {
            tracing::debug!(room = %req.room, from = %me, to = %target, "ICE candidate 暂存（协商未完成）");
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "ICE_PENDING",
                "协商尚未完成，candidate 已暂存，收到 SDP 后自动下发",
            ));
        }
        if let Err(e) = self.router.check_peer(&req.peer) {
            return WsResult::reply_only(WsReply::fail(
                &req.t,
                "FORBIDDEN",
                e.to_string(),
            ));
        }
        let frame = json!({
            "ok": true,
            "to": target,
            "from": me,
            "type": "ice",
            "room": req.room,
            "candidate": candidate,
            "sdpMid": normalized.get("sdpMid").cloned().unwrap_or(Value::Null),
            "sdpMLineIndex": normalized.get("sdpMLineIndex").cloned().unwrap_or(Value::Null),
            "seq": req.seq,
        });
        let mut out = WsResult {
            reply: Some(WsReply {
                ok: true,
                room: Some(req.room.clone()),
                to: Some(target.to_string()),
                from: Some(me.to_string()),
                type_: req.t.clone(),
                code: None,
                error: None,
            }),
            broadcasts: Vec::new(),
        };
        for t in self.fan_out(&req.room, me, &frame) {
            out.broadcasts.push(json!({
                "ok": true,
                "to": t,
                "from": me,
                "type": "ice",
                "room": req.room,
                "candidate": candidate,
                "sdpMid": normalized.get("sdpMid").cloned().unwrap_or(Value::Null),
                "sdpMLineIndex": normalized.get("sdpMLineIndex").cloned().unwrap_or(Value::Null),
                "seq": req.seq,
            }));
        }
        out
    }

    fn leave(&self, req: &WsRequest) -> WsResult {
        let mut st = self.state.lock();
        let removed = st
            .rooms
            .get_mut(&req.room)
            .and_then(|r| r.remove(req.peer.trim()));
        let now_empty = st.rooms.get(&req.room).map(|r| r.is_empty()).unwrap_or(true);
        if now_empty {
            st.rooms.remove(&req.room);
        }
        drop(st);
        let mut out = WsResult::reply_only(WsReply {
            ok: true,
            room: Some(req.room.clone()),
            to: None,
            from: None,
            type_: "leave".to_string(),
            code: None,
            error: None,
        });
        if removed.is_some() {
            for target in self.fan_out(&req.room, req.peer.trim(), &json!({})) {
                out.broadcasts.push(json!({
                    "ok": true,
                    "type": "peer_left",
                    "room": req.room,
                    "peer": req.peer,
                    "to": target,
                }));
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn router() -> WsRouter {
        WsRouter::new(crate::SignalRouter::new(Arc::new(qm_common::AppConfig::default())))
    }

    fn send(r: &WsRouter, raw: &str) -> WsResult {
        r.dispatch(raw)
    }

    #[test]
    fn join_reports_and_broadcasts_peer_joined() {
        let r = router();
        let a = send(
            &r,
            r#"{"t":"join","room":"m","peer":"192.168.0.10:5060"}"#,
        );
        assert!(a.reply.unwrap().ok);
        assert!(a.broadcasts.is_empty(), "第一个人进来没有其他人可通知");

        let b = send(
            &r,
            r#"{"t":"join","room":"m","peer":"192.168.0.11:5060"}"#,
        );
        assert!(b.reply.unwrap().ok);
        assert_eq!(b.broadcasts.len(), 1);
        assert_eq!(b.broadcasts[0]["type"], "peer_joined");
        assert_eq!(b.broadcasts[0]["to"], "192.168.0.10:5060");

        let stats = r.stats();
        assert_eq!(stats["connections"], 2);
        assert_eq!(stats["rooms"], 1);
    }

    #[test]
    fn sdp_offer_and_answer_are_relayed() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.11:1"}"#);

        let offer = send(
            &r,
            r#"{"t":"offer","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","sdp":"v=0\r\no=- 0 0 IN IP4 192.168.0.10","kind":"offer","seq":1}"#,
        );
        assert!(offer.reply.unwrap().ok);
        assert_eq!(offer.broadcasts[0]["to"], "192.168.0.11:1");
        assert_eq!(offer.broadcasts[0]["kind"], "offer");

        let answer = send(
            &r,
            r#"{"t":"answer","room":"m","peer":"192.168.0.11:1","to":"192.168.0.10:1","sdp":"v=0\r\no=- 1 1 IN IP4 192.168.0.11","seq":2}"#,
        );
        assert!(answer.reply.unwrap().ok);
        assert_eq!(answer.broadcasts[0]["to"], "192.168.0.10:1");

        let stats = r.stats();
        assert_eq!(stats["sdpRelayed"], 2);
    }

    #[test]
    fn empty_sdp_and_missing_target_are_rejected() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        let no_target = send(
            &r,
            r#"{"t":"offer","room":"m","peer":"192.168.0.10:1","sdp":"v=0"}"#,
        );
        let code = no_target.reply.unwrap().code.unwrap();
        assert_eq!(code, "BAD_REQUEST");

        let empty = send(
            &r,
            r#"{"t":"offer","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","sdp":"   "}"#,
        );
        assert_eq!(empty.reply.unwrap().code.unwrap(), "BAD_REQUEST");
    }

    #[test]
    fn public_peer_sdp_is_forbidden() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        let resp = send(
            &r,
            r#"{"t":"offer","room":"m","peer":"8.8.8.8:1","to":"192.168.0.10:1","sdp":"v=0"}"#,
        );
        assert_eq!(resp.reply.unwrap().code.unwrap(), "FORBIDDEN");
    }

    #[test]
    fn incomplete_ice_candidate_is_rejected() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.11:1"}"#);
        send(
            &r,
            r#"{"t":"offer","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","sdp":"v=0"}"#,
        );
        send(
            &r,
            r#"{"t":"answer","room":"m","peer":"192.168.0.11:1","to":"192.168.0.10:1","sdp":"v=0"}"#,
        );

        // 缺 sdpMid / sdpMLineIndex：不能挂载到 m-line，必须拒。
        let bad = send(
            &r,
            r#"{"t":"candidate","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","candidate":"candidate:1 1 udp 2130706431 192.168.0.10 5060 typ host"}"#,
        );
        assert_eq!(bad.reply.unwrap().code.unwrap(), "BAD_REQUEST");
        assert_eq!(r.stats()["iceIncompleteRejected"], 1);

        // 完整 candidate：正常下发。
        let ok = send(
            &r,
            r#"{"t":"candidate","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","candidate":"candidate:1 1 udp 2130706431 192.168.0.10 5060 typ host","sdpMid":"0","sdpMLineIndex":0}"#,
        );
        assert!(ok.reply.unwrap().ok);
        assert_eq!(ok.broadcasts[0]["to"], "192.168.0.11:1");
        assert_eq!(r.stats()["iceRelayed"], 1);
    }

    #[test]
    fn ice_candidate_before_answer_is_queued_then_drained() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.11:1"}"#);

        // 场景：本端 candidate 先到，SDP 后到（webrtc-rs 异步时序常态）。
        let early = send(
            &r,
            r#"{"t":"candidate","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","candidate":"candidate:1 1 udp 2130706431 192.168.0.10 5060 typ host","sdpMid":"0","sdpMLineIndex":0}"#,
        );
        assert_eq!(
            early.reply.unwrap().code.unwrap(),
            "ICE_PENDING",
            "协商未完成时不应把 candidate 发给对方"
        );
        assert!(early.broadcasts.is_empty());
        assert_eq!(r.stats()["icePending"], 1);

        // SDP 到达后，暂存的 candidate 必须自动下发 —— 否则首次连接黑屏。
        let offer = send(
            &r,
            r#"{"t":"offer","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","sdp":"v=0\r\no=- 0 0 IN IP4 192.168.0.10","kind":"offer"}"#,
        );
        assert!(offer.broadcasts.iter().any(|f| f["type"] == "ice"));
        assert_eq!(r.stats()["iceRelayed"], 1);
    }

    #[test]
    fn mdns_candidate_is_restored() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.11:1"}"#);
        send(
            &r,
            r#"{"t":"offer","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","sdp":"v=0"}"#,
        );
        send(
            &r,
            r#"{"t":"answer","room":"m","peer":"192.168.0.11:1","to":"192.168.0.10:1","sdp":"v=0"}"#,
        );

        let resp = send(
            &r,
            r#"{"t":"candidate","room":"m","peer":"192.168.0.10:1","to":"192.168.0.11:1","candidate":"candidate:1 1 udp 2130706431 7a9f.local 5060 typ host","sdpMid":"0","sdpMLineIndex":0}"#,
        );
        assert!(resp.reply.unwrap().ok);
        let c = resp.broadcasts[0]["candidate"].as_str().unwrap();
        assert!(
            !c.contains(".local"),
            "mDNS 遮蔽地址必须还原为字面 IP，否则对端连不上（黑屏）：{c}"
        );
    }

    #[test]
    fn leave_removes_peer_and_reclaims_empty_room() {
        let r = router();
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.10:1"}"#);
        send(&r, r#"{"t":"join","room":"m","peer":"192.168.0.11:1"}"#);
        let resp = send(&r, r#"{"t":"leave","room":"m","peer":"192.168.0.11:1"}"#);
        assert!(resp.reply.unwrap().ok);
        assert_eq!(resp.broadcasts[0]["type"], "peer_left");
        assert_eq!(r.stats()["connections"], 1);

        send(&r, r#"{"t":"leave","room":"m","peer":"192.168.0.10:1"}"#);
        assert_eq!(r.stats()["rooms"], 0, "空房间必须自动回收");
        assert_eq!(r.stats()["connections"], 0);
    }

    #[test]
    fn unknown_action_and_malformed_frame_are_rejected() {
        let r = router();
        let bad_json = send(&r, "{not json");
        assert!(!bad_json.reply.unwrap().ok);
        let unknown = send(&r, r#"{"t":"fly","room":"m","peer":"192.168.0.10:1"}"#);
        assert_eq!(unknown.reply.unwrap().code.unwrap(), "BAD_REQUEST");
        let no_room = send(&r, r#"{"t":"join","peer":"192.168.0.10:1"}"#);
        assert_eq!(no_room.reply.unwrap().code.unwrap(), "BAD_REQUEST");
    }

    #[test]
    fn pending_queue_is_bounded() {
        // MAX_PENDING_ICE 上限：持续乱序也不得不受控增长。
        let mut p = WsPeer::new(
            String::from("p"),
            Identity {
                subject: String::from("p"),
                display: String::from("p"),
            },
            1,
        );
        for _ in 0..(MAX_PENDING_ICE + 8) {
            p.queue_ice_pending(json!({"candidate": "c"}));
        }
        assert!(
            p.ice_pending.len() <= MAX_PENDING_ICE,
            "暂存队列必须有上限，当前 {} 条",
            p.ice_pending.len()
        );
    }
}
