//! SFU 路由核心。
//!
//! 与 qm-signaling 的 `SignalRouter::route` 同一设计：纯函数分发，HTTP 无关，
//! 可在 `cargo test` 里离线逐条断言转发路径、订阅隔离、NAT 模拟。
//!
//! 接口（全部 JSON）：
//! * `POST /room/{id}/publish`    → 发布轨道（返回 track_id）
//! * `POST /room/{id}/subscribe`  → 订阅轨道
//! * `POST /room/{id}/unsubscribe` → 退订轨道
//! * `GET  /room/{id}/tracks`      → 房间内轨道列表
//! * `POST /room/{id}/forward`     → 模拟转发一批包（返回决策+统计）
//! * `GET  /room/{id}/ice`         → ICE server 配置 + NAT 模拟
//! * `GET  /healthz`               → 存活探针

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::capacity::{render_report, simulate_capacity, CapacityConfig};
use crate::forwarding::{simulate_forward_batch, ForwardDecision, ForwardStats};
use crate::hwaccel::select_codec_path;
use crate::ice::{select_ice_servers, simulate_nat, IceConfig, NatType};
use crate::recovery::{simulate_loss_recovery, FecGroup, NackCache, NackRequest, RecoveryMode};
use crate::track::{Track, TrackKind, TrackRegistry, TrackState};

/// SFU 路由结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SfuRouteResult {
    pub status: u16,
    pub body: String,
}

impl SfuRouteResult {
    pub fn ok(body: &str) -> Self {
        Self {
            status: 200,
            body: body.to_string(),
        }
    }
    pub fn err(status: u16, msg: &str) -> Self {
        Self {
            status,
            body: msg.to_string(),
        }
    }
    pub fn ok_json(v: &impl Serialize) -> Self {
        Self::ok(&serde_json::to_string(v).unwrap_or_default())
    }
    pub fn err_json(status: u16, msg: &str) -> Self {
        let ack = serde_json::json!({ "ok": false, "error": msg });
        Self::err(status, &serde_json::to_string(&ack).unwrap_or_default())
    }
    pub fn is_ok(&self) -> bool {
        self.status == 200
    }
}

/// 发布请求体。
#[derive(Debug, Serialize, Deserialize)]
pub struct PublishBody {
    pub peer: String,
    pub kind: String, // "audio" or "video"
}

/// 订阅/退订请求体。
#[derive(Debug, Serialize, Deserialize)]
pub struct SubscribeBody {
    pub peer: String,
    pub track_id: String,
}

/// 转发模拟请求体。
#[derive(Debug, Serialize, Deserialize)]
pub struct ForwardBody {
    pub packets_per_track: u32,
    pub packet_bytes: u64,
}

/// /// NACK API request body.
#[derive(Debug, Serialize, Deserialize)]
pub struct NackApiBody {
    pub track_id: String,
    pub kind: String,
    pub publisher: String,
    pub lost_sequences: Vec<u16>,
    pub tick: u64,
    pub max_retries: u32,
    #[serde(default)]
    pub cached_packets: Vec<(u16, u32, u32, u32)>,
}

/// FEC generation request body.
#[derive(Debug, Serialize, Deserialize)]
pub struct FecApiBody {
    pub track_id: String,
    pub kind: String,
    pub group_id: u32,
    pub target_size: u32,
    pub media_packets: Vec<(u16, Vec<u8>)>,
}

/// Recovery simulation request body.
#[derive(Debug, Serialize, Deserialize)]
pub struct RecoveryApiBody {
    pub total_packets: u32,
    pub loss_rate: f64,
    pub mode: String,
    pub seed: u64,
}

/// Benchmark request body.
pub type BenchmarkApiBody = CapacityConfig;

/// 健康检查响应。
#[derive(Debug, Serialize)]
pub struct Health {
    pub name: &'static str,
    pub version: &'static str,
    pub rooms: usize,
    pub total_tracks: usize,
    pub total_subscriptions: usize,
}

/// 房间轨道列表响应。
#[derive(Debug, Serialize)]
pub struct TrackList {
    pub room: String,
    pub tracks: Vec<TrackSummary>,
}

/// 轨道摘要。
#[derive(Debug, Serialize)]
pub struct TrackSummary {
    pub id: String,
    pub publisher: String,
    pub kind: TrackKind,
    pub state: TrackState,
    pub subscribers: Vec<String>,
    pub packets_forwarded: u64,
}

/// 发布响应。
#[derive(Debug, Serialize)]
pub struct PublishAck {
    pub ok: bool,
    pub room: String,
    pub track_id: String,
    pub publisher: String,
    pub kind: TrackKind,
}

/// 订阅响应。
#[derive(Debug, Serialize)]
pub struct SubscribeAck {
    pub ok: bool,
    pub room: String,
    pub track_id: String,
    pub subscriber: String,
    pub subscriber_count: usize,
}

/// 转发模拟响应（验收标准 1）。
#[derive(Debug, Serialize)]
pub struct ForwardResult {
    pub room: String,
    pub decisions: Vec<ForwardDecision>,
    pub stats: ForwardStats,
}

/// ICE 配置响应（验收标准 3）。
#[derive(Debug, Serialize)]
pub struct IceResult {
    pub nat_type: NatType,
    pub selected_servers: usize,
    pub stun_succeeded: bool,
    pub turn_used: bool,
    pub description: String,
}

/// SFU 路由器：状态在 `Arc<Mutex<>>` 里，可自由克隆。
#[derive(Clone)]
pub struct SfuRouter {
    tracks: Arc<Mutex<TrackRegistry>>,
    ice: Arc<Mutex<IceConfig>>,
}

impl SfuRouter {
    pub fn new() -> Self {
        Self {
            tracks: Arc::new(Mutex::new(TrackRegistry::new())),
            ice: Arc::new(Mutex::new(IceConfig::default())),
        }
    }

    pub fn with_ice_config(ice: IceConfig) -> Self {
        Self {
            tracks: Arc::new(Mutex::new(TrackRegistry::new())),
            ice: Arc::new(Mutex::new(ice)),
        }
    }

    /// 房间数。
    pub fn room_count(&self) -> usize {
        self.tracks.lock().room_count()
    }

    /// 房间内轨道数。
    pub fn track_count(&self, room_id: &str) -> usize {
        self.tracks.lock().room_track_count(room_id)
    }

    /// 总订阅关系数。
    pub fn total_subscriptions(&self) -> usize {
        self.tracks.lock().total_subscriptions()
    }

    /// 全部轨道数。
    pub fn total_tracks(&self) -> usize {
        self.tracks.lock().total_track_count()
    }

    /// 路由分发。
    pub fn route(&self, method: &str, path: &str, body: &[u8]) -> SfuRouteResult {
        if path == "/healthz" {
            if method != "GET" {
                return SfuRouteResult::err_json(405, "healthz 只支持 GET");
            }
            let reg = self.tracks.lock();
            let total_tracks = reg.total_track_count();
            let total_subs = reg.total_subscriptions();
            return SfuRouteResult::ok_json(&Health {
                name: qm_common::NAME,
                version: qm_common::VERSION,
                rooms: reg.room_count(),
                total_tracks,
                total_subscriptions: total_subs,
            });
        }

        let mut parts = path.split('/').filter(|s| !s.is_empty());
        if parts.next() != Some("room") {
            return SfuRouteResult::err_json(404, &format!("未知路径: {path}"));
        }
        let room = parts.next().unwrap_or("").to_string();
        let action = parts.next().unwrap_or("").to_string();
        if room.is_empty() {
            return SfuRouteResult::err_json(404, &format!("缺少 room: {path}"));
        }

        match (method, action.as_str()) {
            ("GET", "tracks") => self.list_tracks(&room),
            ("POST", "publish") => self.publish(&room, body),
            ("POST", "subscribe") => self.subscribe(&room, body),
            ("POST", "unsubscribe") => self.unsubscribe(&room, body),
            ("POST", "forward") => self.forward(&room, body),
            ("GET", "ice") => self.ice_info(&room),
            ("POST", "leave") => self.peer_leave(&room, body),
            ("POST", "nack") => self.handle_nack(&room, body),
            ("POST", "fec") => self.generate_fec(&room, body),
            ("POST", "recover") => self.simulate_recovery(&room, body),
            ("GET", "hwaccel") => self.hwaccel_info(&room),
            ("POST", "benchmark") => self.benchmark(&room, body),
            _ => SfuRouteResult::err_json(
                404,
                &format!("不支持的路由: {method} /room/{room}/{action}"),
            ),
        }
    }

    fn list_tracks(&self, room: &str) -> SfuRouteResult {
        let reg = self.tracks.lock();
        let tracks: Vec<TrackSummary> = reg
            .room_tracks(room)
            .iter()
            .map(|t| TrackSummary {
                id: t.id.clone(),
                publisher: t.publisher.clone(),
                kind: t.kind,
                state: t.state,
                subscribers: t.subscribers.iter().cloned().collect(),
                packets_forwarded: t.packets_forwarded,
            })
            .collect();
        SfuRouteResult::ok_json(&TrackList {
            room: room.to_string(),
            tracks,
        })
    }

    fn publish(&self, room: &str, body: &[u8]) -> SfuRouteResult {
        let m: PublishBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return SfuRouteResult::err_json(400, &format!("publish 请求体解析失败: {e}"))
            }
        };
        let kind = match m.kind.as_str() {
            "audio" => TrackKind::Audio,
            "video" => TrackKind::Video,
            _ => {
                return SfuRouteResult::err_json(
                    400,
                    &format!("kind 必须是 audio 或 video, 得到: {}", m.kind),
                )
            }
        };
        let mut reg = self.tracks.lock();
        let track_id = reg.publish(room, &m.peer, kind);
        let ack = PublishAck {
            ok: true,
            room: room.to_string(),
            track_id,
            publisher: m.peer,
            kind,
        };
        SfuRouteResult::ok_json(&ack)
    }

    fn subscribe(&self, room: &str, body: &[u8]) -> SfuRouteResult {
        let m: SubscribeBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return SfuRouteResult::err_json(400, &format!("subscribe 请求体解析失败: {e}"))
            }
        };
        let mut reg = self.tracks.lock();
        match reg.subscribe(room, &m.track_id, &m.peer) {
            Ok(true) => {
                let count = reg
                    .get_track(room, &m.track_id)
                    .map(|t| t.subscriber_count())
                    .unwrap_or(0);
                let ack = SubscribeAck {
                    ok: true,
                    room: room.to_string(),
                    track_id: m.track_id,
                    subscriber: m.peer,
                    subscriber_count: count,
                };
                SfuRouteResult::ok_json(&ack)
            }
            Ok(false) => SfuRouteResult::err_json(409, "该 peer 已订阅此轨道"),
            Err(e) => SfuRouteResult::err_json(404, &e),
        }
    }

    fn unsubscribe(&self, room: &str, body: &[u8]) -> SfuRouteResult {
        let m: SubscribeBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return SfuRouteResult::err_json(400, &format!("unsubscribe 请求体解析失败: {e}"))
            }
        };
        let mut reg = self.tracks.lock();
        match reg.unsubscribe(room, &m.track_id, &m.peer) {
            Ok(true) => {
                let count = reg
                    .get_track(room, &m.track_id)
                    .map(|t| t.subscriber_count())
                    .unwrap_or(0);
                let ack = SubscribeAck {
                    ok: true,
                    room: room.to_string(),
                    track_id: m.track_id,
                    subscriber: m.peer,
                    subscriber_count: count,
                };
                SfuRouteResult::ok_json(&ack)
            }
            Ok(false) => SfuRouteResult::err_json(404, "该 peer 未订阅此轨道"),
            Err(e) => SfuRouteResult::err_json(404, &e),
        }
    }

    fn forward(&self, room: &str, body: &[u8]) -> SfuRouteResult {
        let m: ForwardBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => {
                return SfuRouteResult::err_json(400, &format!("forward 请求体解析失败: {e}"))
            }
        };
        let reg = self.tracks.lock();
        let track_refs: Vec<&Track> = reg.room_tracks(room);
        if track_refs.is_empty() {
            return SfuRouteResult::err_json(404, &format!("房间 {room} 无轨道"));
        }
        let (decisions, stats) =
            simulate_forward_batch(&track_refs, m.packets_per_track, m.packet_bytes);
        // 更新轨道的 packets_forwarded 计数
        drop(reg);
        let mut reg = self.tracks.lock();
        for d in &decisions {
            if let Some(t) = reg.get_track_mut(room, &d.track_id) {
                t.packets_forwarded += stats.packets_forwarded / decisions.len() as u64;
            }
        }
        // 重置 stats 里的值，避免重复累加（display 用原始统计值）
        let result = ForwardResult {
            room: room.to_string(),
            decisions,
            stats,
        };
        SfuRouteResult::ok_json(&result)
    }

    fn ice_info(&self, _room: &str) -> SfuRouteResult {
        let cfg = self.ice.lock();
        let selected = select_ice_servers(&cfg);
        let sim = simulate_nat(cfg.nat_type, &cfg);
        let result = IceResult {
            nat_type: cfg.nat_type,
            selected_servers: selected.len(),
            stun_succeeded: sim.stun_succeeded,
            turn_used: sim.turn_used,
            description: sim.description,
        };
        SfuRouteResult::ok_json(&result)
    }

    fn peer_leave(&self, room: &str, body: &[u8]) -> SfuRouteResult {
        let m: SubscribeBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return SfuRouteResult::err_json(400, &format!("leave 请求体解析失败: {e}")),
        };
        let mut reg = self.tracks.lock();
        let ended = reg.peer_left(room, &m.peer);
        let ack = serde_json::json!({
            "ok": true,
            "room": room,
            "peer": m.peer,
            "ended_tracks": ended,
        });
        SfuRouteResult::ok_json(&ack)
    }

    fn handle_nack(&self, _room: &str, body: &[u8]) -> SfuRouteResult {
        let req: NackApiBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return SfuRouteResult::err_json(400, &format!("nack body: {e}")),
        };
        let kind = match req.kind.as_str() {
            "audio" => TrackKind::Audio,
            "video" => TrackKind::Video,
            _ => return SfuRouteResult::err_json(400, "kind must be audio or video"),
        };
        let nack_req = NackRequest::new(
            &req.track_id,
            kind,
            &req.publisher,
            req.lost_sequences,
            req.tick,
        );
        let mut cache = NackCache::new();
        for cs in &req.cached_packets {
            cache.record(cs.0, cs.1, cs.2, cs.3);
        }
        let resends = cache.handle_nack(&nack_req, req.max_retries);
        SfuRouteResult::ok_json(&serde_json::json!({
            "ok": true,
            "track_id": req.track_id,
            "retransmit_count": resends.len(),
            "retransmit_items": resends,
        }))
    }

    fn generate_fec(&self, _room: &str, body: &[u8]) -> SfuRouteResult {
        let req: FecApiBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return SfuRouteResult::err_json(400, &format!("fec body: {e}")),
        };
        let kind = match req.kind.as_str() {
            "audio" => TrackKind::Audio,
            "video" => TrackKind::Video,
            _ => return SfuRouteResult::err_json(400, "kind must be audio or video"),
        };
        let mut group = FecGroup::new(req.group_id, &req.track_id, kind, req.target_size);
        for (seq, payload) in &req.media_packets {
            group.add_media(*seq, payload);
        }
        match group.generate_redundancy() {
            Some(fec) => {
                SfuRouteResult::ok_json(&serde_json::json!({"ok": true, "fec_packet": fec}))
            }
            None => SfuRouteResult::err_json(400, "FEC group empty"),
        }
    }

    fn simulate_recovery(&self, _room: &str, body: &[u8]) -> SfuRouteResult {
        let req: RecoveryApiBody = match serde_json::from_slice(body) {
            Ok(v) => v,
            Err(e) => return SfuRouteResult::err_json(400, &format!("recover body: {e}")),
        };
        let mode = match req.mode.as_str() {
            "nack" => RecoveryMode::Nack,
            "nack_fec" => RecoveryMode::NackFec,
            "fec_only" => RecoveryMode::FecOnly,
            _ => return SfuRouteResult::err_json(400, "mode must be nack, nack_fec or fec_only"),
        };
        let stats = simulate_loss_recovery(req.total_packets, req.loss_rate, mode, req.seed);
        SfuRouteResult::ok_json(&serde_json::json!({
            "ok": true,
            "stats": stats,
            "audio_no_interruption": stats.audio_no_interruption(),
            "video_no_long_stall": stats.video_no_long_stall(),
            "recovery_rate": stats.recovery_rate(),
            "effective_loss_rate": stats.effective_loss_rate(),
        }))
    }

    fn hwaccel_info(&self, _room: &str) -> SfuRouteResult {
        let path = select_codec_path("h264", TrackKind::Video, &|b: crate::hwaccel::HwBackend| {
            crate::hwaccel::HwAvailability::unavailable(
                b,
                "hardware probe not available in sandbox",
            )
        });
        SfuRouteResult::ok_json(&serde_json::json!({
            "platform": format!("{:?}", crate::hwaccel::detect_platform()),
            "codec_path": path,
            "candidate_backends": crate::hwaccel::candidate_backends(crate::hwaccel::detect_platform()),
        }))
    }

    fn benchmark(&self, _room: &str, body: &[u8]) -> SfuRouteResult {
        let req: BenchmarkApiBody = if body.is_empty()
            || std::str::from_utf8(body).unwrap_or("").trim().is_empty()
        {
            CapacityConfig::default()
        } else {
            match serde_json::from_slice(body) {
                Ok(v) => v,
                Err(e) => return SfuRouteResult::err_json(400, &format!("benchmark body: {e}")),
            }
        };
        let report = simulate_capacity(&req);
        let text = render_report(&report);
        SfuRouteResult::ok_json(&serde_json::json!({
            "ok": true,
            "report": report,
            "rendered": text,
        }))
    }
}

impl Default for SfuRouter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> SfuRouter {
        SfuRouter::new()
    }

    fn publish(router: &SfuRouter, room: &str, peer: &str, kind: &str) -> String {
        let body = serde_json::json!({ "peer": peer, "kind": kind }).to_string();
        let resp = router.route("POST", &format!("/room/{room}/publish"), body.as_bytes());
        assert!(resp.is_ok(), "publish 应成功: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        v["track_id"].as_str().unwrap().to_string()
    }

    fn subscribe(router: &SfuRouter, room: &str, peer: &str, track_id: &str) {
        let body = serde_json::json!({ "peer": peer, "track_id": track_id }).to_string();
        let resp = router.route("POST", &format!("/room/{room}/subscribe"), body.as_bytes());
        assert!(resp.is_ok(), "subscribe 应成功: {}", resp.body);
    }

    #[test]
    fn healthz_works() {
        let r = router();
        let resp = r.route("GET", "/healthz", &[]);
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["name"], "QuickMeet");
        assert_eq!(v["rooms"], 0);
    }

    #[test]
    fn publish_and_list_tracks() {
        let r = router();
        let audio = publish(&r, "room1", "alice", "audio");
        let video = publish(&r, "room1", "alice", "video");

        let resp = r.route("GET", "/room/room1/tracks", &[]);
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        let tracks = v["tracks"].as_array().unwrap();
        assert_eq!(tracks.len(), 2);
        assert!(tracks.iter().any(|t| t["kind"] == "audio"));
        assert!(tracks.iter().any(|t| t["kind"] == "video"));
    }

    #[test]
    fn publish_rejects_bad_kind() {
        let r = router();
        let body = serde_json::json!({ "peer": "alice", "kind": "data" }).to_string();
        let resp = r.route("POST", "/room/r/publish", body.as_bytes());
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn subscribe_and_unsubscribe() {
        let r = router();
        let track_id = publish(&r, "room1", "alice", "audio");
        subscribe(&r, "room1", "bob", &track_id);

        // bob 订阅了
        let resp = r.route("GET", "/room/room1/tracks", &[]);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        let subs = v["tracks"][0]["subscribers"].as_array().unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0], "bob");

        // bob 退订
        let body = serde_json::json!({ "peer": "bob", "track_id": track_id }).to_string();
        let resp = r.route("POST", "/room/room1/unsubscribe", body.as_bytes());
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["subscriber_count"], 0);
    }

    #[test]
    fn cannot_subscribe_own_track() {
        let r = router();
        let track_id = publish(&r, "room1", "alice", "audio");
        let body = serde_json::json!({ "peer": "alice", "track_id": track_id }).to_string();
        let resp = r.route("POST", "/room/room1/subscribe", body.as_bytes());
        assert_eq!(resp.status, 404, "不能订阅自己发布的轨道");
    }

    #[test]
    fn selective_forwarding_no_full_mix() {
        // 验收标准 1：SFU 按订阅者需求转发，无全量混流带宽浪费
        let r = router();
        let audio = publish(&r, "room1", "alice", "audio");
        let video = publish(&r, "room1", "alice", "video");

        // bob 只订阅音频
        subscribe(&r, "room1", "bob", &audio);
        // carol 只订阅视频
        subscribe(&r, "room1", "carol", &video);

        let body = serde_json::json!({ "packets_per_track": 10, "packet_bytes": 200 }).to_string();
        let resp = r.route("POST", "/room/room1/forward", body.as_bytes());
        assert!(resp.is_ok(), "forward 应成功: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();

        let decisions = v["decisions"].as_array().unwrap();
        assert_eq!(decisions.len(), 2, "2 条轨道 → 2 个决策");

        // 音频只发给 bob，视频只发给 carol
        let audio_d = decisions.iter().find(|d| d["kind"] == "audio").unwrap();
        let video_d = decisions.iter().find(|d| d["kind"] == "video").unwrap();
        assert_eq!(audio_d["recipients"].as_array().unwrap().len(), 1);
        assert_eq!(audio_d["recipients"][0], "bob");
        assert_eq!(video_d["recipients"].as_array().unwrap().len(), 1);
        assert_eq!(video_d["recipients"][0], "carol");

        // 20 包全部转发（10 audio + 10 video），0 跳过
        assert_eq!(v["stats"]["packets_forwarded"], 20);
        assert_eq!(v["stats"]["packets_skipped"], 0);
    }

    #[test]
    fn forward_skips_unsubscribed_tracks() {
        let r = router();
        publish(&r, "room1", "alice", "audio"); // 无订阅者

        let body = serde_json::json!({ "packets_per_track": 5, "packet_bytes": 100 }).to_string();
        let resp = r.route("POST", "/room/room1/forward", body.as_bytes());
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["stats"]["packets_forwarded"], 0);
        assert_eq!(v["stats"]["packets_skipped"], 5);
    }

    #[test]
    fn forward_on_empty_room_is_404() {
        let r = router();
        let body = serde_json::json!({ "packets_per_track": 5, "packet_bytes": 100 }).to_string();
        let resp = r.route("POST", "/room/empty/forward", body.as_bytes());
        assert_eq!(resp.status, 404);
    }

    #[test]
    fn unsubscribe_isolates_other_subscribers() {
        // 验收标准 2：断开单一订阅者不影响其他订阅者
        let r = router();
        let track_id = publish(&r, "room1", "alice", "audio");
        subscribe(&r, "room1", "bob", &track_id);
        subscribe(&r, "room1", "carol", &track_id);

        // bob 退订
        let body = serde_json::json!({ "peer": "bob", "track_id": track_id }).to_string();
        r.route("POST", "/room/room1/unsubscribe", body.as_bytes());

        // 转发：只发给 carol
        let body = serde_json::json!({ "packets_per_track": 1, "packet_bytes": 100 }).to_string();
        let resp = r.route("POST", "/room/room1/forward", body.as_bytes());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        let d = &v["decisions"][0];
        assert_eq!(d["recipients"].as_array().unwrap().len(), 1);
        assert_eq!(d["recipients"][0], "carol");
    }

    #[test]
    fn ice_info_default_config() {
        let r = router();
        let resp = r.route("GET", "/room/r/ice", &[]);
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        // 默认无 NAT：不选 server，STUN 成功（直连），不用 TURN
        assert_eq!(v["nat_type"], "none");
        assert_eq!(v["selected_servers"], 0);
        assert_eq!(v["stun_succeeded"], true);
        assert_eq!(v["turn_used"], false);
    }

    #[test]
    fn ice_info_with_symmetric_nat() {
        let r = SfuRouter::with_ice_config(IceConfig {
            servers: crate::ice::IceConfig::default().servers,
            nat_type: NatType::Symmetric,
        });
        let resp = r.route("GET", "/room/r/ice", &[]);
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["nat_type"], "symmetric");
        assert_eq!(v["selected_servers"], 2);
        assert_eq!(v["stun_succeeded"], false);
        assert_eq!(v["turn_used"], true);
    }

    #[test]
    fn peer_leave_ends_tracks_and_notifies() {
        let r = router();
        let audio = publish(&r, "room1", "alice", "audio");
        let video = publish(&r, "room1", "alice", "video");
        subscribe(&r, "room1", "bob", &audio);
        subscribe(&r, "room1", "bob", &video);

        let body = serde_json::json!({ "peer": "alice", "track_id": "" }).to_string();
        let resp = r.route("POST", "/room/room1/leave", body.as_bytes());
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        let ended = v["ended_tracks"].as_array().unwrap();
        assert_eq!(ended.len(), 2, "alice 发布的 2 条轨道应被结束");
    }

    #[test]
    fn unknown_routes_rejected() {
        let r = router();
        assert_eq!(r.route("GET", "/nope", &[]).status, 404);
        assert_eq!(r.route("GET", "/room/r", &[]).status, 404);
        assert_eq!(r.route("DELETE", "/room/r/tracks", &[]).status, 404);
    }

    // === QM-003: NACK/FEC/recovery/hwaccel/benchmark route tests ===

    #[test]
    fn nack_endpoint_returns_retransmit_items() {
        let r = router();
        let body = serde_json::json!({
            "track_id": "t1",
            "kind": "audio",
            "publisher": "alice",
            "lost_sequences": [2, 3],
            "tick": 100,
            "max_retries": 3,
            "cached_packets": [
                [1, 100, 42, 200],
                [2, 110, 42, 200],
                [3, 120, 42, 200],
                [4, 130, 42, 200]
            ]
        })
        .to_string();
        let resp = r.route("POST", "/room/r/nack", body.as_bytes());
        assert!(resp.is_ok(), "nack should succeed: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["retransmit_count"], 2);
    }

    #[test]
    fn nack_endpoint_rejects_bad_kind() {
        let r = router();
        let body = serde_json::json!({
            "track_id": "t1",
            "kind": "data",
            "publisher": "alice",
            "lost_sequences": [2],
            "tick": 0,
            "max_retries": 3,
            "cached_packets": []
        })
        .to_string();
        let resp = r.route("POST", "/room/r/nack", body.as_bytes());
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn fec_endpoint_generates_redundancy() {
        let r = router();
        let body = serde_json::json!({
            "track_id": "t1",
            "kind": "video",
            "group_id": 0,
            "target_size": 3,
            "media_packets": [
                [0, [1, 2, 3]],
                [1, [4, 5, 6]],
                [2, [7, 8, 9]]
            ]
        })
        .to_string();
        let resp = r.route("POST", "/room/r/fec", body.as_bytes());
        assert!(resp.is_ok(), "fec should succeed: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["ok"], true);
        assert!(v["fec_packet"]["redundancy_data"].is_array());
        assert_eq!(
            v["fec_packet"]["group_sequences"].as_array().unwrap().len(),
            3
        );
    }

    #[test]
    fn recover_endpoint_simulates_30pct_loss() {
        let r = router();
        let body = serde_json::json!({
            "total_packets": 1000,
            "loss_rate": 0.30,
            "mode": "nack_fec",
            "seed": 42
        })
        .to_string();
        let resp = r.route("POST", "/room/r/recover", body.as_bytes());
        assert!(resp.is_ok(), "recover should succeed: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["audio_no_interruption"], true);
        assert_eq!(v["video_no_long_stall"], true);
        assert!(v["recovery_rate"].as_f64().unwrap() > 0.9);
    }

    #[test]
    fn recover_endpoint_rejects_bad_mode() {
        let r = router();
        let body = serde_json::json!({
            "total_packets": 100,
            "loss_rate": 0.1,
            "mode": "invalid",
            "seed": 1
        })
        .to_string();
        let resp = r.route("POST", "/room/r/recover", body.as_bytes());
        assert_eq!(resp.status, 400);
    }

    #[test]
    fn hwaccel_endpoint_returns_path() {
        let r = router();
        let resp = r.route("GET", "/room/r/hwaccel", &[]);
        assert!(resp.is_ok(), "hwaccel should succeed: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert!(v["platform"].is_string());
        assert!(v["codec_path"]["backend"].is_string());
        assert!(v["candidate_backends"].is_array());
    }

    #[test]
    fn benchmark_endpoint_default_config() {
        let r = router();
        let resp = r.route("POST", "/room/r/benchmark", b"");
        assert!(resp.is_ok(), "benchmark should succeed: {}", resp.body);
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["ok"], true);
        assert_eq!(v["report"]["config"]["stream_count"], 200);
        assert_eq!(v["report"]["meets_target"], true);
        assert!(v["report"]["avg_latency_ms"].as_f64().unwrap() <= 200.0);
        assert!(v["rendered"].as_str().unwrap().contains("Streams"));
    }

    #[test]
    fn benchmark_endpoint_custom_config() {
        let r = router();
        let body = serde_json::json!({
            "stream_count": 500,
            "width": 1920,
            "height": 1080,
            "fps": 30,
            "bitrate_bps": 4000000,
            "subs_per_stream": 4,
            "recovery_enabled": true
        })
        .to_string();
        let resp = r.route("POST", "/room/r/benchmark", body.as_bytes());
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["report"]["config"]["stream_count"], 500);
    }

    #[test]
    fn healthz_reports_tracks_and_subscriptions() {
        let r = router();
        publish(&r, "room1", "alice", "audio");
        publish(&r, "room1", "alice", "video");
        let track_id = publish(&r, "room1", "bob", "audio");
        subscribe(&r, "room1", "carol", &track_id);

        let resp = r.route("GET", "/healthz", &[]);
        assert!(resp.is_ok());
        let v: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
        assert_eq!(v["rooms"], 1);
        assert_eq!(v["total_tracks"], 3);
        assert_eq!(v["total_subscriptions"], 1);
    }
}
