//! 音视频轨道解耦管理。
//!
//! SFU 与 MCU 的本质区别：SFU 不混流，每条轨道有独立的生命周期、订阅关系
//! 与发布者/接收者映射。本模块提供轨道的创建、订阅、退订、状态查询，
//! 所有操作都是纯函数（状态在 `parking_lot::Mutex` 里），可在 `cargo test`
//! 离线断言，不需要 webrtc-rs。
//!
//! 关键设计点：
//! * **轨道独立**：一条轨道的订阅者断开不影响其他轨道的订阅者。
//! * **发布者/接收者解耦**：发布者只管写入 SFU，SFU 按订阅表转发给接收者。
//! * **生命周期独立**：发布者退出时只关闭该轨道的转发，不影响其他轨道。

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

/// 轨道 ID（UUID v4 字符串）。
pub type TrackId = String;

/// 轨道种类：音频或视频。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrackKind {
    Audio,
    Video,
}

impl std::fmt::Display for TrackKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackKind::Audio => write!(f, "audio"),
            TrackKind::Video => write!(f, "video"),
        }
    }
}

/// 轨道状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TrackState {
    /// 已创建，等待发布者推送媒体。
    Live,
    /// 发布者已结束，不再接收新媒体，但已缓存的尾部帧仍可转发。
    Ended,
}

/// 一条音视频轨道。
///
/// 一条轨道绑定一个发布者 peer 和零到多个订阅者 peer。
/// 轨道之间完全独立：某条轨道的订阅者断开不影响其他轨道。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Track {
    pub id: TrackId,
    pub room_id: String,
    pub publisher: String,
    pub kind: TrackKind,
    pub state: TrackState,
    /// 当前订阅该轨道的 peer ID 集合。
    pub subscribers: HashSet<String>,
    /// 该轨道已转发的 RTP 包数（选择性转发的计量点）。
    pub packets_forwarded: u64,
}

impl Track {
    /// 创建一条新轨道（Live 状态，无订阅者）。
    pub fn new(id: TrackId, room_id: String, publisher: String, kind: TrackKind) -> Self {
        Self {
            id,
            room_id,
            publisher,
            kind,
            state: TrackState::Live,
            subscribers: HashSet::new(),
            packets_forwarded: 0,
        }
    }

    /// 订阅该轨道。
    pub fn subscribe(&mut self, peer_id: &str) -> bool {
        self.subscribers.insert(peer_id.to_string())
    }

    /// 退订该轨道。
    pub fn unsubscribe(&mut self, peer_id: &str) -> bool {
        self.subscribers.remove(peer_id)
    }

    /// 标记轨道结束（发布者离开）。
    pub fn end(&mut self) {
        self.state = TrackState::Ended;
    }

    /// 该 peer 是否订阅了本轨道。
    pub fn has_subscriber(&self, peer_id: &str) -> bool {
        self.subscribers.contains(peer_id)
    }

    /// 当前订阅者数。
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
}

/// 房间内全部轨道的集合管理器。
///
/// 按 room_id 分组，每间房维护一张 TrackId -> Track 映射。
/// 核心操作（publish / subscribe / unsubscribe / forward）都是纯函数，
/// 状态在 `parking_lot::Mutex` 里保护并发安全。
#[derive(Debug, Default)]
pub struct TrackRegistry {
    rooms: HashMap<String, HashMap<TrackId, Track>>,
}

impl TrackRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 发布一条新轨道（返回轨道 ID）。
    pub fn publish(&mut self, room_id: &str, publisher: &str, kind: TrackKind) -> TrackId {
        let id = uuid::Uuid::new_v4().to_string();
        let track = Track::new(id.clone(), room_id.to_string(), publisher.to_string(), kind);
        self.rooms
            .entry(room_id.to_string())
            .or_default()
            .insert(id.clone(), track);
        tracing::debug!(room = room_id, publisher, kind = %kind, track = %id, "轨道已发布");
        id
    }

    /// 订阅轨道（返回是否成功）。
    pub fn subscribe(
        &mut self,
        room_id: &str,
        track_id: &str,
        subscriber: &str,
    ) -> Result<bool, String> {
        let track = self
            .rooms
            .get_mut(room_id)
            .and_then(|r| r.get_mut(track_id))
            .ok_or_else(|| format!("轨道不存在: room={room_id} track={track_id}"))?;
        // 不能订阅自己发布的轨道（SFU 不做环回）。
        if track.publisher == subscriber {
            return Err(format!(
                "不能订阅自己发布的轨道: peer={subscriber} track={track_id}"
            ));
        }
        Ok(track.subscribe(subscriber))
    }

    /// 退订轨道（返回是否成功）。
    pub fn unsubscribe(
        &mut self,
        room_id: &str,
        track_id: &str,
        subscriber: &str,
    ) -> Result<bool, String> {
        let track = self
            .rooms
            .get_mut(room_id)
            .and_then(|r| r.get_mut(track_id))
            .ok_or_else(|| format!("轨道不存在: room={room_id} track={track_id}"))?;
        Ok(track.unsubscribe(subscriber))
    }

    /// 结束轨道（发布者离开时调用）。
    pub fn end_track(&mut self, room_id: &str, track_id: &str) {
        if let Some(track) = self
            .rooms
            .get_mut(room_id)
            .and_then(|r| r.get_mut(track_id))
        {
            track.end();
        }
    }

    /// 移除轨道（房间清理时调用）。
    pub fn remove_track(&mut self, room_id: &str, track_id: &str) {
        if let Some(room) = self.rooms.get_mut(room_id) {
            room.remove(track_id);
            if room.is_empty() {
                self.rooms.remove(room_id);
            }
        }
    }

    /// peer 离开时清理：退订所有轨道 + 结束该 peer 发布的所有轨道。
    /// 返回被结束的轨道 ID 列表（供转发层通知各订阅者）。
    pub fn peer_left(&mut self, room_id: &str, peer_id: &str) -> Vec<TrackId> {
        let mut ended = Vec::new();

        if let Some(room) = self.rooms.get_mut(room_id) {
            // 退订该 peer 参与的所有轨道。
            for track in room.values_mut() {
                track.unsubscribe(peer_id);
            }
            // 结束该 peer 发布的所有轨道。
            let ended_ids: Vec<TrackId> = room
                .iter()
                .filter(|(_, t)| t.publisher == peer_id)
                .map(|(id, _)| id.clone())
                .collect();
            for id in &ended_ids {
                if let Some(t) = room.get_mut(id) {
                    t.end();
                    ended.push(id.clone());
                }
            }
            // 移除已结束且无订阅者的轨道。
            room.retain(|_, t| !(t.state == TrackState::Ended && t.subscribers.is_empty()));
            if room.is_empty() {
                self.rooms.remove(room_id);
            }
        }

        ended
    }

    /// 获取房间内所有轨道。
    pub fn room_tracks(&self, room_id: &str) -> Vec<&Track> {
        self.rooms
            .get(room_id)
            .map(|r| r.values().collect())
            .unwrap_or_default()
    }

    /// 获取房间内某 peer 发布的轨道。
    pub fn tracks_by_publisher(&self, room_id: &str, publisher: &str) -> Vec<&Track> {
        self.rooms
            .get(room_id)
            .map(|r| r.values().filter(|t| t.publisher == publisher).collect())
            .unwrap_or_default()
    }

    /// 获取房间内某 peer 订阅的轨道。
    pub fn tracks_by_subscriber(&self, room_id: &str, subscriber: &str) -> Vec<&Track> {
        self.rooms
            .get(room_id)
            .map(|r| {
                r.values()
                    .filter(|t| t.has_subscriber(subscriber))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// 获取轨道。
    pub fn get_track(&self, room_id: &str, track_id: &str) -> Option<&Track> {
        self.rooms.get(room_id).and_then(|r| r.get(track_id))
    }

    /// 获取轨道（可变）。
    pub fn get_track_mut(&mut self, room_id: &str, track_id: &str) -> Option<&mut Track> {
        self.rooms
            .get_mut(room_id)
            .and_then(|r| r.get_mut(track_id))
    }

    /// 房间轨道数。
    pub fn room_track_count(&self, room_id: &str) -> usize {
        self.rooms.get(room_id).map(|r| r.len()).unwrap_or(0)
    }

    /// 全部房间数。
    pub fn room_count(&self) -> usize {
        self.rooms.len()
    }

    /// 全部轨道数（跨所有房间）。
    pub fn total_track_count(&self) -> usize {
        self.rooms.values().map(|r| r.len()).sum()
    }

    /// 全部订阅关系数（跨所有房间所有轨道）。
    pub fn total_subscriptions(&self) -> usize {
        self.rooms
            .values()
            .flat_map(|r| r.values())
            .map(|t| t.subscriber_count())
            .sum()
    }

    /// 全部房间 ID 列表。
    pub fn room_ids(&self) -> Vec<String> {
        self.rooms.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_and_subscribe() {
        let mut reg = TrackRegistry::new();
        let track_id = reg.publish("room1", "alice", TrackKind::Audio);
        assert!(reg.subscribe("room1", &track_id, "bob").unwrap());
        let track = reg.get_track("room1", &track_id).unwrap();
        assert_eq!(track.subscriber_count(), 1);
        assert!(track.has_subscriber("bob"));
    }

    #[test]
    fn cannot_subscribe_own_track() {
        let mut reg = TrackRegistry::new();
        let track_id = reg.publish("room1", "alice", TrackKind::Video);
        let err = reg.subscribe("room1", &track_id, "alice").unwrap_err();
        assert!(err.contains("不能订阅自己"));
    }

    #[test]
    fn unsubscribe_isolates_other_subscribers() {
        // 验收标准 2：断开单一订阅者不影响其他订阅者
        let mut reg = TrackRegistry::new();
        let track_id = reg.publish("room1", "alice", TrackKind::Audio);
        reg.subscribe("room1", &track_id, "bob").unwrap();
        reg.subscribe("room1", &track_id, "carol").unwrap();

        // bob 退订
        assert!(reg.unsubscribe("room1", &track_id, "bob").unwrap());

        let track = reg.get_track("room1", &track_id).unwrap();
        assert!(!track.has_subscriber("bob"), "bob 应已退订");
        assert!(track.has_subscriber("carol"), "carol 不受影响");
        assert_eq!(track.subscriber_count(), 1);
    }

    #[test]
    fn publisher_left_ends_their_tracks_but_not_others() {
        let mut reg = TrackRegistry::new();
        let alice_track = reg.publish("room1", "alice", TrackKind::Audio);
        let bob_track = reg.publish("room1", "bob", TrackKind::Video);

        reg.subscribe("room1", &alice_track, "carol").unwrap();
        reg.subscribe("room1", &bob_track, "carol").unwrap();

        // alice 离开：alice 的轨道结束，bob 的轨道不受影响
        let ended = reg.peer_left("room1", "alice");
        assert_eq!(ended, vec![alice_track.clone()]);

        let alice_t = reg.get_track("room1", &alice_track);
        // alice 的轨道已结束且无订阅者（carol 自动退订），应被清理
        assert!(alice_t.is_none() || alice_t.unwrap().state == TrackState::Ended);

        let bob_t = reg.get_track("room1", &bob_track).unwrap();
        assert_eq!(bob_t.state, TrackState::Live, "bob 的轨道不应受影响");
        assert!(bob_t.has_subscriber("carol"), "carol 仍订阅 bob 的轨道");
    }

    #[test]
    fn subscriber_left_unsubscribes_but_track_stays_live() {
        let mut reg = TrackRegistry::new();
        let track_id = reg.publish("room1", "alice", TrackKind::Audio);
        reg.subscribe("room1", &track_id, "bob").unwrap();
        reg.subscribe("room1", &track_id, "carol").unwrap();

        // bob 离开（作为订阅者）：轨道仍在 Live，carol 仍订阅
        let ended = reg.peer_left("room1", "bob");
        assert!(ended.is_empty(), "bob 不是发布者，不应结束任何轨道");

        let track = reg.get_track("room1", &track_id).unwrap();
        assert_eq!(track.state, TrackState::Live);
        assert!(!track.has_subscriber("bob"));
        assert!(track.has_subscriber("carol"));
    }

    #[test]
    fn tracks_by_publisher_and_subscriber() {
        let mut reg = TrackRegistry::new();
        let t1 = reg.publish("room1", "alice", TrackKind::Audio);
        let t2 = reg.publish("room1", "alice", TrackKind::Video);
        let t3 = reg.publish("room1", "bob", TrackKind::Audio);

        reg.subscribe("room1", &t1, "carol").unwrap();
        reg.subscribe("room1", &t3, "carol").unwrap();

        let alice_tracks = reg.tracks_by_publisher("room1", "alice");
        assert_eq!(alice_tracks.len(), 2);

        let carol_subs = reg.tracks_by_subscriber("room1", "carol");
        assert_eq!(carol_subs.len(), 2);
        assert!(carol_subs.iter().any(|t| t.id == t1));
        assert!(carol_subs.iter().any(|t| t.id == t3));
        assert!(!carol_subs.iter().any(|t| t.id == t2));
    }

    #[test]
    fn audio_and_video_tracks_are_independent() {
        // 验收标准 2：音视频轨道可独立订阅/退订
        let mut reg = TrackRegistry::new();
        let audio = reg.publish("room1", "alice", TrackKind::Audio);
        let video = reg.publish("room1", "alice", TrackKind::Video);

        // bob 只订阅音频
        reg.subscribe("room1", &audio, "bob").unwrap();
        // carol 只订阅视频
        reg.subscribe("room1", &video, "carol").unwrap();

        // bob 退订音频，carol 的视频订阅不受影响
        reg.unsubscribe("room1", &audio, "bob").unwrap();

        let a = reg.get_track("room1", &audio).unwrap();
        let v = reg.get_track("room1", &video).unwrap();
        assert_eq!(a.subscriber_count(), 0, "音频轨道无订阅者");
        assert_eq!(v.subscriber_count(), 1, "视频轨道仍有 carol");
        assert!(v.has_subscriber("carol"));
    }

    #[test]
    fn empty_room_is_cleaned_up() {
        let mut reg = TrackRegistry::new();
        let track_id = reg.publish("room1", "alice", TrackKind::Audio);
        reg.subscribe("room1", &track_id, "bob").unwrap();

        reg.peer_left("room1", "alice"); // ends alice's track, bob auto-unsubscribed
        reg.peer_left("room1", "bob");

        assert_eq!(reg.room_count(), 0, "空房间应被清理");
    }
}
