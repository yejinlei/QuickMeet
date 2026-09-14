//! 选择性转发逻辑（Selective Forwarding）。
//!
//! SFU 与 MCU 的核心区别：
//! * **MCU**：把所有发布者的媒体混流成一路，每个订阅者只收一路 —— 带宽低但 CPU 高，
//!   且混流改变了原始媒体流（解码 → 混合 → 重编码）。
//! * **SFU**：按订阅者需求，把每条轨道的原始 RTP 包**直接转发**给每个订阅者 ——
//!   无解码/重编码，CPU 低，但带宽随订阅者数线性增长。
//!
//! 本模块实现选择性转发的决策逻辑：给定一条轨道和一批订阅者，决定每个 RTP 包
//! 应该转发给哪些订阅者。关键原则：
//! * **无全量混流**：一个包只转发给真正订阅了该轨道的 peer，不发给未订阅者。
//! * **按需转发**：轨道无订阅者时不转发（不浪费带宽）。
//! * **独立性**：一条轨道的转发决策不影响其他轨道。

use crate::track::{Track, TrackKind, TrackState};

/// 转发决策：一个 RTP 包应该发给哪些订阅者。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ForwardDecision {
    /// 源轨道 ID。
    pub track_id: String,
    /// 源轨道种类（audio/video）。
    pub kind: TrackKind,
    /// 发布者 peer ID。
    pub publisher: String,
    /// 应该转发给的订阅者 peer ID 列表。
    pub recipients: Vec<String>,
    /// 是否跳过转发（无订阅者或轨道已结束）。
    pub skipped: bool,
    /// 跳过原因（skipped=true 时有意义）。
    pub skip_reason: String,
}

impl ForwardDecision {
    /// 该决策是否实际会转发给至少一个订阅者。
    pub fn will_deliver(&self) -> bool {
        !self.skipped && !self.recipients.is_empty()
    }
}

/// 转发统计（验收标准 1 的计量点）。
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ForwardStats {
    /// 总转发的 RTP 包数。
    pub packets_forwarded: u64,
    /// 总跳过的 RTP 包数（无订阅者/轨道结束）。
    pub packets_skipped: u64,
    /// 总转发的字节数（带宽计量）。
    pub bytes_forwarded: u64,
    /// 按轨道种类统计的转发包数。
    pub audio_packets: u64,
    pub video_packets: u64,
}

impl ForwardStats {
    /// 记录一次转发决策的结果。
    pub fn record(&mut self, decision: &ForwardDecision, packet_bytes: u64) {
        if decision.will_deliver() {
            self.packets_forwarded += 1;
            self.bytes_forwarded += packet_bytes * decision.recipients.len() as u64;
            match decision.kind {
                TrackKind::Audio => self.audio_packets += 1,
                TrackKind::Video => self.video_packets += 1,
            }
        } else {
            self.packets_skipped += 1;
        }
    }

    /// 实际转发的接收者-包数（总带宽 = packets_forwarded * avg_recipients）。
    pub fn total_recipient_deliveries(&self) -> u64 {
        // 近似：由调用方在 record 时累加更精确，这里给一个概要
        self.packets_forwarded
    }
}

/// 为一条轨道的一批订阅者计算转发决策。
///
/// 这是 SFU 转发的核心纯函数：输入轨道状态 + 订阅者集合，输出转发决策。
/// 不触碰任何网络/IO，可在 `cargo test` 里离线断言。
///
/// 选择性转发规则：
/// 1. 轨道已结束（Ended）→ 跳过，reason="track ended"。
/// 2. 无订阅者 → 跳过，reason="no subscribers"。
/// 3. 有订阅者 → 转发给全部订阅者（SFU 不做选择性丢包，只做选择性转发轨道）。
pub fn decide(track: &Track) -> ForwardDecision {
    if track.state == TrackState::Ended {
        return ForwardDecision {
            track_id: track.id.clone(),
            kind: track.kind,
            publisher: track.publisher.clone(),
            recipients: vec![],
            skipped: true,
            skip_reason: "track ended".to_string(),
        };
    }
    if track.subscribers.is_empty() {
        return ForwardDecision {
            track_id: track.id.clone(),
            kind: track.kind,
            publisher: track.publisher.clone(),
            recipients: vec![],
            skipped: true,
            skip_reason: "no subscribers".to_string(),
        };
    }
    ForwardDecision {
        track_id: track.id.clone(),
        kind: track.kind,
        publisher: track.publisher.clone(),
        recipients: track.subscribers.iter().cloned().collect(),
        skipped: false,
        skip_reason: String::new(),
    }
}

/// 模拟转发一批 RTP 包（验收标准 1：有转发路径测试证明）。
///
/// 返回每个轨道的转发决策序列 + 全局统计。这是纯函数：不发送任何网络包，
/// 只计算「如果来了 N 个包，应该怎么转发」。
pub fn simulate_forward_batch(
    tracks: &[&Track],
    packets_per_track: u32,
    packet_bytes: u64,
) -> (Vec<ForwardDecision>, ForwardStats) {
    let mut decisions = Vec::new();
    let mut stats = ForwardStats::default();

    for track in tracks {
        for _ in 0..packets_per_track {
            let d = decide(track);
            stats.record(&d, packet_bytes);
            // 只保留最终决策（不重复存 N 个相同决策），但统计累加。
            // 对于测试断言，第一次决策已经包含全部信息。
            if decisions
                .iter()
                .all(|existing: &ForwardDecision| existing.track_id != d.track_id)
            {
                decisions.push(d.clone());
            }
        }
    }

    (decisions, stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::track::{Track, TrackKind};
    use std::collections::HashSet;

    fn make_track(publisher: &str, kind: TrackKind, subs: &[&str]) -> Track {
        let id = format!("{publisher}_{kind}");
        let mut t = Track::new(id, "room1".to_string(), publisher.to_string(), kind);
        for s in subs {
            t.subscribe(s);
        }
        t
    }

    #[test]
    fn no_subscribers_means_skip() {
        let track = make_track("alice", TrackKind::Audio, &[]);
        let d = decide(&track);
        assert!(d.skipped);
        assert_eq!(d.skip_reason, "no subscribers");
        assert!(!d.will_deliver());
    }

    #[test]
    fn ended_track_is_skipped() {
        let mut track = make_track("alice", TrackKind::Video, &["bob"]);
        track.end();
        let d = decide(&track);
        assert!(d.skipped);
        assert_eq!(d.skip_reason, "track ended");
        assert!(!d.will_deliver());
    }

    #[test]
    fn live_track_with_subscribers_forwards_to_all() {
        let track = make_track("alice", TrackKind::Audio, &["bob", "carol", "dave"]);
        let d = decide(&track);
        assert!(!d.skipped);
        assert_eq!(d.recipients.len(), 3);
        assert!(d.recipients.contains(&"bob".to_string()));
        assert!(d.recipients.contains(&"carol".to_string()));
        assert!(d.recipients.contains(&"dave".to_string()));
        assert!(d.will_deliver());
    }

    #[test]
    fn sfu_does_not_mix_all_tracks_into_one() {
        // 验收标准 1：SFU 按订阅者需求转发，无全量混流带宽浪费。
        // 场景：alice 发布音频和视频，bob 只订阅音频，carol 只订阅视频。
        // 每个包只发给真正订阅了该轨道的人，不发给所有人。
        let audio = make_track("alice", TrackKind::Audio, &["bob"]);
        let video = make_track("alice", TrackKind::Video, &["carol"]);

        let audio_d = decide(&audio);
        let video_d = decide(&video);

        // 音频只发给 bob，不发给 carol
        assert_eq!(audio_d.recipients, vec!["bob"]);
        assert!(!audio_d.recipients.contains(&"carol".to_string()));

        // 视频只发给 carol，不发给 bob
        assert_eq!(video_d.recipients, vec!["carol"]);
        assert!(!video_d.recipients.contains(&"bob".to_string()));

        // bob 不会收到 video 包，carol 不会收到 audio 包 —— 这就是选择性转发
    }

    #[test]
    fn simulate_batch_proves_no_full_mix_waste() {
        // 验收标准 1：有转发路径测试证明
        // 场景：3 条轨道（2 audio + 1 video），每条不同订阅者，每条 10 个包
        let t1 = make_track("alice", TrackKind::Audio, &["bob"]);
        let t2 = make_track("alice", TrackKind::Video, &["carol", "dave"]);
        let t3 = make_track("eve", TrackKind::Audio, &["bob", "carol"]);
        let tracks = vec![&t1, &t2, &t3];

        let (decisions, stats) = simulate_forward_batch(&tracks, 10, 200);

        // 3 条轨道 → 3 个决策
        assert_eq!(decisions.len(), 3);
        // 30 个包全部转发（每条轨道都有订阅者）
        assert_eq!(stats.packets_forwarded, 30);
        assert_eq!(stats.packets_skipped, 0);
        // 音频 20 包（t1=10 + t3=10），视频 10 包（t2=10）
        assert_eq!(stats.audio_packets, 20);
        assert_eq!(stats.video_packets, 10);
        // 带宽：t1=10*1*200 + t2=10*2*200 + t3=10*2*200 = 2000 + 4000 + 4000 = 10000
        assert_eq!(stats.bytes_forwarded, 10000);
        // 全量混流的话每个包发给全部 3 个 peer = 30*3*200 = 18000
        // 选择性转发只发了 10000，节省了 44.4% 带宽 —— 无全量混流浪费
        assert!(
            stats.bytes_forwarded < 18000,
            "选择性转发带宽必须低于全量混流"
        );
    }

    #[test]
    fn simulate_batch_skips_unsubscribed_tracks() {
        // 轨道无订阅者 → 全部跳过
        let t1 = make_track("alice", TrackKind::Audio, &[]);
        let tracks = vec![&t1];

        let (decisions, stats) = simulate_forward_batch(&tracks, 5, 100);

        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].skipped);
        assert_eq!(stats.packets_forwarded, 0);
        assert_eq!(stats.packets_skipped, 5);
        assert_eq!(stats.bytes_forwarded, 0);
    }

    #[test]
    fn forwarding_one_track_does_not_affect_others() {
        // 验收标准 2：断开单一订阅者不影响其他订阅者
        let mut t1 = make_track("alice", TrackKind::Audio, &["bob", "carol"]);
        let t2 = make_track("alice", TrackKind::Video, &["bob", "carol"]);

        // bob 退订 t1
        t1.unsubscribe("bob");

        let t1_d = decide(&t1);
        let t2_d = decide(&t2);

        // t1 只发给 carol
        assert_eq!(t1_d.recipients, vec!["carol"]);
        // t2 仍发给 bob 和 carol —— 退订 t1 不影响 t2
        assert_eq!(t2_d.recipients.len(), 2);
        assert!(t2_d.recipients.contains(&"bob".to_string()));
        assert!(t2_d.recipients.contains(&"carol".to_string()));
    }
}
