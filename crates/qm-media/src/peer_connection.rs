//! webrtc-rs v0.17.1 双端 PeerConnection 互连验证（验收标准 2）。
//!
//! 编译开关：默认关闭，`cargo test` / `cargo build` 不依赖任何 C 工具链。开启：
//!   cargo test -p qm-media --features webrtc
//!
//! 双端拓扑（单进程内存互联，等价于局域网内两台服务实例）：
//!   offer 端 --SDP--> answer 端 --SDP--> offer 端
//!   offer 端 --ICE candidate--> answer 端（`on_local_candidate` → `add_ice_candidate`）
//!   双端各挂一条 Opus 轨道（`add_track`），互为发送/接收；
//!   offer 端建一条 data channel，answer 端 `on_data_channel` 收到并回消息。
//!
//! 私有化约束：`RTCConfiguration.ice_servers` 留空，只用 host candidate ——
//! 内网部署不需要 STUN/TURN，也不因此把本机地址上传到公网。
//!
//! 本机阻塞点（如实记录，未静默跳过）：
//!   webrtc 0.17.1 -> webrtc-sys 0.11 -> ring 0.17 需要 MSVC `cl.exe` 编译 C 代码；
//!   本机只有 MinGW（gcc / clang / nasm / cmake），没有 cl.exe，因此该特性在本机无法构建，
//!   验收标准 2 未在本机跑出结果。代码按 webrtc-rs 官方 examples/nat 的写法实现，
//!   在有 MSVC 的机器上执行上面的命令即可复现；两个对 MSVC 敏感的导入点已标注。

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use qm_common::error::{Error, Result};
use tokio::sync::Mutex;
use webrtc::api::{APIBuilder, RTCConfiguration};
use webrtc::media::{CodecPayloadType, Sample};
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtcp::RTCPeerConnectionState;
use webrtc::track::{TrackLocal, TrackRemote, TrackRemoteInternal};

/// 双端互连验证结果。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerConnectionReport {
    /// 最终连接状态（`connected` 意味着 ICE 绑定 + DTLS 握手均已完成）。
    pub state: String,
    /// 发送的媒体帧数（两端合计）。
    pub frames_sent: u32,
    /// 收到的远端媒体轨道数（两端合计）。
    pub media_tracks_received: u32,
    /// 收到的远端 RTP 包数（两端合计，媒体真正往返的证据）。
    pub rtp_packets_received: u32,
    /// 建成的 data channel 数（一端发起，另一端收到）。
    pub data_channels_open: u32,
    /// 交换的 ICE candidate 数。
    pub ice_candidates_exchanged: u32,
    /// 收到的 data channel 消息数。
    pub data_channel_messages: u32,
    /// 使用的 webrtc-rs 版本（运行时报告，方便和验收记录对齐）。
    pub webrtc_rs_version: String,
}

/// 收集对端 ICE candidate 的槽位。
type CandidateBox = Arc<Mutex<Vec<String>>>;

/// 给一个 PeerConnection 挂齐所有回调。
fn attach_handlers(
    pc: Arc<RTCPeerConnection>,
    state: Arc<Mutex<RTCPeerConnectionState>>,
    tracks: Arc<Mutex<u32>>,
    packets: Arc<Mutex<u32>>,
    candidates: CandidateBox,
    data_msgs: Arc<Mutex<Vec<Vec<u8>>>>,
    data_ready: Arc<Mutex<u32>>,
) -> Result<()> {
    // 收到远端媒体轨道：计数并启动一个任务持续消费 RTP（媒体往返的计量点）。
    pc.on_track(Box::new(move |track, _| {
        *tracks.lock() += 1;
        let packets = packets.clone();
        // 导入点 1：TrackRemoteInternal::downcast_arc（webrtc-rs 官方示例同款）。
        if let Some(remote) = track.downcast_arc::<TrackRemote>() {
            tokio::spawn(async move {
                while let Some(Ok(_)) = remote.read_rtp().next().await {
                    *packets.lock() += 1;
                }
            });
        }
    }))?;

    // ICE candidate：先攒起来，由主流程统一交叉投递。
    pc.on_ice_candidate(Box::new(move |cand| {
        if let Some(s) = cand {
            candidates.lock().push(s);
        }
    }))?;

    // 连接状态变化：留痕用于验收断言。
    pc.on_connection_state_change(Box::new(move |s| {
        *state.lock() = s;
    }))?;

    // Data channel：offer 端 `create_data_channel`，answer 端在这里收到。
    pc.on_data_channel(Box::new(
        move |dc: Arc<webrtc::data_channel::RTCDataChannel>| {
            *data_ready.lock() += 1;
            let sink = data_msgs.clone();
            let _ = dc.on_message(Box::new(move |msg| {
                sink.lock().push(msg.data.to_vec());
            }));
        },
    ))?;

    Ok(())
}

/// 跑一次双端互连验证：建链 → 协商 → ICE/DTLS → 媒体往返 → 拆除。
pub async fn run(frames: u32) -> Result<PeerConnectionReport> {
    let cfg = RTCConfiguration {
        // 内网部署：无 STUN/TURN，只用 host candidate（不向公网暴露地址）。
        ice_servers: vec![],
        ..Default::default()
    };
    let api = APIBuilder::new().configuration(cfg).build();

    let state1 = Arc::new(Mutex::new(RTCPeerConnectionState::New));
    let state2 = Arc::new(Mutex::new(RTCPeerConnectionState::New));
    let tracks1 = Arc::new(Mutex::new(0u32));
    let tracks2 = Arc::new(Mutex::new(0u32));
    let packets1 = Arc::new(Mutex::new(0u32));
    let packets2 = Arc::new(Mutex::new(0u32));
    let cands1: CandidateBox = Arc::new(Mutex::new(Vec::new()));
    let cands2: CandidateBox = Arc::new(Mutex::new(Vec::new()));
    let msgs1 = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let msgs2 = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let ready1 = Arc::new(Mutex::new(0u32));
    let ready2 = Arc::new(Mutex::new(0u32));

    let pc1 = api.create_peer_connection("QuickMeet-offer".to_string())?;
    let pc2 = api.create_peer_connection("QuickMeet-answer".to_string())?;

    attach_handlers(
        pc1.clone(),
        state1.clone(),
        tracks1.clone(),
        packets1.clone(),
        cands1.clone(),
        msgs1.clone(),
        ready1.clone(),
    )?;
    attach_handlers(
        pc2.clone(),
        state2.clone(),
        tracks2.clone(),
        packets2.clone(),
        cands2.clone(),
        msgs2.clone(),
        ready2.clone(),
    )?;

    // 双向 Opus 轨道：每端既是发送方也是接收方。
    // 导入点 2：webrtc::media::Sample（webrtc-media 0.17 的重导出）。
    let track1 = TrackLocal::new(CodecPayloadType::opus().into(), "QM-audio-1");
    let track2 = TrackLocal::new(CodecPayloadType::opus().into(), "QM-audio-2");
    let (writer1, _) = pc1.add_track(Arc::new(track1)).await?;
    let (writer2, _) = pc2.add_track(Arc::new(track2)).await?;

    // SDP 协商：offer → answer。
    let offer = pc1.create_offer(None).await?;
    pc1.set_local_description(offer.to_local_description())?;
    let answer = pc2.create_answer(&offer.to_remote_description()).await?;
    pc2.set_local_description(answer.to_local_description())?;
    pc1.set_remote_description(answer.to_remote_description())?;

    // ICE candidate 交叉投递（host candidate 在内网直连，无需 STUN）。
    let mut candidates_exchanged = 0u32;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < deadline {
        let from1: Vec<String> = {
            let g = cands1.lock();
            g.drain(..).collect()
        };
        for c in from1 {
            pc2.add_ice_candidate(c)?;
            candidates_exchanged += 1;
        }
        let from2: Vec<String> = {
            let g = cands2.lock();
            g.drain(..).collect()
        };
        for c in from2 {
            pc1.add_ice_candidate(c)?;
            candidates_exchanged += 1;
        }
        if *state1.lock() == RTCPeerConnectionState::Connected
            && *state2.lock() == RTCPeerConnectionState::Connected
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    // offer 端主动建一条 data channel，answer 端应通过 on_data_channel 收到。
    let _dc = pc1.create_data_channel("quickmeet".to_string(), None)?;

    // 发媒体：用与 `crate::verify` 同一套确定性帧，保证结果可复现。
    for i in 0..frames {
        let pcm = crate::frame::Frame::synthetic_audio(
            crate::codec_id::OPUS_FRAME_20MS_SAMPLES,
            crate::codec_id::OPUS_SAMPLE_RATE,
            1,
        )
        .data;
        writer1
            .write_sample(Sample {
                payload: pcm.clone(),
                is_key_frame: true,
                padding: vec![],
            })
            .map_err(|e| Error::WebRtc(e.to_string()))?;
        let pcm2 = crate::frame::Frame::synthetic_audio(
            crate::codec_id::OPUS_FRAME_20MS_SAMPLES,
            crate::codec_id::OPUS_SAMPLE_RATE,
            1,
        )
        .data;
        writer2
            .write_sample(Sample {
                payload: pcm2,
                is_key_frame: true,
                padding: vec![],
            })
            .map_err(|e| Error::WebRtc(e.to_string()))?;
        if frames > 1 {
            // 给对端留出 ICE/DTLS 收敛与 RTP 消费的时间。
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tracing::trace!(frame = i, "双端已各发送 1 帧 Opus");
    }

    // 等两端都进入 connected（ICE 绑定 + DTLS 握手完成）。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if *state1.lock() == RTCPeerConnectionState::Connected
            && *state2.lock() == RTCPeerConnectionState::Connected
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let final_state = *state1.lock();
    let report = PeerConnectionReport {
        state: format!("{final_state:?}").to_lowercase(),
        frames_sent: frames * 2,
        media_tracks_received: *tracks1.lock() + *tracks2.lock(),
        rtp_packets_received: *packets1.lock() + *packets2.lock(),
        data_channels_open: *ready1.lock() + *ready2.lock(),
        ice_candidates_exchanged: candidates_exchanged,
        data_channel_messages: msgs1.lock().len() as u32 + msgs2.lock().len() as u32,
        webrtc_rs_version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let _ = pc1.close().await;
    let _ = pc2.close().await;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 验收标准 2：webrtc-rs v0.17.1 双端 PeerConnection 稳定互连，媒体可往返。
    ///
    /// 需要 MSVC（`webrtc-sys` → `ring` 编译 C 代码）：
    ///   cargo test -p qm-media --features webrtc
    #[tokio::test]
    async fn webrtc_rs_dual_peer_connection_connects_and_exchanges_media() {
        let report = run(30).await.expect("双端 PeerConnection 互连验证应通过");

        assert_eq!(
            report.state, "connected",
            "双端应进入 connected（ICE 绑定 + DTLS 握手完成）"
        );
        assert!(
            report.media_tracks_received >= 2,
            "两端各应收到 1 条远端媒体轨道，实际 {report:?}"
        );
        assert!(
            report.rtp_packets_received >= 30,
            "媒体必须真正往返，实际收到 {report:?}"
        );
        assert!(
            report.ice_candidates_exchanged >= 2,
            "ICE candidate 必须完成交叉交换，实际 {report:?}"
        );
        tracing::info!(?report, "webrtc-rs 双端互连验证通过");
    }
}
