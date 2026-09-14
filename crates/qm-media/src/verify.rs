//! VP8 / H.264 / Opus 可复现收发兼容验证（验收标准 3）。
//!
//! 三个 codec 走同一条真实链路：
//! `Frame -> Codec::encode -> MediaStream::packetize -> rtp::marshal`
//!      -> `rtp::unmarshal -> rtp::unpack -> Codec::decode` -> 与原始帧逐字节比对
//! 全链路纯 Rust、无网络、无随机数、无 C 依赖，所以 `cargo test -p qm-media`
//! 在任何机器上都能得到完全一致的字节流与报告文本（"可复现"）。
//!
//! 每一项验证覆盖四个断言：
//! 1. 无损 —— 解码帧与源帧字节级一致（含分辨率 / 采样率 / 声道 / 关键帧标记）；
//! 2. 可复现 —— 同一输入跑两遍，线上字节完全相同（`deterministic == true`）；
//! 3. 头部正确 —— 每个 RTP 包序列化后再反序列化，字段逐一相等；
//! 4. 完整性护栏 —— 篡改载荷中一个字节后，解码必须报错（FNV-1a CRC 生效）。
//!
//! 说明：默认 payload 由确定性封装生成，不是真实 VP8/H.264/Opus 压缩码流；
//! 压缩码流的真实互通需要接 `--features native-opus,native-h264,native-vp8` 复测，
//! 替换点集中在 [`crate::registry`]，上层调用不变。

use qm_common::error::{Error, Result};

use crate::codec::Codec;
use crate::codec_id::{CodecId, OPUS_FRAME_20MS_SAMPLES, OPUS_SAMPLE_RATE};
use crate::frame::Frame;
use crate::registry::{implementation_report, supported_codecs};
use crate::rtp::{marshal, unmarshal, unpack, RTP_HEADER_LEN, MediaStream, RtpPacket};

/// 视频测试画面宽度。
pub const VIDEO_WIDTH: u32 = 640;
/// 视频测试画面高度。
pub const VIDEO_HEIGHT: u32 = 480;
/// 默认验证帧数（demo 通过 `--frames` 放大）。
pub const DEFAULT_FRAMES: u32 = 10;

/// 单个 codec 的收发兼容验证结果。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RoundTrip {
    /// 被测编解码。
    pub codec: CodecId,
    /// 当前构建的实际实现来源（确定性封装 / 原生库）。
    pub implementation: String,
    /// SDP/RTP 协商的 payload type。
    pub payload_type: u8,
    /// 时钟频率（视频 90 kHz，Opus 48 kHz）。
    pub clock_rate: u32,
    /// 发送帧数。
    pub frames_sent: u32,
    /// 成功还原的帧数。
    pub frames_received: u32,
    /// 原始媒体字节数。
    pub bytes_in: u64,
    /// 线上（含 12 字节 RTP 头）字节数。
    pub bytes_on_wire: u64,
    /// 通过头部一致性检查的 RTP 包数。
    pub rtp_headers_checked: u32,
    /// 无损：解码帧与源帧字节级一致。
    pub lossless: bool,
    /// 可复现：两次独立运行的线上字节完全相同。
    pub deterministic: bool,
    /// 完整性护栏：篡改载荷后解码被拒绝。
    pub integrity_guarded: bool,
}

impl RoundTrip {
    /// 该 codec 是否全部通过。
    pub fn ok(&self) -> bool {
        self.lossless
            && self.deterministic
            && self.integrity_guarded
            && self.frames_sent == self.frames_received
            && self.rtp_headers_checked == self.frames_sent
    }
}

/// 构造确定性的源帧序列（无随机数）。
pub fn source_frames(codec: CodecId, frames: u32) -> Vec<Frame> {
    if codec.is_video() {
        (0..frames)
            .map(|i| Frame::synthetic_video(VIDEO_WIDTH, VIDEO_HEIGHT, i, i == 0))
            .collect()
    } else {
        (0..frames)
            .map(|_| Frame::synthetic_audio(OPUS_FRAME_20MS_SAMPLES, OPUS_SAMPLE_RATE, 1))
            .collect()
    }
}

/// 编码 + 封装：返回源帧、RTP 包、以及序列化后的线上字节。
fn capture(codec: CodecId, frames: u32) -> Result<(Vec<Frame>, Vec<RtpPacket>, Vec<Vec<u8>>)> {
    let enc = Codec::new(codec)?;
    let source = source_frames(codec, frames);
    // SSRC 保证非零（RFC 3550 要求），并按 codec 区分，便于混合流排查。
    let mut stream = MediaStream::new(codec, 0x1122_0000 + u32::from(codec.default_payload_type()));
    let mut rtp = Vec::with_capacity(frames as usize);
    let mut wire = Vec::with_capacity(frames as usize);
    for (i, frame) in source.iter().enumerate() {
        let pkts = enc.encode(frame)?;
        if pkts.len() != 1 {
            return Err(Error::Codec {
                codec: codec.to_string(),
                message: format!(
                    "验证前提不成立：第 {i} 帧产出 {} 个载荷包（本验证按 1 帧 1 包设计）",
                    pkts.len()
                ),
            });
        }
        let pkt = stream.packetize(&pkts[0]);
        wire.push(marshal(&pkt)?);
        rtp.push(pkt);
    }
    Ok((source, rtp, wire))
}

/// 篡改线上载荷中的一个字节后，解码必须失败。
fn integrity_guarded(codec: CodecId, wire: &[Vec<u8>]) -> bool {
    if wire.is_empty() {
        return false;
    }
    let mut bad = wire[0].clone();
    // 篡改点必须落在**载荷体**（CRC 覆盖区）里，而不是载荷头：
    // 载荷头里的 w/h/ts 等字段解码后不会回比原帧，改那里检不出来。
    // 取载荷体中点，既避开魔数/头尾，也一定在 CRC 保护范围内。
    let header = codec.payload_header_len();
    let body_len = bad.len() - RTP_HEADER_LEN - header;
    let at = RTP_HEADER_LEN + header + body_len / 2;
    if at >= bad.len() || body_len == 0 {
        return false;
    }
    bad[at] ^= 0xFF;
    let dec = match Codec::new(codec) {
        Ok(c) => c,
        Err(_) => return false,
    };
    let pkt = match unmarshal(&bad) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let payloads = match unpack(&[pkt]) {
        Ok(p) => p,
        Err(_) => return false,
    };
    let mut guarded = true;
    for p in &payloads {
        if dec.decode(p).map(|f| f.is_some()).unwrap_or(false) {
            guarded = false;
        }
    }
    guarded
}

/// 两帧的媒体内容与参数是否完全一致。
fn frames_equal(a: &Frame, b: &Frame) -> bool {
    a.data == b.data
        && a.width == b.width
        && a.height == b.height
        && a.sample_rate == b.sample_rate
        && a.channels == b.channels
        && a.keyframe == b.keyframe
}

/// 执行单个 codec 的完整收发兼容验证。
pub fn verify(codec: CodecId, frames: u32) -> Result<RoundTrip> {
    let (source, rtp, wire) = capture(codec, frames)?;
    // 再跑一遍同样的输入，用于可复现性断言。
    let (_, _, wire_again) = capture(codec, frames)?;
    let deterministic = wire == wire_again;

    let mut headers_checked = 0u32;
    let mut packets = Vec::with_capacity(wire.len());
    for (i, w) in wire.iter().enumerate() {
        let back = unmarshal(w)?;
        if back == rtp[i] {
            headers_checked += 1;
        }
        packets.push(back);
    }

    let payloads = unpack(&packets)?;
    let dec = Codec::new(codec)?;
    let mut received = Vec::with_capacity(payloads.len());
    for (i, pkt) in payloads.iter().enumerate() {
        let frame = dec
            .decode(pkt)?
            .ok_or_else(|| Error::Codec {
                codec: codec.to_string(),
                message: format!("第 {i} 个载荷包不足以还原一帧"),
            })?;
        received.push(frame);
    }

    let lossless = source.len() == received.len()
        && source.iter().zip(received.iter()).all(|(a, b)| frames_equal(a, b));

    Ok(RoundTrip {
        codec,
        implementation: implementation_report()
            .into_iter()
            .find(|(id, _)| *id == codec)
            .map(|(_, s)| s.to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        payload_type: codec.default_payload_type(),
        clock_rate: codec.clock_rate(),
        frames_sent: frames,
        frames_received: received.len() as u32,
        bytes_in: source.iter().map(|f| f.data.len() as u64).sum(),
        bytes_on_wire: wire.iter().map(|w| w.len() as u64).sum(),
        rtp_headers_checked: headers_checked,
        lossless,
        deterministic,
        integrity_guarded: integrity_guarded(codec, &wire),
    })
}

/// 验证全部已注册 codec。
pub fn run_all() -> Result<Vec<RoundTrip>> {
    supported_codecs().into_iter().map(|c| verify(c, DEFAULT_FRAMES)).collect()
}

/// 验证全部 codec，指定帧数。
pub fn run_all_frames(frames: u32) -> Result<Vec<RoundTrip>> {
    supported_codecs().into_iter().map(|c| verify(c, frames)).collect()
}

/// 渲染成可直接贴进 PR / 评论区的文本报告。
pub fn render(reports: &[RoundTrip]) -> String {
    let mut out = String::new();
    out.push_str("QuickMeet codec 收发兼容验证\n");
    out.push_str(&format!(
        "{:<7} {:<20} {:>4} {:>8} {:>5} {:>5} {:>10} {:>10} {:>6}\n",
        "codec", "implementation", "pt", "clock", "sent", "recv", "bytes_in", "bytes_wire", "verdict"
    ));
    out.push_str(&format!("{} {} {:>10}\n", "─────".repeat(7), "─────".repeat(20), "─────".repeat(6)));
    for r in reports {
        out.push_str(&format!(
            "{:<7} {:<20} {:>4} {:>8} {:>5} {:>5} {:>10} {:>10} {:>6}\n",
            r.codec,
            r.implementation,
            r.payload_type,
            r.clock_rate,
            r.frames_sent,
            r.frames_received,
            r.bytes_in,
            r.bytes_on_wire,
            if r.ok() { "PASS" } else { "FAIL" }
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_pass(r: &RoundTrip) {
        assert_eq!(r.frames_sent, r.frames_received, "收发帧数应一致");
        assert_eq!(r.rtp_headers_checked, r.frames_sent, "所有 RTP 包头部应一致");
        assert!(r.lossless, "收发必须无损");
        assert!(r.deterministic, "收发必须可复现");
        assert!(r.integrity_guarded, "篡改载荷必须被拒绝");
        assert!(r.ok());
    }

    #[test]
    fn vp8_round_trip_passes() {
        assert_pass(&verify(CodecId::Vp8, 12).unwrap());
    }

    #[test]
    fn h264_round_trip_passes() {
        assert_pass(&verify(CodecId::H264, 12).unwrap());
    }

    #[test]
    fn opus_round_trip_passes() {
        assert_pass(&verify(CodecId::Opus, 25).unwrap());
    }

    #[test]
    fn run_all_covers_every_registered_codec() {
        let reports = run_all().unwrap();
        assert_eq!(reports.len(), 3);
        for r in &reports {
            assert_pass(r);
        }
    }

    #[test]
    fn render_contains_verdict_per_codec() {
        let reports = run_all().unwrap();
        let text = render(&reports);
        assert!(text.contains("PASS"));
        assert!(text.contains("vp8"));
        assert!(text.contains("h264"));
        assert!(text.contains("opus"));
    }

    #[test]
    fn frames_argument_is_honoured() {
        let r = verify(CodecId::Opus, 3).unwrap();
        assert_eq!(r.frames_sent, 3);
        assert_pass(&r);
    }
}
