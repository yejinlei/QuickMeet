//! 编解码标识与 RFC 常量。
//!
//! 这些常量全部来自公开 RFC / ITU 标准，用于把内部 codec 名称映射到
//! SDP/RTP 协商用的参数，也让"收发兼容"验证有一个共同参照：
//!
//! - Opus  32 kHz mono  10 ms → 320 采样/帧（RFC 6716 §6）
//! - Opus  48 kHz mono  20 ms → 960 采样/帧（Opus 推荐帧长，RFC 6716 §2.2）
//! - VP8   RFC 7741（payload + PLI/FIR/REMB 反馈）
//! - H.264 RFC 6184（STAP-A 聚合 + SEP 关键帧）
//! - RTP   RFC 3550（90 kHz 视频时钟、序列号、SSRC）

/// 视频时间戳使用的时钟频率（RFC 3550 / RFC 7741 / RFC 6184）。
pub const VIDEO_CLOCK_HZ: u32 = 90_000;
/// VP8 默认帧率。
pub const VP8_FPS: u32 = 30;
/// H.264 默认帧率。
pub const H264_FPS: u32 = 30;
/// 单帧视频时间戳步长。
pub const VIDEO_TS_STEP: u64 = (VIDEO_CLOCK_HZ as u64) / (VP8_FPS as u64);

/// Opus 最小帧长（10 ms）。
pub const OPUS_FRAME_10MS_SAMPLES: u32 = 320;
/// Opus 推荐帧长（20 ms）。
pub const OPUS_FRAME_20MS_SAMPLES: u32 = 960;
/// 音频默认采样率。
pub const OPUS_SAMPLE_RATE: u32 = 48_000;

/// 单帧像素（Y）缓冲上限保护：`1080p * 1.5`（I420 平面布局）。
pub const MAX_VIDEO_PLANE_BYTES: usize = 1920 * 1080 * 3 / 2;

/// 编解码标识。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum CodecId {
    /// VP8（RFC 7741），SDP payload type 96。
    Vp8,
    /// H.264（RFC 6184），SDP payload type 100。
    H264,
    /// Opus（RFC 6716），SDP payload type 111。
    Opus,
}

impl CodecId {
    /// SDP/RTP 协商用的 payload type（与 webrtc-rs 默认媒体引擎一致）。
    pub fn default_payload_type(self) -> u8 {
        match self {
            CodecId::Vp8 => 96,
            CodecId::H264 => 100,
            CodecId::Opus => 111,
        }
    }

    /// 编解码器自己载荷头的长度（字节）。
    ///
    /// 这个数是"编码器写了多少字节"与"解码器/CRC 校验从哪里开始"的唯一契约来源。
    /// 曾出现头长常量写 16、实际写入 24 的情况（`w/h` 是 `u32` 写 4 字节、`ts` 是
    /// `u64` 写 8 字节），CRC 因此偏移到时间戳区，帧 0 被误报成"期望 CRC 0x0"。
    pub fn payload_header_len(self) -> usize {
        match self {
            CodecId::Vp8 => 24,
            CodecId::H264 => 30,
            CodecId::Opus => 22,
        }
    }

    pub fn is_video(self) -> bool {
        matches!(self, CodecId::Vp8 | CodecId::H264)
    }

    pub fn is_audio(self) -> bool {
        matches!(self, CodecId::Opus)
    }

    /// SDP `a=rtpmap` 中的编码名称。
    pub fn rtpmap(self) -> &'static str {
        match self {
            CodecId::Vp8 => "VP8",
            CodecId::H264 => "H264",
            CodecId::Opus => "opus",
        }
    }

    /// 时钟频率（Hz）：视频 90 kHz，Opus 48 kHz。
    pub fn clock_rate(self) -> u32 {
        match self {
            CodecId::Vp8 | CodecId::H264 => VIDEO_CLOCK_HZ,
            CodecId::Opus => OPUS_SAMPLE_RATE,
        }
    }

    /// 声道数（仅音频有意义）。
    pub fn channels(self) -> u16 {
        match self {
            CodecId::Opus => 1,
            CodecId::Vp8 | CodecId::H264 => 0,
        }
    }
}

impl std::fmt::Display for CodecId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecId::Vp8 => write!(f, "vp8"),
            CodecId::H264 => write!(f, "h264"),
            CodecId::Opus => write!(f, "opus"),
        }
    }
}
