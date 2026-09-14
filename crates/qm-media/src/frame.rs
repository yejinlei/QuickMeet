//! 帧类型与媒体样本表示。
//!
//! 刻意保持"纯字节 + 元数据"的自包含表达，不依赖 `rtp` / `webrtc-media`
//! 的具体类型：codec 层是纯函数式转换，RTP 封装由 [`crate::rtp`] 负责，
//! 便于离线单元测试复现（验收标准 3）。
//!
//! 像素/采样布局约定：
//! - 视频（VP8 / H.264）：I420 planar，`Y` 平面 `w*h` 字节，
//!   随后 `U` / `V` 各 `ceil(w/2)*ceil(h/2)` 字节。
//! - 音频（Opus）：`s16le` 交错 PCM，`channels * samples * 2` 字节。

/// 媒体帧的大类，用于分发到音频/视频处理路径。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum FrameKind {
    /// 视频帧（VP8 / H.264）。
    Video,
    /// 音频帧（Opus）。
    Audio,
}

impl FrameKind {
    pub fn is_video(self) -> bool {
        matches!(self, FrameKind::Video)
    }

    pub fn is_audio(self) -> bool {
        matches!(self, FrameKind::Audio)
    }
}

/// 一帧原始媒体样本（编码前 / 解码后）。
#[derive(Debug, Clone)]
pub struct Frame {
    /// 载荷字节。
    pub data: Vec<u8>,
    /// 采样时间戳（视频 = 帧序号，音频 = 采样数）。
    pub timestamp: u64,
    /// 视频帧宽；音频为 `0`。
    pub width: u32,
    /// 视频帧高；音频为 `0`。
    pub height: u32,
    /// 音频采样率；视频为 `0`。
    pub sample_rate: u32,
    /// 音频声道数；视频为 `0`。
    pub channels: u16,
    /// 视频关键帧标记。
    pub keyframe: bool,
}

impl Frame {
    pub fn new_video(data: Vec<u8>, width: u32, height: u32, keyframe: bool) -> Self {
        Self {
            data,
            timestamp: 0,
            width,
            height,
            sample_rate: 0,
            channels: 0,
            keyframe,
        }
    }

    pub fn new_audio(data: Vec<u8>, sample_rate: u32, channels: u16) -> Self {
        Self {
            data,
            timestamp: 0,
            width: 0,
            height: 0,
            sample_rate,
            channels,
            keyframe: true,
        }
    }

    pub fn kind(&self) -> FrameKind {
        if self.sample_rate != 0 {
            FrameKind::Audio
        } else {
            FrameKind::Video
        }
    }

    /// 生成一帧确定性的测试画面（可复现，无随机数）。
    pub fn synthetic_video(width: u32, height: u32, index: u32, keyframe: bool) -> Self {
        let y_plane = (width * height) as usize;
        let mut buf = vec![0u8; y_plane + y_plane / 2];
        for i in 0..y_plane {
            // 灰度渐变 + 随帧位移的条带，保证每帧都不同
            buf[i] = ((i % 256) as u32 + (index * 7) % 64) as u8;
        }
        let cw = width.div_ceil(2) as usize;
        let ch = height.div_ceil(2) as usize;
        for i in 0..cw * ch {
            buf[y_plane + i] = 84 + ((i + index as usize) % 16) as u8;
            buf[y_plane + cw * ch + i] = 128 + ((i * 3 + index as usize) % 20) as u8;
        }
        Self {
            data: buf,
            timestamp: index as u64,
            width,
            height,
            sample_rate: 0,
            channels: 0,
            keyframe,
        }
    }

    /// 生成一帧确定性的测试音频（单频正弦波 + 随帧直流偏移）。
    pub fn synthetic_audio(frames: u32, sample_rate: u32, channels: u16) -> Self {
        let ch = if channels == 0 { 1 } else { channels };
        let mut buf = Vec::with_capacity(frames as usize * ch as usize * 2);
        for i in 0..frames {
            let phase = (i as f64 / 44100.0) * 440.0 * 2.0 * std::f64::consts::PI;
            // 440 Hz，幅度约 0.35 满幅，加固定直流偏置，保证非零且可复现
            let s = (phase.sin() * 12000.0 + 800.0) as i16;
            for _ in 0..ch {
                buf.extend_from_slice(&s.to_le_bytes());
            }
        }
        Self::new_audio(buf, sample_rate, ch)
    }
}

/// 一条已封装的媒体载荷（等价于一个 RTP payload / 一个编码包）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Packet {
    /// 载荷字节（编码输出，不含 RTP 头部）。
    pub data: Vec<u8>,
    /// 视频帧末包标记。
    pub marker: bool,
    /// 该包携带的采样数（音频 = PCM 采样数，视频 = 帧计数），用于时间戳推进。
    pub samples: u32,
}
