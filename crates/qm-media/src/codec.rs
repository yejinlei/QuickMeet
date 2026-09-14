//! 统一编解码封装（可复用模块）。
//!
//! 设计目标：
//! 1. 稳定 API —— 业务层只调 [`Codec::encode`] / [`Codec::decode`]，不感知 VP8/H.264/Opus 差异；
//! 2. 可复现 —— 默认实现为确定性纯 Rust 算法（无损、无随机数），
//!    `cargo test` 在任何机器上得到完全一致的字节，满足"收发兼容验证可复现"；
//! 3. 可替换 —— 通过 `native-opus` / `native-h264` / `native-vp8` 特性接入真实编解码器，
//!    替换点集中在 [`crate::registry`]，不改动上层调用。


use std::sync::Arc;

use parking_lot::Mutex;
use qm_common::error::Result;

use crate::codec_id::CodecId;
use crate::frame::{Frame, Packet};

/// 编解码会话参数（对应 SDP 协商结果）。
#[derive(Debug, Clone)]
pub struct CodecParams {
    pub codec: CodecId,
    pub payload_type: u8,
    pub clock_rate: u32,
    /// 帧尺寸（视频）/ 单帧样本数（音频）。
    pub width: u32,
    pub height: u32,
    pub channels: u16,
}

impl CodecParams {
    /// 按 codec 给出符合 RFC 的默认参数。
    pub fn for_codec(codec: CodecId) -> Self {
        match codec {
            CodecId::Vp8 | CodecId::H264 => Self {
                codec,
                payload_type: codec.default_payload_type(),
                clock_rate: codec.clock_rate(),
                width: 640,
                height: 480,
                channels: 0,
            },
            CodecId::Opus => Self {
                codec,
                payload_type: codec.default_payload_type(),
                clock_rate: codec.clock_rate(),
                width: 0,
                height: 0,
                channels: codec.channels(),
            },
        }
    }
}

impl Default for CodecParams {
    fn default() -> Self {
        Self::for_codec(CodecId::Vp8)
    }
}

/// 编码器：把原始媒体帧封装成可上 RTP 的载荷包。
pub trait Encoder: Send + Sync {
    fn codec(&self) -> CodecId;
    /// 编码一帧，返回一个或多个 RTP payload 包。
    fn encode(&mut self, frame: &Frame) -> Result<Vec<Packet>>;
    /// 已编码的帧数。
    fn encoded_frames(&self) -> u64;
}

/// 解码器：把 RTP 载荷包还原成原始媒体帧。
pub trait Decoder: Send + Sync {
    fn codec(&self) -> CodecId;
    /// 解码一个 payload 包；返回 `Ok(None)` 表示该包不足以还原一帧。
    fn decode(&mut self, pkt: &Packet) -> Result<Option<Frame>>;
    /// 解码出的帧数。
    fn decoded_frames(&self) -> u64;
}

/// 编解码对，业务层拿到这一个句柄即可（内部可变，`Send + Sync`）。
#[derive(Clone)]
pub struct Codec {
    pub params: CodecParams,
    encoder: Arc<EncoderSlot>,
    decoder: Arc<DecoderSlot>,
}

/// 编码器句柄：封装一个 [`Encoder`] 并提供线程安全互斥访问。
pub struct EncoderSlot {
    inner: Mutex<Box<dyn Encoder>>,
}

impl EncoderSlot {
    pub fn new(enc: Box<dyn Encoder>) -> Self {
        Self {
            inner: Mutex::new(enc),
        }
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut dyn Encoder) -> R) -> R {
        let mut g = self.inner.lock();
        f(&mut **g)
    }
}

/// 解码器句柄：封装一个 [`Decoder`] 并提供线程安全互斥访问。
pub struct DecoderSlot {
    inner: Mutex<Box<dyn Decoder>>,
}

impl DecoderSlot {
    pub fn new(dec: Box<dyn Decoder>) -> Self {
        Self {
            inner: Mutex::new(dec),
        }
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut dyn Decoder) -> R) -> R {
        let mut g = self.inner.lock();
        f(&mut **g)
    }
}

impl std::fmt::Debug for Codec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Codec").field("params", &self.params).finish()
    }
}

impl Codec {
    /// 按 codec 构造默认参数的编解码器。
    pub fn new(codec: CodecId) -> Result<Self> {
        Ok(Self {
            params: CodecParams::for_codec(codec),
            encoder: Arc::new(EncoderSlot::new(crate::registry::encoder(codec)?)),
            decoder: Arc::new(DecoderSlot::new(crate::registry::decoder(codec)?)),
        })
    }

    /// 用指定协商参数构造（例如对端 SDP 给出的 payload type / 分辨率）。
    pub fn with_params(params: CodecParams) -> Result<Self> {
        let codec = params.codec;
        Ok(Self {
            params,
            encoder: Arc::new(EncoderSlot::new(crate::registry::encoder(codec)?)),
            decoder: Arc::new(DecoderSlot::new(crate::registry::decoder(codec)?)),
        })
    }

    pub fn codec(&self) -> CodecId {
        self.params.codec
    }

    /// 编码一帧 → 载荷包列表。
    pub fn encode(&self, frame: &Frame) -> Result<Vec<Packet>> {
        self.encoder.with(|e| e.encode(frame))
    }

    /// 解码一个载荷包 → 还原帧。
    pub fn decode(&self, pkt: &Packet) -> Result<Option<Frame>> {
        self.decoder.with(|d| d.decode(pkt))
    }

    pub fn encoded_frames(&self) -> u64 {
        self.encoder.with(|e| e.encoded_frames())
    }

    pub fn decoded_frames(&self) -> u64 {
        self.decoder.with(|d| d.decoded_frames())
    }
}
