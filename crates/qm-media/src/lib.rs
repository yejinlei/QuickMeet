//! # qm-media — QuickMeet 媒体 / 编解码层
//!
//! 组成：
//! * [`crate::codec`] —— 统一 codec 封装（可复用模块，业务层唯一入口）
//! * [`crate::registry`] —— CodecId → 编解码器实现（接真实编解码器只改这里）
//! * [`crate::rtp`] —— 载荷与 RTP 头部组装/解析（RFC 3550）
//! * [`crate::verify`] —— VP8 / H.264 / Opus 可复现收发兼容验证（验收标准 3）
//! * [`crate::peer_connection`] —— webrtc-rs 0.17.1 双端 PeerConnection 互连验证（验收标准 2）

pub mod bitstream;
pub mod codec;
pub mod codec_id;
pub mod frame;
pub mod impls;
pub mod registry;
pub mod rtp;

#[cfg(feature = "webrtc")]
pub mod peer_connection;

pub mod verify;

pub use codec::{Codec, CodecParams, Decoder, Encoder};
pub use codec_id::CodecId;
pub use frame::{Frame, FrameKind, Packet};
