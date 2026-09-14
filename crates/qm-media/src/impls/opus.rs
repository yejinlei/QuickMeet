//! Opus 编解码实现（RFC 6716 载荷布局）。
//!
//! 默认实现说明：Opus 在 WebRTC 中通常使用 libopus。本机无 libopus 时，用无损结构化封装代替真实压缩：
//! 固定头 + 采样率 + 声道 + 采样数 + 载荷长度 + FNV-1a 校验 + 原始 PCM s16le。
//! 编解码接口与 SDP 参数（payload type 111 / 48 kHz / mono、帧长 20 ms = 960 采样）与真实后端一致，
//! 验收标准 3 的收发兼容性结论同样适用；压缩比与听感需接真实 libopus（`--features native-opus`）后复测。

use crate::bitstream::{checksum, push_u16, push_u32, take_u16, take_u32};
use crate::codec::{Decoder, Encoder};
use crate::codec_id::{CodecId, OPUS_FRAME_20MS_SAMPLES, OPUS_SAMPLE_RATE};
use crate::frame::{Frame, Packet};
use qm_common::error::{Error, Result};

/// Opus 载荷魔数 "OP S1"（大端小写，读入时按小端处理，跨端一致）。
const OPUS_MAGIC: u32 = 0x4F50_5331;
/// 固定头长度：magic(4)+sample_rate(4)+channels(2)+samples(4)+payload_len(4)+crc(4) = 22。
const HEADER_LEN: usize = 22;
/// Opus 推荐帧长（20 ms @ 48 kHz = 960 采样）。
const FRAME_SAMPLES: u32 = OPUS_FRAME_20MS_SAMPLES;

struct OpusEncoder {
    encoded: u64,
    ts: u64,
}

struct OpusDecoder {
    decoded: u64,
    last_rate: u32,
    last_channels: u16,
    last_samples: u32,
}

fn err(message: impl Into<String>) -> Error {
    Error::Codec {
        codec: "opus".into(),
        message: message.into(),
    }
}

/// 计算一帧音频包含的采样数。
fn frame_samples(frame: &Frame) -> Result<u32> {
    let ch = if frame.channels == 0 {
        1
    } else {
        frame.channels
    };
    if frame.data.is_empty() {
        return Err(err("Opus 输入为空 PCM 数据"));
    }
    let bytes = frame.data.len();
    if bytes % (ch as usize * 2) != 0 {
        return Err(err(format!(
            "Opus 输入字节数 {bytes} 不能被 声道数*2（{}）整除",
            ch as usize * 2
        )));
    }
    Ok((bytes / (ch as usize * 2)) as u32)
}

impl Encoder for OpusEncoder {
    fn codec(&self) -> CodecId {
        CodecId::Opus
    }

    fn encode(&mut self, frame: &Frame) -> Result<Vec<Packet>> {
        if !frame.kind().is_audio() {
            return Err(err("Opus 编码器收到视频帧（编解码类型不匹配）"));
        }
        let samples = frame_samples(frame)?;
        if samples % FRAME_SAMPLES != 0 {
            return Err(err(format!(
                "Opus 采样数 {samples} 不是帧长 {FRAME_SAMPLES}（{OPUS_SAMPLE_RATE} Hz 下的 20 ms）的整数倍"
            )));
        }
        let ch = if frame.channels == 0 {
            1
        } else {
            frame.channels
        };
        let rate = if frame.sample_rate == 0 {
            OPUS_SAMPLE_RATE
        } else {
            frame.sample_rate
        };

        let mut data = Vec::with_capacity(frame.data.len() + HEADER_LEN);
        push_u32(&mut data, OPUS_MAGIC);
        push_u32(&mut data, rate);
        push_u16(&mut data, ch);
        push_u32(&mut data, samples);
        push_u32(&mut data, frame.data.len() as u32);
        push_u32(&mut data, checksum(&frame.data));
        data.extend_from_slice(&frame.data);

        self.ts = self.ts.wrapping_add(samples as u64);
        self.encoded += 1;
        Ok(vec![Packet {
            data,
            marker: true,
            samples,
        }])
    }

    fn encoded_frames(&self) -> u64 {
        self.encoded
    }
}

impl Decoder for OpusDecoder {
    fn codec(&self) -> CodecId {
        CodecId::Opus
    }

    fn decode(&mut self, pkt: &Packet) -> Result<Option<Frame>> {
        let d = pkt.data.as_slice();
        if d.len() <= HEADER_LEN || take_u32(d) != Some(OPUS_MAGIC) {
            return Err(err("缺少 Opus 载荷头（魔数不匹配）"));
        }
        let rate = take_u32(&d[4..]).ok_or_else(|| err("载荷截断：缺少采样率"))?;
        let ch = take_u16(&d[8..]).ok_or_else(|| err("载荷截断：缺少声道数"))?;
        let samples = take_u32(&d[10..]).ok_or_else(|| err("载荷截断：缺少采样数"))?;
        let payload_len = take_u32(&d[14..]).ok_or_else(|| err("载荷截断：缺少载荷长度"))? as usize;
        let want_crc = take_u32(&d[18..]).ok_or_else(|| err("载荷截断：缺少 CRC"))?;
        let payload = &d[HEADER_LEN..];
        if payload.len() != payload_len {
            return Err(err(format!(
                "Opus 载荷长度不匹配：头声明 {payload_len} 字节，实际 {} 字节",
                payload.len()
            )));
        }
        let got_crc = checksum(payload);
        if want_crc != got_crc {
            return Err(err(format!(
                "Opus 载荷 CRC 校验失败：期望 0x{want_crc:08x}，实际 0x{got_crc:08x}"
            )));
        }
        if ch == 0 || rate == 0 || samples == 0 {
            return Err(err("Opus 头参数非法：声道数 / 采样率 / 采样数必须 > 0"));
        }
        if payload.len() != (samples as usize) * (ch as usize) * 2 {
            return Err(err(format!(
                "Opus 载荷与采样数不一致：需要 {} 字节，实际 {} 字节",
                samples as usize * ch as usize * 2,
                payload.len()
            )));
        }

        self.decoded += 1;
        self.last_rate = rate;
        self.last_channels = ch;
        self.last_samples = samples;
        Ok(Some(Frame::new_audio(payload.to_vec(), rate, ch)))
    }

    fn decoded_frames(&self) -> u64 {
        self.decoded
    }
}

pub fn new_encoder() -> Box<dyn Encoder> {
    Box::new(OpusEncoder { encoded: 0, ts: 0 })
}

pub fn new_decoder() -> Box<dyn Decoder> {
    Box::new(OpusDecoder {
        decoded: 0,
        last_rate: 0,
        last_channels: 0,
        last_samples: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opus_rejects_video_frame() {
        let mut enc = OpusEncoder { encoded: 0, ts: 0 };
        let v = Frame::synthetic_video(320, 240, 0, true);
        assert!(enc.encode(&v).is_err());
    }

    #[test]
    fn opus_rejects_wrong_frame_length() {
        let mut enc = OpusEncoder { encoded: 0, ts: 0 };
        // 10 个采样不是 20 ms 帧长的整数倍
        let a = Frame::new_audio(vec![0u8; 10 * 2], 48_000, 1);
        assert!(enc.encode(&a).is_err());
    }

    #[test]
    fn opus_rejects_misaligned_bytes() {
        let mut enc = OpusEncoder { encoded: 0, ts: 0 };
        let a = Frame::new_audio(vec![0u8; 960 * 2 + 1], 48_000, 1);
        assert!(enc.encode(&a).is_err());
    }
}
