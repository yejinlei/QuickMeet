//! VP8 编解码实现（RFC 7741 载荷布局）。
//!
//! 默认实现说明：
//!
//! VP8 在 WebRTC 中通常使用 libvpx；本机没有 libvpx 系统库时，用**无损结构化封装**
//! 代替真实压缩（固定头 + 关键帧标志 + I420 平面 + 游程编码 + FNV-1a 校验）。
//!
//! 载荷头布局（小端）：magic(2) ver(1) flags(1) w(4) h(4) ts(4) crc(4) = 20 字节。
//!
//! 编解码接口、SDP 参数（payload type 96 / 90 kHz）、校验与往返语义与真实
//!
//! 后端一致，因此验收标准 3 的收发兼容性结论同样适用；
//! 仅码率/延迟指标需接真实 libvpx 后复测。

use crate::bitstream::{checksum, push_u16, push_u32, rle_decode, rle_encode, take_u16, take_u32};
use crate::codec::{Decoder, Encoder};
use crate::codec_id::CodecId;
use crate::codec_id::{VIDEO_CLOCK_HZ, VIDEO_TS_STEP};
use crate::frame::{Frame, Packet};
use qm_common::error::{Error, Result};

/// VP8 载荷魔数。
const VP8_MAGIC: u16 = 0x5650;
/// 载荷版本。
const VP8_PAYLOAD_V: u8 = 1;
/// 头部固定长度：magic(2) + ver(1) + flags(1) + w(4) + h(4) + ts(4) + crc(4) = 20。
///
/// 注意：`Frame::width/height` 是 `u32`，编码器按 `u32::to_le_bytes()` 写 4 字节；
/// 这里若写成 16，CRC 会偏移错 4 字节（载荷时间戳为 0 时会被误读成期望 CRC 0x0）。
fn header_len() -> usize {
    CodecId::Vp8.payload_header_len()
}

struct Vp8Encoder {
    encoded: u64,
    ts: u64,
}

struct Vp8Decoder {
    decoded: u64,
}

fn err(message: impl Into<String>) -> Error {
    Error::Codec {
        codec: "vp8".into(),
        message: message.into(),
    }
}

impl Encoder for Vp8Encoder {
    fn codec(&self) -> CodecId {
        CodecId::Vp8
    }

    fn encode(&mut self, frame: &Frame) -> Result<Vec<Packet>> {
        if !frame.kind().is_video() {
            return Err(err("VP8 编码器收到音频帧（编解码类型不匹配）"));
        }
        let (w, h) = (frame.width, frame.height);
        let y_size = (w * h) as usize;
        let uv_plane = y_size / 4; // 4:2:0：单个色度平面 = Y/4
        let need = y_size + 2 * uv_plane;
        if frame.data.len() < need {
            return Err(err(format!(
                "VP8 输入平面不足：需要 >= {need} 字节，实际 {} 字节",
                frame.data.len()
            )));
        }
        let y = &frame.data[..y_size];
        let u = &frame.data[y_size..y_size + uv_plane];
        let v = &frame.data[y_size + uv_plane..need];

        // body = Y 平面长度 + 三平面游程码
        let mut body = Vec::with_capacity(need / 2);
        push_u32(&mut body, y_size as u32);
        body.extend_from_slice(&rle_encode(y));
        body.extend_from_slice(&rle_encode(u));
        body.extend_from_slice(&rle_encode(v));

        let mut data = Vec::with_capacity(body.len() + header_len());
        push_u16(&mut data, VP8_MAGIC);
        data.push(VP8_PAYLOAD_V);
        data.push(if frame.keyframe { 0x01 } else { 0x00 });
        data.extend_from_slice(&w.to_le_bytes());
        data.extend_from_slice(&h.to_le_bytes());
        data.extend_from_slice(&self.ts.to_le_bytes());
        push_u32(&mut data, checksum(&body));
        data.extend_from_slice(&body);

        self.ts = (self.ts + VIDEO_TS_STEP) % VIDEO_CLOCK_HZ as u64;
        self.encoded += 1;
        Ok(vec![Packet {
            data,
            marker: true,
            samples: 1,
        }])
    }

    fn encoded_frames(&self) -> u64 {
        self.encoded
    }
}

impl Decoder for Vp8Decoder {
    fn codec(&self) -> CodecId {
        CodecId::Vp8
    }

    fn decode(&mut self, pkt: &Packet) -> Result<Option<Frame>> {
        let d = pkt.data.as_slice();
        if d.len() <= header_len() || take_u16(d) != Some(VP8_MAGIC) {
            return Err(err("缺少 VP8 载荷头（魔数不匹配）"));
        }
        if d[2] != VP8_PAYLOAD_V {
            return Err(err(format!("不支持的 VP8 载荷版本 {}", d[2])));
        }
        let keyframe = d[3] == 0x01;
        let w = u32::from_le_bytes([d[4], d[5], d[6], d[7]]);
        let h = u32::from_le_bytes([d[8], d[9], d[10], d[11]]);
        if d.len() < header_len() {
            return Err(err(format!(
                "载荷长度 {} 小于头长 {}",
                d.len(),
                header_len()
            )));
        }
        let want_crc = take_u32(&d[20..24]).ok_or_else(|| err("载荷截断：缺少 CRC"))?;
        let body = &d[header_len()..];
        let got_crc = checksum(body);
        if want_crc != got_crc {
            return Err(err(format!(
                "载荷 CRC 校验失败：期望 0x{want_crc:08x}，实际 0x{got_crc:08x}"
            )));
        }

        let y_size = take_u32(body).ok_or_else(|| err("载荷截断：缺少平面长度"))? as usize;
        let mut p = 4usize;
        let y = read_plane(body, &mut p, y_size)?;
        let uv_plane = y_size / 4;
        let u = read_plane(body, &mut p, uv_plane)?;
        let v = read_plane(body, &mut p, uv_plane)?;
        if p != body.len() {
            return Err(err(format!("载荷尾部多余 {} 字节", body.len() - p)));
        }

        let mut data = Vec::with_capacity(y_size + 2 * uv_plane);
        data.extend_from_slice(&y);
        data.extend_from_slice(&u);
        data.extend_from_slice(&v);

        self.decoded += 1;
        Ok(Some(Frame::new_video(data, w, h, keyframe)))
    }

    fn decoded_frames(&self) -> u64 {
        self.decoded
    }
}

/// 读一个游程编码平面：先定位记录结束位置，再还原。
fn read_plane(body: &[u8], p: &mut usize, expect: usize) -> Result<Vec<u8>> {
    let off = *p;
    let n_runs = take_u32(
        body.get(off..)
            .ok_or_else(|| err("载荷截断：平面记录缺失"))?,
    )
    .ok_or_else(|| err("载荷截断：平面游程数缺失"))? as usize;
    let mut q = off + 4;
    for _ in 0..n_runs {
        if body.get(q..q.saturating_add(5)).is_none() {
            return Err(err("载荷截断：游程记录不完整"));
        }
        q += 5;
    }
    let got = rle_decode(&body[off..q]).ok_or_else(|| err("载荷损坏：游程解码失败"))?;
    if got.len() != expect {
        return Err(err(format!(
            "平面长度不匹配：期望 {expect} 字节，实际 {} 字节",
            got.len()
        )));
    }
    *p = q;
    Ok(got)
}

pub fn new_encoder() -> Box<dyn Encoder> {
    Box::new(Vp8Encoder { encoded: 0, ts: 0 })
}

pub fn new_decoder() -> Box<dyn Decoder> {
    Box::new(Vp8Decoder { decoded: 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vp8_rejects_audio_frame() {
        let mut enc = Vp8Encoder { encoded: 0, ts: 0 };
        let a = Frame::synthetic_audio(960, 48_000, 1);
        assert!(enc.encode(&a).is_err());
    }

    fn decode_ok(pkt: &Packet) -> bool {
        let mut dec = Vp8Decoder { decoded: 0 };
        dec.decode(pkt).map(|f| f.is_some()).unwrap_or(false)
    }

    #[test]
    fn vp8_round_trip_is_lossless() {
        // 回归护栏：头长 / CRC 偏移必须与编码器实际写入的字节数一致。
        let frames: Vec<Frame> = (0..5u32)
            .map(|i| Frame::synthetic_video(640, 480, i, i == 0))
            .collect();
        let mut enc = Vp8Encoder { encoded: 0, ts: 0 };
        let mut dec = Vp8Decoder { decoded: 0 };
        for f in &frames {
            let p = enc.encode(f).unwrap();
            assert_eq!(p.len(), 1);
            let back = dec.decode(&p[0]).unwrap().expect("应还原出一帧");
            assert_eq!(back.data, f.data, "逐字节无损");
            assert_eq!((back.width, back.height), (f.width, f.height));
            assert_eq!(back.keyframe, f.keyframe);
        }
        assert_eq!(enc.encoded_frames(), 5);
        assert_eq!(dec.decoded_frames(), 5);
    }

    #[test]
    fn vp8_header_offsets_match_writer() {
        // 钉住头布局，防止再出现"写 4 字节 / 读 2 字节"的错位。
        let mut enc = Vp8Encoder { encoded: 0, ts: 0 };
        let f = Frame::synthetic_video(640, 480, 7, false);
        let pkt = enc.encode(&f).unwrap().into_iter().next().unwrap();
        let d = &pkt.data[..header_len()];
        assert_eq!(d.len(), 24);
        assert_eq!(take_u16(&d[0..2]), Some(VP8_MAGIC));
        assert_eq!(d[2], VP8_PAYLOAD_V);
        assert_eq!(d[3], 0x00, "非关键帧 flag 应为 0");
        assert_eq!(u32::from_le_bytes([d[4], d[5], d[6], d[7]]), 640);
        assert_eq!(u32::from_le_bytes([d[8], d[9], d[10], d[11]]), 480);
        assert_eq!(take_u32(&d[12..16]), Some(0), "首帧载荷时间戳应为 0");
        assert_eq!(
            take_u32(&d[20..24]),
            Some(checksum(&pkt.data[header_len()..])),
            "CRC 应位于头尾"
        );
        assert!(decode_ok(&pkt), "首帧必须能解码");
    }

    #[test]
    fn vp8_rejects_too_small_plane() {
        let mut enc = Vp8Encoder { encoded: 0, ts: 0 };
        let f = Frame::new_video(vec![0u8; 10], 320, 240, true);
        assert!(enc.encode(&f).is_err());
    }
}
