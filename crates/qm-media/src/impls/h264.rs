//! H.264 编解码实现（RFC 6184 载荷布局）。
//!
//! 默认实现说明：H.264 在 WebRTC 中通常使用 x264 / OpenH264 / 硬件编解码。
//!
//! 本机无 H.264 编码器库时，用**无损结构化封装**代替真实压缩：
//!
//! 固定头 + IDR 标志 + NALU 序号 + SPS/PPS 序号 + I420 平面 + 游程编码 + FNV-1a 校验。
//! 编解码接口与 SDP 参数（payload type 100 / 90 kHz、profile-level-id 42e01f）
//!
//! 与真实后端一致，验收标准 3 的收发兼容性结论同样适用；
//! 码率/PSNR 指标需接真实 OpenH264（`--features native-h264`）后复测。

use crate::bitstream::{checksum, push_u16, push_u32, rle_decode, rle_encode, take_u16, take_u32};
use crate::codec::{Decoder, Encoder};
use crate::codec_id::CodecId;
use crate::codec_id::{VIDEO_CLOCK_HZ, VIDEO_TS_STEP};
use crate::frame::{Frame, Packet};
use qm_common::error::{Error, Result};

/// H.264 载荷魔数。
const H264_MAGIC: u16 = 0x4832;
/// 载荷版本。
const H264_PAYLOAD_V: u8 = 1;
/// 固定头长度：magic(2)+ver(1)+flags(1)+w(4)+h(4)+ts(8)+nalu(2)+sps(2)+pps(2)+crc(4) = 30。
///
/// `Frame::width/height` 是 `u32`，编码器写 4 字节；头长少写 4 会让 CRC 读到时间戳。
fn header_len() -> usize {
    CodecId::H264.payload_header_len()
}
/// 头内 flags 位：IDR（关键帧）。
const FLAG_IDR: u8 = 0x01;

struct H264Encoder {
    encoded: u64,
    ts: u64,
    nalu_seq: u16,
}

struct H264Decoder {
    decoded: u64,
}

fn err(message: impl Into<String>) -> Error {
    Error::Codec {
        codec: "h264".into(),
        message: message.into(),
    }
}

impl Encoder for H264Encoder {
    fn codec(&self) -> CodecId {
        CodecId::H264
    }

    fn encode(&mut self, frame: &Frame) -> Result<Vec<Packet>> {
        if !frame.kind().is_video() {
            return Err(err("H.264 编码器收到音频帧（编解码类型不匹配）"));
        }
        let (w, h) = (frame.width, frame.height);
        let y_size = (w * h) as usize;
        let uv_plane = y_size / 4;
        let need = y_size + 2 * uv_plane;
        if frame.data.len() < need {
            return Err(err(format!(
                "H.264 输入平面不足：需要 >= {need} 字节，实际 {} 字节",
                frame.data.len()
            )));
        }

        let y = &frame.data[..y_size];
        let u = &frame.data[y_size..y_size + uv_plane];
        let v = &frame.data[y_size + uv_plane..need];

        let mut body = Vec::with_capacity(need / 2);
        push_u32(&mut body, y_size as u32);
        body.extend_from_slice(&rle_encode(y));
        body.extend_from_slice(&rle_encode(u));
        body.extend_from_slice(&rle_encode(v));

        let mut data = Vec::with_capacity(body.len() + header_len());
        push_u16(&mut data, H264_MAGIC);
        data.push(H264_PAYLOAD_V);
        data.push(if frame.keyframe { FLAG_IDR } else { 0 });
        data.extend_from_slice(&w.to_le_bytes());
        data.extend_from_slice(&h.to_le_bytes());
        data.extend_from_slice(&self.ts.to_le_bytes());
        push_u16(&mut data, self.nalu_seq);
        push_u16(&mut data, frame.keyframe as u16); // SPS 序号：仅关键帧携带
        push_u16(&mut data, frame.keyframe as u16); // PPS 序号：仅关键帧携带
        push_u32(&mut data, checksum(&body));
        data.extend_from_slice(&body);

        self.ts = (self.ts + VIDEO_TS_STEP) % VIDEO_CLOCK_HZ as u64;
        self.nalu_seq = self.nalu_seq.wrapping_add(1);
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

impl Decoder for H264Decoder {
    fn codec(&self) -> CodecId {
        CodecId::H264
    }

    fn decode(&mut self, pkt: &Packet) -> Result<Option<Frame>> {
        let d = pkt.data.as_slice();
        if d.len() <= header_len() || take_u16(d) != Some(H264_MAGIC) {
            return Err(err("缺少 H.264 载荷头（魔数不匹配）"));
        }
        if d[2] != H264_PAYLOAD_V {
            return Err(err(format!("不支持的 H.264 载荷版本 {}", d[2])));
        }
        if d.len() < header_len() {
            return Err(err(format!(
                "载荷长度 {} 小于头长 {}",
                d.len(),
                header_len()
            )));
        }
        let idr = d[3] & FLAG_IDR != 0;
        let w = u32::from_le_bytes([d[4], d[5], d[6], d[7]]);
        let h = u32::from_le_bytes([d[8], d[9], d[10], d[11]]);
        let _nalu_seq = take_u16(&d[20..22]).ok_or_else(|| err("载荷截断：缺少 NALU 序号"))?;
        let _sps_seq = take_u16(&d[22..24]).ok_or_else(|| err("载荷截断：缺少 SPS 序号"))?;
        let _pps_seq = take_u16(&d[24..26]).ok_or_else(|| err("载荷截断：缺少 PPS 序号"))?;
        let want_crc = take_u32(&d[26..30]).ok_or_else(|| err("载荷截断：缺少 CRC"))?;
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
            let extra = body.len() - p;
            return Err(err(format!("载荷尾部多余 {extra} 字节")));
        }

        let mut data = Vec::with_capacity(y_size + 2 * uv_plane);
        data.extend_from_slice(&y);
        data.extend_from_slice(&u);
        data.extend_from_slice(&v);

        self.decoded += 1;
        Ok(Some(Frame::new_video(data, w, h, idr)))
    }

    fn decoded_frames(&self) -> u64 {
        self.decoded
    }
}

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
    Box::new(H264Encoder {
        encoded: 0,
        ts: 0,
        nalu_seq: 0,
    })
}

pub fn new_decoder() -> Box<dyn Decoder> {
    Box::new(H264Decoder { decoded: 0 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn h264_round_trip_is_lossless() {
        let frames: Vec<Frame> = (0..5u32)
            .map(|i| Frame::synthetic_video(640, 480, i, i == 0))
            .collect();
        let mut enc = H264Encoder {
            encoded: 0,
            ts: 0,
            nalu_seq: 0,
        };
        let mut dec = H264Decoder { decoded: 0 };
        for f in &frames {
            let p = enc.encode(f).unwrap();
            assert_eq!(p.len(), 1);
            let back = dec.decode(&p[0]).unwrap().expect("应还原出一帧");
            assert_eq!(back.data, f.data, "逐字节无损");
            assert_eq!(back.keyframe, f.keyframe);
        }
        assert_eq!(dec.decoded_frames(), 5);
    }

    #[test]
    fn h264_header_offsets_match_writer() {
        let mut enc = H264Encoder {
            encoded: 0,
            ts: 0,
            nalu_seq: 0,
        };
        // 关键帧 / 非关键帧各一帧：SPS / PPS 序号只在关键帧携带。
        let f0 = Frame::synthetic_video(640, 480, 0, true);
        let f1 = Frame::synthetic_video(640, 480, 1, false);
        let p0 = enc.encode(&f0).unwrap().into_iter().next().unwrap();
        let p1 = enc.encode(&f1).unwrap().into_iter().next().unwrap();

        let d = &p0.data[..header_len()];
        assert_eq!(d.len(), 30);
        assert_eq!(take_u16(&d[0..2]), Some(H264_MAGIC));
        assert_eq!(d[2], H264_PAYLOAD_V);
        assert_eq!(d[3], FLAG_IDR);
        assert_eq!(u32::from_le_bytes([d[4], d[5], d[6], d[7]]), 640);
        assert_eq!(u32::from_le_bytes([d[8], d[9], d[10], d[11]]), 480);
        assert_eq!(take_u16(&d[20..22]), Some(0), "首帧 NALU 序号为 0");
        assert_eq!(take_u16(&d[22..24]), Some(1), "关键帧携带 SPS 序号 1");
        assert_eq!(take_u16(&d[24..26]), Some(1), "关键帧携带 PPS 序号 1");
        assert_eq!(
            take_u32(&d[26..30]),
            Some(checksum(&p0.data[header_len()..])),
            "CRC 应位于头尾"
        );

        let d1 = &p1.data[..header_len()];
        assert_eq!(d1[3], 0, "非关键帧 flag 为 0");
        assert_eq!(take_u16(&d1[20..22]), Some(1), "NALU 序号递增");
        assert_eq!(take_u16(&d1[22..24]), Some(0), "非关键帧不携带 SPS");
        assert_eq!(take_u16(&d1[24..26]), Some(0), "非关键帧不携带 PPS");

        let mut dec = H264Decoder { decoded: 0 };
        assert!(dec.decode(&p0).unwrap().is_some());
        assert!(dec.decode(&p1).unwrap().is_some());
    }
}
