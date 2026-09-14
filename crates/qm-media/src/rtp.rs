//! RTP 封装/解封装（RFC 3550 固定头，12 字节）。
//!
//! 只做固定头，不引入扩展/CSRC/负载块切分：
//! * 载荷层（[`crate::impls`]）已产出完整一帧的字节；
//! * 验证目标是"编解码 × 头部组装"的收发一致性，而不是分片算法。

use qm_common::error::{Error, Result};

use crate::bitstream::{push_u16, push_u32, take_u16, take_u32};
use crate::codec::Codec;
use crate::codec_id::CodecId;
use crate::frame::{Frame, Packet};

/// RTP 固定头长度（RFC 3550 §3.1）。
pub const RTP_HEADER_LEN: usize = 12;
/// RTP 版本（v2）。
pub const RTP_VERSION: u8 = 2;

/// 一个已封装的 RTP 包。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RtpPacket {
    pub version: u8,
    pub marker: bool,
    pub payload_type: u8,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub payload: Vec<u8>,
}

/// 媒体流状态：序列号与时间戳推进（发送端持有）。
#[derive(Debug, Clone)]
pub struct MediaStream {
    pub ssrc: u32,
    pub payload_type: u8,
    pub clock_rate: u32,
    pub ts_step: u32,
    next_seq: u16,
    next_ts: u32,
}

impl MediaStream {
    /// 按编解码给出默认流状态（视频 90 kHz 按 30 fps 步进，音频步进 = 帧采样数）。
    pub fn new(codec: CodecId, ssrc: u32) -> Self {
        let ts_step = match codec {
            CodecId::Vp8 | CodecId::H264 => crate::codec_id::VIDEO_TS_STEP as u32,
            CodecId::Opus => crate::codec_id::OPUS_FRAME_20MS_SAMPLES,
        };
        Self {
            ssrc,
            payload_type: codec.default_payload_type(),
            clock_rate: codec.clock_rate(),
            ts_step,
            next_seq: 0,
            next_ts: 0,
        }
    }

    /// 取当前序列号并自增（模 2^16）。
    pub fn take_seq(&mut self) -> u16 {
        let s = self.next_seq;
        self.next_seq = s.wrapping_add(1);
        s
    }

    /// 取当前时间戳并推进 `samples` 个采样（`samples == 0` 时按一帧步进）。
    pub fn take_ts(&mut self, samples: u32) -> u32 {
        let t = self.next_ts;
        self.next_ts = t.wrapping_add(if samples == 0 { self.ts_step } else { samples });
        t
    }

    /// 把一个载荷包封装成 RTP 包。
    pub fn packetize(&mut self, pkt: &Packet) -> RtpPacket {
        RtpPacket {
            version: RTP_VERSION,
            marker: pkt.marker,
            payload_type: self.payload_type,
            sequence_number: self.take_seq(),
            timestamp: self.take_ts(pkt.samples),
            ssrc: self.ssrc,
            payload: pkt.data.clone(),
        }
    }

    /// 编码 + 封装一步完成。
    pub fn push_frame(&mut self, codec: &Codec, frame: &Frame) -> Result<RtpPacket> {
        let pkts = codec.encode(frame)?;
        if pkts.is_empty() {
            return Err(Error::codec(codec.codec().to_string(), "编码器未产出任何载荷包"));
        }
        Ok(self.packetize(&pkts[pkts.len() - 1]))
    }
}

fn truncated() -> Error {
    Error::Codec { codec: "rtp".into(), message: "RTP 包截断".into() }
}

/// 序列化 RTP 包为线上字节。
pub fn marshal(pkt: &RtpPacket) -> Result<Vec<u8>> {
    if pkt.payload_type > 0x7F {
        return Err(Error::codec("rtp", format!("payload type {} 超过 7 位上限", pkt.payload_type)));
    }
    if pkt.ssrc == 0 {
        return Err(Error::codec("rtp", "ssrc 不能为 0（RFC 3550 要求非零）"));
    }
    // byte0 = V(2)|P|X|CC，本实现 P=X=0、CC=0；byte1 = M(1)|PT(7)。
    let b0 = pkt.version << 6;
    let b1 = (u8::from(pkt.marker) << 7) | pkt.payload_type;
    let mut out = Vec::with_capacity(RTP_HEADER_LEN + pkt.payload.len());
    out.push(b0);
    out.push(b1);
    push_u16(&mut out, pkt.sequence_number);
    push_u32(&mut out, pkt.timestamp);
    push_u32(&mut out, pkt.ssrc);
    out.extend_from_slice(&pkt.payload);
    Ok(out)
}

/// 从线上字节还原 RTP 包。
pub fn unmarshal(data: &[u8]) -> Result<RtpPacket> {
    if data.len() < RTP_HEADER_LEN {
        return Err(Error::codec(
            "rtp",
            format!("RTP 包长度 {} 小于固定头 {RTP_HEADER_LEN} 字节", data.len()),
        ));
    }
    let version = data[0] >> 6;
    if version != RTP_VERSION {
        return Err(Error::codec("rtp", format!("不支持的 RTP 版本 {version}")));
    }
    let marker = data[1] & 0x80 != 0;
    let payload_type = data[1] & 0x7F;
    let sequence_number = take_u16(&data[2..]).ok_or_else(truncated)?;
    let timestamp = take_u32(&data[4..]).ok_or_else(truncated)?;
    let ssrc = take_u32(&data[8..]).ok_or_else(truncated)?;
    let payload = data[RTP_HEADER_LEN..].to_vec();
    Ok(RtpPacket {
        version,
        marker,
        payload_type,
        sequence_number,
        timestamp,
        ssrc,
        payload,
    })
}

/// 把一组 RTP 包按 `(ssrc, timestamp)` 分组、按序列号排序后合并成载荷包。
///
/// 合并后的 [`Packet::samples`] 填的是**分片数**而非采样数：采样数的权威来源
/// 是载荷自身的头（三个 codec 都在载荷里内嵌采样数/尺寸 + CRC），RTP 层不猜。
pub fn unpack(packets: &[RtpPacket]) -> Result<Vec<Packet>> {
    let mut groups: Vec<(u32, u32, Vec<&RtpPacket>)> = Vec::new();
    for p in packets {
        match groups.iter_mut().find(|g| g.0 == p.ssrc && g.1 == p.timestamp) {
            Some(g) => g.2.push(p),
            None => groups.push((p.ssrc, p.timestamp, vec![p])),
        }
    }
    groups.sort_by_key(|g| (g.0, g.1));
    groups
        .into_iter()
        .map(|(_ssrc, _ts, mut group)| {
            group.sort_by_key(|p| p.sequence_number);
            let mut data = Vec::new();
            let mut marker = false;
            let mut fragments = 0u32;
            for p in &group {
                data.extend_from_slice(&p.payload);
                marker = marker || p.marker;
                fragments += 1;
            }
            Ok(Packet { data, marker, samples: fragments })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk() -> RtpPacket {
        RtpPacket {
            version: 2,
            marker: true,
            payload_type: 96,
            sequence_number: 0,
            timestamp: 0,
            ssrc: 0,
            payload: vec![0xAA, 0xBB],
        }
    }

    #[test]
    fn rtp_marshal_unmarshal_round_trip() {
        let mut stream = MediaStream::new(CodecId::Vp8, 0x1234_5678);
        let pkt = stream.packetize(&Packet { data: vec![1, 2, 3, 4, 5], marker: true, samples: 1 });
        let wire = marshal(&pkt).unwrap();
        assert_eq!(wire.len(), RTP_HEADER_LEN + 5);
        let back = unmarshal(&wire).unwrap();
        assert_eq!(back, pkt);
        // 位域级断言，防再串位：byte0 = V|P|X|CC，byte1 = M|PT(7)
        assert_eq!(wire[0] >> 6, 2, "版本位应为 v2");
        assert_eq!(wire[0] & 0x3F, 0, "P / X / CC 必须为 0");
        assert_ne!(wire[1] & 0x80, 0, "marker 应落在 byte1 的 MSB");
        assert_eq!(wire[1] & 0x7F, pkt.payload_type, "payload type 占 byte1 低 7 位");
    }

    #[test]
    fn rtp_rejects_short_packet() {
        assert!(unmarshal(&[0x80, 0x60, 0, 0]).is_err());
    }

    #[test]
    fn rtp_rejects_bad_version() {
        let mut b = vec![0u8; 16];
        b[0] = 0x40;
        assert!(unmarshal(&b).is_err());
    }

    #[test]
    fn rtp_rejects_zero_ssrc_and_bad_pt() {
        let p = RtpPacket { ssrc: 0, payload_type: 96, ..mk() };
        assert!(marshal(&p).is_err());
        let p2 = RtpPacket { ssrc: 1, payload_type: 200, ..mk() };
        assert!(marshal(&p2).is_err());
    }

    #[test]
    fn stream_advances_seq_and_ts() {
        let mut s = MediaStream::new(CodecId::Vp8, 1);
        let a = s.packetize(&Packet { data: vec![0], marker: true, samples: crate::codec_id::VIDEO_TS_STEP as u32 });
        let b = s.packetize(&Packet { data: vec![0], marker: true, samples: 1 });
        assert_eq!(a.sequence_number, 0);
        assert_eq!(b.sequence_number, 1);
        assert_eq!(b.timestamp - a.timestamp, crate::codec_id::VIDEO_TS_STEP as u32);
    }

    #[test]
    fn unpack_groups_by_timestamp_and_seq() {
        let p1 = RtpPacket { sequence_number: 3, ssrc: 7, timestamp: 100, ..mk() };
        let p2 = RtpPacket { sequence_number: 2, ssrc: 7, timestamp: 100, ..mk() };
        let p3 = RtpPacket { sequence_number: 5, ssrc: 7, timestamp: 200, ..mk() };
        let (b1, b2, b3) = (p1.payload.len(), p2.payload.len(), p3.payload.len());
        let out = unpack(&[p3, p1, p2]).unwrap();
        assert_eq!(out.len(), 2);
        // unpack 按 (ssrc, timestamp) 排序，ts=100 的那组排前面：
        // out[0] = ts=100 的两包（按序列号合并），out[1] = ts=200 的单包。
        assert_eq!(out[0].data.len(), b1 + b2, "分组内两包载荷应被完整合并");
        assert_eq!(out[0].samples, 2, "samples 填的是分片数");
        assert_eq!(out[0].marker, true, "任一分片带 marker，整帧即 marker");
        assert_eq!(out[1].data.len(), b3, "单包分组原样保留");
        assert_eq!(out[1].samples, 1);
    }
}
