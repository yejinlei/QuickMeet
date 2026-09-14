//! 比特流工具：字节序、校验和、平面游程编码。
//!
//!
//! 三件工具都被三个 codec 复用，且**全部确定性**（无随机数、无哈希随机种子），
//!
//! 这是"可复现收发验证"的前提。

/// 游程记录：`count` 个连续相同字节 `value`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Run {
    count: u32,
    value: u8,
}

impl Run {
    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.count.to_le_bytes());
        out.push(self.value);
    }
}

fn read_u32(buf: &[u8]) -> Option<u32> {
    buf.get(..4).map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// 把平坦/斜纹平面压成游程记录（确定性，可精确还原）。
pub fn rle_encode(plane: &[u8]) -> Vec<u8> {
    let mut runs = Vec::new();
    let mut i = 0;
    while i < plane.len() {
        let value = plane[i];
        let mut count = 1u32;
        while i + (count as usize) < plane.len() && plane[i + count as usize] == value {
            count += 1;
        }
        runs.push(Run { count, value });
        i += count as usize;
    }
    let mut out = Vec::new();
    // 记录数用 4 字节小端，与 run 里的 count 同宽；不要用 usize::to_le_bytes()
    // （64 位上是 8 字节，会跟 4 字节的解码端对不上）。
    push_u32(&mut out, runs.len() as u32);
    for r in &runs {
        r.write(&mut out);
    }
    out
}

/// 还原 [`rle_encode`] 的输出；任何结构异常返回 `None`。
///
/// 布局与编码器严格对称：`count` 是 **4 字节小端**，`value` 是 1 字节。
/// 之前按 1 字节读 count，会把"游程值 + 后面 3 个字节"当成长度，
/// 解码结果与编码结果完全对不上（且不会报错，很难排查）。
pub fn rle_decode(out: &[u8]) -> Option<Vec<u8>> {
    let n = read_u32(out)? as usize;
    let mut p = 4usize;
    let mut plain = Vec::new();
    for _ in 0..n {
        let count = read_u32(&out[p..])?;
        p += 4;
        let value = *out.get(p)?;
        p += 1;
        if count == 0 {
            return None;
        }
        plain.resize(plain.len() + count as usize, value);
    }
    Some(plain)
}

/// FNV-1a 校验和：确定性、快速，用于载荷完整性校验（非安全用途）。
pub fn checksum(data: &[u8]) -> u32 {
    let mut h = 0x811c9dc5u32;
    for &b in data {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// 32 位写入。
pub fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// 读取 32 位。
pub fn take_u32(buf: &[u8]) -> Option<u32> {
    read_u32(buf)
}

/// 读取 16 位。
pub fn take_u16(buf: &[u8]) -> Option<u16> {
    buf.get(..2).map(|b| u16::from_le_bytes([b[0], b[1]]))
}

/// 写入 16 位。
pub fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rle_round_trip_uniform_and_patterned() {
        let uniform = vec![42u8; 10_000];
        assert_eq!(rle_decode(&rle_encode(&uniform)).unwrap(), uniform);
        assert!(rle_encode(&uniform).len() < uniform.len(), "均匀平面应显著压缩");

        let mut patterned = Vec::new();
        for i in 0..4096u32 {
            patterned.extend_from_slice(&((i % 300) as u8).to_le_bytes());
        }
        assert_eq!(rle_decode(&rle_encode(&patterned)).unwrap(), patterned);

        let empty = Vec::<u8>::new();
        assert!(rle_decode(&rle_encode(&empty)).unwrap().is_empty());
    }

    #[test]
    fn rle_rejects_corrupt() {
        assert!(rle_decode(&[]).is_none());
        assert!(rle_decode(&[1, 0, 0, 0, 0, 0, 0, 0, 0x42]).is_none()); // count == 0
        let b = vec![1, 0, 0, 0]; // n=1 但缺少记录
        assert!(rle_decode(&b).is_none());
    }

    #[test]
    fn checksum_is_deterministic_and_sensitive() {
        let a = b"vp8 payload";
        assert_eq!(checksum(a), checksum(a));
        assert_ne!(checksum(a), checksum(b"vp8 Payload"));
    }
}
