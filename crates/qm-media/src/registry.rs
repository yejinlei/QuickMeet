//! 编解码器注册表：`CodecId` → 编码器/解码器实现。
//!
//! 接入真实编解码器时只改这里，业务层与验证代码无需改动。

use qm_common::error::{Error, Result};

use crate::codec::{Decoder, Encoder};
use crate::codec_id::CodecId;

/// 构造编码器。
pub fn encoder(codec: CodecId) -> Result<Box<dyn Encoder>> {
    Ok(match codec {
        CodecId::Vp8 => crate::impls::vp8::new_encoder(),
        CodecId::H264 => crate::impls::h264::new_encoder(),
        CodecId::Opus => crate::impls::opus::new_encoder(),
    })
}

/// 构造解码器。
pub fn decoder(codec: CodecId) -> Result<Box<dyn Decoder>> {
    Ok(match codec {
        CodecId::Vp8 => crate::impls::vp8::new_decoder(),
        CodecId::H264 => crate::impls::h264::new_decoder(),
        CodecId::Opus => crate::impls::opus::new_decoder(),
    })
}

/// 列出本构建支持的全部编解码（验收报告用）。
pub fn supported_codecs() -> Vec<CodecId> {
    vec![CodecId::Vp8, CodecId::H264, CodecId::Opus]
}

/// 运行时报告实现来源（真实库 / 确定性封装）。
pub fn implementation_report() -> Vec<(CodecId, &'static str)> {
    #[allow(unused_mut)]
    let mut out = vec![
        (CodecId::Vp8, "lossless-container"),
        (CodecId::H264, "lossless-container"),
        (CodecId::Opus, "lossless-container"),
    ];
    #[cfg(feature = "native-opus")]
    {
        out.iter_mut().find(|c| c.0 == CodecId::Opus).unwrap().1 = "opus-rs/native";
    }
    #[cfg(feature = "native-h264")]
    {
        out.iter_mut().find(|c| c.0 == CodecId::H264).unwrap().1 = "openh264/native";
    }
    #[cfg(feature = "native-vp8")]
    {
        out.iter_mut().find(|c| c.0 == CodecId::Vp8).unwrap().1 = "libvpx/native";
    }
    out
}

/// 未实现的编解码统一报错（预留扩展位）。
pub fn unsupported(name: &str) -> Error {
    Error::Codec {
        codec: name.to_string(),
        message: "该编解码尚未接入实现".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_covers_all_three_codecs() {
        for c in supported_codecs() {
            assert!(encoder(c).is_ok(), "{c:?} 编码器应可构造");
            assert!(decoder(c).is_ok(), "{c:?} 解码器应可构造");
            assert_eq!(encoder(c).unwrap().codec(), c);
            assert_eq!(decoder(c).unwrap().codec(), c);
        }
    }

    #[test]
    fn implementation_report_shape() {
        assert_eq!(implementation_report().len(), 3);
    }

    #[test]
    fn unsupported_reports_codec_error() {
        use qm_common::error::ErrorKind;
        assert_eq!(unsupported("av1").kind(), ErrorKind::Codec);
    }
}
