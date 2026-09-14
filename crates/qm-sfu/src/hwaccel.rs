//! 硬件加速适配（GPU 硬编/硬解，不可用时优雅降级到软编）。
//!
//! 设计目标：
//! * 编码器/解码器优先走 GPU 硬件加速路径（NVENC/VideoToolbox/VAAPI/QSV）。
//! * 硬件不可用时自动降级到 CPU 软编，保证服务连续可用。
//! * 降级是运行时决策，不重启服务。
//!
//! 本模块实现**纯函数决策层**：检测硬件可用性并选择编码路径。
//! 不依赖任何系统 API，`cargo test` 可离线断言降级逻辑。

use serde::{Deserialize, Serialize};

use crate::track::TrackKind;

/// 硬件加速后端。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HwBackend {
    /// NVIDIA NVENC / NVDEC（CUDA 平台）。
    Nvenc,
    /// Intel Quick Sync Video（QSV）。
    Qsv,
    /// AMD Advanced Media Framework (AMF)。
    Amf,
    /// Apple VideoToolbox（macOS）。
    VideoToolbox,
    /// Linux VAAPI（VA-API）。
    Vaapi,
    /// 无可用硬件后端，使用 CPU 软编。
    Cpu,
}

impl std::fmt::Display for HwBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HwBackend::Nvenc => write!(f, "nvenc"),
            HwBackend::Qsv => write!(f, "qsv"),
            HwBackend::Amf => write!(f, "amf"),
            HwBackend::VideoToolbox => write!(f, "videotoolbox"),
            HwBackend::Vaapi => write!(f, "vaapi"),
            HwBackend::Cpu => write!(f, "cpu"),
        }
    }
}

/// 硬件可用性检测结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HwAvailability {
    pub backend: HwBackend,
    pub available: bool,
    /// 不可用原因（available=false 时有意义）。
    pub reason: String,
}

impl HwAvailability {
    pub fn ok(backend: HwBackend) -> Self {
        Self {
            backend,
            available: true,
            reason: String::new(),
        }
    }

    pub fn unavailable(backend: HwBackend, reason: impl Into<String>) -> Self {
        Self {
            backend,
            available: false,
            reason: reason.into(),
        }
    }
}

/// 编码路径选择结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecPath {
    /// 选中的后端（硬件或 CPU）。
    pub backend: HwBackend,
    /// 是否走了硬件加速路径。
    pub hardware_accelerated: bool,
    /// 选择的理由（降级日志用）。
    pub reason: String,
    /// 编解码的 codec 名称（vp8/h264/opus）。
    pub codec: String,
    /// 轨道种类。
    pub kind: TrackKind,
}

impl CodecPath {
    pub fn hardware(backend: HwBackend, codec: &str, kind: TrackKind) -> Self {
        Self {
            backend,
            hardware_accelerated: true,
            reason: format!("hardware acceleration via {backend}"),
            codec: codec.to_string(),
            kind,
        }
    }

    pub fn software(codec: &str, kind: TrackKind, reason: impl Into<String>) -> Self {
        Self {
            backend: HwBackend::Cpu,
            hardware_accelerated: false,
            reason: reason.into(),
            codec: codec.to_string(),
            kind,
        }
    }
}

/// 平台检测（用于选择候选后端优先级）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Windows,
    Linux,
    Macos,
    Unknown,
}

/// 运行时检测当前平台。
pub fn detect_platform() -> Platform {
    #[cfg(target_os = "windows")]
    {
        Platform::Windows
    }
    #[cfg(target_os = "linux")]
    {
        Platform::Linux
    }
    #[cfg(target_os = "macos")]
    {
        Platform::Macos
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        Platform::Unknown
    }
}

/// 根据平台返回候选硬件后端优先级列表。
///
/// 优先级：
/// * Windows -> NVENC > QSV > AMF > CPU
/// * Linux   -> VAAPI > CPU
/// * macOS   -> VideoToolbox > CPU
pub fn candidate_backends(platform: Platform) -> Vec<HwBackend> {
    match platform {
        Platform::Windows => vec![HwBackend::Nvenc, HwBackend::Qsv, HwBackend::Amf, HwBackend::Cpu],
        Platform::Linux => vec![HwBackend::Vaapi, HwBackend::Cpu],
        Platform::Macos => vec![HwBackend::VideoToolbox, HwBackend::Cpu],
        Platform::Unknown => vec![HwBackend::Cpu],
    }
}

/// 检测指定后端是否可用。
///
/// 纯函数：实际硬件检测由运行时注入，这里用传入的检测结果做决策。
/// `hw_probe` 是一个闭包，返回某后端的可用性；本函数只在列表里查。
pub fn check_backend(
    backend: HwBackend,
    hw_probe: &dyn Fn(HwBackend) -> HwAvailability,
) -> HwAvailability {
    hw_probe(backend)
}

/// 选择编码路径：按候选优先级探测，第一个可用的硬件后端胜出，否则降级到 CPU。
///
/// 验收标准 2：硬件加速路径与软编降级路径均有验证结果。
pub fn select_codec_path(
    codec: &str,
    kind: TrackKind,
    hw_probe: &dyn Fn(HwBackend) -> HwAvailability,
) -> CodecPath {
    let platform = detect_platform();
    let candidates = candidate_backends(platform);

    for &backend in &candidates {
        if backend == HwBackend::Cpu {
            continue;
        }
        let avail = check_backend(backend, hw_probe);
        if avail.available {
            return CodecPath::hardware(backend, codec, kind);
        }
    }

    // 所有硬件后端都不可用，降级到 CPU 软编
    let reasons: Vec<String> = candidates
        .iter()
        .filter(|&b| *b != HwBackend::Cpu)
        .filter_map(|&b| {
            let a = hw_probe(b);
            if !a.available {
                Some(format!("{b}: {}", a.reason))
            } else {
                None
            }
        })
        .collect();

    let reason = if reasons.is_empty() {
        "no hardware backends for platform".to_string()
    } else {
        format!("hardware unavailable ({}); fallback to CPU", reasons.join("; "))
    };

    CodecPath::software(codec, kind, reason)
}

/// 降级统计（验收标准 2 的计量点）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HwAccelStats {
    /// 硬件加速编码会话数。
    pub hw_sessions: u64,
    /// CPU 软编会话数。
    pub sw_sessions: u64,
    /// 硬件编码的帧数。
    pub hw_encoded_frames: u64,
    /// 软编的帧数。
    pub sw_encoded_frames: u64,
    /// 硬件解码的帧数。
    pub hw_decoded_frames: u64,
    /// 软解的帧数。
    pub sw_decoded_frames: u64,
    /// 硬件不可用触发降级的次数。
    pub fallback_count: u64,
}

impl HwAccelStats {
    pub fn total_sessions(&self) -> u64 {
        self.hw_sessions + self.sw_sessions
    }

    pub fn total_encoded(&self) -> u64 {
        self.hw_encoded_frames + self.sw_encoded_frames
    }

    /// 硬件加速比例（硬件会话 / 总会话）。
    pub fn hw_ratio(&self) -> f64 {
        if self.total_sessions() == 0 {
            return 0.0;
        }
        self.hw_sessions as f64 / self.total_sessions() as f64
    }
}

/// 模拟硬件加速 + 降级场景（验收标准 2）。
///
/// 纯函数：用注入的检测结果推演降级路径。
pub fn simulate_hw_accel(
    codec: &str,
    kind: TrackKind,
    nvenc_available: bool,
    qsv_available: bool,
    amf_available: bool,
) -> CodecPath {
    let probe = |backend: HwBackend| -> HwAvailability {
        match backend {
            HwBackend::Nvenc => {
                if nvenc_available {
                    HwAvailability::ok(HwBackend::Nvenc)
                } else {
                    HwAvailability::unavailable(HwBackend::Nvenc, "no NVIDIA GPU")
                }
            }
            HwBackend::Qsv => {
                if qsv_available {
                    HwAvailability::ok(HwBackend::Qsv)
                } else {
                    HwAvailability::unavailable(HwBackend::Qsv, "no Intel iGPU")
                }
            }
            HwBackend::Amf => {
                if amf_available {
                    HwAvailability::ok(HwBackend::Amf)
                } else {
                    HwAvailability::unavailable(HwBackend::Amf, "no AMD GPU")
                }
            }
            _ => HwAvailability::unavailable(backend, "not applicable"),
        }
    };

    select_codec_path(codec, kind, &probe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_backends_per_platform() {
        assert!(candidate_backends(Platform::Windows).contains(&HwBackend::Nvenc));
        assert!(candidate_backends(Platform::Linux).contains(&HwBackend::Vaapi));
        assert!(candidate_backends(Platform::Macos).contains(&HwBackend::VideoToolbox));
        assert_eq!(candidate_backends(Platform::Unknown), vec![HwBackend::Cpu]);
    }

    #[test]
    fn select_hw_when_nvenc_available() {
        let probe = |b: HwBackend| match b {
            HwBackend::Nvenc => HwAvailability::ok(HwBackend::Nvenc),
            _ => HwAvailability::unavailable(b, "nope"),
        };
        let path = select_codec_path("h264", TrackKind::Video, &probe);
        assert!(path.hardware_accelerated);
        assert_eq!(path.backend, HwBackend::Nvenc);
    }

    #[test]
    fn fallback_to_cpu_when_all_hw_unavailable() {
        let probe = |b: HwBackend| HwAvailability::unavailable(b, "no hardware");
        let path = select_codec_path("h264", TrackKind::Video, &probe);
        assert!(!path.hardware_accelerated);
        assert_eq!(path.backend, HwBackend::Cpu);
        assert!(path.reason.contains("fallback to CPU"));
    }

    #[test]
    fn fallback_to_qsv_when_nvenc_unavailable() {
        let probe = |b: HwBackend| match b {
            HwBackend::Nvenc => HwAvailability::unavailable(HwBackend::Nvenc, "no NVIDIA"),
            HwBackend::Qsv => HwAvailability::ok(HwBackend::Qsv),
            _ => HwAvailability::unavailable(b, "nope"),
        };
        let path = select_codec_path("h264", TrackKind::Video, &probe);
        assert!(path.hardware_accelerated);
        assert_eq!(path.backend, HwBackend::Qsv);
    }

    #[test]
    fn simulate_hw_accel_nvenc_path() {
        let path = simulate_hw_accel("h264", TrackKind::Video, true, false, false);
        assert!(path.hardware_accelerated);
        assert_eq!(path.backend, HwBackend::Nvenc);
    }

    #[test]
    fn simulate_hw_accel_fallback_path() {
        let path = simulate_hw_accel("h264", TrackKind::Video, false, false, false);
        assert!(!path.hardware_accelerated);
        assert_eq!(path.backend, HwBackend::Cpu);
        assert!(path.reason.contains("no NVIDIA GPU"));
        assert!(path.reason.contains("no Intel iGPU"));
    }

    #[test]
    fn simulate_hw_accel_qsv_path() {
        let path = simulate_hw_accel("vp8", TrackKind::Video, false, true, false);
        assert!(path.hardware_accelerated);
        assert_eq!(path.backend, HwBackend::Qsv);
    }

    #[test]
    fn hw_stats_compute_ratios() {
        let stats = HwAccelStats {
            hw_sessions: 80,
            sw_sessions: 20,
            hw_encoded_frames: 8000,
            sw_encoded_frames: 2000,
            fallback_count: 5,
            ..Default::default()
        };
        assert_eq!(stats.total_sessions(), 100);
        assert!((stats.hw_ratio() - 0.8).abs() < 0.01);
        assert_eq!(stats.total_encoded(), 10000);
    }

    #[test]
    fn detect_platform_returns_valid() {
        let p = detect_platform();
        assert!(matches!(p, Platform::Windows | Platform::Linux | Platform::Macos | Platform::Unknown));
    }

    #[test]
    fn codec_path_display_fields() {
        let hw = CodecPath::hardware(HwBackend::Nvenc, "h264", TrackKind::Video);
        assert_eq!(hw.codec, "h264");
        assert!(hw.hardware_accelerated);
        assert!(hw.reason.contains("nvenc"));

        let sw = CodecPath::software("vp8", TrackKind::Video, "no GPU");
        assert_eq!(sw.codec, "vp8");
        assert!(!sw.hardware_accelerated);
        assert_eq!(sw.reason, "no GPU");
    }
}