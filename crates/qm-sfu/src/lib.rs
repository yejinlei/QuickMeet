//! # qm-sfu - QuickMeet SFU layer
//!
//! Selective Forwarding Unit:
//! * [`crate::track`] - audio/video track decoupled management
//! * [`crate::forwarding`] - selective forwarding logic
//! * [`crate::ice`] - STUN/TURN traversal config
//! * [`crate::router`] - SFU routing core (pure function dispatch)
//! * [`crate::recovery`] - NACK retransmission + FEC forward error correction
//! * [`crate::hwaccel`] - hardware acceleration (GPU encode with CPU fallback)
//! * [`crate::capacity`] - capacity benchmark (200+ streams, latency, CPU/mem)

pub mod capacity;
pub mod forwarding;
pub mod hwaccel;
pub mod ice;
pub mod recovery;
pub mod router;
pub mod track;

pub use capacity::{render_report, simulate_capacity, BenchmarkReport, CapacityConfig};
pub use forwarding::{ForwardDecision, ForwardStats};
pub use hwaccel::{select_codec_path, CodecPath, HwAccelStats, HwAvailability, HwBackend};
pub use ice::{IceConfig, IceServer, IceServerKind};
pub use recovery::{
    detect_loss, evaluate_recovery, simulate_loss_recovery, try_fec_recover, FecGroup, FecPacket,
    NackCache, NackRequest, RecoveryDecision, RecoveryMode, RecoveryStats,
};
pub use router::{SfuRouteResult, SfuRouter};
pub use track::{Track, TrackId, TrackKind, TrackState};
