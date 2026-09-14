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

pub use forwarding::{ForwardDecision, ForwardStats};
pub use hwaccel::{CodecPath, HwBackend, HwAccelStats, HwAvailability, select_codec_path};
pub use ice::{IceConfig, IceServer, IceServerKind};
pub use recovery::{
    detect_loss, evaluate_recovery, NackCache, NackRequest, RecoveryDecision, RecoveryMode,
    RecoveryStats, simulate_loss_recovery, FecGroup, FecPacket, try_fec_recover,
};
pub use capacity::{BenchmarkReport, CapacityConfig, simulate_capacity, render_report};
pub use router::{SfuRouter, SfuRouteResult};
pub use track::{Track, TrackId, TrackKind, TrackState};