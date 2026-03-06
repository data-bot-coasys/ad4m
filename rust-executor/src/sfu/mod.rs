//! SFU (Selective Forwarding Unit) service for the AD4M executor.
//!
//! Embeds a str0m-based WebRTC SFU as a built-in executor service,
//! following the same pattern as the Holochain conductor, Prolog, and SurrealDB services.
//!
//! The SFU receives each participant's media stream once and selectively forwards
//! it to all other participants in the room, reducing per-peer upload from O(N) to O(1).

#[cfg(feature = "sfu")]
pub mod relay;
#[cfg(feature = "sfu")]
pub mod room;
#[cfg(feature = "sfu")]
pub mod cascade;
#[cfg(feature = "sfu")]
pub mod server;

#[cfg(feature = "sfu")]
mod service;

#[cfg(feature = "sfu")]
pub mod graphql_types;

#[cfg(feature = "sfu")]
pub use service::{get_sfu_service, SfuConfig, SfuService};

#[cfg(not(feature = "sfu"))]
pub mod stub {
    /// Stub SfuService when the `sfu` feature is not enabled.
    pub struct SfuService;

    impl SfuService {
        pub fn is_available() -> bool {
            false
        }
    }
}

#[cfg(not(feature = "sfu"))]
pub use stub::SfuService;
