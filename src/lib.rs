//! criome — Spartan BLS-attestation daemon skeleton.
//!
//! Today's crate owns the Criome daemon runtime. The wire
//! vocabulary lives in `signal-criome`; durable state lives in this
//! component's own memory-engine database.

pub mod actors;
pub mod admission;
pub mod command;
pub mod conveyance;
pub mod daemon;
#[cfg(feature = "dotos-text")]
pub mod deploy_encode;
pub mod error;
pub mod founding;
pub mod language;
pub mod master_key;
pub mod router_client;
pub mod tables;
#[cfg(feature = "dotos-text")]
pub mod text;
pub mod transport;

pub use error::{Error, Result};
pub use kameo::actor::ActorRef;
pub use tables::StoreLocation;
