//! criome — Spartan BLS-attestation daemon skeleton.
//!
//! Today's crate owns the Criome daemon runtime. The wire
//! vocabulary lives in `signal-criome`; durable state lives in this
//! component's own sema-engine database.

pub mod actors;
pub mod admission;
pub mod ceremony;
pub mod command;
pub mod daemon;
pub mod error;
pub mod language;
pub mod master_key;
pub mod tables;
#[cfg(feature = "nota-text")]
pub mod text;
pub mod transport;

pub use error::{Error, Result};
pub use kameo::actor::ActorRef;
pub use tables::StoreLocation;
