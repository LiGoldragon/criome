//! criome — Spartan BLS-attestation daemon skeleton.
//!
//! Today's crate owns the Criome daemon runtime. The wire
//! vocabulary lives in `signal-criome`; durable state lives in this
//! component's own Sema database.

pub mod actors;
pub mod command;
pub mod daemon;
pub mod error;
pub mod tables;
pub mod text;
pub mod transport;

pub use error::{Error, Result};
pub use kameo::actor::ActorRef;
pub use tables::StoreLocation;
