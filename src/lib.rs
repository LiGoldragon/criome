//! criome — sema's engine.
//!
//! Receives [`signal::Frame`] envelopes from the nexus daemon
//! over a Unix socket. For each request:
//!
//! - [`Request::Handshake`](signal::Request::Handshake) →
//!   handshake-version negotiation; reply with
//!   `HandshakeAccepted` or `HandshakeRejected`.
//! - [`Request::Assert`](signal::Request::Assert) → rkyv-encode
//!   the inner record and append it to sema; reply with
//!   `Outcome(Ok)`.
//! - [`Request::Query`](signal::Request::Query) → scan sema,
//!   try-decode each record as the requested kind, filter by
//!   the query's `PatternField`s, reply with typed
//!   [`Records`](signal::Records).
//! - Other verbs → `Outcome(Diagnostic E0099)` ("not in M0").
//!
//! The full validator pipeline (schema-check, ref-resolve,
//! invariant-check, permission-check, write, cascade) lands at
//! M1+ — see [criome ARCHITECTURE.md §4](https://github.com/LiGoldragon/criome/blob/main/ARCHITECTURE.md#4--the-three-daemons-expanded).
//! M0 is the minimum end-to-end loop: nexus text → nexus
//! daemon → signal frame → criome → sema → records → criome
//! reply → nexus daemon → nexus text.

pub mod assert;
pub mod dispatch;
pub mod error;
pub mod handshake;
pub mod kinds;
pub mod query;
pub mod uds;
pub mod validator;

pub use error::{Error, Result};
