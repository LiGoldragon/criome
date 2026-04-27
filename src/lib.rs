//! criome — sema's engine.
//!
//! Receives [`signal::Frame`] envelopes from the nexus daemon
//! over a Unix socket. The [`Daemon`] type owns sema and
//! handles every frame; per-verb logic lives in `impl Daemon`
//! blocks split across [`handshake`], [`assert`], [`query`],
//! and [`dispatch`].
//!
//! M0 verb scope:
//!
//! - `Handshake` → version-negotiate; reply with
//!   `HandshakeAccepted` / `HandshakeRejected`.
//! - `Assert` → rkyv-encode the inner record, prepend the kind
//!   tag, append to sema; reply with `Outcome(Ok)`.
//! - `Query` → scan sema, filter records by kind tag, decode,
//!   filter by `PatternField`s, reply with typed
//!   [`Records`](signal::Records).
//! - Other verbs → `Outcome(Diagnostic E0099)` ("not in M0").
//!
//! The full validator pipeline (schema-check, ref-resolve,
//! invariant-check, permission-check, write, cascade) lands at
//! M1+ — see
//! [criome ARCHITECTURE.md §4](https://github.com/LiGoldragon/criome/blob/main/ARCHITECTURE.md#4--the-three-daemons-expanded).

pub mod assert;
pub mod daemon;
pub mod dispatch;
pub mod error;
pub mod handshake;
pub mod kinds;
pub mod query;
pub mod uds;
pub mod validator;

pub use daemon::Daemon;
pub use error::{Error, Result};
