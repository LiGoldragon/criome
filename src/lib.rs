//! criome — sema's engine.
//!
//! Receives [`signal::Frame`] envelopes from clients (the
//! nexus daemon over UDS, or any signal-speaking peer)
//! and dispatches them through a ractor supervision tree:
//!
//! ```text
//! Daemon (root)
//!   ├── Engine          (writes + handshake + deferred verbs)
//!   ├── Reader × N      (reads, concurrent via redb MVCC; N from sema.reader_count())
//!   └── Listener
//!         ├── Connection × M  (one per accepted UDS client)
//!         └── ...
//! ```
//!
//! M0 verb scope:
//!
//! - `Handshake` → version-negotiate; reply with
//!   `HandshakeAccepted` / `HandshakeRejected`.
//! - `Assert` → rkyv-encode the inner record, prepend the kind
//!   tag, append to sema; reply with `Outcome(Ok)`.
//! - `Query` → scan sema, filter records by kind tag, decode,
//!   filter by `PatternField`s, reply with typed
//!   [`Records`](signal::Records). Routed to one of the
//!   `Reader` actors round-robin.
//! - Other verbs → `Outcome(Diagnostic E0099)` ("not in M0").
//!
//! For sync use (the `criome-handle-frame` one-shot binary,
//! integration tests), [`engine::State::handle_frame`] is the
//! all-verbs entry point that bypasses the actor system.
//!
//! The full validator pipeline (schema-check, ref-resolve,
//! invariant-check, permission-check, write, cascade) lands
//! at M1+ — see
//! [criome ARCHITECTURE.md §4](https://github.com/LiGoldragon/criome/blob/main/ARCHITECTURE.md#4--the-three-daemons-expanded).

pub mod connection;
pub mod daemon;
pub mod engine;
pub mod error;
pub mod kinds;
pub mod listener;
pub mod reader;
pub mod validator;

pub use daemon::Daemon;
pub use error::{Error, Result};
