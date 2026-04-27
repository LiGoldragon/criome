//! `Daemon` — the noun that owns sema and handles every signal
//! frame. Each verb's `impl Daemon { … }` block lives in its
//! own file under `src/` (handshake.rs / assert.rs / query.rs /
//! dispatch.rs); this file holds the type definition and the
//! cross-cutting entry points.

use std::sync::Arc;

use sema::Sema;
use signal::Frame;

pub struct Daemon {
    sema: Arc<Sema>,
}

impl Daemon {
    /// Construct a daemon over the given sema handle.
    pub fn new(sema: Arc<Sema>) -> Self {
        Self { sema }
    }

    /// Borrow the sema handle. Used by per-verb impl blocks
    /// in sibling modules.
    pub(crate) fn sema(&self) -> &Sema {
        &self.sema
    }

    /// Process one inbound `Frame` and produce the reply
    /// `Frame`. Wraps the per-verb dispatch on
    /// [`Self::handle_request`] (defined in `dispatch.rs`).
    pub fn handle_frame(&self, frame: Frame) -> Frame {
        let reply = match frame.body {
            signal::Body::Request(request) => self.handle_request(request),
            signal::Body::Reply(_) => Self::protocol_error(
                "E0098",
                "client sent Body::Reply where Body::Request expected".to_string(),
            ),
        };
        Frame {
            principal_hint: None,
            auth_proof: None,
            body: signal::Body::Reply(reply),
        }
    }
}
