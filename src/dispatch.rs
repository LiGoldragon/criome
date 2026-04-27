//! Per-request dispatch table — `impl Daemon { handle_request, deferred_verb, protocol_error }`.
//!
//! M0 verb scope:
//! - `Handshake` / `Assert` / `Query` are implemented by their
//!   per-verb `impl Daemon` blocks (handshake.rs / assert.rs /
//!   query.rs).
//! - `Mutate` / `Retract` / `AtomicBatch` / `Subscribe` /
//!   `Validate` return `Diagnostic E0099` ("not in M0").

use signal::{Diagnostic, DiagnosticLevel, OutcomeMessage, Reply, Request};

use crate::daemon::Daemon;

impl Daemon {
    pub(crate) fn handle_request(&self, request: Request) -> Reply {
        match request {
            Request::Handshake(request) => Self::handle_handshake(request),
            Request::Assert(operation) => self.handle_assert(operation),
            Request::Query(operation) => self.handle_query(operation),
            Request::Mutate(_) => Self::deferred_verb("Mutate", "M1"),
            Request::Retract(_) => Self::deferred_verb("Retract", "M1"),
            Request::AtomicBatch(_) => Self::deferred_verb("AtomicBatch", "M1"),
            Request::Subscribe(_) => Self::deferred_verb("Subscribe", "M2"),
            Request::Validate(_) => Self::deferred_verb("Validate", "M1"),
        }
    }

    pub(crate) fn deferred_verb(verb: &str, milestone: &str) -> Reply {
        Self::protocol_error(
            "E0099",
            format!("{verb} verb not implemented in M0; planned for {milestone}"),
        )
    }

    pub(crate) fn protocol_error(code: &str, message: String) -> Reply {
        Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic {
            level: DiagnosticLevel::Error,
            code: code.to_string(),
            message,
            primary_site: None,
            context: vec![],
            suggestions: vec![],
            durable_record: None,
        }))
    }
}
