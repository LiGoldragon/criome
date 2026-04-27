//! Per-frame dispatch — `Request` → `Reply`.
//!
//! M0 verb scope:
//! - `Handshake` / `Assert` / `Query` are implemented by their
//!   per-verb handler modules.
//! - `Mutate` / `Retract` / `AtomicBatch` / `Subscribe` /
//!   `Validate` return `Diagnostic E0099` ("not in M0").
//! - A `Body::Reply` arriving in the request slot is a
//!   protocol error — return `E0098`.

use std::sync::Arc;

use sema::Sema;
use signal::{Body, Diagnostic, DiagnosticLevel, Frame, OutcomeMessage, Reply, Request};

use crate::{assert, handshake, query};

pub fn handle(frame: Frame, sema: &Arc<Sema>) -> Frame {
    let reply = match frame.body {
        Body::Request(request) => process_request(request, sema),
        Body::Reply(_) => protocol_error(
            "E0098",
            "client sent Body::Reply where Body::Request expected".to_string(),
        ),
    };
    Frame {
        principal_hint: None,
        auth_proof: None,
        body: Body::Reply(reply),
    }
}

fn process_request(request: Request, sema: &Arc<Sema>) -> Reply {
    match request {
        Request::Handshake(request) => handshake::handle(request),
        Request::Assert(operation) => assert::handle(operation, sema),
        Request::Query(operation) => query::handle(operation, sema),
        Request::Mutate(_) => deferred_verb("Mutate", "M1"),
        Request::Retract(_) => deferred_verb("Retract", "M1"),
        Request::AtomicBatch(_) => deferred_verb("AtomicBatch", "M1"),
        Request::Subscribe(_) => deferred_verb("Subscribe", "M2"),
        Request::Validate(_) => deferred_verb("Validate", "M1"),
    }
}

fn deferred_verb(verb: &str, milestone: &str) -> Reply {
    protocol_error(
        "E0099",
        format!("{verb} verb not implemented in M0; planned for {milestone}"),
    )
}

fn protocol_error(code: &str, message: String) -> Reply {
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
