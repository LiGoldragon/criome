//! Integration tests for the criome dispatcher — exercises
//! `dispatch::handle` directly with assembled `Frame`s. The
//! UDS path itself is exercised end-to-end by the
//! nexus-cli ↔ nexus-daemon ↔ criome integration test that
//! lands at M0 step 6/7.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criome::dispatch;
use sema::Sema;
use signal::{
    AssertOperation, Body, Diagnostic, DiagnosticLevel, Edge, Frame, MutateOperation, Node,
    NodeQuery, OutcomeMessage, PatternField, QueryOperation, Records, RelationKind, Reply,
    Request, RetractOperation, Revision, Slot,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_sema() -> (Arc<Sema>, PathBuf) {
    let mut path = std::env::temp_dir();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    path.push(format!(
        "criome_dispatch_test_{}_{}.redb",
        std::process::id(),
        counter
    ));
    let _ = std::fs::remove_file(&path);
    let sema = Arc::new(Sema::open(&path).unwrap());
    (sema, path)
}

fn request_frame(request: Request) -> Frame {
    Frame {
        principal_hint: None,
        auth_proof: None,
        body: Body::Request(request),
    }
}

fn extract_reply(frame: Frame) -> Reply {
    match frame.body {
        Body::Reply(reply) => reply,
        Body::Request(_) => panic!("dispatcher returned a Request, not a Reply"),
    }
}

#[test]
fn assert_node_then_query_finds_it() {
    let (sema, path) = temp_sema();

    let assert = request_frame(Request::Assert(AssertOperation::Node(Node {
        name: "Alice".into(),
    })));
    let outcome = extract_reply(dispatch::handle(assert, &sema));
    assert!(matches!(outcome, Reply::Outcome(OutcomeMessage::Ok(_))));

    let query = request_frame(Request::Query(QueryOperation::Node(NodeQuery {
        name: PatternField::Wildcard,
    })));
    match extract_reply(dispatch::handle(query, &sema)) {
        Reply::Records(Records::Node(nodes)) => {
            assert_eq!(nodes.len(), 1);
            assert_eq!(nodes[0].name, "Alice");
        }
        other => panic!("expected Records::Node, got {other:?}"),
    }

    drop(sema);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn assert_three_kinds_query_filters_correctly() {
    let (sema, path) = temp_sema();

    let _ = dispatch::handle(
        request_frame(Request::Assert(AssertOperation::Node(Node { name: "User".into() }))),
        &sema,
    );
    let _ = dispatch::handle(
        request_frame(Request::Assert(AssertOperation::Edge(Edge {
            from: Slot::from(100u64),
            to: Slot::from(101u64),
            kind: RelationKind::DependsOn,
        }))),
        &sema,
    );
    let _ = dispatch::handle(
        request_frame(Request::Assert(AssertOperation::Node(Node { name: "Admin".into() }))),
        &sema,
    );

    let query = request_frame(Request::Query(QueryOperation::Node(NodeQuery {
        name: PatternField::Wildcard,
    })));
    match extract_reply(dispatch::handle(query, &sema)) {
        Reply::Records(Records::Node(nodes)) => {
            assert_eq!(nodes.len(), 2, "Edge should not appear in Node query results");
            let names: Vec<&str> = nodes.iter().map(|n| n.name.as_str()).collect();
            assert!(names.contains(&"User"));
            assert!(names.contains(&"Admin"));
        }
        other => panic!("expected Records::Node, got {other:?}"),
    }

    drop(sema);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn query_with_match_filters_by_value() {
    let (sema, path) = temp_sema();

    for name in ["Alice", "Bob", "Alice"] {
        let _ = dispatch::handle(
            request_frame(Request::Assert(AssertOperation::Node(Node {
                name: name.into(),
            }))),
            &sema,
        );
    }

    let query = request_frame(Request::Query(QueryOperation::Node(NodeQuery {
        name: PatternField::Match("Alice".into()),
    })));
    match extract_reply(dispatch::handle(query, &sema)) {
        Reply::Records(Records::Node(nodes)) => {
            assert_eq!(nodes.len(), 2);
            for node in &nodes {
                assert_eq!(node.name, "Alice");
            }
        }
        other => panic!("expected Records::Node, got {other:?}"),
    }

    drop(sema);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unimplemented_mutate_verb_returns_e0099_diagnostic() {
    let (sema, path) = temp_sema();

    let mutate = request_frame(Request::Mutate(MutateOperation::Node {
        slot: Slot::from(100u64),
        new: Node { name: "Alice".into() },
        expected_rev: Some(Revision::from(1u64)),
    }));
    match extract_reply(dispatch::handle(mutate, &sema)) {
        Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic { level, code, message, .. })) => {
            assert_eq!(level, DiagnosticLevel::Error);
            assert_eq!(code, "E0099");
            assert!(message.contains("Mutate"));
            assert!(message.contains("M1"));
        }
        other => panic!("expected E0099 Diagnostic, got {other:?}"),
    }

    drop(sema);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn unimplemented_retract_verb_returns_e0099_diagnostic() {
    let (sema, path) = temp_sema();
    let retract = request_frame(Request::Retract(RetractOperation {
        slot: Slot::from(50u64),
        expected_rev: None,
    }));
    match extract_reply(dispatch::handle(retract, &sema)) {
        Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic { code, message, .. })) => {
            assert_eq!(code, "E0099");
            assert!(message.contains("Retract"));
        }
        other => panic!("expected E0099 Diagnostic, got {other:?}"),
    }
    drop(sema);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn handshake_compatible_version_accepts() {
    let (sema, _path) = temp_sema();
    let handshake = request_frame(Request::Handshake(signal::HandshakeRequest {
        client_version: signal::SIGNAL_PROTOCOL_VERSION,
        client_name: "test-client".into(),
    }));
    match extract_reply(dispatch::handle(handshake, &sema)) {
        Reply::HandshakeAccepted(reply) => {
            assert_eq!(reply.server_version, signal::SIGNAL_PROTOCOL_VERSION);
        }
        other => panic!("expected HandshakeAccepted, got {other:?}"),
    }
}
