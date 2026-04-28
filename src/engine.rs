//! `Engine` actor — write side of criome's sema.
//!
//! Owns a shared `Arc<Sema>` reference; serialises every write
//! verb (Assert today; Mutate / Retract / AtomicBatch when they
//! land) through one mailbox. The handshake handler also lives
//! here because it's stateless and one-per-connection.
//!
//! Read verbs (`Query`, `Subscribe`) go through the
//! [`crate::reader`] pool instead — multiple Reader actors share
//! the same `Arc<Sema>` and answer queries concurrently via
//! redb's MVCC.
//!
//! For sync use (the `criome-handle-frame` one-shot binary,
//! integration tests), [`State::handle_frame`] is the
//! all-verbs entry point that doesn't go through ractor at all.

use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use sema::Sema;
use signal::{
    AssertOperation, Body, Diagnostic, Frame, HandshakeRejectionReason, HandshakeReply,
    HandshakeRequest, Ok as OkRecord, OutcomeMessage, ProtocolVersion, Reply, Request, Slot,
    SIGNAL_PROTOCOL_VERSION,
};

use crate::{kinds, reader};

pub struct Engine;

pub struct State {
    sema: Arc<Sema>,
}

pub struct Arguments {
    pub sema: Arc<Sema>,
}

pub enum Message {
    Handshake {
        request: HandshakeRequest,
        reply_port: RpcReplyPort<HandshakeOutcome>,
    },
    Assert {
        operation: AssertOperation,
        reply_port: RpcReplyPort<OutcomeMessage>,
    },
    /// Verbs that are not yet implemented in M0. Returns a
    /// canonical `E0099` diagnostic. `Mutate`, `Retract`,
    /// `AtomicBatch`, `Subscribe`, `Validate` all route here.
    DeferredVerb {
        verb: &'static str,
        milestone: &'static str,
        reply_port: RpcReplyPort<OutcomeMessage>,
    },
}

pub enum HandshakeOutcome {
    Accepted(HandshakeReply),
    Rejected(HandshakeRejectionReason),
}

impl State {
    pub fn new(sema: Arc<Sema>) -> Self {
        Self { sema }
    }

    /// Sync façade dispatching every verb. Used by the
    /// `criome-handle-frame` one-shot binary and by the
    /// integration tests; the actor system goes through the
    /// per-verb [`Message`] variants instead.
    pub fn handle_frame(&self, frame: Frame) -> Frame {
        let reply = match frame.body {
            Body::Request(request) => self.handle_request(request),
            Body::Reply(_) => Self::protocol_error(
                "E0098",
                "client sent Body::Reply where Body::Request expected".to_string(),
            ),
        };
        Frame { principal_hint: None, auth_proof: None, body: Body::Reply(reply) }
    }

    fn handle_request(&self, request: Request) -> Reply {
        match request {
            Request::Handshake(request) => match Self::handle_handshake(request) {
                HandshakeOutcome::Accepted(reply) => Reply::HandshakeAccepted(reply),
                HandshakeOutcome::Rejected(reason) => Reply::HandshakeRejected(reason),
            },
            Request::Assert(operation) => Reply::Outcome(self.handle_assert(operation)),
            Request::Query(operation) => {
                Reply::Records(reader::State::new(Arc::clone(&self.sema)).handle_query(operation))
            }
            Request::Mutate(_) => Reply::Outcome(Self::handle_deferred("Mutate", "M1")),
            Request::Retract(_) => Reply::Outcome(Self::handle_deferred("Retract", "M1")),
            Request::AtomicBatch(_) => Reply::Outcome(Self::handle_deferred("AtomicBatch", "M1")),
            Request::Subscribe(_) => Reply::Outcome(Self::handle_deferred("Subscribe", "M2")),
            Request::Validate(_) => Reply::Outcome(Self::handle_deferred("Validate", "M1")),
        }
    }

    /// Determined entirely by the request and the build-time
    /// `SIGNAL_PROTOCOL_VERSION`. M0 single-instance fills
    /// `server_id` with `Slot::from(0u64)`; multi-instance
    /// criome assigns a real `CriomeDaemonInstance` slot here.
    pub fn handle_handshake(request: HandshakeRequest) -> HandshakeOutcome {
        if request.client_version.is_compatible_with(SIGNAL_PROTOCOL_VERSION) {
            return HandshakeOutcome::Accepted(HandshakeReply {
                server_version: SIGNAL_PROTOCOL_VERSION,
                server_id: Slot::from(0u64),
            });
        }
        HandshakeOutcome::Rejected(Self::rejection_reason(
            request.client_version,
            SIGNAL_PROTOCOL_VERSION,
        ))
    }

    fn rejection_reason(
        client: ProtocolVersion,
        server: ProtocolVersion,
    ) -> HandshakeRejectionReason {
        if client.major != server.major {
            HandshakeRejectionReason::IncompatibleMajor { client, server }
        } else {
            HandshakeRejectionReason::ClientMinorAhead { client, server }
        }
    }

    pub fn handle_assert(&self, operation: AssertOperation) -> OutcomeMessage {
        let tagged = match &operation {
            AssertOperation::Node(value) => Self::prepend_tag(kinds::NODE, value),
            AssertOperation::Edge(value) => Self::prepend_tag(kinds::EDGE, value),
            AssertOperation::Graph(value) => Self::prepend_tag(kinds::GRAPH, value),
            AssertOperation::KindDecl(value) => Self::prepend_tag(kinds::KIND_DECL, value),
        };
        match tagged {
            Ok(bytes) => match self.sema.store(&bytes) {
                Ok(_slot) => OutcomeMessage::Ok(OkRecord::default()),
                Err(error) => OutcomeMessage::Diagnostic(Diagnostic::error(
                    "E0500",
                    format!("sema write failed: {error}"),
                )),
            },
            Err(error) => OutcomeMessage::Diagnostic(Diagnostic::error(
                "E0501",
                format!("rkyv encode failed: {error}"),
            )),
        }
    }

    pub fn handle_deferred(verb: &'static str, milestone: &'static str) -> OutcomeMessage {
        OutcomeMessage::Diagnostic(Diagnostic::error(
            "E0099",
            format!("{verb} verb not implemented in M0; planned for {milestone}"),
        ))
    }

    fn prepend_tag<T>(tag: u8, value: &T) -> Result<Vec<u8>, String>
    where
        T: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
    {
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?;
        let mut tagged = Vec::with_capacity(archive.len() + 1);
        tagged.push(tag);
        tagged.extend_from_slice(&archive);
        Ok(tagged)
    }

    fn protocol_error(code: &str, message: String) -> Reply {
        Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic::error(code, message)))
    }
}

#[ractor::async_trait]
impl Actor for Engine {
    type Msg = Message;
    type State = State;
    type Arguments = Arguments;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        arguments: Arguments,
    ) -> std::result::Result<Self::State, ActorProcessingErr> {
        Ok(State::new(arguments.sema))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Message,
        state: &mut State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        match message {
            Message::Handshake { request, reply_port } => {
                let _ = reply_port.send(State::handle_handshake(request));
            }
            Message::Assert { operation, reply_port } => {
                let _ = reply_port.send(state.handle_assert(operation));
            }
            Message::DeferredVerb { verb, milestone, reply_port } => {
                let _ = reply_port.send(State::handle_deferred(verb, milestone));
            }
        }
        Ok(())
    }
}

