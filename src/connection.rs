//! `Connection` actor — per-client frame shuttle.
//!
//! Reads length-prefixed [`signal::Frame`]s from one
//! `UnixStream`, decodes the per-verb body, dispatches typed
//! messages to either the [`engine`](crate::engine) actor
//! (writes + handshake + deferred verbs) or one of the
//! [`reader`](crate::reader) actors (queries, round-robin
//! through the pool), wraps the typed reply back into a Frame,
//! writes it to the same socket. Stops when the client closes.
//!
//! The Frame is **always decomposed** before crossing an actor
//! boundary — no `HandleFrame` god-message. Per-verb specificity
//! lives at this seam (criome/ARCHITECTURE.md §2 Invariant D).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use ractor::{Actor, ActorProcessingErr, ActorRef};
use signal::{Body, Diagnostic, Frame, OutcomeMessage, Reply, Request};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::error::{Error, Result};
use crate::{engine, reader};

pub struct Connection;

pub struct State {
    stream: UnixStream,
    engine: ActorRef<engine::Message>,
    readers: Vec<ActorRef<reader::Message>>,
    reader_cursor: Arc<AtomicUsize>,
}

pub struct Arguments {
    pub stream: UnixStream,
    pub engine: ActorRef<engine::Message>,
    pub readers: Vec<ActorRef<reader::Message>>,
    pub reader_cursor: Arc<AtomicUsize>,
}

pub enum Message {
    /// Self-cast tick that drives the read loop. Each `ReadNext`
    /// reads one frame, dispatches it, writes the reply, and
    /// re-arms (or stops on EOF / error).
    ReadNext,
    /// Out-of-band push from the engine — a registered
    /// subscription has new matching records. Written to the
    /// socket as a `Reply::Records` frame.
    SubscriptionPush { records: signal::Records },
}

/// Convert a ractor `CallResult<T>` into the crate's `Result<T>`.
/// `Success` unwraps; `Timeout` / `SenderError` map to
/// `Error::ActorCall` with a labelled message so the caller's
/// log points at the specific call site.
fn call_into<T>(result: ractor::rpc::CallResult<T>, label: &'static str) -> Result<T> {
    match result {
        ractor::rpc::CallResult::Success(value) => Ok(value),
        ractor::rpc::CallResult::Timeout => {
            Err(Error::ActorCall(format!("{label}: call timed out")))
        }
        ractor::rpc::CallResult::SenderError => {
            Err(Error::ActorCall(format!("{label}: sender dropped before reply")))
        }
    }
}

impl State {
    fn pick_reader(&self) -> Option<&ActorRef<reader::Message>> {
        if self.readers.is_empty() {
            return None;
        }
        let index = self.reader_cursor.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        self.readers.get(index)
    }

    async fn read_frame(&mut self) -> Result<Frame> {
        let mut length_bytes = [0u8; 4];
        self.stream.read_exact(&mut length_bytes).await?;
        let length = u32::from_be_bytes(length_bytes) as usize;
        let mut frame_bytes = vec![0u8; length];
        self.stream.read_exact(&mut frame_bytes).await?;
        Ok(Frame::decode(&frame_bytes)?)
    }

    async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        let bytes = frame.encode();
        let length = u32::try_from(bytes.len())
            .map_err(|_| Error::FrameTooLarge { length: bytes.len() })?;
        self.stream.write_all(&length.to_be_bytes()).await?;
        self.stream.write_all(&bytes).await?;
        Ok(())
    }

    async fn dispatch(&self, request: Request) -> Result<Reply> {
        let reply = match request {
            Request::Handshake(request) => {
                let raw = self
                    .engine
                    .call(
                        |port| engine::Message::Handshake { request, reply_port: port },
                        None,
                    )
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                let outcome = call_into(raw, "engine handshake")?;
                match outcome {
                    engine::HandshakeOutcome::Accepted(reply) => Reply::HandshakeAccepted(reply),
                    engine::HandshakeOutcome::Rejected(reason) => Reply::HandshakeRejected(reason),
                }
            }
            Request::Assert(operation) => {
                let raw = self
                    .engine
                    .call(
                        |port| engine::Message::Assert { operation, reply_port: port },
                        None,
                    )
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?;
                let outcome = call_into(raw, "engine assert")?;
                Reply::Outcome(outcome)
            }
            Request::Query(operation) => match self.pick_reader() {
                Some(reader_ref) => {
                    let raw = reader_ref
                        .call(
                            |port| reader::Message::Query { operation, reply_port: port },
                            None,
                        )
                        .await
                        .map_err(|error| Error::ActorCall(error.to_string()))?;
                    let records = call_into(raw, "reader query")?;
                    Reply::Records(records)
                }
                None => Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic::error(
                    "E0500",
                    "no reader actors available — daemon misconfiguration".to_string(),
                ))),
            },
            Request::Mutate(_) => self.deferred("Mutate", "M1").await?,
            Request::Retract(_) => self.deferred("Retract", "M1").await?,
            Request::AtomicBatch(_) => self.deferred("AtomicBatch", "M1").await?,
            Request::Subscribe(_) => {
                // Subscribe is dispatched by the connection
                // actor's handle method (it needs `myself` to
                // pass to the engine). The path here is a
                // placeholder; real dispatch happens in the
                // handle() arm where we have the ActorRef.
                Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic::error(
                    "E0510",
                    "subscribe routing skipped this dispatch; bug".to_string(),
                )))
            }
            Request::Validate(_) => self.deferred("Validate", "M1").await?,
        };
        Ok(reply)
    }

    async fn deferred(&self, verb: &'static str, milestone: &'static str) -> Result<Reply> {
        let raw = self
            .engine
            .call(
                |port| engine::Message::DeferredVerb { verb, milestone, reply_port: port },
                None,
            )
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        let outcome = call_into(raw, "engine deferred")?;
        Ok(Reply::Outcome(outcome))
    }
}

#[ractor::async_trait]
impl Actor for Connection {
    type Msg = Message;
    type State = State;
    type Arguments = Arguments;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        arguments: Arguments,
    ) -> std::result::Result<Self::State, ActorProcessingErr> {
        ractor::cast!(myself, Message::ReadNext)?;
        Ok(State {
            stream: arguments.stream,
            engine: arguments.engine,
            readers: arguments.readers,
            reader_cursor: arguments.reader_cursor,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Message,
        state: &mut State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        match message {
            Message::SubscriptionPush { records } => {
                let frame = Frame {
                    principal_hint: None,
                    auth_proof: None,
                    body: Body::Reply(Reply::Records(records)),
                };
                state
                    .write_frame(frame)
                    .await
                    .map_err(|e| Box::new(e) as ActorProcessingErr)?;
                return Ok(());
            }
            Message::ReadNext => { /* fall through to read-and-dispatch below */ }
        }

        let frame = match state.read_frame().await {
            Ok(frame) => frame,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                myself.stop(Some("client closed".into()));
                return Ok(());
            }
            Err(error) => return Err(Box::new(error)),
        };

        let reply = match frame.body {
            Body::Request(Request::Subscribe(operation)) => {
                let raw = state
                    .engine
                    .call(
                        |port| engine::Message::Subscribe {
                            operation,
                            connection: myself.clone(),
                            reply_port: port,
                        },
                        None,
                    )
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))
                    .map_err(|e| Box::new(e) as ActorProcessingErr)?;
                let records = call_into(raw, "engine subscribe")
                    .map_err(|e| Box::new(e) as ActorProcessingErr)?;
                Reply::Records(records)
            }
            Body::Request(request) => state.dispatch(request).await.map_err(|e| Box::new(e) as ActorProcessingErr)?,
            Body::Reply(_) => Reply::Outcome(OutcomeMessage::Diagnostic(Diagnostic::error(
                "E0098",
                "client sent Body::Reply where Body::Request expected".to_string(),
            ))),
        };

        let reply_frame = Frame {
            principal_hint: None,
            auth_proof: None,
            body: Body::Reply(reply),
        };
        state.write_frame(reply_frame).await.map_err(|e| Box::new(e) as ActorProcessingErr)?;
        ractor::cast!(myself, Message::ReadNext)?;
        Ok(())
    }
}
