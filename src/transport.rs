use std::hash::{Hash, Hasher};
use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use meta_signal_criome::{Frame as CriomeMetaFrame, FrameBody as CriomeMetaFrameBody};
use signal_criome::{
    AuthorizationObservation, AuthorizationObservationSnapshot, AuthorizationObservationToken,
    AuthorizationStateRecord, AuthorizationUpdate, CriomeEvent, CriomeFrame,
    CriomeFrameBody as FrameBody, CriomeReply, CriomeRequest, SignalCallAuthorization,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload, SessionEpoch,
    StreamEventIdentifier, SubReply, SubscriptionTokenInner,
};

use crate::{Error, Result};

fn synthetic_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CriomeFrameCodec {
    maximum_frame_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CriomeMetaFrameCodec {
    maximum_frame_bytes: usize,
}

pub enum CriomeStreamItem {
    Reply(CriomeReply),
    Event(CriomeEvent),
}

impl Default for CriomeFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

impl Default for CriomeMetaFrameCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

impl CriomeFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_request(&self, reader: &mut impl Read) -> Result<CriomeRequest> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Request { request, .. } => Ok(request.payloads.into_head()),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_request(&self, writer: &mut impl Write, request: CriomeRequest) -> Result<()> {
        let frame = CriomeFrame::new(FrameBody::Request {
            exchange: synthetic_exchange(),
            request: request.into_request(),
        });
        self.write_frame(writer, frame)
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> Result<CriomeReply> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedSignalFrame {
                        got: format!("{other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: format!("rejected: {reason}"),
                }),
            },
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_reply(&self, writer: &mut impl Write, reply: CriomeReply) -> Result<()> {
        let frame = CriomeFrame::new(FrameBody::Reply {
            exchange: synthetic_exchange(),
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        self.write_frame(writer, frame)
    }

    pub fn read_stream_item(&self, reader: &mut impl Read) -> Result<CriomeStreamItem> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(CriomeStreamItem::Reply(payload)),
                    other => Err(Error::UnexpectedSignalFrame {
                        got: format!("{other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: format!("rejected: {reason}"),
                }),
            },
            FrameBody::SubscriptionEvent { event, .. } => Ok(CriomeStreamItem::Event(event)),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_authorization_update(
        &self,
        writer: &mut impl Write,
        event_sequence: u64,
        token: &AuthorizationObservationToken,
        state: AuthorizationStateRecord,
    ) -> Result<()> {
        let frame = CriomeFrame::new(FrameBody::SubscriptionEvent {
            event_identifier: StreamEventIdentifier::new(
                SessionEpoch::new(0),
                ExchangeLane::Acceptor,
                LaneSequence::new(event_sequence),
            ),
            token: token_inner(token),
            event: CriomeEvent::AuthorizationUpdate(AuthorizationUpdate::new(state)),
        });
        self.write_frame(writer, frame)
    }

    fn read_frame(&self, reader: &mut impl Read) -> Result<CriomeFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("frame length {length} exceeds {}", self.maximum_frame_bytes),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(CriomeFrame::decode_length_prefixed(&bytes)?)
    }

    fn write_frame(&self, writer: &mut impl Write, frame: CriomeFrame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

fn token_inner(token: &AuthorizationObservationToken) -> SubscriptionTokenInner {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    token.payload().as_str().hash(&mut hasher);
    SubscriptionTokenInner::new(hasher.finish())
}

impl CriomeMetaFrameCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    pub fn read_request(&self, reader: &mut impl Read) -> Result<meta_signal_criome::Input> {
        match self.read_frame(reader)?.into_body() {
            CriomeMetaFrameBody::Request { request, .. } => Ok(request.payloads.into_head()),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_request(
        &self,
        writer: &mut impl Write,
        request: meta_signal_criome::Input,
    ) -> Result<()> {
        let frame = CriomeMetaFrame::new(CriomeMetaFrameBody::Request {
            exchange: synthetic_exchange(),
            request: request.into_request(),
        });
        self.write_frame(writer, frame)
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> Result<meta_signal_criome::Output> {
        match self.read_frame(reader)?.into_body() {
            CriomeMetaFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(payload) => Ok(payload),
                    other => Err(Error::UnexpectedSignalFrame {
                        got: format!("{other:?}"),
                    }),
                },
                Reply::Rejected { reason } => Err(Error::UnexpectedSignalFrame {
                    got: format!("rejected: {reason}"),
                }),
            },
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_reply(
        &self,
        writer: &mut impl Write,
        reply: meta_signal_criome::Output,
    ) -> Result<()> {
        let frame = CriomeMetaFrame::new(CriomeMetaFrameBody::Reply {
            exchange: synthetic_exchange(),
            reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply))),
        });
        self.write_frame(writer, frame)
    }

    fn read_frame(&self, reader: &mut impl Read) -> Result<CriomeMetaFrame> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("frame length {length} exceeds {}", self.maximum_frame_bytes),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(CriomeMetaFrame::decode_length_prefixed(&bytes)?)
    }

    fn write_frame(&self, writer: &mut impl Write, frame: CriomeMetaFrame) -> Result<()> {
        let bytes = frame.encode_length_prefixed()?;
        writer.write_all(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

pub struct CriomeClient {
    socket: std::path::PathBuf,
    codec: CriomeFrameCodec,
}

pub struct CriomeAuthorizationObservationSession {
    token: AuthorizationObservationToken,
    snapshot: AuthorizationObservationSnapshot,
    stream: BufReader<UnixStream>,
    codec: CriomeFrameCodec,
}

pub struct CriomeMetaClient {
    socket: std::path::PathBuf,
    codec: CriomeMetaFrameCodec,
}

impl CriomeClient {
    pub fn new(socket: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            codec: CriomeFrameCodec::default(),
        }
    }

    pub fn from_environment() -> Self {
        let socket = std::env::var_os("CRIOME_SOCKET")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/criome.sock"));
        Self::new(socket)
    }

    pub fn send(&self, request: CriomeRequest) -> Result<CriomeReply> {
        if !Path::new(&self.socket).exists() {
            return Err(Error::MissingSocket {
                path: self.socket.clone(),
            });
        }
        let stream = UnixStream::connect(&self.socket)?;
        let mut stream = BufReader::new(stream);
        self.codec.write_request(stream.get_mut(), request)?;
        self.codec.read_reply(&mut stream)
    }

    pub fn observe_authorization(
        &self,
        observation: AuthorizationObservation,
    ) -> Result<CriomeAuthorizationObservationSession> {
        if !Path::new(&self.socket).exists() {
            return Err(Error::MissingSocket {
                path: self.socket.clone(),
            });
        }
        let token = AuthorizationObservationToken::new(observation.payload().clone());
        let stream = UnixStream::connect(&self.socket)?;
        let mut stream = BufReader::new(stream);
        self.codec.write_request(
            stream.get_mut(),
            CriomeRequest::ObserveAuthorization(observation),
        )?;
        let reply = self.codec.read_reply(&mut stream)?;
        let CriomeReply::AuthorizationObservationSnapshot(snapshot) = reply else {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("{reply:?}"),
            });
        };
        Ok(CriomeAuthorizationObservationSession {
            token,
            snapshot,
            stream,
            codec: self.codec,
        })
    }

    pub fn authorize_signal_call(
        &self,
        authorization: SignalCallAuthorization,
    ) -> Result<CriomeAuthorizationObservationSession> {
        if !Path::new(&self.socket).exists() {
            return Err(Error::MissingSocket {
                path: self.socket.clone(),
            });
        }
        let stream = UnixStream::connect(&self.socket)?;
        let mut stream = BufReader::new(stream);
        self.codec.write_request(
            stream.get_mut(),
            CriomeRequest::AuthorizeSignalCall(authorization),
        )?;
        let reply = self.codec.read_reply(&mut stream)?;
        let CriomeReply::AuthorizationObservationSnapshot(snapshot) = reply else {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("{reply:?}"),
            });
        };
        let Some(state) = snapshot.states().first() else {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("{snapshot:?}"),
            });
        };
        Ok(CriomeAuthorizationObservationSession {
            token: AuthorizationObservationToken::new(state.request_slot.clone()),
            snapshot,
            stream,
            codec: self.codec,
        })
    }
}

impl CriomeAuthorizationObservationSession {
    pub fn token(&self) -> &AuthorizationObservationToken {
        &self.token
    }

    pub fn snapshot(&self) -> &AuthorizationObservationSnapshot {
        &self.snapshot
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        self.stream.get_ref().set_read_timeout(timeout)?;
        Ok(())
    }

    pub fn next_update(&mut self) -> Result<AuthorizationStateRecord> {
        loop {
            match self.codec.read_stream_item(&mut self.stream)? {
                CriomeStreamItem::Event(CriomeEvent::AuthorizationUpdate(update)) => {
                    let state = update.into_payload();
                    if state.request_slot == *self.token.payload() {
                        return Ok(state);
                    }
                }
                CriomeStreamItem::Event(_) => {}
                CriomeStreamItem::Reply(reply) => {
                    return Err(Error::UnexpectedSignalFrame {
                        got: format!("{reply:?}"),
                    });
                }
            }
        }
    }
}

impl CriomeMetaClient {
    pub fn new(socket: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            codec: CriomeMetaFrameCodec::default(),
        }
    }

    pub fn send(&self, request: meta_signal_criome::Input) -> Result<meta_signal_criome::Output> {
        if !Path::new(&self.socket).exists() {
            return Err(Error::MissingSocket {
                path: self.socket.clone(),
            });
        }
        let stream = UnixStream::connect(&self.socket)?;
        let mut stream = BufReader::new(stream);
        self.codec.write_request(stream.get_mut(), request)?;
        self.codec.read_reply(&mut stream)
    }
}
