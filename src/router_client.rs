//! The router working-socket client — criome's origination-side twin of
//! [`crate::transport::CriomeClient`].
//!
//! `RouterSubmission` dials its own local router's working socket over this
//! client to hand off a `signal-router` `SubmitRoutedObjects` origination and
//! read back the router's accept/refuse reply. The wire shape mirrors what
//! `router::daemon::RouterEngine::handle_working_connection` reads on the
//! other end: a `triad_runtime`-style `[u32 BE length][body]` frame whose body
//! is a `signal_router::Frame` (`signal_frame::ExchangeFrame<Input, Output>`)
//! carrying a single-payload `Request`/`Reply`.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, RequestPayload, SessionEpoch, SubReply,
};
use signal_router::{Frame as RouterFrame, FrameBody as RouterFrameBody, Input, Output};

use crate::{Error, Result};

fn synthetic_exchange() -> ExchangeIdentifier {
    ExchangeIdentifier::new(
        SessionEpoch::new(0),
        ExchangeLane::Connector,
        LaneSequence::first(),
    )
}

/// A client that dials a local `signal-router` working socket and carries one
/// request/reply exchange over it.
pub struct RouterClient {
    socket: PathBuf,
}

impl RouterClient {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    /// Send one `signal-router` request to the local router's working socket
    /// and return its reply.
    pub fn send(&self, request: Input) -> Result<Output> {
        if !Path::new(&self.socket).exists() {
            return Err(Error::MissingSocket {
                path: self.socket.clone(),
            });
        }
        let mut stream = UnixStream::connect(&self.socket)?;
        let frame = RouterFrame::new(RouterFrameBody::Request {
            exchange: synthetic_exchange(),
            request: request.into_request(),
        });
        stream.write_all(&frame.encode_length_prefixed()?)?;
        stream.flush()?;
        Self::read_reply(&mut stream)
    }

    fn read_reply(stream: &mut UnixStream) -> Result<Output> {
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        stream.read_exact(&mut bytes[4..])?;
        match RouterFrame::decode_length_prefixed(&bytes)?.into_body() {
            RouterFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(output) => Ok(output),
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
}
