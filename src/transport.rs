use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use signal_core::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload, SessionEpoch,
    SignalVerb, SubReply,
};
use signal_criome::{CriomeFrame, CriomeFrameBody as FrameBody, CriomeReply, CriomeRequest};

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

impl Default for CriomeFrameCodec {
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
            FrameBody::Request { request, .. } => {
                let checked =
                    request
                        .into_checked()
                        .map_err(|(reason, _)| Error::UnexpectedSignalFrame {
                            got: reason.to_string(),
                        })?;
                Ok(checked.operations.into_head().payload)
            }
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
                    SubReply::Ok { payload, .. } => Ok(payload),
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
            reply: Reply::completed(NonEmpty::single(SubReply::Ok {
                verb: SignalVerb::Match,
                payload: reply,
            })),
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

pub struct CriomeClient {
    socket: std::path::PathBuf,
    codec: CriomeFrameCodec,
}

impl CriomeClient {
    pub fn new(socket: impl Into<std::path::PathBuf>) -> Self {
        Self {
            socket: socket.into(),
            codec: CriomeFrameCodec::default(),
        }
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
}
