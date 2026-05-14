use std::io::{BufReader, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

use signal_core::{FrameBody, Reply, Request};
use signal_criome::{CriomeReply, CriomeRequest, Frame as CriomeFrame};

use crate::{Error, Result};

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
            FrameBody::Request(request) => {
                request
                    .into_payload_checked()
                    .map_err(|error| Error::UnexpectedSignalFrame {
                        got: error.to_string(),
                    })
            }
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_request(&self, writer: &mut impl Write, request: CriomeRequest) -> Result<()> {
        let frame = CriomeFrame::new(FrameBody::Request(Request::from_payload(request)));
        self.write_frame(writer, frame)
    }

    pub fn read_reply(&self, reader: &mut impl Read) -> Result<CriomeReply> {
        match self.read_frame(reader)?.into_body() {
            FrameBody::Reply(Reply::Operation(payload)) => Ok(payload),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    pub fn write_reply(&self, writer: &mut impl Write, reply: CriomeReply) -> Result<()> {
        let frame = CriomeFrame::new(FrameBody::Reply(Reply::operation(reply)));
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
