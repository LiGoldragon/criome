use std::io::{BufReader, Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixStream;
use std::path::Path;

use meta_signal_criome::{Frame as CriomeMetaFrame, FrameBody as CriomeMetaFrameBody};
use signal_criome::{
    BlsPublicKey, CriomeFrame, CriomeFrameBody as FrameBody, CriomeReply, CriomeRequest,
    PeerEnvelope,
};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, NonEmpty, Reply, RequestPayload, SessionEpoch,
    SubReply,
};

use crate::master_key::{MasterKey, VerifyBls};
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

/// The shared length-prefix framing discipline: a 4-byte big-endian length
/// header followed by exactly that many bytes, capped at `maximum_frame_bytes`.
/// Data-bearing (it carries the cap), so the cap travels with the read; reused
/// by [`CriomePeerCodec`] for both the envelope blob and the raw frame blob of a
/// peer frame. The cap is enforced on read so a malicious peer cannot ask the
/// reader to allocate an unbounded buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LengthPrefixed {
    maximum_frame_bytes: usize,
}

impl LengthPrefixed {
    const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    /// Write `payload` as a 4-byte big-endian length header followed by the
    /// payload bytes. The payload is rejected if it exceeds the cap.
    fn write_blob(&self, writer: &mut impl Write, payload: &[u8]) -> Result<()> {
        if payload.len() > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!(
                    "peer blob length {} exceeds {}",
                    payload.len(),
                    self.maximum_frame_bytes
                ),
            });
        }
        writer.write_all(&(payload.len() as u32).to_be_bytes())?;
        writer.write_all(payload)?;
        Ok(())
    }

    /// Read one length-prefixed blob, returning the raw payload bytes WITHOUT
    /// the length header — the caller decodes it.
    fn read_blob(&self, reader: &mut impl Read) -> Result<Vec<u8>> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("peer blob length {length} exceeds {}", self.maximum_frame_bytes),
            });
        }
        let mut payload = vec![0_u8; length];
        reader.read_exact(&mut payload)?;
        Ok(payload)
    }

    /// Read one length-prefixed blob and return the EXACT bytes INCLUDING the
    /// 4-byte header. This is the preimage a peer-frame signature covers: the
    /// sender signs `CriomeFrame::encode_length_prefixed()` (header included),
    /// so the verifier must reconstruct the same header+body to verify, then
    /// decode the same bytes.
    fn read_blob_with_header(&self, reader: &mut impl Read) -> Result<Vec<u8>> {
        let mut prefix = [0_u8; 4];
        reader.read_exact(&mut prefix)?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > self.maximum_frame_bytes {
            return Err(Error::UnexpectedSignalFrame {
                got: format!("peer blob length {length} exceeds {}", self.maximum_frame_bytes),
            });
        }
        let mut bytes = Vec::with_capacity(4 + length);
        bytes.extend_from_slice(&prefix);
        bytes.resize(4 + length, 0);
        reader.read_exact(&mut bytes[4..])?;
        Ok(bytes)
    }
}

/// The peer transport codec: a frame on the peer wire is two length-prefixed
/// blobs — `[length-prefixed PeerEnvelope][length-prefixed CriomeFrame bytes]`.
/// The envelope is a thin authenticated header (sender master public key plus a
/// BLS signature); it does not carry the frame. The sender signs the EXACT
/// length-prefixed frame bytes (the second blob) under the peer-frame domain
/// tag, so the receiver authenticates the frame BEFORE decoding it. Provides
/// authenticity, not confidentiality — the tailnet supplies confidentiality.
///
/// Data-bearing: carries the same 1 MiB frame cap discipline as
/// [`CriomeFrameCodec`], applied independently to each blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CriomePeerCodec {
    maximum_frame_bytes: usize,
}

impl Default for CriomePeerCodec {
    fn default() -> Self {
        Self::new(1024 * 1024)
    }
}

impl CriomePeerCodec {
    pub const fn new(maximum_frame_bytes: usize) -> Self {
        Self {
            maximum_frame_bytes,
        }
    }

    fn framing(&self) -> LengthPrefixed {
        LengthPrefixed::new(self.maximum_frame_bytes)
    }

    /// Wrap a `CriomeRequest` into a `CriomeFrame` and write it as an enveloped
    /// peer frame signed by `sender`. Convenience over [`Self::write_enveloped`]
    /// for the request direction.
    pub fn write_request(
        &self,
        writer: &mut impl Write,
        request: CriomeRequest,
        sender: &MasterKey,
    ) -> Result<()> {
        let frame = CriomeFrame::new(FrameBody::Request {
            exchange: synthetic_exchange(),
            request: request.into_request(),
        });
        self.write_enveloped(writer, frame, sender)
    }

    /// Read and authenticate an enveloped peer frame, then extract the request
    /// payload. Returns the verified sender public key alongside the request.
    pub fn read_request(
        &self,
        reader: &mut impl Read,
        admitted: &[BlsPublicKey],
    ) -> Result<(BlsPublicKey, CriomeRequest)> {
        let (sender, frame) = self.read_enveloped(reader, admitted)?;
        match frame.into_body() {
            FrameBody::Request { request, .. } => Ok((sender, request.payloads.into_head())),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }

    /// Sign `frame`'s exact length-prefixed bytes with `sender` under the
    /// peer-frame domain tag, then write
    /// `[length-prefixed PeerEnvelope][length-prefixed frame bytes]`.
    pub fn write_enveloped(
        &self,
        writer: &mut impl Write,
        frame: CriomeFrame,
        sender: &MasterKey,
    ) -> Result<()> {
        let frame_bytes = frame.encode_length_prefixed()?;
        let signature = sender.sign_peer_frame(&frame_bytes);
        let envelope = PeerEnvelope::new(sender.public_key(), signature);
        let envelope_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&envelope)
            .map_err(|_| Error::ConfigurationArchiveEncode)?
            .to_vec();
        let framing = self.framing();
        framing.write_blob(writer, &envelope_bytes)?;
        // The frame blob is already length-prefixed (encode_length_prefixed),
        // so write it verbatim — that is the exact preimage the signature
        // covers and the reader reconstructs.
        writer.write_all(&frame_bytes)?;
        writer.flush()?;
        Ok(())
    }

    /// Read an enveloped peer frame: read the envelope, read the raw frame bytes
    /// WITHOUT decoding, reject an unadmitted sender ([`Error::UnknownPeer`]),
    /// verify the BLS signature over the exact frame bytes under the peer-frame
    /// domain tag ([`Error::PeerSignatureRejected`] on failure), and ONLY THEN
    /// decode the frame. Returns the verified sender public key and the frame.
    pub fn read_enveloped(
        &self,
        reader: &mut impl Read,
        admitted: &[BlsPublicKey],
    ) -> Result<(BlsPublicKey, CriomeFrame)> {
        let framing = self.framing();
        let envelope_bytes = framing.read_blob(reader)?;
        let envelope = rkyv::from_bytes::<PeerEnvelope, rkyv::rancor::Error>(&envelope_bytes)
            .map_err(|_| Error::ConfigurationArchiveDecode)?;
        // The frame blob carries its own 4-byte length header; read it verbatim
        // so the verified preimage is byte-identical to what the sender signed.
        let frame_bytes = framing.read_blob_with_header(reader)?;
        let sender = envelope.sender_public_key;
        if !admitted.iter().any(|peer| peer == &sender) {
            return Err(Error::UnknownPeer(sender));
        }
        if !sender.verify_peer_frame(&envelope.signature, &frame_bytes) {
            return Err(Error::PeerSignatureRejected(sender));
        }
        let frame = CriomeFrame::decode_length_prefixed(&frame_bytes)?;
        Ok((sender, frame))
    }
}

/// A synchronous client to a peer criome daemon over TCP. Holds the peer
/// `host:port` address and the peer codec; signs each request frame with the
/// local master key and authenticates the enveloped reply against the peer's
/// admitted public key. Mirrors [`CriomeClient`]'s std/blocking shape — the
/// peer transport primitive is proven before the daemon serve-loop integration
/// (increment 4) wires it in.
pub struct CriomePeerClient {
    address: String,
    codec: CriomePeerCodec,
}

impl CriomePeerClient {
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            codec: CriomePeerCodec::default(),
        }
    }

    /// Connect to the peer over TCP, send `request` enveloped and signed by
    /// `local` (the local master key), and read + authenticate the enveloped
    /// reply, admitting only `peer_public_key` as the reply signer. Returns the
    /// reply payload.
    pub fn send(
        &self,
        request: CriomeRequest,
        local: &MasterKey,
        peer_public_key: &BlsPublicKey,
    ) -> Result<CriomeReply> {
        let stream = TcpStream::connect(&self.address).map_err(|source| Error::PeerConnect {
            address: self.address.clone(),
            source,
        })?;
        let mut stream = BufReader::new(stream);
        self.codec.write_request(stream.get_mut(), request, local)?;
        let admitted = [peer_public_key.clone()];
        let (_sender, frame) = self.codec.read_enveloped(&mut stream, &admitted)?;
        match frame.into_body() {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;
    use std::net::TcpListener;
    use std::thread;

    use signal_criome::{
        ContractDigest, ContractMissing, Output, ParkedAuthorizationObservation,
    };

    /// Build the simplest constructible request frame: an empty parked-authorization
    /// observation. The whole point is a real `CriomeFrame` whose bytes round-trip.
    fn sample_request() -> CriomeRequest {
        CriomeRequest::ObserveParkedAuthorizations(ParkedAuthorizationObservation::new())
    }

    fn sample_reply() -> CriomeReply {
        Output::ContractMissing(ContractMissing::new(ContractDigest::from_bytes(
            b"peer-transport-test-digest",
        )))
    }

    #[test]
    fn enveloped_request_round_trips_over_real_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address").to_string();

        let sender = MasterKey::generate().expect("generate sender key");
        let admitted = sender.public_key();
        let expected = sample_request();

        // Server: accept one connection, read+authenticate the enveloped request.
        let admitted_for_server = admitted.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept peer connection");
            let mut reader = BufReader::new(stream);
            let codec = CriomePeerCodec::default();
            codec
                .read_request(&mut reader, std::slice::from_ref(&admitted_for_server))
                .expect("read enveloped request")
        });

        // Client: connect over real TCP and write the enveloped, signed request.
        let mut client = TcpStream::connect(&address).expect("connect to peer");
        let codec = CriomePeerCodec::default();
        codec
            .write_request(&mut client, expected.clone(), &sender)
            .expect("write enveloped request");
        client.flush().expect("flush request");

        let (verified_sender, received) = server.join().expect("server thread");
        assert_eq!(
            verified_sender.as_str(),
            admitted.as_str(),
            "verified sender is the admitted peer"
        );
        assert_eq!(received, expected, "request decodes to the same value");
    }

    #[test]
    fn enveloped_reply_round_trips_through_peer_client() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address").to_string();

        // The peer (server) signs with its own master key; the client admits only
        // the peer's public key for the reply.
        let peer = MasterKey::generate().expect("generate peer key");
        let local = MasterKey::generate().expect("generate local key");
        let peer_public_key = peer.public_key();
        let local_public_key = local.public_key();
        let expected_reply = sample_reply();

        let reply_for_server = expected_reply.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept peer connection");
            let mut reader = BufReader::new(stream);
            let codec = CriomePeerCodec::default();
            // Authenticate the client's request (admit the local key), then reply.
            let (sender, _request) = codec
                .read_request(&mut reader, std::slice::from_ref(&local_public_key))
                .expect("read enveloped request");
            assert_eq!(sender.as_str(), local_public_key.as_str());
            let reply_frame = CriomeFrame::new(FrameBody::Reply {
                exchange: synthetic_exchange(),
                reply: Reply::committed(NonEmpty::single(SubReply::Ok(reply_for_server))),
            });
            codec
                .write_enveloped(reader.get_mut(), reply_frame, &peer)
                .expect("write enveloped reply");
        });

        let client = CriomePeerClient::new(address);
        let reply = client
            .send(sample_request(), &local, &peer_public_key)
            .expect("client send round-trips");
        server.join().expect("server thread");
        assert_eq!(reply, expected_reply, "reply decodes to the same value");
    }

    #[test]
    fn non_admitted_sender_is_rejected_as_unknown_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind loopback listener");
        let address = listener.local_addr().expect("listener address").to_string();

        let sender = MasterKey::generate().expect("generate sender key");
        // A DIFFERENT key is the only admitted peer; the sender is not admitted.
        let other = MasterKey::generate().expect("generate other key");
        let admitted = other.public_key();

        let admitted_for_server = admitted.clone();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().expect("accept peer connection");
            let mut reader = BufReader::new(stream);
            let codec = CriomePeerCodec::default();
            codec
                .read_request(&mut reader, std::slice::from_ref(&admitted_for_server))
                .expect_err("unadmitted sender must be rejected")
        });

        let mut client = TcpStream::connect(&address).expect("connect to peer");
        let codec = CriomePeerCodec::default();
        codec
            .write_request(&mut client, sample_request(), &sender)
            .expect("write enveloped request");
        client.flush().expect("flush request");

        let error = server.join().expect("server thread");
        match error {
            Error::UnknownPeer(public_key) => {
                assert_eq!(public_key.as_str(), sender.public_key().as_str());
            }
            other => panic!("expected UnknownPeer, got {other:?}"),
        }
    }

    #[test]
    fn tampered_frame_bytes_are_rejected_as_peer_signature_rejected() {
        // Build a valid enveloped frame into a buffer, flip a byte of the frame
        // body (after the envelope blob), then verify the reader rejects it.
        let sender = MasterKey::generate().expect("generate sender key");
        let admitted = sender.public_key();
        let codec = CriomePeerCodec::default();

        let mut wire = Vec::new();
        codec
            .write_request(&mut wire, sample_request(), &sender)
            .expect("write enveloped request");

        // The first 4 bytes are the envelope blob length; skip the envelope, then
        // skip the frame's own 4-byte length header, and flip a body byte so the
        // signed preimage no longer matches.
        let envelope_length =
            u32::from_be_bytes(wire[0..4].try_into().expect("envelope length")) as usize;
        let frame_body_start = 4 + envelope_length + 4;
        wire[frame_body_start] ^= 0xff;

        let mut reader = std::io::Cursor::new(wire);
        let error = codec
            .read_enveloped(&mut reader, std::slice::from_ref(&admitted))
            .expect_err("tampered frame must be rejected");
        match error {
            Error::PeerSignatureRejected(public_key) => {
                assert_eq!(public_key.as_str(), admitted.as_str());
            }
            other => panic!("expected PeerSignatureRejected, got {other:?}"),
        }
    }
}
