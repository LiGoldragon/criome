//! The quorum voice — how a criome conveys vote solicitations and votes to its
//! peer members' criome daemons during a quorum-collection round.
//!
//! Conveyance is fire-and-forget and best-effort: `convey` never blocks the
//! caller and never surfaces a delivery error. A round that cannot reach a peer
//! simply stays `Gathering` — the "unreachable peer ⇒ waits" behavior. A durable
//! outbound backlog with push redial is a later milestone (primary-nbmq.5); this
//! module carries only the two shipping shapes the round needs today.
//!
//!   - [`RouterQuorumVoice`] rides the router's opaque routed-object carriage:
//!     it wraps the peer criome request as a `signal-router` `SubmitRoutedObjects`
//!     origination whose payload octets are the criome working-socket frame the
//!     peer decodes unchanged. This is the cross-node (network) transport.
//!   - [`DirectDialQuorumVoice`] dials the peer criome's working socket directly.
//!     This is the single-host, multi-user deployment mode (peers under different
//!     Unix users on one host), and the shape the round is witnessed under.
//!   - [`SilentVoice`] is the unarmed default: proposals still self-vote, but no
//!     solicitation leaves the node.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use signal_criome::{CriomeRequest, Identity};
use signal_frame::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
};
use signal_router::{
    ActorIdentifier, ContractName, ContractOperation, ContractPayloadSize, ForwardedMessagePayload,
    Frame as RouterFrame, FrameBody as RouterFrameBody, Input as RouterInput,
    Integer as RouterInteger, Output as RouterOutput, RoutedContractObject,
};
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, LengthPrefixedCodec};

use crate::transport::{CriomeClient, CriomeFrameCodec};
use crate::{Error, Result};

/// The contract-name label stamped on the routed object. The router relays the
/// octets payload-blind; the name is an attestation/audit label naming the
/// contract the octets belong to.
const CRIOME_CONTRACT_NAME: &str = "signal-criome";

/// A conveyance from a local criome to a peer member's criome. Implementations
/// deliver `request` to `recipient` asynchronously and best-effort; `convey`
/// never blocks and never surfaces a delivery error, so an unreachable peer
/// leaves the round pending rather than failing it.
pub trait QuorumVoice: Send + Sync {
    fn convey(&self, recipient: &Identity, request: CriomeRequest);
}

/// The unarmed voice: a criome with no configured peers. An M-of-1 contract
/// authorizes on the self-vote alone; an M-of-N (N>1) round stays `Gathering`.
pub struct SilentVoice;

impl QuorumVoice for SilentVoice {
    fn convey(&self, _recipient: &Identity, _request: CriomeRequest) {}
}

/// One peer member mapped to the criome working socket that reaches it.
#[derive(Clone, Debug)]
pub struct PeerSocketRoute {
    peer: Identity,
    socket: PathBuf,
}

impl PeerSocketRoute {
    pub fn new(peer: Identity, socket: impl Into<PathBuf>) -> Self {
        Self {
            peer,
            socket: socket.into(),
        }
    }
}

/// Direct peer-dial conveyance: the single-host, multi-user quorum mode. Each
/// solicitation/vote is a plain criome request dialed straight at the peer
/// criome's working socket.
pub struct DirectDialQuorumVoice {
    routes: Vec<PeerSocketRoute>,
}

impl DirectDialQuorumVoice {
    pub fn new(routes: Vec<PeerSocketRoute>) -> Self {
        Self { routes }
    }

    fn socket_for(&self, recipient: &Identity) -> Option<PathBuf> {
        self.routes
            .iter()
            .find(|route| &route.peer == recipient)
            .map(|route| route.socket.clone())
    }
}

impl QuorumVoice for DirectDialQuorumVoice {
    fn convey(&self, recipient: &Identity, request: CriomeRequest) {
        let Some(socket) = self.socket_for(recipient) else {
            return;
        };
        std::thread::spawn(move || {
            let _ = CriomeClient::new(socket).send(request);
        });
    }
}

/// One peer member mapped to the router destination-actor name the local router
/// resolves a remote route for.
#[derive(Clone, Debug)]
pub struct PeerActorRoute {
    peer: Identity,
    destination: ActorIdentifier,
}

impl PeerActorRoute {
    pub fn new(peer: Identity, destination: ActorIdentifier) -> Self {
        Self { peer, destination }
    }
}

/// Router-mediated conveyance: the cross-node transport. Each solicitation/vote
/// is wrapped as one `RoutedContractObject` and handed to the local router as a
/// `SubmitRoutedObjects` origination; the router carries it opaquely to the peer
/// node and delivers the octets to the peer criome's working socket unchanged.
pub struct RouterQuorumVoice {
    router_socket: PathBuf,
    source_actor: ActorIdentifier,
    routes: Vec<PeerActorRoute>,
}

impl RouterQuorumVoice {
    pub fn new(
        router_socket: impl Into<PathBuf>,
        source_actor: ActorIdentifier,
        routes: Vec<PeerActorRoute>,
    ) -> Self {
        Self {
            router_socket: router_socket.into(),
            source_actor,
            routes,
        }
    }

    fn destination_for(&self, recipient: &Identity) -> Option<ActorIdentifier> {
        self.routes
            .iter()
            .find(|route| &route.peer == recipient)
            .map(|route| route.destination.clone())
    }

    /// The criome working-socket frame octets the router carries verbatim. The
    /// router re-prefixes with its own length, and the peer criome socket reads
    /// `[len][body]`, so the routed object carries the frame *body* — the codec's
    /// length-prefixed encoding with its own 4-byte length prefix removed.
    pub fn request_octets(request: CriomeRequest) -> Result<Vec<u8>> {
        let mut framed = Vec::new();
        CriomeFrameCodec::default().write_request(&mut framed, request)?;
        if framed.len() < 4 {
            return Err(Error::VoiceDelivery(
                "criome frame shorter than its length prefix".to_string(),
            ));
        }
        Ok(framed.split_off(4))
    }

    fn payload(
        &self,
        destination: ActorIdentifier,
        request: CriomeRequest,
    ) -> Result<ForwardedMessagePayload> {
        let operation_label = format!("{:?}", request.route());
        let octets = Self::request_octets(request)?;
        let routed_object = RoutedContractObject::new(
            ContractName::new(CRIOME_CONTRACT_NAME),
            ContractOperation::new(operation_label.clone()),
            ContractPayloadSize::new(octets.len() as RouterInteger),
            octets.into_iter().map(RouterInteger::from).collect(),
        );
        Ok(ForwardedMessagePayload::new(
            self.source_actor.clone(),
            destination,
            operation_label,
            Vec::new(),
            vec![routed_object],
        ))
    }

    fn submit(router_socket: &std::path::Path, payload: ForwardedMessagePayload) -> Result<()> {
        let mut stream = UnixStream::connect(router_socket)?;
        let exchange = ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        );
        let request_octets = RouterInput::submit_routed_objects(payload)
            .into_frame(exchange)
            .encode()
            .map_err(|source| Error::VoiceDelivery(source.to_string()))?;
        let codec = LengthPrefixedCodec::default();
        codec
            .write_body(&mut stream, &LengthPrefixedFrameBody::new(request_octets))
            .map_err(|source| Error::VoiceDelivery(source.to_string()))?;
        stream.flush()?;
        let reply_body = codec
            .read_body(&mut stream)
            .map_err(|source| Error::VoiceDelivery(source.to_string()))?;
        let reply_frame = RouterFrame::decode(reply_body.bytes())
            .map_err(|source| Error::VoiceDelivery(source.to_string()))?;
        Self::accepted(reply_frame)
    }

    fn accepted(frame: RouterFrame) -> Result<()> {
        match frame.into_body() {
            RouterFrameBody::Reply { reply, .. } => match reply {
                Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
                    SubReply::Ok(RouterOutput::RoutedObjectsAccepted(_)) => Ok(()),
                    other => Err(Error::VoiceDelivery(format!("unexpected reply: {other:?}"))),
                },
                Reply::Rejected { reason } => {
                    Err(Error::VoiceDelivery(format!("router refused: {reason}")))
                }
            },
            other => Err(Error::VoiceDelivery(format!("unexpected frame: {other:?}"))),
        }
    }
}

impl QuorumVoice for RouterQuorumVoice {
    fn convey(&self, recipient: &Identity, request: CriomeRequest) {
        let Some(destination) = self.destination_for(recipient) else {
            return;
        };
        let Ok(payload) = self.payload(destination, request) else {
            return;
        };
        let router_socket = self.router_socket.clone();
        std::thread::spawn(move || {
            let _ = Self::submit(&router_socket, payload);
        });
    }
}
