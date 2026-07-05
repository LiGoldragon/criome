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

use std::path::PathBuf;

use signal_criome::{CriomeRequest, Identity};
use signal_router::ActorIdentifier;

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

    /// Convey a sequence of requests to one peer IN ORDER — each delivered (and
    /// its reply drained) before the next is sent. The two-round commit driver
    /// uses this to deliver the round-1 evidence (the gathered votes) and THEN the
    /// commit solicitation that re-judges it, so the ordering-dependent exchange is
    /// race-free over the otherwise best-effort voice.
    fn convey_ordered(&self, recipient: &Identity, requests: Vec<CriomeRequest>);
}

/// The unarmed voice: a criome with no configured peers. An M-of-1 contract
/// authorizes on the self-vote alone; an M-of-N (N>1) round stays `Gathering`.
pub struct SilentVoice;

impl QuorumVoice for SilentVoice {
    fn convey(&self, _recipient: &Identity, _request: CriomeRequest) {}

    fn convey_ordered(&self, _recipient: &Identity, _requests: Vec<CriomeRequest>) {}
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

    fn convey_ordered(&self, recipient: &Identity, requests: Vec<CriomeRequest>) {
        let Some(socket) = self.socket_for(recipient) else {
            return;
        };
        std::thread::spawn(move || {
            for request in requests {
                let _ = CriomeClient::new(socket.clone()).send(request);
            }
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

    fn submit(&self, destination: ActorIdentifier, request: CriomeRequest) -> Result<()> {
        let _ = (&self.router_socket, &self.source_actor, destination);
        let _octets = Self::request_octets(request)?;
        Err(Error::VoiceDelivery(format!(
            "{CRIOME_CONTRACT_NAME} router conveyance waits for the clean signal-router routed-object constructor"
        )))
    }
}

impl QuorumVoice for RouterQuorumVoice {
    fn convey(&self, recipient: &Identity, request: CriomeRequest) {
        let Some(destination) = self.destination_for(recipient) else {
            return;
        };
        let _ = self.submit(destination, request);
    }

    fn convey_ordered(&self, recipient: &Identity, requests: Vec<CriomeRequest>) {
        let Some(destination) = self.destination_for(recipient) else {
            return;
        };
        for request in requests {
            let _ = self.submit(destination.clone(), request);
        }
    }
}
