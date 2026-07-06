//! The peer conveyance — how a criome conveys vote solicitations and votes to its
//! peer members' criome daemons during a quorum-collection round.
//!
//! Conveyance is fire-and-forget and best-effort: `convey` never blocks the
//! caller and never surfaces a delivery error. A round that cannot reach a peer
//! simply stays `Gathering` — the "unreachable peer ⇒ waits" behavior. A durable
//! outbound backlog with push redial is a later milestone (primary-nbmq.5); this
//! module carries only the two shipping shapes the round needs today.
//!
//!   - [`RouterSubmission`] rides the router's opaque routed-object carriage:
//!     it wraps the peer criome request as a `signal-router` `SubmitRoutedObjects`
//!     origination whose payload octets are the criome working-socket frame the
//!     peer decodes unchanged. This is the cross-node (network) transport.
//!   - [`DirectDialConveyance`] dials the peer criome's working socket directly.
//!     This is the single-host, multi-user deployment mode (peers under different
//!     Unix users on one host), and the shape the round is witnessed under.
//!   - [`NoConveyance`] is the default: proposals still self-vote, but no
//!     solicitation leaves the node.

use std::path::PathBuf;

use signal_criome::{CriomeRequest, Identity};
use signal_router::{
    ActorIdentifier, ContractName, ContractOperation, ContractPayloadSize, ForwardedMessagePayload,
    Input as RouterInput, Output as RouterOutput, RoutedContractObject,
};

use crate::router_client::RouterClient;
use crate::transport::{CriomeClient, CriomeFrameCodec};
use crate::{Error, Result};

/// The contract-name label stamped on the routed object. The router relays the
/// octets payload-blind; the name is an attestation/audit label naming the
/// contract the octets belong to.
const CRIOME_CONTRACT_NAME: &str = "signal-criome";

/// Which concrete conveyance is selected — a closed identification of the
/// selected conveyance path, not a flag: exactly one variant names the
/// transport instead of a boolean per implementation.
/// `CriomeDaemon::from_configuration` selection is asserted against this, and
/// it doubles as the transport label the daemon can log at startup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerConveyanceKind {
    /// No conveyance: proposals self-vote, but no solicitation leaves the node.
    None,
    /// Direct peer-dial: the single-host, multi-user deployment mode.
    DirectDial,
    /// Router-mediated: the cross-node (network) transport.
    Router,
}

/// A conveyance from a local criome to a peer member's criome. Implementations
/// deliver `request` to `recipient` asynchronously and best-effort; `convey`
/// never blocks and never surfaces a delivery error, so an unreachable peer
/// leaves the round pending rather than failing it.
pub trait PeerConveyance: Send + Sync {
    fn convey(&self, recipient: &Identity, request: CriomeRequest);

    /// Convey a sequence of requests to one peer IN ORDER — each delivered (and
    /// its reply drained) before the next is sent. The two-round commit driver
    /// uses this to deliver the round-1 evidence (the gathered votes) and THEN the
    /// commit solicitation that re-judges it, so the ordering-dependent exchange is
    /// race-free over the otherwise best-effort conveyance.
    fn convey_ordered(&self, recipient: &Identity, requests: Vec<CriomeRequest>);

    /// Which concrete conveyance this is. See [`PeerConveyanceKind`].
    fn kind(&self) -> PeerConveyanceKind;
}

/// The conveyance of a criome with no configured peers: it conveys nothing.
/// An M-of-1 contract authorizes on the self-vote alone; an M-of-N (N>1)
/// round stays `Gathering`.
pub struct NoConveyance;

impl PeerConveyance for NoConveyance {
    fn convey(&self, _recipient: &Identity, _request: CriomeRequest) {}

    fn convey_ordered(&self, _recipient: &Identity, _requests: Vec<CriomeRequest>) {}

    fn kind(&self) -> PeerConveyanceKind {
        PeerConveyanceKind::None
    }
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
pub struct DirectDialConveyance {
    routes: Vec<PeerSocketRoute>,
}

impl DirectDialConveyance {
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

impl PeerConveyance for DirectDialConveyance {
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

    fn kind(&self) -> PeerConveyanceKind {
        PeerConveyanceKind::DirectDial
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

    /// Build a route-table entry from its `CriomeDaemonConfiguration` wire
    /// twin (`signal_criome::PeerActorRoute`), converting the router
    /// destination-actor identifier into `signal_router`'s own type.
    pub fn from_configuration(route: &signal_criome::PeerActorRoute) -> Self {
        Self::new(
            route.peer().clone(),
            ActorIdentifier::new(route.destination().as_str()),
        )
    }
}

/// Router-mediated conveyance: the cross-node transport. Each solicitation/vote
/// is wrapped as one `RoutedContractObject` and handed to the local router as a
/// `SubmitRoutedObjects` origination; the router carries it opaquely to the peer
/// node and delivers the octets to the peer criome's working socket unchanged.
#[derive(Clone)]
pub struct RouterSubmission {
    router_socket: PathBuf,
    source_actor: ActorIdentifier,
    routes: Vec<PeerActorRoute>,
}

impl RouterSubmission {
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

    /// Build the router-mediated conveyance from its `CriomeDaemonConfiguration`
    /// wire twin (`signal_criome::RouterSubmissionConfiguration`): the local
    /// router socket to originate over, the source actor this daemon
    /// originates as, and the peer route table. This is what
    /// `CriomeDaemon::from_configuration` arms when `router_submission` is
    /// configured, in place of the `NoConveyance` default.
    pub fn from_configuration(
        configuration: &signal_criome::RouterSubmissionConfiguration,
    ) -> Self {
        Self::new(
            configuration.router_socket_path().as_str(),
            ActorIdentifier::new(configuration.source_actor().as_str()),
            configuration
                .peer_routes()
                .iter()
                .map(PeerActorRoute::from_configuration)
                .collect(),
        )
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
            return Err(Error::PeerDelivery(
                "criome frame shorter than its length prefix".to_string(),
            ));
        }
        Ok(framed.split_off(4))
    }

    /// Hand `request` to the local router as a `SubmitRoutedObjects`
    /// origination addressed to `destination`, and map the router's reply
    /// back to a conveyance delivery result. `pub` (rather than the trait's
    /// fire-and-forget `convey`/`convey_ordered`) so the origination round
    /// trip — the routed octets decoding back to the same `CriomeRequest`,
    /// and the accept/refuse mapping — is directly testable.
    pub fn submit(&self, destination: ActorIdentifier, request: CriomeRequest) -> Result<()> {
        let operation = format!("{:?}", request.route());
        let octets = Self::request_octets(request)?;
        let payload_size = u64::try_from(octets.len()).map_err(|_| {
            Error::PeerDelivery(format!(
                "{CRIOME_CONTRACT_NAME} conveyance payload exceeds the routed-object size type"
            ))
        })?;
        let object = RoutedContractObject::new(
            ContractName::new(CRIOME_CONTRACT_NAME),
            ContractOperation::new(operation),
            ContractPayloadSize::new(payload_size),
            octets.into_iter().map(u64::from).collect(),
        );
        let payload = ForwardedMessagePayload::new(
            self.source_actor.clone(),
            destination,
            String::new(),
            Vec::new(),
            vec![object],
        );
        let client = RouterClient::new(self.router_socket.clone());
        match client.send(RouterInput::submit_routed_objects(payload))? {
            RouterOutput::RoutedObjectsAccepted(_) => Ok(()),
            RouterOutput::RoutedObjectsRefused(refusal) => Err(Error::PeerDelivery(format!(
                "{CRIOME_CONTRACT_NAME} router conveyance refused: {:?}",
                refusal.into_payload().into_payload()
            ))),
            other => Err(Error::UnexpectedSignalFrame {
                got: format!("{other:?}"),
            }),
        }
    }
}

impl PeerConveyance for RouterSubmission {
    fn convey(&self, recipient: &Identity, request: CriomeRequest) {
        let Some(destination) = self.destination_for(recipient) else {
            return;
        };
        // Fire-and-forget by trait contract, delivered on its OWN thread like
        // [`DirectDialConveyance`]. This is load-bearing, not incidental: a
        // synchronous `submit` here blocks the `CriomeRoot` handler that called
        // `convey`, and the router round-trip re-enters that same root during a
        // multi-node founding's accept/distribute steps (initiator distributes
        // the finished root back to the peer whose signature it is processing) —
        // a deadlock the direct-dial conveyance never had (primary-79z1.23). Spawning
        // keeps `convey` non-blocking; the M2 loud-log of a refusal
        // (primary-79z1.22) still fires, now from the spawned thread.
        let conveyance = self.clone();
        let recipient = recipient.clone();
        std::thread::spawn(move || {
            if let Err(error) = conveyance.submit(destination, request) {
                eprintln!(
                    "criome router submission to {recipient:?} was refused and NOT delivered: {error}"
                );
            }
        });
    }

    fn convey_ordered(&self, recipient: &Identity, requests: Vec<CriomeRequest>) {
        let Some(destination) = self.destination_for(recipient) else {
            return;
        };
        // One thread carries the whole ordering-dependent sequence IN ORDER (the
        // two-round commit's round-1 evidence then the commit solicitation), so
        // `convey_ordered` stays non-blocking while the first refusal still stops
        // the rest of the sequence (primary-79z1.22) — the same re-entrancy-safe
        // shape as `convey` above (primary-79z1.23).
        let conveyance = self.clone();
        let recipient = recipient.clone();
        std::thread::spawn(move || {
            let total = requests.len();
            for (index, request) in requests.into_iter().enumerate() {
                if let Err(error) = conveyance.submit(destination.clone(), request) {
                    eprintln!(
                        "criome ordered router submission to {recipient:?} was refused and NOT delivered at step {}/{total}: {error}; the remaining ordering-dependent steps in this sequence are not sent",
                        index + 1,
                    );
                    break;
                }
            }
        });
    }

    fn kind(&self) -> PeerConveyanceKind {
        PeerConveyanceKind::Router
    }
}
