//! `criome-founding` — the owner-only operator CLI for the cross-node root
//! founding ceremony.
//!
//! Founding assembles a UNANIMOUS multi-node root: the owner reads each node's
//! Criome master public key out-of-band, builds a `RootGenesis` cohort, initiates
//! the founding on one node, then explicitly accepts on every node. This CLI is
//! the thin operator surface over the daemon's meta socket (and the public
//! working socket for the read-op); it holds no state and makes no policy — every
//! signature is minted only by an explicit `accept-founding` (no auto-approval).
//!
//! Commands:
//!
//! ```text
//! observe-node-public-key     read this node's master public key (working socket)
//! initiate-founding <cohort>  initiate a founding over the given RootGenesis cohort
//! observe-founding            show this node's founding state + pending-founding queue
//! accept-founding <anchor>    explicitly accept (sign) the pending founding for the anchor
//! ```
//!
//! `<cohort>` is inline NOTA when it starts with `(`, otherwise a path to a NOTA
//! file. Sockets: `CRIOME_SOCKET` (working, default `/tmp/criome.sock`) and
//! `CRIOME_META_SOCKET` (owner-only, default `<working>.meta`).

use std::path::{Path, PathBuf};

use criome::transport::{CriomeClient, CriomeMetaClient};
use meta_signal_criome::{
    Input as MetaInput, Output as MetaOutput, RootFoundingAcceptance, RootFoundingInitiation,
    RootFoundingObservation, RootFoundingStatus,
};
use nota_next::{NotaEncode, NotaSource};
use signal_criome::{
    CriomeReply, CriomeRequest, NodePublicKeyObservation, ObjectDigest, RootAnchorDigest,
    RootGenesis,
};

#[derive(Debug, thiserror::Error)]
enum OperatorError {
    #[error(
        "usage: criome-founding <command>\n  \
         observe-node-public-key\n  \
         initiate-founding <cohort.nota|inline-nota>\n  \
         observe-founding\n  \
         accept-founding <anchor>"
    )]
    Usage,
    #[error(transparent)]
    Transport(#[from] criome::Error),
    #[error("read cohort {path}: {source}")]
    CohortRead {
        path: String,
        source: std::io::Error,
    },
    #[error("parse cohort NOTA: {0}")]
    CohortParse(nota_next::NotaDecodeError),
    #[error("unexpected reply: {0}")]
    UnexpectedReply(String),
    #[error("no pending founding for anchor {0}")]
    NoPendingFounding(String),
}

/// The founding operator's resolved socket endpoints. The public read-op dials
/// the working socket; every owner action dials the meta socket.
struct FoundingOperator {
    working_socket: PathBuf,
    meta_socket: PathBuf,
}

impl FoundingOperator {
    fn from_environment() -> Self {
        let working_socket = std::env::var_os("CRIOME_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/criome.sock"));
        let meta_socket = std::env::var_os("CRIOME_META_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| Self::default_meta_socket(&working_socket));
        Self {
            working_socket,
            meta_socket,
        }
    }

    /// The daemon's default meta-socket path — the working socket file name with a
    /// `.meta` suffix, matching `CriomeDaemon::default_meta_socket_path`.
    fn default_meta_socket(working: &Path) -> PathBuf {
        match working.file_name().and_then(|name| name.to_str()) {
            Some(name) => working.with_file_name(format!("{name}.meta")),
            None => working.with_extension("meta"),
        }
    }

    fn run(&self, mut arguments: impl Iterator<Item = String>) -> Result<(), OperatorError> {
        let command = arguments.next().ok_or(OperatorError::Usage)?;
        match command.as_str() {
            "observe-node-public-key" => self.observe_node_public_key(),
            "initiate-founding" => {
                let cohort = arguments.next().ok_or(OperatorError::Usage)?;
                self.initiate_founding(&cohort)
            }
            "observe-founding" => self.observe_founding(),
            "accept-founding" => {
                let anchor = arguments.next().ok_or(OperatorError::Usage)?;
                self.accept_founding(&anchor)
            }
            _ => Err(OperatorError::Usage),
        }
    }

    /// Read this node's Criome master public key on the public working socket, so
    /// the owner can enroll it into a cohort out-of-band.
    fn observe_node_public_key(&self) -> Result<(), OperatorError> {
        let reply = self
            .working_client()
            .send(CriomeRequest::observe_node_public_key(
                NodePublicKeyObservation::new(),
            ))?;
        match reply {
            CriomeReply::NodePublicKey(key) => {
                println!("{}", key.to_nota());
                Ok(())
            }
            other => Err(OperatorError::UnexpectedReply(format!("{other:?}"))),
        }
    }

    /// Initiate a founding on this node over the given cohort: the daemon records
    /// the gathering and conveys a proposal to each peer. Prints the resulting
    /// founding status.
    fn initiate_founding(&self, cohort_argument: &str) -> Result<(), OperatorError> {
        let cohort = Self::read_root_genesis(cohort_argument)?;
        let reply = self.meta_client().send(MetaInput::InitiateRootFounding(
            RootFoundingInitiation::new(cohort),
        ))?;
        Self::print_meta(&reply)
    }

    /// Show this node's founding state and its pending-founding queue.
    fn observe_founding(&self) -> Result<(), OperatorError> {
        let status = self.observe_status()?;
        Self::print_meta(&MetaOutput::RootFoundingStatus(status))
    }

    /// Explicitly accept (sign) the pending founding for `anchor`. The cohort is
    /// resolved from the daemon's pending-founding queue so the owner accepts by
    /// anchor alone; the daemon returns this node's signature to the initiator.
    fn accept_founding(&self, anchor_argument: &str) -> Result<(), OperatorError> {
        let anchor = RootAnchorDigest::new(ObjectDigest::new(anchor_argument.to_string()));
        let cohort = self
            .observe_status()?
            .pending
            .into_iter()
            .find(|pending| pending.anchor == anchor)
            .map(|pending| pending.cohort)
            .ok_or_else(|| OperatorError::NoPendingFounding(anchor_argument.to_string()))?;
        let reply =
            self.meta_client()
                .send(MetaInput::AcceptRootFounding(RootFoundingAcceptance::new(
                    anchor, cohort,
                )))?;
        Self::print_meta(&reply)
    }

    fn observe_status(&self) -> Result<RootFoundingStatus, OperatorError> {
        let reply = self.meta_client().send(MetaInput::ObserveRootFounding(
            RootFoundingObservation::new(),
        ))?;
        match reply {
            MetaOutput::RootFoundingStatus(status) => Ok(status),
            other => Err(OperatorError::UnexpectedReply(format!("{other:?}"))),
        }
    }

    fn read_root_genesis(argument: &str) -> Result<RootGenesis, OperatorError> {
        let text = if argument.trim_start().starts_with('(') {
            argument.to_string()
        } else {
            std::fs::read_to_string(argument).map_err(|source| OperatorError::CohortRead {
                path: argument.to_string(),
                source,
            })?
        };
        NotaSource::new(&text)
            .parse::<RootGenesis>()
            .map_err(OperatorError::CohortParse)
    }

    fn print_meta(reply: &MetaOutput) -> Result<(), OperatorError> {
        println!("{}", reply.to_nota());
        Ok(())
    }

    fn working_client(&self) -> CriomeClient {
        CriomeClient::new(self.working_socket.clone())
    }

    fn meta_client(&self) -> CriomeMetaClient {
        CriomeMetaClient::new(self.meta_socket.clone())
    }
}

fn main() -> Result<(), OperatorError> {
    FoundingOperator::from_environment().run(std::env::args().skip(1))
}
