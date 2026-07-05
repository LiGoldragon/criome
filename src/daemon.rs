use std::io::BufReader;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use kameo::actor::ActorRef;
use signal_criome::{
    AuthorizationMode, AuthorizationObservation, AuthorizationRequestSlot, AuthorizationStatus,
    BlsPublicKey, CriomeReply, CriomeRequest, Identity, SignalCallAuthorization,
};
use triad_runtime::SignalFile;

use crate::actors::root::{
    Arguments as RootArguments, AuthorizationObservationOpened, CriomeRoot,
    OpenAuthorizationObservation, SubmitMetaRequest, SubmitRequest,
};
use crate::master_key::SystemClock;
use crate::tables::StoreLocation;
use crate::transport::{CriomeFrameCodec, CriomeMetaFrameCodec};
use crate::voice::{QuorumVoice, SilentVoice};
use crate::{Error, Result};

#[derive(Clone)]
pub struct CriomeDaemon {
    socket: PathBuf,
    meta_socket: PathBuf,
    store: StoreLocation,
    cluster_root: Option<BlsPublicKey>,
    authorization_mode: AuthorizationMode,
    node_identity: Identity,
    voice: Arc<dyn QuorumVoice>,
    clock: SystemClock,
}

pub use signal_criome::CriomeDaemonConfiguration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriomeDaemonConfigurationFile {
    path: PathBuf,
}

impl CriomeDaemon {
    pub fn new(socket: impl Into<PathBuf>, store: StoreLocation) -> Self {
        let socket = socket.into();
        let meta_socket = Self::default_meta_socket_path(&socket);
        Self {
            socket,
            meta_socket,
            store,
            cluster_root: None,
            authorization_mode: AuthorizationMode::Quorum,
            node_identity: RootArguments::default_node_identity(),
            voice: Arc::new(SilentVoice),
            clock: SystemClock::system(),
        }
    }

    pub fn from_configuration(configuration: CriomeDaemonConfiguration) -> Self {
        let socket = PathBuf::from(configuration.socket_path.as_str());
        let meta_socket = configuration
            .meta_socket_path()
            .map(|path| PathBuf::from(path.as_str()))
            .unwrap_or_else(|| Self::default_meta_socket_path(&socket));
        let node_identity = configuration
            .node_identity()
            .cloned()
            .unwrap_or_else(RootArguments::default_node_identity);
        Self {
            socket,
            meta_socket,
            store: StoreLocation::new(configuration.store_path.as_str()),
            cluster_root: configuration.cluster_root().cloned(),
            authorization_mode: *configuration.authorization_mode(),
            node_identity,
            voice: Arc::new(SilentVoice),
            clock: SystemClock::system(),
        }
    }

    pub fn from_environment() -> Self {
        let socket = std::env::var_os("CRIOME_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp/criome.sock"));
        Self::new(socket, StoreLocation::from_environment())
    }

    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn meta_socket(&self) -> &PathBuf {
        &self.meta_socket
    }

    pub fn store(&self) -> &StoreLocation {
        &self.store
    }

    pub fn with_meta_socket(mut self, meta_socket: impl Into<PathBuf>) -> Self {
        self.meta_socket = meta_socket.into();
        self
    }

    pub fn with_authorization_mode(mut self, authorization_mode: AuthorizationMode) -> Self {
        self.authorization_mode = authorization_mode;
        self
    }

    /// Arm this node's quorum voice — how it conveys solicitations and votes to
    /// peer members. Unset, the node self-votes but originates no solicitation.
    pub fn with_quorum_voice(mut self, voice: Arc<dyn QuorumVoice>) -> Self {
        self.voice = voice;
        self
    }

    /// Set the identity this criome signs attestations as. Distinct per node so
    /// peers cross-verify by registered key.
    pub fn with_node_identity(mut self, node_identity: Identity) -> Self {
        self.node_identity = node_identity;
        self
    }

    /// Pin this node's clock (the witness-clock the quorum gate consults). Unset,
    /// the node uses the real wall clock; a test pins it so an out-of-window
    /// refusal is deterministic without reading wall time.
    pub fn with_clock(mut self, clock: SystemClock) -> Self {
        self.clock = clock;
        self
    }

    pub fn run(self) -> Result<()> {
        let bound = self.bind()?;
        eprintln!("criome socket={}", bound.socket().display());
        eprintln!("criome meta_socket={}", bound.meta_socket().display());
        bound.serve_forever()
    }

    pub fn serve_one(self) -> Result<signal_criome::CriomeReply> {
        self.bind()?.serve_one()
    }

    pub fn bind(self) -> Result<BoundCriomeDaemon> {
        // The working socket is a shared IPC surface: a co-resident peer (the
        // persona-router's milestone-3 criome client) dials it, so it is bound
        // group-accessible (0660) and the deployment puts that peer in criome's
        // group. The meta socket and the master key stay owner-private (0600 /
        // 0700 state dir), so group membership never exposes the signing key.
        let listener = Self::bind_socket(&self.socket, 0o660)?;
        let meta_listener = Self::bind_socket(&self.meta_socket, 0o600)?;
        let meta_authority = MetaSocketAuthority::for_socket(&self.meta_socket)?;
        let runtime = tokio::runtime::Runtime::new()?;
        let root = runtime.block_on(CriomeRoot::start(RootArguments {
            store: self.store,
            cluster_root: self.cluster_root,
            authorization_mode: self.authorization_mode,
            node_identity: self.node_identity,
            voice: self.voice,
            clock: self.clock,
        }))?;
        Ok(BoundCriomeDaemon {
            socket: self.socket,
            meta_socket: self.meta_socket,
            runtime,
            listener,
            meta_listener,
            meta_authority,
            root,
        })
    }

    fn default_meta_socket_path(socket: &Path) -> PathBuf {
        match socket.file_name().and_then(|name| name.to_str()) {
            Some(name) => socket.with_file_name(format!("{name}.meta")),
            None => socket.with_extension("meta"),
        }
    }

    fn bind_socket(socket: &Path, mode: u32) -> Result<UnixListener> {
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(socket);
        let listener = UnixListener::bind(socket)?;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(mode))?;
        Ok(listener)
    }

    fn handle_connection(
        runtime: &tokio::runtime::Runtime,
        root: &ActorRef<CriomeRoot>,
        stream: UnixStream,
    ) -> Result<signal_criome::CriomeReply> {
        let mut connection = CriomeConnection::new(stream);
        let request = connection.read_request()?;
        let reply = runtime.block_on(async {
            root.ask(SubmitRequest::new(request))
                .await
                .map(crate::actors::CriomeActorReply::into_reply)
                .map_err(|error| Error::ActorCall(error.to_string()))
        })?;
        connection.write_reply(reply.clone())?;
        Ok(reply)
    }

    fn handle_streaming_connection(
        runtime: &tokio::runtime::Handle,
        root: &ActorRef<CriomeRoot>,
        stream: UnixStream,
    ) -> Result<Option<signal_criome::CriomeReply>> {
        let mut connection = CriomeConnection::new(stream);
        let request = connection.read_request()?;
        match request {
            CriomeRequest::AuthorizeSignalCall(authorization) => {
                connection.stream_authorization_submission(runtime, root, authorization)?;
                Ok(None)
            }
            CriomeRequest::ObserveAuthorization(observation) => {
                connection.stream_authorization_observation(runtime, root, observation)?;
                Ok(None)
            }
            request => {
                let reply = connection.submit_request(runtime, root, request)?;
                connection.write_reply(reply.clone())?;
                Ok(Some(reply))
            }
        }
    }

    fn handle_meta_connection(
        runtime: &tokio::runtime::Runtime,
        root: &ActorRef<CriomeRoot>,
        stream: UnixStream,
    ) -> Result<meta_signal_criome::Output> {
        let mut connection = CriomeMetaConnection::new(stream);
        let request = connection.read_request()?;
        let reply = runtime.block_on(async {
            root.ask(SubmitMetaRequest::new(request))
                .await
                .map(crate::actors::root::CriomeMetaActorReply::into_reply)
                .map_err(|error| Error::ActorCall(error.to_string()))
        })?;
        connection.write_reply(reply.clone())?;
        Ok(reply)
    }
}

impl CriomeDaemonConfigurationFile {
    pub fn from_signal_file(file: SignalFile) -> Self {
        Self {
            path: file.into_path(),
        }
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn configuration(&self) -> Result<CriomeDaemonConfiguration> {
        let bytes = std::fs::read(&self.path).map_err(|source| Error::ConfigurationRead {
            path: self.path.clone(),
            source,
        })?;
        CriomeDaemonConfiguration::from_rkyv_bytes(&bytes)
            .map_err(|_| Error::ConfigurationArchiveDecode)
    }

    pub fn write_configuration(&self, configuration: &CriomeDaemonConfiguration) -> Result<()> {
        let bytes = configuration
            .to_rkyv_bytes()
            .map_err(|_| Error::ConfigurationArchiveEncode)?;
        std::fs::write(&self.path, bytes).map_err(|source| Error::ConfigurationWrite {
            path: self.path.clone(),
            source,
        })
    }
}

pub struct BoundCriomeDaemon {
    socket: PathBuf,
    meta_socket: PathBuf,
    runtime: tokio::runtime::Runtime,
    listener: UnixListener,
    meta_listener: UnixListener,
    meta_authority: MetaSocketAuthority,
    root: ActorRef<CriomeRoot>,
}

impl BoundCriomeDaemon {
    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn meta_socket(&self) -> &PathBuf {
        &self.meta_socket
    }

    pub fn serve_one(self) -> Result<signal_criome::CriomeReply> {
        let reply = self.serve_next()?;
        self.shutdown()?;
        Ok(reply)
    }

    pub fn serve_next(&self) -> Result<signal_criome::CriomeReply> {
        let (stream, _address) = self.listener.accept()?;
        CriomeDaemon::handle_connection(&self.runtime, &self.root, stream)
    }

    pub fn serve_next_streaming(&self) -> Result<Option<signal_criome::CriomeReply>> {
        let (stream, _address) = self.listener.accept()?;
        CriomeDaemon::handle_streaming_connection(self.runtime.handle(), &self.root, stream)
    }

    pub fn serve_next_meta(&self) -> Result<meta_signal_criome::Output> {
        let (stream, _address) = self.meta_listener.accept()?;
        self.meta_authority.authorize(&stream)?;
        CriomeDaemon::handle_meta_connection(&self.runtime, &self.root, stream)
    }

    pub fn serve_forever(self) -> Result<()> {
        self.listener.set_nonblocking(true)?;
        self.meta_listener.set_nonblocking(true)?;
        loop {
            let served_working = self.try_serve_working_connection()?;
            let served_meta = self.try_serve_meta_connection()?;
            if !served_working && !served_meta {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    pub fn shutdown(self) -> Result<()> {
        self.runtime.block_on(CriomeRoot::stop(self.root))?;
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_file(&self.meta_socket);
        Ok(())
    }

    fn try_serve_working_connection(&self) -> Result<bool> {
        match self.listener.accept() {
            Ok((stream, _address)) => {
                let runtime = self.runtime.handle().clone();
                let root = self.root.clone();
                std::thread::spawn(move || {
                    let _ = CriomeDaemon::handle_streaming_connection(&runtime, &root, stream);
                });
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn try_serve_meta_connection(&self) -> Result<bool> {
        match self.meta_listener.accept() {
            Ok((stream, _address)) => {
                // Refusing a non-owner peer must not stop the daemon: a hostile
                // same-host user could otherwise wedge the serve loop by dialing
                // the meta socket. Log and drop the connection, keep serving.
                if let Err(error) = self.meta_authority.authorize(&stream) {
                    eprintln!("criome meta connection refused: {error}");
                    return Ok(true);
                }
                let _reply =
                    CriomeDaemon::handle_meta_connection(&self.runtime, &self.root, stream)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

/// The owner-authority the meta socket enforces at the kernel boundary.
///
/// The meta socket carries privileged policy orders (Configure, parked-request
/// decisions, intercept-policy mutation), so the daemon reads `SO_PEERCRED` on
/// every accepted meta connection and serves only the Unix user that owns the
/// socket. This makes the meta-vs-working authority boundary kernel-enforced
/// rather than path-secrecy-only: even under a loosened socket mode, a bind-time
/// race, a runtime-directory mistake, or a symlink race, a different-UID peer is
/// refused before any meta request is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaSocketAuthority {
    owner_uid: u32,
}

impl MetaSocketAuthority {
    /// The authorized owner is whoever owns the bound meta socket inode — the
    /// Unix user criome runs as. Read once at bind time through the safe std
    /// metadata wrapper, so no `unsafe` `geteuid` call is needed.
    fn for_socket(socket: &Path) -> Result<Self> {
        let owner_uid = std::fs::metadata(socket)?.uid();
        Ok(Self { owner_uid })
    }

    /// Refuse a meta connection whose peer credential is not the owning Unix
    /// user. `SO_PEERCRED` is read through the safe `rustix` wrapper, so this
    /// stays on stable Rust with no `unsafe` `getsockopt` call.
    fn authorize(&self, stream: &UnixStream) -> Result<()> {
        let uid = rustix::net::sockopt::socket_peercred(stream)
            .map_err(std::io::Error::from)?
            .uid
            .as_raw();
        if uid == self.owner_uid {
            Ok(())
        } else {
            Err(Error::MetaSocketUnauthorized {
                uid,
                owner_uid: self.owner_uid,
            })
        }
    }
}

pub struct CriomeConnection {
    stream: BufReader<UnixStream>,
    codec: CriomeFrameCodec,
    authorization_event_sequence: u64,
}

impl CriomeConnection {
    pub fn new(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            codec: CriomeFrameCodec::default(),
            authorization_event_sequence: 0,
        }
    }

    pub fn read_request(&mut self) -> Result<signal_criome::CriomeRequest> {
        self.codec.read_request(&mut self.stream)
    }

    pub fn write_reply(&mut self, reply: signal_criome::CriomeReply) -> Result<()> {
        self.codec.write_reply(self.stream.get_mut(), reply)
    }

    pub fn stream_authorization_submission(
        &mut self,
        runtime: &tokio::runtime::Handle,
        root: &ActorRef<CriomeRoot>,
        authorization: SignalCallAuthorization,
    ) -> Result<()> {
        let reply = self.submit_request(
            runtime,
            root,
            CriomeRequest::AuthorizeSignalCall(authorization),
        )?;
        let Some(request_slot) = self.submitted_authorization_slot(&reply) else {
            self.write_reply(reply)?;
            return Ok(());
        };
        let opened = self.open_authorization_observation(runtime, root, request_slot)?;
        self.write_authorization_observation(opened)
    }

    pub fn stream_authorization_observation(
        &mut self,
        runtime: &tokio::runtime::Handle,
        root: &ActorRef<CriomeRoot>,
        observation: AuthorizationObservation,
    ) -> Result<()> {
        let request_slot = observation.into_payload();
        let opened = self.open_authorization_observation(runtime, root, request_slot)?;
        self.write_authorization_observation(opened)
    }

    fn submit_request(
        &self,
        runtime: &tokio::runtime::Handle,
        root: &ActorRef<CriomeRoot>,
        request: CriomeRequest,
    ) -> Result<CriomeReply> {
        runtime.block_on(async {
            root.ask(SubmitRequest::new(request))
                .await
                .map(crate::actors::CriomeActorReply::into_reply)
                .map_err(|error| Error::ActorCall(error.to_string()))
        })
    }

    fn open_authorization_observation(
        &self,
        runtime: &tokio::runtime::Handle,
        root: &ActorRef<CriomeRoot>,
        request_slot: AuthorizationRequestSlot,
    ) -> Result<AuthorizationObservationOpened> {
        runtime.block_on(async {
            root.ask(OpenAuthorizationObservation::new(request_slot))
                .await
                .map_err(|error| Error::ActorCall(error.to_string()))
        })
    }

    fn write_authorization_observation(
        &mut self,
        opened: AuthorizationObservationOpened,
    ) -> Result<()> {
        let token = opened.token().clone();
        self.write_reply(CriomeReply::AuthorizationObservationSnapshot(
            opened.snapshot().clone(),
        ))?;
        if opened.snapshot().states().iter().any(|state| {
            state.request_slot == *token.payload()
                && matches!(
                    state.status,
                    AuthorizationStatus::Granted
                        | AuthorizationStatus::Denied
                        | AuthorizationStatus::Expired
                        | AuthorizationStatus::Unavailable
                )
        }) {
            return Ok(());
        }
        let mut updates = opened.into_updates();
        loop {
            let state = match updates.blocking_recv() {
                Ok(state) => state,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_count)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
            };
            if state.request_slot != *token.payload() {
                continue;
            }
            self.codec.write_authorization_update(
                self.stream.get_mut(),
                self.authorization_event_sequence,
                &token,
                state.clone(),
            )?;
            self.authorization_event_sequence = self.authorization_event_sequence.wrapping_add(1);
            if matches!(
                state.status,
                signal_criome::AuthorizationStatus::Granted
                    | signal_criome::AuthorizationStatus::Denied
                    | signal_criome::AuthorizationStatus::Expired
                    | signal_criome::AuthorizationStatus::Unavailable
            ) {
                return Ok(());
            }
        }
    }

    fn submitted_authorization_slot(
        &self,
        reply: &CriomeReply,
    ) -> Option<AuthorizationRequestSlot> {
        match reply {
            CriomeReply::AuthorizationPending(pending) => Some(pending.request_slot.clone()),
            CriomeReply::AuthorizationGranted(grant) => Some(grant.request_slot.clone()),
            CriomeReply::AuthorizationDenied(denied) => Some(denied.request_slot.clone()),
            CriomeReply::AuthorizationExpired(expired) => Some(expired.request_slot.clone()),
            CriomeReply::AuthorizationUnavailable(unavailable) => {
                Some(unavailable.request_slot.clone())
            }
            _ => None,
        }
    }
}

pub struct CriomeMetaConnection {
    stream: BufReader<UnixStream>,
    codec: CriomeMetaFrameCodec,
}

impl CriomeMetaConnection {
    pub fn new(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            codec: CriomeMetaFrameCodec::default(),
        }
    }

    pub fn read_request(&mut self) -> Result<meta_signal_criome::Input> {
        self.codec.read_request(&mut self.stream)
    }

    pub fn write_reply(&mut self, reply: meta_signal_criome::Output) -> Result<()> {
        self.codec.write_reply(self.stream.get_mut(), reply)
    }
}

#[cfg(test)]
mod meta_socket_authority_tests {
    use super::*;

    /// The connecting peer of a same-process socket pair carries this process's
    /// own uid, so the owning-uid authority admits it — the operator path.
    #[test]
    fn admits_the_owning_uid() {
        let (near, _far) = UnixStream::pair().expect("socket pair");
        let ours = rustix::net::sockopt::socket_peercred(&near)
            .expect("peer credentials")
            .uid
            .as_raw();
        let authority = MetaSocketAuthority { owner_uid: ours };
        assert!(authority.authorize(&near).is_ok());
    }

    /// A peer whose uid does not match the socket owner is refused before any
    /// meta request is read — the kernel-enforced non-owner boundary.
    #[test]
    fn refuses_a_non_owner_uid() {
        let (near, _far) = UnixStream::pair().expect("socket pair");
        let ours = rustix::net::sockopt::socket_peercred(&near)
            .expect("peer credentials")
            .uid
            .as_raw();
        let authority = MetaSocketAuthority {
            owner_uid: ours.wrapping_add(1),
        };
        assert!(matches!(
            authority.authorize(&near),
            Err(Error::MetaSocketUnauthorized { .. })
        ));
    }
}
