use std::io::BufReader;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use kameo::actor::ActorRef;
use signal_criome::{
    AuthorizationMode, AuthorizationObservation, BlsPublicKey, CriomeReply, CriomeRequest,
};
use triad_runtime::SignalFile;

use crate::actors::root::{
    Arguments as RootArguments, CriomeRoot, OpenAuthorizationObservation, SubmitMetaRequest,
    SubmitRequest,
};
use crate::tables::StoreLocation;
use crate::transport::{CriomeFrameCodec, CriomeMetaFrameCodec};
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriomeDaemon {
    socket: PathBuf,
    meta_socket: PathBuf,
    store: StoreLocation,
    cluster_root: Option<BlsPublicKey>,
    authorization_mode: AuthorizationMode,
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
        }
    }

    pub fn from_configuration(configuration: CriomeDaemonConfiguration) -> Self {
        let socket = PathBuf::from(configuration.socket_path.as_str());
        let meta_socket = configuration
            .meta_socket_path()
            .map(|path| PathBuf::from(path.as_str()))
            .unwrap_or_else(|| Self::default_meta_socket_path(&socket));
        Self {
            socket,
            meta_socket,
            store: StoreLocation::new(configuration.store_path.as_str()),
            cluster_root: configuration.cluster_root().cloned(),
            authorization_mode: *configuration.authorization_mode(),
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
        let listener = Self::bind_private_socket(&self.socket)?;
        let meta_listener = Self::bind_private_socket(&self.meta_socket)?;
        let runtime = tokio::runtime::Runtime::new()?;
        let root = runtime.block_on(CriomeRoot::start(RootArguments {
            store: self.store,
            cluster_root: self.cluster_root,
            authorization_mode: self.authorization_mode,
        }))?;
        Ok(BoundCriomeDaemon {
            socket: self.socket,
            meta_socket: self.meta_socket,
            runtime,
            listener,
            meta_listener,
            root,
        })
    }

    fn default_meta_socket_path(socket: &Path) -> PathBuf {
        match socket.file_name().and_then(|name| name.to_str()) {
            Some(name) => socket.with_file_name(format!("{name}.meta")),
            None => socket.with_extension("meta"),
        }
    }

    fn bind_private_socket(socket: &Path) -> Result<UnixListener> {
        if let Some(parent) = socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(socket);
        let listener = UnixListener::bind(socket)?;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
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
        let CriomeRequest::ObserveAuthorization(observation) = request else {
            let reply = runtime.block_on(async {
                root.ask(SubmitRequest::new(request))
                    .await
                    .map(crate::actors::CriomeActorReply::into_reply)
                    .map_err(|error| Error::ActorCall(error.to_string()))
            })?;
            connection.write_reply(reply.clone())?;
            return Ok(Some(reply));
        };
        connection.stream_authorization_observation(runtime, root, observation)?;
        Ok(None)
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

    pub fn serve_next_meta(&self) -> Result<meta_signal_criome::Output> {
        let (stream, _address) = self.meta_listener.accept()?;
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
                let _reply =
                    CriomeDaemon::handle_meta_connection(&self.runtime, &self.root, stream)?;
                Ok(true)
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(false),
            Err(error) => Err(error.into()),
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

    pub fn stream_authorization_observation(
        &mut self,
        runtime: &tokio::runtime::Handle,
        root: &ActorRef<CriomeRoot>,
        observation: AuthorizationObservation,
    ) -> Result<()> {
        let request_slot = observation.into_payload();
        let opened = runtime.block_on(async {
            Ok::<_, Error>(
                root.ask(OpenAuthorizationObservation::new(request_slot))
                    .await
                    .map_err(|error| Error::ActorCall(error.to_string()))?,
            )
        })?;
        let token = opened.token().clone();
        self.write_reply(CriomeReply::AuthorizationObservationSnapshot(
            opened.snapshot().clone(),
        ))?;
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
