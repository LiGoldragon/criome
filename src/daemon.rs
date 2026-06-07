use std::io::BufReader;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::ActorRef;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use triad_runtime::SignalFile;

use crate::actors::root::{Arguments as RootArguments, CriomeRoot, SubmitRequest};
use crate::tables::StoreLocation;
use crate::transport::CriomeFrameCodec;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriomeDaemon {
    socket: PathBuf,
    store: StoreLocation,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CriomeDaemonConfiguration {
    pub socket_path: String,
    pub store_path: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriomeDaemonConfigurationFile {
    path: PathBuf,
}

impl CriomeDaemon {
    pub fn new(socket: impl Into<PathBuf>, store: StoreLocation) -> Self {
        Self {
            socket: socket.into(),
            store,
        }
    }

    pub fn from_configuration(configuration: CriomeDaemonConfiguration) -> Self {
        Self::new(
            PathBuf::from(configuration.socket_path),
            StoreLocation::new(configuration.store_path),
        )
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

    pub fn store(&self) -> &StoreLocation {
        &self.store
    }

    pub fn run(self) -> Result<()> {
        let bound = self.bind()?;
        eprintln!("criome socket={}", bound.socket().display());
        bound.serve_forever()
    }

    pub fn serve_one(self) -> Result<signal_criome::CriomeReply> {
        self.bind()?.serve_one()
    }

    pub fn bind(self) -> Result<BoundCriomeDaemon> {
        if let Some(parent) = self.socket.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let _ = std::fs::remove_file(&self.socket);
        let listener = UnixListener::bind(&self.socket)?;
        std::fs::set_permissions(&self.socket, std::fs::Permissions::from_mode(0o600))?;
        let runtime = tokio::runtime::Runtime::new()?;
        let root = runtime.block_on(CriomeRoot::start(RootArguments::new(self.store)))?;
        Ok(BoundCriomeDaemon {
            socket: self.socket,
            runtime,
            listener,
            root,
        })
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
}

impl CriomeDaemonConfiguration {
    pub fn new(socket_path: impl Into<String>, store_path: impl Into<String>) -> Self {
        Self {
            socket_path: socket_path.into(),
            store_path: store_path.into(),
        }
    }

    pub fn from_rkyv_bytes(bytes: &[u8]) -> Result<Self> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|_| Error::ConfigurationArchiveDecode)
    }

    pub fn to_rkyv_bytes(&self) -> Result<Vec<u8>> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map_err(|_| Error::ConfigurationArchiveEncode)?;
        Ok(bytes.into_vec())
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
    }

    pub fn write_configuration(&self, configuration: &CriomeDaemonConfiguration) -> Result<()> {
        std::fs::write(&self.path, configuration.to_rkyv_bytes()?).map_err(|source| {
            Error::ConfigurationWrite {
                path: self.path.clone(),
                source,
            }
        })
    }
}

pub struct BoundCriomeDaemon {
    socket: PathBuf,
    runtime: tokio::runtime::Runtime,
    listener: UnixListener,
    root: ActorRef<CriomeRoot>,
}

impl BoundCriomeDaemon {
    pub fn socket(&self) -> &PathBuf {
        &self.socket
    }

    pub fn serve_one(self) -> Result<signal_criome::CriomeReply> {
        let (stream, _address) = self.listener.accept()?;
        let reply = CriomeDaemon::handle_connection(&self.runtime, &self.root, stream)?;
        self.shutdown()?;
        Ok(reply)
    }

    pub fn serve_forever(self) -> Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let _reply = CriomeDaemon::handle_connection(&self.runtime, &self.root, stream)?;
        }
        Ok(())
    }

    pub fn shutdown(self) -> Result<()> {
        self.runtime.block_on(CriomeRoot::stop(self.root))?;
        let _ = std::fs::remove_file(&self.socket);
        Ok(())
    }
}

pub struct CriomeConnection {
    stream: BufReader<UnixStream>,
    codec: CriomeFrameCodec,
}

impl CriomeConnection {
    pub fn new(stream: UnixStream) -> Self {
        Self {
            stream: BufReader::new(stream),
            codec: CriomeFrameCodec::default(),
        }
    }

    pub fn read_request(&mut self) -> Result<signal_criome::CriomeRequest> {
        self.codec.read_request(&mut self.stream)
    }

    pub fn write_reply(&mut self, reply: signal_criome::CriomeReply) -> Result<()> {
        self.codec.write_reply(self.stream.get_mut(), reply)
    }
}
