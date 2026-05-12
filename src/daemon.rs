use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;

use kameo::actor::ActorRef;

use crate::actors::root::{Arguments as RootArguments, CriomeRoot, SubmitRequest};
use crate::tables::StoreLocation;
use crate::transport::CriomeFrameCodec;
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriomeDaemon {
    socket: PathBuf,
    store: StoreLocation,
}

impl CriomeDaemon {
    pub fn new(socket: impl Into<PathBuf>, store: StoreLocation) -> Self {
        Self {
            socket: socket.into(),
            store,
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
        self.runtime.block_on(CriomeRoot::stop(self.root))?;
        let _ = std::fs::remove_file(&self.socket);
        Ok(reply)
    }

    pub fn serve_forever(self) -> Result<()> {
        for stream in self.listener.incoming() {
            let stream = stream?;
            let _reply = CriomeDaemon::handle_connection(&self.runtime, &self.root, stream)?;
        }
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
