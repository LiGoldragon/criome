use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::daemon::CriomeDaemon;
use crate::tables::StoreLocation;
use crate::text::{ReplyDocument, RequestDocument};
use crate::transport::CriomeClient;
use crate::{Error, Result};

#[derive(Debug, Parser)]
#[command(name = "criome")]
#[command(about = "Criome trust and attestation daemon")]
pub struct CriomeCommandLine {
    #[command(subcommand)]
    command: Option<CriomeSubcommand>,
    #[arg(long)]
    socket: Option<PathBuf>,
    request: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum CriomeSubcommand {
    Daemon {
        #[arg(long)]
        socket: Option<PathBuf>,
        #[arg(long)]
        store: Option<PathBuf>,
    },
}

impl CriomeCommandLine {
    pub fn from_environment() -> Self {
        Self::parse()
    }

    pub fn run(self) -> Result<()> {
        match self.command {
            Some(CriomeSubcommand::Daemon { socket, store }) => {
                let socket = socket
                    .or(self.socket)
                    .or_else(|| std::env::var_os("CRIOME_SOCKET").map(PathBuf::from))
                    .unwrap_or_else(|| PathBuf::from("/tmp/criome.sock"));
                let store = store
                    .map(StoreLocation::new)
                    .unwrap_or_else(StoreLocation::from_environment);
                CriomeDaemon::new(socket, store).run()
            }
            None => self.run_client(),
        }
    }

    fn run_client(self) -> Result<()> {
        let request = match self.request.as_slice() {
            [] => return Err(Error::MissingRequestRecord),
            [record] => RequestDocument::parse(record)?.into_request(),
            _ => return Err(Error::TooManyRequestRecords),
        };
        let socket = self
            .socket
            .or_else(|| std::env::var_os("CRIOME_SOCKET").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("/tmp/criome.sock"));
        let reply = CriomeClient::new(socket).send(request)?;
        println!("{}", ReplyDocument::new(reply).render()?);
        Ok(())
    }
}
