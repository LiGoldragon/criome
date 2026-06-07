use triad_runtime::{ComponentArgument, ComponentCommand, InlineNota, NotaFile};

use crate::daemon::{CriomeDaemon, CriomeDaemonConfiguration, CriomeDaemonConfigurationFile};
use crate::text::{ReplyDocument, RequestDocument};
use crate::transport::CriomeClient;
use crate::{Error, Result};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriomeCommandLine {
    command: ComponentCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriomeDaemonCommand {
    command: ComponentCommand,
}

pub struct CriomeRequestArgument {
    argument: ComponentArgument,
}

pub struct CriomeRequestFile {
    file: NotaFile,
}

impl CriomeCommandLine {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
        }
    }

    pub fn run(self) -> Result<()> {
        let request = CriomeRequestArgument::new(self.command.nota_argument()?).request()?;
        let reply = CriomeClient::from_environment().send(request)?;
        println!("{}", ReplyDocument::new(reply).render()?);
        Ok(())
    }
}

impl CriomeDaemonCommand {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
        }
    }

    pub fn configuration(&self) -> Result<CriomeDaemonConfiguration> {
        match self.command.signal_file_argument()? {
            ComponentArgument::SignalFile(file) => {
                CriomeDaemonConfigurationFile::from_signal_file(file).configuration()
            }
            ComponentArgument::InlineNota(_) | ComponentArgument::NotaFile(_) => {
                Err(triad_runtime::ArgumentError::ExpectedSignalFile.into())
            }
        }
    }

    pub fn run(self) -> Result<()> {
        CriomeDaemon::from_configuration(self.configuration()?).run()
    }
}

impl CriomeRequestArgument {
    pub fn new(argument: ComponentArgument) -> Self {
        Self { argument }
    }

    pub fn request(self) -> Result<signal_criome::CriomeRequest> {
        match self.argument {
            ComponentArgument::InlineNota(nota) => Self::request_from_inline_nota(nota),
            ComponentArgument::NotaFile(file) => CriomeRequestFile::new(file).request(),
            ComponentArgument::SignalFile(_) => Err(Error::ExpectedNotaRequest),
        }
    }

    fn request_from_inline_nota(nota: InlineNota) -> Result<signal_criome::CriomeRequest> {
        if nota.as_str().starts_with("--") {
            return Err(Error::FlagArgument(nota.into_string()));
        }
        Ok(RequestDocument::parse(nota.as_str())?.into_request())
    }
}

impl CriomeRequestFile {
    pub fn new(file: NotaFile) -> Self {
        Self { file }
    }

    pub fn request(self) -> Result<signal_criome::CriomeRequest> {
        let text = std::fs::read_to_string(self.file.as_path())?;
        Ok(RequestDocument::parse(&text)?.into_request())
    }
}
