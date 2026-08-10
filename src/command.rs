use triad_runtime::{ComponentArgument, ComponentCommand};
#[cfg(feature = "dotos-text")]
use triad_runtime::{DotosFile, InlineDotos};

#[cfg(feature = "dotos-text")]
use crate::Error;
use crate::Result;
use crate::daemon::{CriomeDaemon, CriomeDaemonConfiguration, CriomeDaemonConfigurationFile};
#[cfg(feature = "dotos-text")]
use crate::text::{ReplyDocument, RequestDocument};
#[cfg(feature = "dotos-text")]
use crate::transport::CriomeClient;

#[cfg(feature = "dotos-text")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriomeCommandLine {
    command: ComponentCommand,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CriomeDaemonCommand {
    command: ComponentCommand,
}

#[cfg(feature = "dotos-text")]
pub struct CriomeRequestArgument {
    argument: ComponentArgument,
}

#[cfg(feature = "dotos-text")]
pub struct CriomeRequestFile {
    file: DotosFile,
}

#[cfg(feature = "dotos-text")]
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
        let request = CriomeRequestArgument::new(self.command.dotos_argument()?).request()?;
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
            ComponentArgument::InlineDotos(_) | ComponentArgument::DotosFile(_) => {
                Err(triad_runtime::ArgumentError::ExpectedSignalFile.into())
            }
        }
    }

    pub fn run(self) -> Result<()> {
        CriomeDaemon::from_configuration(self.configuration()?).run()
    }
}

#[cfg(feature = "dotos-text")]
impl CriomeRequestArgument {
    pub fn new(argument: ComponentArgument) -> Self {
        Self { argument }
    }

    pub fn request(self) -> Result<signal_criome::CriomeRequest> {
        match self.argument {
            ComponentArgument::InlineDotos(dotos) => Self::request_from_inline_dotos(dotos),
            ComponentArgument::DotosFile(file) => CriomeRequestFile::new(file).request(),
            ComponentArgument::SignalFile(_) => Err(Error::ExpectedDotosRequest),
        }
    }

    fn request_from_inline_dotos(dotos: InlineDotos) -> Result<signal_criome::CriomeRequest> {
        if dotos.as_str().starts_with("--") {
            return Err(Error::FlagArgument(dotos.into_string()));
        }
        Ok(RequestDocument::parse(dotos.as_str())?.into_request())
    }
}

#[cfg(feature = "dotos-text")]
impl CriomeRequestFile {
    pub fn new(file: DotosFile) -> Self {
        Self { file }
    }

    pub fn request(self) -> Result<signal_criome::CriomeRequest> {
        let text = std::fs::read_to_string(self.file.as_path())?;
        Ok(RequestDocument::parse(&text)?.into_request())
    }
}
