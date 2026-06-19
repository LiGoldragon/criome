//! Deploy-time NOTA → rkyv encoder for the criome daemon's startup
//! configuration.
//!
//! The criome daemon takes exactly one pre-generated rkyv startup message and
//! parses no NOTA (`CriomeDaemonCommand` only accepts a `SignalFile`). A
//! deployment therefore needs a bootstrap step that turns the typed NOTA the
//! deploy/module surface authors into the binary `CriomeDaemonConfiguration`
//! rkyv the daemon consumes.
//!
//! `criome-encode-configuration` is that step: it takes ONE NOTA argument — a
//! `CriomeConfigurationArtifact` record carrying the full typed
//! `CriomeDaemonConfiguration` plus the path the rkyv is written to — decodes
//! the configuration with the schema-derived `NotaDecode`, and writes the rkyv
//! via the daemon's own `CriomeDaemonConfigurationFile`. No flags; one rkyv
//! file out.
//!
//! This mirrors the router's `router-encode-configuration`. It lives behind the
//! `nota-text` feature so the daemon package (no `nota-text`) cannot decode
//! NOTA at all — the encode is exclusively a deploy-time step.

use std::path::{Path, PathBuf};

use nota_next::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError, NotaEncode};
use signal_criome::CriomeDaemonConfiguration;
use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::daemon::CriomeDaemonConfigurationFile;
use crate::{Error, Result};

/// A path field inside a deploy-encode request — a bare NOTA string projected
/// to a filesystem path.
#[derive(Debug, Clone, PartialEq, Eq, NotaDecode, NotaEncode)]
pub struct ArtifactPath(String);

impl ArtifactPath {
    fn as_path(&self) -> &Path {
        Path::new(self.0.as_str())
    }
}

/// `(CriomeConfigurationArtifact <CriomeDaemonConfiguration> <output-path>)`:
/// the full typed daemon configuration plus the path the rkyv is written to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriomeConfigurationArtifact {
    configuration: CriomeDaemonConfiguration,
    output_path: ArtifactPath,
}

impl CriomeConfigurationArtifact {
    fn write(self) -> Result<ArtifactWritten> {
        let output_path = self.output_path.clone();
        CriomeDaemonConfigurationFile::new(output_path.as_path())
            .write_configuration(&self.configuration)?;
        Ok(ArtifactWritten { output_path })
    }
}

impl NotaDecode for CriomeConfigurationArtifact {
    fn from_nota_block(block: &Block) -> std::result::Result<Self, NotaDecodeError> {
        let body = NotaBlock::new(block)
            .expect_body(Delimiter::Parenthesis, "CriomeConfigurationArtifact")?;
        let objects = body.root_objects();
        if objects.len() != 3 {
            return Err(NotaDecodeError::ExpectedRootCount {
                type_name: "CriomeConfigurationArtifact",
                expected: 3,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("CriomeConfigurationArtifact") => {}
            Some(variant) => {
                return Err(NotaDecodeError::UnknownVariant {
                    enum_name: "CriomeConfigurationArtifact",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(NotaDecodeError::ExpectedAtom {
                    type_name: "CriomeConfigurationArtifact",
                });
            }
        }
        Ok(Self {
            configuration: CriomeDaemonConfiguration::from_nota_block(&objects[1])?,
            output_path: ArtifactPath::from_nota_block(&objects[2])?,
        })
    }
}

/// The result of writing the configuration rkyv: the path it landed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWritten {
    output_path: ArtifactPath,
}

impl NotaEncode for ArtifactWritten {
    fn to_nota(&self) -> String {
        format!("(ArtifactWritten {})", self.output_path.to_nota())
    }
}

/// The configuration encoder process: one NOTA arg in, one config rkyv out.
#[derive(Debug)]
pub struct CriomeConfigurationEncoder {
    command: ComponentCommand,
}

impl CriomeConfigurationEncoder {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
        }
    }

    pub fn run(&self) -> Result<()> {
        let written = self.request()?.write()?;
        println!("{}", written.to_nota());
        Ok(())
    }

    fn request(&self) -> Result<CriomeConfigurationArtifact> {
        let text = ArtifactSource::read(&self.command)?;
        Ok(nota_next::NotaSource::new(text.as_str()).parse()?)
    }
}

/// The decoded NOTA text source for the deploy-encode request — an inline NOTA
/// string or a `.nota` file. A `.signal` rkyv input is rejected (this is the
/// text edge that produces rkyv, not consumes it).
struct ArtifactSource {
    text: String,
}

impl ArtifactSource {
    fn read(command: &ComponentCommand) -> Result<Self> {
        match command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => {
                if argument.as_str().starts_with("--") {
                    return Err(Error::FlagArgument(argument.into_string()));
                }
                Ok(Self {
                    text: argument.into_string(),
                })
            }
            ComponentArgument::NotaFile(file) => {
                let path: PathBuf = file.into_path();
                let text = std::fs::read_to_string(&path)
                    .map_err(|source| Error::ConfigurationRead { path, source })?;
                Ok(Self { text })
            }
            ComponentArgument::SignalFile(_) => Err(Error::ExpectedNotaRequest),
        }
    }

    fn as_str(&self) -> &str {
        &self.text
    }
}
