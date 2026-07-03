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
//! the configuration with the schema-derived `NotaDecode` on
//! `CriomeDaemonConfiguration`, and writes the rkyv via the daemon's own
//! `CriomeDaemonConfigurationFile`. No flags; one rkyv file out.
//!
//! This mirrors the router's `router-encode-configuration` and mirror's
//! `mirror-write-configuration`. It lives behind the `nota-text` feature so the
//! daemon's runtime path (no `nota-text`) cannot decode NOTA at all — the
//! encode is exclusively a deploy-time step.

use std::path::{Path, PathBuf};

use nota::{Block, Delimiter, NotaBlock, NotaDecode, NotaDecodeError};
use signal_criome::CriomeDaemonConfiguration;
use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::daemon::CriomeDaemonConfigurationFile;
use crate::{Error, Result};

/// `(CriomeConfigurationArtifact <CriomeDaemonConfiguration> <output-path>)`:
/// the full typed daemon configuration plus the path the rkyv is written to.
///
/// criome aliases the NOTA codec crate as `nota-next` (package `nota`), so the
/// `nota` derive macros — which resolve the crate as bare `nota` — are
/// unavailable here; this record decodes itself through the `nota` block
/// API, the same way criome's other crate-local NOTA boundaries do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CriomeConfigurationArtifact {
    configuration: CriomeDaemonConfiguration,
    output_path: PathBuf,
}

impl CriomeConfigurationArtifact {
    fn write(self) -> Result<ArtifactWritten> {
        let output_path = self.output_path.clone();
        CriomeDaemonConfigurationFile::new(&self.output_path)
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
        let output_path = objects[2]
            .demote_to_string()
            .ok_or(NotaDecodeError::ExpectedAtom {
                type_name: "CriomeConfigurationArtifact",
            })?;
        Ok(Self {
            configuration: CriomeDaemonConfiguration::from_nota_block(&objects[1])?,
            output_path: PathBuf::from(output_path),
        })
    }
}

/// The result of writing the configuration rkyv: the path it landed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactWritten {
    output_path: PathBuf,
}

impl ArtifactWritten {
    /// The deploy log line — a `(ArtifactWritten <path>)` NOTA atom naming where
    /// the rkyv landed, printed so an `ExecStartPre` step records its output.
    pub fn report(&self) -> String {
        format!("(ArtifactWritten {})", self.output_path.display())
    }

    pub fn output_path(&self) -> &Path {
        &self.output_path
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
        println!("{}", written.report());
        Ok(())
    }

    fn request(&self) -> Result<CriomeConfigurationArtifact> {
        let text = ArtifactSource::read(&self.command)?;
        Ok(nota::NotaSource::new(text.as_str()).parse()?)
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
