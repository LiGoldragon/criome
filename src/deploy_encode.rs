//! Deploy-time DOTOS → rkyv encoder for the criome daemon's startup
//! configuration.
//!
//! The criome daemon takes exactly one pre-generated rkyv startup message and
//! parses no DOTOS (`CriomeDaemonCommand` only accepts a `SignalFile`). A
//! deployment therefore needs a bootstrap step that turns the typed DOTOS the
//! deploy/module surface authors into the binary `CriomeDaemonConfiguration`
//! rkyv the daemon consumes.
//!
//! `criome-encode-configuration` is that step: it takes ONE DOTOS argument — a
//! `CriomeConfigurationArtifact` record carrying the full typed
//! `CriomeDaemonConfiguration` plus the path the rkyv is written to — decodes
//! the configuration with the schema-derived `DotosDecode` on
//! `CriomeDaemonConfiguration`, and writes the rkyv via the daemon's own
//! `CriomeDaemonConfigurationFile`. No flags; one rkyv file out.
//!
//! This mirrors the router's `router-encode-configuration` and mirror's
//! `mirror-write-configuration`. It lives behind the `dotos-text` feature so the
//! daemon's runtime path (no `dotos-text`) cannot decode DOTOS at all — the
//! encode is exclusively a deploy-time step.

use std::path::{Path, PathBuf};

use dotos::{Block, Delimiter, DotosBlock, DotosDecode, DotosDecodeError, DotosSource};
use signal_criome::CriomeDaemonConfiguration;
use triad_runtime::{ComponentArgument, ComponentCommand};

use crate::daemon::CriomeDaemonConfigurationFile;
use crate::{Error, Result};

/// `(CriomeConfigurationArtifact <CriomeDaemonConfiguration> <output-path>)`:
/// the full typed daemon configuration plus the path the rkyv is written to.
///
/// This record decodes itself through the DOTOS block API, the same way
/// criome's other crate-local DOTOS boundaries do.
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

impl DotosDecode for CriomeConfigurationArtifact {
    fn from_dotos_block(block: &Block) -> std::result::Result<Self, DotosDecodeError> {
        let body = DotosBlock::new(block)
            .expect_body(Delimiter::Parenthesis, "CriomeConfigurationArtifact")?;
        let objects = body.root_objects();
        if objects.len() != 3 {
            return Err(DotosDecodeError::ExpectedRootCount {
                type_name: "CriomeConfigurationArtifact",
                expected: 3,
                found: objects.len(),
            });
        }
        match objects[0].demote_to_string() {
            Some("CriomeConfigurationArtifact") => {}
            Some(variant) => {
                return Err(DotosDecodeError::UnknownVariant {
                    enum_name: "CriomeConfigurationArtifact",
                    variant: variant.to_owned(),
                });
            }
            None => {
                return Err(DotosDecodeError::ExpectedAtom {
                    type_name: "CriomeConfigurationArtifact",
                });
            }
        }
        let output_path = objects[2]
            .demote_to_string()
            .ok_or(DotosDecodeError::ExpectedAtom {
                type_name: "CriomeConfigurationArtifact",
            })?;
        Ok(Self {
            configuration: CriomeDaemonConfiguration::from_dotos_block(&objects[1])?,
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
    /// The deploy log line — a `(ArtifactWritten <path>)` DOTOS atom naming where
    /// the rkyv landed, printed so an `ExecStartPre` step records its output.
    pub fn report(&self) -> String {
        format!("(ArtifactWritten {})", self.output_path.display())
    }

    pub fn output_path(&self) -> &Path {
        &self.output_path
    }
}

/// The configuration encoder process: one DOTOS arg in, one config rkyv out.
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
        Ok(DotosSource::new(text.as_str()).parse()?)
    }
}

/// The decoded DOTOS text source for the deploy-encode request — an inline DOTOS
/// string or a `.dotos` file. A `.signal` rkyv input is rejected (this is the
/// text edge that produces rkyv, not consumes it).
struct ArtifactSource {
    text: String,
}

impl ArtifactSource {
    fn read(command: &ComponentCommand) -> Result<Self> {
        match command.dotos_argument()? {
            ComponentArgument::InlineDotos(argument) => {
                if argument.as_str().starts_with("--") {
                    return Err(Error::FlagArgument(argument.into_string()));
                }
                Ok(Self {
                    text: argument.into_string(),
                })
            }
            ComponentArgument::DotosFile(file) => {
                let path: PathBuf = file.into_path();
                let text = std::fs::read_to_string(&path)
                    .map_err(|source| Error::ConfigurationRead { path, source })?;
                Ok(Self { text })
            }
            ComponentArgument::SignalFile(_) => Err(Error::ExpectedDotosRequest),
        }
    }

    fn as_str(&self) -> &str {
        &self.text
    }
}
