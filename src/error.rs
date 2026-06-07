use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("actor call: {0}")]
    ActorCall(String),
    #[error("actor spawn: {0}")]
    ActorSpawn(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("nota: {0}")]
    Nota(#[from] nota_codec::Error),
    #[error("sema: {0}")]
    Sema(#[from] sema::Error),
    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),
    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },
    #[error("authorization replay attempted")]
    AuthorizationReplayAttempted,
    #[error("missing request record")]
    MissingRequestRecord,
    #[error("too many request records: expected exactly one")]
    TooManyRequestRecords,
    #[error("socket does not exist: {}", .path.display())]
    MissingSocket { path: PathBuf },
}

pub type Result<T> = std::result::Result<T, Error>;
