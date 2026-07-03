use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("actor call: {0}")]
    ActorCall(String),
    #[error("master key: {0}")]
    MasterKey(String),
    #[error("startup: {0}")]
    Startup(String),
    #[error("actor spawn: {0}")]
    ActorSpawn(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[cfg(feature = "nota-text")]
    #[error("nota: {0}")]
    Nota(#[from] nota::NotaDecodeError),
    #[error("sema: {0}")]
    Sema(#[from] sema_engine::Error),
    #[error("signal frame: {0}")]
    SignalFrame(#[from] signal_frame::FrameError),
    #[error("argument: {0}")]
    Argument(#[from] triad_runtime::ArgumentError),
    #[error("configuration archive decode failed")]
    ConfigurationArchiveDecode,
    #[error("configuration archive encode failed")]
    ConfigurationArchiveEncode,
    #[error("configuration read failed at {path}: {source}")]
    ConfigurationRead {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("configuration write failed at {path}: {source}")]
    ConfigurationWrite {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("unexpected signal frame: {got}")]
    UnexpectedSignalFrame { got: String },
    #[error("authorization replay attempted")]
    AuthorizationReplayAttempted,
    #[error("intercept policy overlaps an active same-priority policy")]
    InterceptPolicyOverlapRejected,
    #[error("parked Spirit request missing")]
    ParkedSpiritRequestMissing,
    #[error("contract admission rejected: {0:?}")]
    ContractAdmissionRejected(signal_criome::ContractAdmissionRejectionReason),
    #[error("missing request record")]
    MissingRequestRecord,
    #[error("too many request records: expected exactly one")]
    TooManyRequestRecords,
    #[error("expected a NOTA request record")]
    ExpectedNotaRequest,
    #[error("flag-style arguments are not part of component binaries: {0}")]
    FlagArgument(String),
    #[error("socket does not exist: {}", .path.display())]
    MissingSocket { path: PathBuf },
    #[error("meta socket connection refused: peer uid {uid} is not the owning uid {owner_uid}")]
    MetaSocketUnauthorized { uid: u32, owner_uid: u32 },
}

pub type Result<T> = std::result::Result<T, Error>;
