//! Error type for the criome daemon.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("sema: {0}")]
    Sema(#[from] sema::Error),

    #[error("frame decode: {0}")]
    Frame(#[from] signal::FrameDecodeError),

    #[error("frame too large for length-prefix: {length} bytes (max {max})", max = u32::MAX)]
    FrameTooLarge { length: usize },

    /// A ractor `call` failed (timeout, sender dropped). Carries
    /// a free-form detail string so the caller can log; the
    /// connection actor maps these to `Reply::Outcome(Diagnostic
    /// E0500)`.
    #[error("actor call: {0}")]
    ActorCall(String),

    /// `Actor::spawn` failed during daemon startup.
    #[error("actor spawn: {0}")]
    ActorSpawn(String),
}

pub type Result<T> = std::result::Result<T, Error>;
