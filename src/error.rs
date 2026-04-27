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
}

pub type Result<T> = std::result::Result<T, Error>;
