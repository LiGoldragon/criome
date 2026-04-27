//! `criome-handle-frame` — one-shot Frame dispatcher.
//!
//! Read a length-prefixed [`signal::Frame`] from stdin, open
//! sema at `$SEMA_PATH`, dispatch the frame through
//! [`criome::Daemon::handle_frame`], write the reply Frame
//! (length-prefixed) to stdout.
//!
//! Sema state mutates in place at `$SEMA_PATH`; the wrapping
//! caller (typically a nix derivation) is responsible for
//! copy-in / copy-out around read-only inputs.

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

use criome::{Daemon, Error, Result};
use sema::Sema;
use signal::Frame;

fn main() -> Result<()> {
    let sema_path: PathBuf = std::env::var("SEMA_PATH")
        .map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "SEMA_PATH environment variable required",
            ))
        })?
        .into();

    let sema = Arc::new(Sema::open(&sema_path)?);
    let daemon = Daemon::new(sema);

    let mut stdin = std::io::stdin().lock();
    let mut length_bytes = [0u8; 4];
    stdin.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    let mut frame_bytes = vec![0u8; length];
    stdin.read_exact(&mut frame_bytes)?;

    let frame = Frame::decode(&frame_bytes)?;
    let reply_frame = daemon.handle_frame(frame);

    let bytes = reply_frame.encode();
    let length = u32::try_from(bytes.len())
        .map_err(|_| Error::FrameTooLarge { length: bytes.len() })?;
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&length.to_be_bytes())?;
    stdout.write_all(&bytes)?;

    Ok(())
}
