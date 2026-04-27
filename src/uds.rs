//! Unix-socket listener — accepts signal frames from clients.
//!
//! Wire framing: 4-byte big-endian length prefix + N rkyv bytes
//! per [`signal::Frame::encode`] / `decode`. Each connection is
//! a sequence of request-frames followed by reply-frames in
//! lockstep (FIFO; no correlation IDs).

use std::sync::Arc;

use sema::Sema;
use signal::Frame;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::dispatch;
use crate::error::{Error, Result};

pub struct Listener {
    listener: UnixListener,
}

impl Listener {
    /// Bind a UDS listener at `socket_path`. Removes any stale
    /// socket file first.
    pub async fn bind(socket_path: &str) -> Result<Self> {
        let _ = std::fs::remove_file(socket_path);
        let listener = UnixListener::bind(socket_path)?;
        Ok(Self { listener })
    }

    /// Accept connections forever; each connection runs
    /// independently in its own tokio task.
    pub async fn run(self, sema: Arc<Sema>) -> Result<()> {
        loop {
            let (socket, _) = self.listener.accept().await?;
            let sema = Arc::clone(&sema);
            tokio::spawn(async move {
                if let Err(error) = handle_connection(socket, sema).await {
                    eprintln!("criome: connection error: {error}");
                }
            });
        }
    }
}

async fn handle_connection(mut socket: UnixStream, sema: Arc<Sema>) -> Result<()> {
    loop {
        let frame = match read_frame(&mut socket).await {
            Ok(frame) => frame,
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        let reply_frame = dispatch::handle(frame, &sema);
        write_frame(&mut socket, reply_frame).await?;
    }
    Ok(())
}

async fn read_frame(socket: &mut UnixStream) -> Result<Frame> {
    let mut length_bytes = [0u8; 4];
    socket.read_exact(&mut length_bytes).await?;
    let length = u32::from_be_bytes(length_bytes) as usize;
    let mut frame_bytes = vec![0u8; length];
    socket.read_exact(&mut frame_bytes).await?;
    Frame::decode(&frame_bytes).map_err(Error::Frame)
}

async fn write_frame(socket: &mut UnixStream, frame: Frame) -> Result<()> {
    let bytes = frame.encode();
    let length =
        u32::try_from(bytes.len()).map_err(|_| Error::FrameTooLarge { length: bytes.len() })?;
    socket.write_all(&length.to_be_bytes()).await?;
    socket.write_all(&bytes).await?;
    Ok(())
}
