//! criome — daemon entry point.
//!
//! Opens sema at `$SEMA_PATH` (default `/tmp/sema.redb`),
//! binds a UDS listener at `/tmp/criome.sock` (or
//! `$CRIOME_SOCKET`), and runs the accept loop until killed.

use std::path::PathBuf;
use std::sync::Arc;

use criome::{uds::Listener, Result};
use sema::Sema;

const DEFAULT_SOCKET_PATH: &str = "/tmp/criome.sock";
const DEFAULT_SEMA_PATH: &str = "/tmp/sema.redb";

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path =
        std::env::var("CRIOME_SOCKET").unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string());
    let sema_path: PathBuf = std::env::var("SEMA_PATH")
        .unwrap_or_else(|_| DEFAULT_SEMA_PATH.to_string())
        .into();

    eprintln!("criome: opening sema at {}", sema_path.display());
    let sema = Arc::new(Sema::open(&sema_path)?);

    eprintln!("criome: binding UDS at {socket_path}");
    let listener = Listener::bind(&socket_path).await?;

    eprintln!("criome: ready");
    listener.run(sema).await
}
