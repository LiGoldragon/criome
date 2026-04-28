//! criome-daemon — entry point.
//!
//! Reads `$SEMA_PATH` (default `/tmp/sema.redb`) and
//! `$CRIOME_SOCKET` (default `/tmp/criome.sock`) from the
//! environment, brings up the [`Daemon`] supervision tree
//! ([`Engine`](criome::engine::Engine) +
//! [`Reader`](criome::reader::Reader) pool +
//! [`Listener`](criome::listener::Listener)), waits.

use std::path::PathBuf;

use criome::daemon::{Arguments, Daemon};
use criome::Result;

const DEFAULT_SOCKET_PATH: &str = "/tmp/criome.sock";
const DEFAULT_SEMA_PATH: &str = "/tmp/sema.redb";

#[tokio::main]
async fn main() -> Result<()> {
    let socket_path: PathBuf = std::env::var("CRIOME_SOCKET")
        .unwrap_or_else(|_| DEFAULT_SOCKET_PATH.to_string())
        .into();
    let sema_path: PathBuf = std::env::var("SEMA_PATH")
        .unwrap_or_else(|_| DEFAULT_SEMA_PATH.to_string())
        .into();

    eprintln!("criome-daemon: opening sema at {}", sema_path.display());
    eprintln!("criome-daemon: binding UDS at {}", socket_path.display());

    let (_daemon_ref, daemon_handle) =
        Daemon::start(Arguments { socket_path, sema_path }).await?;

    eprintln!("criome-daemon: ready");
    daemon_handle.await.map_err(|error| {
        criome::Error::ActorCall(format!("daemon join: {error}"))
    })?;
    Ok(())
}
