//! Encode a `criome-daemon` rkyv configuration from a socket + store path.
//!
//! A build/deploy-time bootstrap encoder (designer report 704, Stage A): the
//! criome NixOS service module runs this in `ExecStartPre` to turn the typed
//! configuration into the single rkyv startup argument `criome-daemon` consumes —
//! daemons take exactly one pre-generated rkyv message and never parse text.
//! Takes the three deploy values as positional arguments (socket, store, output
//! path), the build-tool counterpart of `mirror-write-configuration`.

use std::path::PathBuf;

use criome::daemon::CriomeDaemonConfigurationFile;
use signal_criome::CriomeDaemonConfiguration;

/// The encode request: where the daemon will bind, where it stores state, and
/// where the rkyv configuration is written.
struct ConfigurationEncoding {
    socket: String,
    store: String,
    output: PathBuf,
}

impl ConfigurationEncoding {
    fn from_arguments() -> Self {
        let mut arguments = std::env::args().skip(1);
        let usage = "usage: criome-write-configuration <socket-path> <store-path> <output-rkyv>";
        let socket = arguments.next().expect(usage);
        let store = arguments.next().expect(usage);
        let output = arguments.next().expect(usage);
        Self {
            socket,
            store,
            output: PathBuf::from(output),
        }
    }

    fn run(self) {
        let configuration = CriomeDaemonConfiguration::new(self.socket, self.store);
        CriomeDaemonConfigurationFile::new(&self.output)
            .write_configuration(&configuration)
            .expect("write the criome-daemon rkyv configuration");
        eprintln!(
            "criome-write-configuration: wrote {}",
            self.output.display()
        );
    }
}

fn main() {
    ConfigurationEncoding::from_arguments().run();
}
