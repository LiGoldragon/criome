//! The cluster-root admission-signing ceremony.
//!
//! Per the psyche's trust-root decision (Spirit `ermr`): an identity is admitted
//! into a criome registry only when the **cluster-root** has signed it. The
//! verifying half — [`crate::admission::ClusterRoot::admits`] — already exists
//! and is tested. This module is the *minting* half: the one-shot, offline tool
//! that takes the cluster-root secret key and an unadmitted
//! [`IdentityRegistration`] (the identity↔key↔purpose binding) and produces the
//! same registration carrying a cluster-root [`SignatureEnvelope`] over the
//! canonical [`RegistrationStatement`]. The minted envelope is exactly what
//! `ClusterRoot::admits` accepts, closing the M3/at7x/5zur unblock.
//!
//! The ceremony never opens the daemon socket: admission is signed against the
//! local cluster-root key file, not by the running daemon. The secret never
//! leaves the [`MasterKey`] it is loaded into.

use signal_criome::{BlsPublicKey, IdentityRegistration, SignatureEnvelope, SignatureScheme};

use crate::Result;
use crate::admission::RegistrationStatement;
use crate::master_key::MasterKey;

/// The cluster-root signer for a one-shot admission ceremony. Owns the loaded
/// cluster-root [`MasterKey`] — this is the data-bearing noun whose job is to
/// mint admissions; erase its key field and there is no ceremony left.
pub struct ClusterRootCeremony {
    root: MasterKey,
}

impl ClusterRootCeremony {
    /// Load (or, on first run, generate and persist at `0600`) the cluster-root
    /// signing key from `path`. The same key file always reloads to the same
    /// public key, so admissions minted across separate ceremony runs verify
    /// under one stable trust anchor.
    pub fn from_key_file(path: &std::path::Path) -> Result<Self> {
        Ok(Self {
            root: MasterKey::load_or_generate(path)?,
        })
    }

    /// Mint a cluster-root admission for `registration` and return the same
    /// registration with its `admission` set to the freshly-signed envelope.
    /// The envelope covers the canonical registration statement (the exact
    /// identity↔key↔purpose binding), so it admits this registration and no
    /// other under [`crate::admission::ClusterRoot::admits`].
    pub fn admit(&self, mut registration: IdentityRegistration) -> IdentityRegistration {
        let statement = RegistrationStatement::from_registration(&registration).to_signing_bytes();
        registration.admission = Some(SignatureEnvelope {
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: self.root.public_key(),
            signature: self.root.sign(&statement),
        });
        registration
    }

    /// The cluster-root public key — the trust anchor an operator configures as
    /// criome's `cluster_root` so the daemon's gate admits these admissions.
    pub fn cluster_root_public_key(&self) -> BlsPublicKey {
        self.root.public_key()
    }
}

/// The offline `criome-cluster-root` command line. Takes the single NOTA
/// argument (an unadmitted [`IdentityRegistration`], inline or a file, per the
/// one-argument rule), loads the cluster-root key from the path named by the
/// `CRIOME_CLUSTER_ROOT_KEY` environment variable (out-of-band operational
/// config, not the NOTA argument), mints the admission, and prints the admitted
/// registration as NOTA. It never opens the daemon socket.
#[cfg(feature = "nota-text")]
pub struct ClusterRootCeremonyCommand {
    command: triad_runtime::ComponentCommand,
}

/// The environment variable naming the cluster-root secret key file.
#[cfg(feature = "nota-text")]
const KEY_FILE_VARIABLE: &str = "CRIOME_CLUSTER_ROOT_KEY";

/// The default cluster-root key path when `CRIOME_CLUSTER_ROOT_KEY` is unset.
#[cfg(feature = "nota-text")]
const DEFAULT_KEY_FILE: &str = "/var/lib/criome/cluster-root.secret";

#[cfg(feature = "nota-text")]
impl ClusterRootCeremonyCommand {
    pub fn from_environment() -> Self {
        Self {
            command: triad_runtime::ComponentCommand::from_environment(),
        }
    }

    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: triad_runtime::ComponentCommand::from_arguments(arguments),
        }
    }

    /// The cluster-root key file path from `CRIOME_CLUSTER_ROOT_KEY`, defaulting
    /// to [`DEFAULT_KEY_FILE`].
    fn key_file() -> std::path::PathBuf {
        std::env::var_os(KEY_FILE_VARIABLE)
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_KEY_FILE))
    }

    /// Decode the single NOTA argument into an unadmitted registration.
    fn registration(&self) -> Result<IdentityRegistration> {
        use nota_next::NotaSource;
        use triad_runtime::ComponentArgument;

        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(nota) => {
                if nota.as_str().starts_with("--") {
                    return Err(crate::Error::FlagArgument(nota.into_string()));
                }
                Ok(NotaSource::new(nota.as_str()).parse::<IdentityRegistration>()?)
            }
            ComponentArgument::NotaFile(file) => {
                let text = std::fs::read_to_string(file.as_path())?;
                Ok(NotaSource::new(&text).parse::<IdentityRegistration>()?)
            }
            ComponentArgument::SignalFile(_) => Err(crate::Error::ExpectedNotaRequest),
        }
    }

    pub fn run(self) -> Result<()> {
        use nota_next::NotaEncode;

        let ceremony = ClusterRootCeremony::from_key_file(&Self::key_file())?;
        let admitted = ceremony.admit(self.registration()?);
        println!("{}", admitted.to_nota());
        Ok(())
    }
}

// The ceremony tests exercise the NOTA round-trip, so they run under the
// `nota-text` feature (the authoritative `test-nota-text` flake gate) — the
// audit-228 lesson: a default-feature `cargo test` must still compile, which it
// cannot if these reference `to_nota`/`NotaSource` it doesn't build.
#[cfg(all(test, feature = "nota-text"))]
mod tests {
    use super::*;
    use crate::admission::ClusterRoot;
    use nota_next::{NotaEncode, NotaSource};
    use signal_criome::{Identity, KeyPurpose, PublicKeyFingerprint};

    fn ceremony_at(path: &std::path::Path) -> ClusterRootCeremony {
        ClusterRootCeremony::from_key_file(path).expect("load cluster-root key")
    }

    fn key_path(label: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "criome-cluster-root-ceremony-{label}-{}.secret",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn unadmitted_registration() -> IdentityRegistration {
        IdentityRegistration {
            identity: Identity::agent("worker".to_string()),
            public_key: BlsPublicKey::new("member-public-key".to_string()),
            fingerprint: PublicKeyFingerprint::new("member-fingerprint".to_string()),
            purpose: KeyPurpose::AgentRequest,
            admission: None,
        }
    }

    #[test]
    fn minted_admission_passes_the_real_cluster_root_gate() {
        let path = key_path("round-trip");
        let ceremony = ceremony_at(&path);
        let admitted = ceremony.admit(unadmitted_registration());
        let gate = ClusterRoot::new(ceremony.cluster_root_public_key());
        assert!(
            gate.admits(
                &admitted,
                admitted
                    .admission
                    .as_ref()
                    .expect("ceremony set the admission envelope"),
            )
        );
        std::fs::remove_file(&path).expect("clean up key file");
    }

    #[test]
    fn admitted_registration_survives_a_nota_round_trip() {
        let path = key_path("nota-round-trip");
        let ceremony = ceremony_at(&path);
        let admitted = ceremony.admit(unadmitted_registration());

        let text = admitted.to_nota();
        let decoded = NotaSource::new(&text)
            .parse::<IdentityRegistration>()
            .expect("decode admitted registration from NOTA");

        assert_eq!(decoded, admitted);
        let gate = ClusterRoot::new(ceremony.cluster_root_public_key());
        assert!(
            gate.admits(
                &decoded,
                decoded
                    .admission
                    .as_ref()
                    .expect("decoded registration is still admitted"),
            )
        );
        std::fs::remove_file(&path).expect("clean up key file");
    }

    #[test]
    fn persisted_key_reloads_and_still_admits() {
        let path = key_path("persisted");
        let first = ceremony_at(&path);
        let first_key = first.cluster_root_public_key();

        // Second load reads the persisted 0600 file rather than generating anew.
        let second = ClusterRootCeremony::from_key_file(&path).expect("reload persisted key");
        assert_eq!(
            first_key.as_str(),
            second.cluster_root_public_key().as_str()
        );

        // An admission minted after the reload admits under the same anchor.
        let admitted = second.admit(unadmitted_registration());
        let gate = ClusterRoot::new(first_key);
        assert!(
            gate.admits(
                &admitted,
                admitted
                    .admission
                    .as_ref()
                    .expect("post-reload admission envelope"),
            )
        );
        std::fs::remove_file(&path).expect("clean up key file");
    }

    #[test]
    fn admission_minted_by_a_different_key_is_not_admitted() {
        let ceremony_path = key_path("impostor-ceremony");
        let impostor_path = key_path("impostor-other");
        let ceremony = ceremony_at(&ceremony_path);
        let impostor = ceremony_at(&impostor_path);

        // The impostor mints over the same registration with its own key.
        let forged = impostor.admit(unadmitted_registration());
        let gate = ClusterRoot::new(ceremony.cluster_root_public_key());
        assert!(
            !gate.admits(
                &forged,
                forged
                    .admission
                    .as_ref()
                    .expect("impostor produced an envelope"),
            )
        );
        std::fs::remove_file(&ceremony_path).expect("clean up ceremony key file");
        std::fs::remove_file(&impostor_path).expect("clean up impostor key file");
    }
}
