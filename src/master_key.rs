//! criome's master signing keypair and BLS verification.
//!
//! The master secret key never leaves [`MasterKey`]: generation, persistence,
//! public-key derivation, and signing are all methods here. Verification is a
//! trait on the wire `BlsPublicKey` — the public key is the noun that verifies
//! a signature over a message. The placeholder
//! `criome-skeleton-bls-signature` string is retired; this is real
//! BLS12-381 (min-pk) via `blst`.
//!
//! Key custody is the transitional bootstrap chosen by the psyche
//! (Spirit `psc6`): generate on first run, persist the secret to a `0600`
//! file. The secret never leaves criome. The eventual model is an
//! authenticated `meta-signal-criome` key configuration.

use std::io::{Read, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::str::FromStr;
use std::time::SystemTime;

use blst::BLST_ERROR;
use blst::min_pk::{PublicKey, SecretKey, Signature};
use signal_criome::{
    Attestation, AuditContext, BlsPublicKey, BlsSignature, ContentPurpose, ContentReference,
    Identity, PublicKeyFingerprint, SignatureScheme, TimeWindow, TimestampNanos,
};

use crate::{Error, Result};

/// Domain-separation tag binding every signature to criome's attestation
/// scheme (BLS12-381, signatures in G2). Sign and verify must use the same
/// tag; a signature minted under this tag cannot be replayed under another.
const ATTESTATION_DST: &[u8] = b"CRIOME-ATTESTATION-BLS12381G2-XMD:SHA-256_SSWU_RO_V1";

/// criome's master signing keypair. Holds the BLS12-381 secret key; the
/// secret is readable only as raw bytes for persistence and never crosses a
/// wire boundary.
#[derive(Clone)]
pub struct MasterKey {
    secret: SecretKey,
}

impl MasterKey {
    /// Generate a fresh master keypair from operating-system entropy.
    pub fn generate() -> Result<Self> {
        let mut input_keying_material = [0u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut input_keying_material)?;
        let secret = SecretKey::key_gen(&input_keying_material, &[])
            .map_err(|error| Error::MasterKey(format!("key generation failed: {error:?}")))?;
        input_keying_material.fill(0);
        Ok(Self { secret })
    }

    /// Load the master key from `path`, generating and persisting a fresh one
    /// (mode `0600`) when the file does not yet exist.
    pub fn load_or_generate(path: &Path) -> Result<Self> {
        if path.exists() {
            return Self::from_secret_file(path);
        }
        let key = Self::generate()?;
        key.persist(path)?;
        Ok(key)
    }

    fn from_secret_file(path: &Path) -> Result<Self> {
        // Reject unsafe key files before reading: no symlinks, must be a regular
        // file, and no group/other permission bits (0600 or stricter).
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_file() {
            return Err(Error::MasterKey(
                "master key file is not a regular file (refusing a symlink or special file)"
                    .to_string(),
            ));
        }
        if metadata.mode() & 0o077 != 0 {
            return Err(Error::MasterKey(format!(
                "master key file has unsafe permissions {:o}; expected 0600 or stricter",
                metadata.mode() & 0o777
            )));
        }
        let bytes = std::fs::read(path)?;
        let secret = SecretKey::from_bytes(&bytes)
            .map_err(|error| Error::MasterKey(format!("secret key decode failed: {error:?}")))?;
        Ok(Self { secret })
    }

    fn persist(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Create at 0600 atomically (create_new + mode) so the secret never
        // exists with broader permissions, even briefly under a permissive
        // umask; fsync before returning.
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(&self.secret.to_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    /// The master public key, hex-encoded into the wire `BlsPublicKey`.
    pub fn public_key(&self) -> BlsPublicKey {
        BlsPublicKey::new(Hexadecimal::from_bytes(&self.secret.sk_to_pk().to_bytes()).to_string())
    }

    /// A stable fingerprint of the master public key (blake3 hex). Delegated to
    /// [`FingerprintKey`] on the public key so a founding member's fingerprint —
    /// derived from its wire public key alone — matches the one this node stamps
    /// on its own identity.
    pub fn fingerprint(&self) -> PublicKeyFingerprint {
        self.public_key().fingerprint()
    }

    /// Sign `message` with the master key, hex-encoding the signature into the
    /// wire `BlsSignature`.
    pub fn sign(&self, message: &[u8]) -> BlsSignature {
        let signature = self.secret.sign(message, ATTESTATION_DST, &[]);
        BlsSignature::new(Hexadecimal::from_bytes(&signature.to_bytes()).to_string())
    }
}

/// Derive a stable fingerprint from a public key. Implemented on the wire
/// `BlsPublicKey` because the fingerprint is a property of the key itself, so a
/// node that holds only a peer's public key (a founding member) derives the same
/// fingerprint that peer stamps on its own identity.
pub trait FingerprintKey {
    fn fingerprint(&self) -> PublicKeyFingerprint;
}

impl FingerprintKey for BlsPublicKey {
    fn fingerprint(&self) -> PublicKeyFingerprint {
        // Hash the raw key bytes when the wire form decodes as hexadecimal (the
        // canonical encoding), matching `MasterKey` which hashes the same raw
        // bytes; fall back to the literal wire bytes for a non-hex key so a
        // malformed key still yields a deterministic, distinct fingerprint.
        let bytes = Hexadecimal::from_str(self.as_str())
            .map(|hex| hex.0)
            .unwrap_or_else(|_| self.as_str().as_bytes().to_vec());
        PublicKeyFingerprint::new(blake3::hash(&bytes).to_hex().to_string())
    }
}

/// Verify a BLS signature over a message under this public key. Implemented on
/// the wire `BlsPublicKey` because the public key is the noun that verifies.
pub trait VerifyBls {
    fn verify_bls(&self, signature: &BlsSignature, message: &[u8]) -> bool;
}

impl VerifyBls for BlsPublicKey {
    fn verify_bls(&self, signature: &BlsSignature, message: &[u8]) -> bool {
        let Ok(public_bytes) = Hexadecimal::from_str(self.as_str()) else {
            return false;
        };
        let Ok(signature_bytes) = Hexadecimal::from_str(signature.as_str()) else {
            return false;
        };
        let Ok(public_key) = PublicKey::from_bytes(public_bytes.as_slice()) else {
            return false;
        };
        let Ok(parsed_signature) = Signature::from_bytes(signature_bytes.as_slice()) else {
            return false;
        };
        parsed_signature.verify(true, message, ATTESTATION_DST, &[], &public_key, true)
            == BLST_ERROR::BLST_SUCCESS
    }
}

/// The canonical byte preimage criome's BLS signature covers: the full signed
/// statement of an attestation — content reference (the per-operation digest,
/// decision C), signer identity, audit context (the caller origin, decision A),
/// the validity interval, and the scheme — i.e. everything except the envelope
/// signature itself. Signer and verifier build the identical preimage from the
/// same attestation via `from_attestation`, so no signed field (notably the
/// expiry) can be altered without breaking the signature.
pub struct AttestationPreimage<'a> {
    content: &'a ContentReference,
    signer: &'a Identity,
    audit_context: &'a AuditContext,
    scheme: &'a SignatureScheme,
    issued_at: u64,
    expires_at: Option<u64>,
}

impl<'a> AttestationPreimage<'a> {
    /// Build the preimage from an attestation, covering every field the
    /// signature binds (all of the attestation except `envelope.signature`).
    pub fn from_attestation(attestation: &'a Attestation) -> Self {
        Self {
            content: &attestation.content_reference,
            signer: &attestation.identity,
            audit_context: &attestation.audit_context,
            scheme: &attestation.signature_envelope.signature_scheme,
            issued_at: attestation.timestamp_nanos.into_u64(),
            expires_at: attestation.expires_at().map(TimestampNanos::into_u64),
        }
    }

    /// The length-delimited, domain-tagged bytes that the signature covers.
    pub fn to_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(self.content.content_purpose.tag());
        bytes.push(self.audit_context.content_purpose.tag());
        bytes.push(match self.scheme {
            SignatureScheme::Bls12_381MinPk => 0u8,
            SignatureScheme::Bls12_381MinSig => 1u8,
        });
        let (signer_tag, signer_name) = match self.signer {
            Identity::Persona(name) => (0u8, name.as_str()),
            Identity::Agent(name) => (1u8, name.as_str()),
            Identity::Host(name) => (2u8, name.as_str()),
            Identity::Developer(name) => (3u8, name.as_str()),
            Identity::Cluster(name) => (4u8, name.as_str()),
        };
        bytes.push(signer_tag);
        bytes.extend_from_slice(&self.issued_at.to_le_bytes());
        match self.expires_at {
            Some(deadline) => {
                bytes.push(1);
                bytes.extend_from_slice(&deadline.to_le_bytes());
            }
            None => bytes.push(0),
        }
        for field in [
            signer_name,
            self.content.object_digest.as_str(),
            self.content.principal_name.as_str(),
            self.audit_context.audience.as_str(),
            self.audit_context.policy_version.as_str(),
            self.audit_context.replay_nonce.as_str(),
        ] {
            bytes.extend_from_slice(&(field.len() as u32).to_le_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes
    }
}

/// A clock for stamping, expiry checks, and the per-signer witness-clock gate,
/// mirroring the nanos-since-epoch `TimestampNanos` the wire uses. Data-bearing
/// (it holds its own source of now) rather than a free `now()` helper; shared by
/// the signer (stamp `issued_at`, gate the time-signature) and the verifier
/// (reject expired attestations).
///
/// The clock is either the real wall clock or a pinned instant. A pinned clock
/// makes the witness-clock gate deterministic under test — a signer's refusal of
/// an out-of-window request is proven without reading the real wall clock.
#[derive(Clone, Copy)]
pub enum SystemClock {
    Wall { epoch: SystemTime },
    Pinned { instant: TimestampNanos },
}

/// The verdict of the per-signer witness-clock gate: whether a signer's own
/// clock places the present inside a requested `TimeWindow`. A quorum signer
/// emits its time-signature only on `Inside`, so each signature testifies "now
/// is inside this window" rather than merely "a quorum co-signed this window."
///
/// A closed variant set (not a bool) so a phase-2 self-ownership lease can add
/// its own refusal (`OutsideLease`) beside `OutsideTimeWindow`, each mapping to a
/// distinct typed reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WindowAdmission {
    /// `now ∈ [opens_at, closes_at]`: the signer witnesses the present inside it.
    Inside,
    /// `now ∉ [opens_at, closes_at]`: the signer refuses to time-sign; maps to
    /// `EvaluationRejectionReason::OutsideTimeWindow`.
    OutsideTimeWindow,
}

impl SystemClock {
    pub fn system() -> Self {
        Self::Wall {
            epoch: SystemTime::UNIX_EPOCH,
        }
    }

    /// A clock pinned to a fixed instant. Deterministic: injected in tests so the
    /// witness-clock gate is exercised without reading the real wall clock.
    pub fn pinned(instant: TimestampNanos) -> Self {
        Self::Pinned { instant }
    }

    pub fn timestamp(&self) -> TimestampNanos {
        match self {
            Self::Wall { epoch } => {
                let nanos = SystemTime::now()
                    .duration_since(*epoch)
                    .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
                    .unwrap_or(0);
                TimestampNanos::new(nanos)
            }
            Self::Pinned { instant } => *instant,
        }
    }

    /// Whether `deadline` is strictly in the past relative to now.
    pub fn is_past(&self, deadline: &TimestampNanos) -> bool {
        self.timestamp().into_u64() > (*deadline).into_u64()
    }

    /// The per-signer witness-clock gate: does this clock's present lie inside
    /// `window`? A quorum signer consults its OWN clock and emits its
    /// time-signature only on `Inside`, so a proposer cannot manufacture "now" by
    /// choosing a convenient window — an honest signer refuses a window its clock
    /// is not inside.
    ///
    /// The gate is a conjunction of admission predicates. Today it is the single
    /// clock-in-window predicate; a phase-2 self-ownership lease joins it here as
    /// a second conjunct (`now ∈ window` AND `window ⊆ lease`), a signer-side
    /// refusal beside this one.
    pub fn admits_window(&self, window: &TimeWindow) -> WindowAdmission {
        let now = self.timestamp().into_u64();
        let inside = now >= window.opens_at.into_u64() && now <= window.closes_at.into_u64();
        if inside {
            WindowAdmission::Inside
        } else {
            WindowAdmission::OutsideTimeWindow
        }
    }
}

/// A stable one-byte discriminant for a content purpose, so a signature minted
/// for one purpose cannot be replayed under another.
trait PurposeTag {
    fn tag(&self) -> u8;
}

impl PurposeTag for ContentPurpose {
    fn tag(&self) -> u8 {
        match self {
            ContentPurpose::SignedObject => 0,
            ContentPurpose::ComponentRelease => 1,
            ContentPurpose::ChannelGrant => 2,
            ContentPurpose::ChannelRetract => 3,
            ContentPurpose::Authorization => 4,
            ContentPurpose::Archive => 5,
            ContentPurpose::PrivilegeElevation => 6,
        }
    }
}

/// Bytes rendered as lowercase hexadecimal — the wire encoding for BLS key and
/// signature material inside the string-typed `signal-criome` newtypes.
struct Hexadecimal(Vec<u8>);

impl Hexadecimal {
    fn from_bytes(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for Hexadecimal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Hexadecimal {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self> {
        if !text.len().is_multiple_of(2) {
            return Err(Error::MasterKey("odd-length hexadecimal".to_string()));
        }
        let bytes = (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16))
            .collect::<std::result::Result<Vec<u8>, _>>()
            .map_err(|error| Error::MasterKey(format!("invalid hexadecimal: {error}")))?;
        Ok(Self(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn sign_then_verify_round_trips() {
        let key = MasterKey::generate().expect("generate master key");
        let message = b"criome attestation preimage bytes";
        let signature = key.sign(message);
        assert!(key.public_key().verify_bls(&signature, message));
    }

    #[test]
    fn tampered_message_fails_verification() {
        let key = MasterKey::generate().expect("generate master key");
        let signature = key.sign(b"original attestation content");
        assert!(
            !key.public_key()
                .verify_bls(&signature, b"tampered attestation content")
        );
    }

    #[test]
    fn other_key_fails_verification() {
        let signer = MasterKey::generate().expect("generate signer key");
        let other = MasterKey::generate().expect("generate other key");
        let message = b"bound to the signing key";
        let signature = signer.sign(message);
        assert!(!other.public_key().verify_bls(&signature, message));
    }

    #[test]
    fn persisted_key_reloads_to_the_same_public_key() {
        let path = std::env::temp_dir().join(format!(
            "criome-master-key-test-{}.secret",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        let first = MasterKey::load_or_generate(&path).expect("first load generates");
        let second = MasterKey::load_or_generate(&path).expect("second load reads file");
        assert_eq!(first.public_key().as_str(), second.public_key().as_str());
        let mode = std::fs::metadata(&path)
            .expect("key file metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        std::fs::remove_file(&path).expect("clean up key file");
    }

    #[test]
    fn rejects_key_file_with_unsafe_permissions() {
        let path = std::env::temp_dir().join(format!(
            "criome-unsafe-key-test-{}.secret",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        std::fs::write(&path, [7u8; 32]).expect("write key bytes");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("set loose permissions");
        assert!(MasterKey::load_or_generate(&path).is_err());
        std::fs::remove_file(&path).expect("clean up");
    }

    #[test]
    fn rejects_corrupt_key_file() {
        let path = std::env::temp_dir().join(format!(
            "criome-corrupt-key-test-{}.secret",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .expect("create key file")
            .write_all(b"too short")
            .expect("write corrupt bytes");
        assert!(MasterKey::load_or_generate(&path).is_err());
        std::fs::remove_file(&path).expect("clean up");
    }
}
