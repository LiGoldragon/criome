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
    Identity, PublicKeyFingerprint, SignatureScheme, TimestampNanos,
};

use crate::{Error, Result};

/// Domain-separation tag binding every signature to criome's attestation
/// scheme (BLS12-381, signatures in G2). Sign and verify must use the same
/// tag; a signature minted under this tag cannot be replayed under another.
const ATTESTATION_DST: &[u8] = b"CRIOME-ATTESTATION-BLS12381G2-XMD:SHA-256_SSWU_RO_V1";

/// Domain-separation tag binding a peer-frame envelope signature: the sender
/// signs the exact length-prefixed `CriomeFrame` bytes under this tag so a peer
/// daemon can authenticate a frame before decoding it. It MUST be distinct from
/// [`ATTESTATION_DST`] — a signature minted over an attestation preimage can
/// never be replayed as a peer-frame envelope (and vice versa), because the
/// distinct tag changes the BLS hash-to-curve domain.
const PEER_FRAME_DST: &[u8] = b"CRIOME-PEER-FRAME-BLS12381G2-XMD:SHA-256_SSWU_RO_V1";

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

    /// A stable fingerprint of the master public key (blake3 hex).
    pub fn fingerprint(&self) -> PublicKeyFingerprint {
        let digest = blake3::hash(&self.secret.sk_to_pk().to_bytes());
        PublicKeyFingerprint::new(digest.to_hex().to_string())
    }

    /// Sign `message` with the master key, hex-encoding the signature into the
    /// wire `BlsSignature`.
    pub fn sign(&self, message: &[u8]) -> BlsSignature {
        let signature = self.secret.sign(message, ATTESTATION_DST, &[]);
        BlsSignature::new(Hexadecimal::from_bytes(&signature.to_bytes()).to_string())
    }

    /// Sign the exact length-prefixed peer-frame `bytes` under
    /// [`PEER_FRAME_DST`], hex-encoding the signature into the wire
    /// `BlsSignature`. The peer-frame domain tag keeps these envelope
    /// signatures unforgeable as attestations and vice versa.
    pub fn sign_peer_frame(&self, bytes: &[u8]) -> BlsSignature {
        let signature = self.secret.sign(bytes, PEER_FRAME_DST, &[]);
        BlsSignature::new(Hexadecimal::from_bytes(&signature.to_bytes()).to_string())
    }
}

/// Verify a BLS signature over a message under this public key. Implemented on
/// the wire `BlsPublicKey` because the public key is the noun that verifies.
/// The domain-tagged verification body lives once in `verify_bls_with_domain`;
/// the two public methods select the matching domain tag so an attestation
/// signature and a peer-frame envelope signature can never cross-verify.
pub trait VerifyBls {
    /// Verify `signature` over `message` under criome's attestation domain
    /// ([`ATTESTATION_DST`]).
    fn verify_bls(&self, signature: &BlsSignature, message: &[u8]) -> bool {
        self.verify_bls_with_domain(signature, message, ATTESTATION_DST)
    }

    /// Verify `signature` over the exact length-prefixed peer-frame `bytes`
    /// under the peer-frame domain ([`PEER_FRAME_DST`]).
    fn verify_peer_frame(&self, signature: &BlsSignature, bytes: &[u8]) -> bool {
        self.verify_bls_with_domain(signature, bytes, PEER_FRAME_DST)
    }

    /// Verify `signature` over `message` under an explicit domain-separation
    /// tag. Returns `false` on any malformed key or signature material.
    fn verify_bls_with_domain(
        &self,
        signature: &BlsSignature,
        message: &[u8],
        domain: &[u8],
    ) -> bool;
}

impl VerifyBls for BlsPublicKey {
    fn verify_bls_with_domain(
        &self,
        signature: &BlsSignature,
        message: &[u8],
        domain: &[u8],
    ) -> bool {
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
        parsed_signature.verify(true, message, domain, &[], &public_key, true)
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
            content: &attestation.content,
            signer: &attestation.signer,
            audit_context: &attestation.audit_context,
            scheme: &attestation.envelope.scheme,
            issued_at: attestation.issued_at.into_u64(),
            expires_at: attestation.expires_at().map(TimestampNanos::into_u64),
        }
    }

    /// The length-delimited, domain-tagged bytes that the signature covers.
    pub fn to_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.push(self.content.purpose.tag());
        bytes.push(self.audit_context.purpose.tag());
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
            self.content.digest.as_str(),
            self.content.schema_version.as_str(),
            self.audit_context.audience.as_str(),
            self.audit_context.policy_version.as_str(),
            self.audit_context.nonce.as_str(),
        ] {
            bytes.extend_from_slice(&(field.len() as u32).to_le_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes
    }
}

/// A wall clock for stamping and expiry checks, mirroring the nanos-since-epoch
/// `TimestampNanos` the wire uses. Data-bearing (it holds its epoch) rather than
/// a free `now()` helper; shared by the signer (stamp `issued_at`) and the
/// verifier (reject expired attestations).
pub struct SystemClock {
    epoch: SystemTime,
}

impl SystemClock {
    pub fn system() -> Self {
        Self {
            epoch: SystemTime::UNIX_EPOCH,
        }
    }

    pub fn timestamp(&self) -> TimestampNanos {
        let nanos = SystemTime::now()
            .duration_since(self.epoch)
            .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        TimestampNanos::new(nanos)
    }

    /// Whether `deadline` is strictly in the past relative to now.
    pub fn is_past(&self, deadline: &TimestampNanos) -> bool {
        self.timestamp().into_u64() > (*deadline).into_u64()
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
    fn attestation_and_peer_frame_signatures_never_cross_verify() {
        // The single most load-bearing security invariant of the peer transport:
        // a signature minted under ATTESTATION_DST must NEVER verify under
        // PEER_FRAME_DST, and vice versa, even over the identical message with
        // the identical key. The distinct domain-separation tags change the BLS
        // hash-to-curve domain, so the two signatures cannot be substituted.
        let key = MasterKey::generate().expect("generate master key");
        let public_key = key.public_key();
        let message = b"identical preimage signed under two different domains";

        // Attestation signature must fail peer-frame verification...
        let attestation_signature = key.sign(message);
        assert!(
            public_key.verify_bls(&attestation_signature, message),
            "attestation signature verifies under its own domain (sanity)"
        );
        assert!(
            !public_key.verify_peer_frame(&attestation_signature, message),
            "attestation signature must NOT verify as a peer frame"
        );

        // ...and a peer-frame signature must fail attestation verification.
        let peer_frame_signature = key.sign_peer_frame(message);
        assert!(
            public_key.verify_peer_frame(&peer_frame_signature, message),
            "peer-frame signature verifies under its own domain (sanity)"
        );
        assert!(
            !public_key.verify_bls(&peer_frame_signature, message),
            "peer-frame signature must NOT verify as an attestation"
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
