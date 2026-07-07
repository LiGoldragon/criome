//! Cluster-root admission gate for criome's identity registry.
//!
//! Per the psyche's trust-root decision (Spirit `ermr`): a key is admitted into
//! a criome registry only when the **cluster-root** has signed it. criome holds
//! the cluster-root public key (its trust anchor) and, before registering an
//! identity, verifies a cluster-root signature over the canonical registration
//! statement. This closes the self-asserted-registry gap the SO audits (225/226)
//! called out: without it, any caller reaching the working socket could register
//! an arbitrary identity↔key binding and then forge a "Valid" attestation.
//!
//! This module is the working core of the gate (the cryptography + the
//! decision). Wiring it into `RegisterIdentity` needs three `signal-criome`
//! contract additions, after which `IdentityRegistry::register` gates on it:
//! `IdentityRegistration` gains an `admission` (the cluster-root's
//! `SignatureEnvelope` over the registration statement); `CriomeDaemonConfiguration`
//! gains a `cluster_root` public key (the trust anchor, absent only in dev/virgin
//! bootstrap); and `RejectionReason` gains `UnauthorizedRegistration`. `register`
//! then rejects any registration the configured `ClusterRoot` does not admit.

use signal_criome::{
    BlsPublicKey, Identity, IdentityRegistration, KeyPurpose, SignatureEnvelope, SignatureScheme,
};

use crate::master_key::VerifyBls;

/// The canonical bytes the cluster-root signs to admit a key: the identity, its
/// public key, and the key's purpose, domain-separated. The cluster-root signer
/// and criome's verifier build the identical statement, so an admission is valid
/// only for the exact identity↔key↔purpose binding it was issued for.
pub struct RegistrationStatement<'a> {
    identity: &'a Identity,
    public_key: &'a BlsPublicKey,
    purpose: &'a KeyPurpose,
}

impl<'a> RegistrationStatement<'a> {
    pub fn from_registration(registration: &'a IdentityRegistration) -> Self {
        Self {
            identity: &registration.identity,
            public_key: &registration.bls_public_key,
            purpose: &registration.key_purpose,
        }
    }

    pub fn to_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"CRIOME-REGISTRATION-ADMISSION-V1".to_vec();
        let (identity_tag, identity_name) = match self.identity {
            Identity::Persona(name) => (0u8, name.as_str()),
            Identity::Agent(name) => (1u8, name.as_str()),
            Identity::Host(name) => (2u8, name.as_str()),
            Identity::Developer(name) => (3u8, name.as_str()),
            Identity::Cluster(name) => (4u8, name.as_str()),
        };
        bytes.push(identity_tag);
        bytes.push(match self.purpose {
            KeyPurpose::CriomeRoot => 0,
            KeyPurpose::PersonaRequest => 1,
            KeyPurpose::AgentRequest => 2,
            KeyPurpose::ReleaseAuthorization => 3,
            KeyPurpose::HostPublication => 4,
        });
        for field in [identity_name, self.public_key.as_str()] {
            bytes.extend_from_slice(&(field.len() as u32).to_le_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes
    }
}

/// criome's configured cluster-root trust anchor. A registration is admitted
/// only if its `admission` envelope is a valid cluster-root signature over the
/// registration statement.
#[derive(Clone)]
pub struct ClusterRoot {
    public_key: BlsPublicKey,
}

impl ClusterRoot {
    pub fn new(public_key: BlsPublicKey) -> Self {
        Self { public_key }
    }

    /// Whether `admission` admits `registration` under this cluster root: the
    /// envelope must be issued under the configured cluster-root key and be a
    /// valid signature over the registration statement.
    pub fn admits(
        &self,
        registration: &IdentityRegistration,
        admission: &SignatureEnvelope,
    ) -> bool {
        // Only the implemented scheme is accepted; an envelope claiming another
        // scheme is rejected, never verified as min-pk bytes (algorithm confusion).
        if !matches!(admission.signature_scheme, SignatureScheme::Bls12_381MinPk) {
            return false;
        }
        if admission.bls_public_key != self.public_key {
            return false;
        }
        let statement = RegistrationStatement::from_registration(registration).to_signing_bytes();
        self.public_key
            .verify_bls(&admission.bls_signature, &statement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_key::MasterKey;
    use signal_criome::{BlsSignature, PublicKeyFingerprint, SignatureScheme};

    fn member_registration(identity: Identity, public_key: BlsPublicKey) -> IdentityRegistration {
        let fingerprint = PublicKeyFingerprint::new(format!("fingerprint-{}", public_key.as_str()));
        IdentityRegistration::new(
            identity,
            public_key,
            fingerprint,
            KeyPurpose::AgentRequest,
            None,
        )
    }

    fn cluster_root_admission(
        root: &MasterKey,
        registration: &IdentityRegistration,
    ) -> SignatureEnvelope {
        let statement = RegistrationStatement::from_registration(registration).to_signing_bytes();
        SignatureEnvelope {
            signature_scheme: SignatureScheme::Bls12_381MinPk,
            bls_public_key: root.public_key(),
            bls_signature: root.sign(&statement),
        }
    }

    #[test]
    fn cluster_root_signed_registration_is_admitted() {
        let root = MasterKey::generate().expect("cluster root key");
        let member = MasterKey::generate().expect("member key");
        let registration =
            member_registration(Identity::agent("worker".to_string()), member.public_key());
        let admission = cluster_root_admission(&root, &registration);
        let gate = ClusterRoot::new(root.public_key());
        assert!(gate.admits(&registration, &admission));
    }

    #[test]
    fn registration_signed_by_a_non_root_key_is_rejected() {
        let root = MasterKey::generate().expect("cluster root key");
        let impostor = MasterKey::generate().expect("impostor key");
        let member = MasterKey::generate().expect("member key");
        let registration =
            member_registration(Identity::agent("worker".to_string()), member.public_key());
        let admission = cluster_root_admission(&impostor, &registration);
        let gate = ClusterRoot::new(root.public_key());
        assert!(!gate.admits(&registration, &admission));
    }

    #[test]
    fn tampered_registration_is_rejected() {
        let root = MasterKey::generate().expect("cluster root key");
        let member = MasterKey::generate().expect("member key");
        let registration =
            member_registration(Identity::agent("worker".to_string()), member.public_key());
        let admission = cluster_root_admission(&root, &registration);
        // The cluster-root admitted "worker"; an attacker swaps the identity.
        let relabelled =
            member_registration(Identity::agent("attacker".to_string()), member.public_key());
        let gate = ClusterRoot::new(root.public_key());
        assert!(!gate.admits(&relabelled, &admission));
    }

    #[test]
    fn malformed_admission_is_rejected() {
        let root = MasterKey::generate().expect("cluster root key");
        let member = MasterKey::generate().expect("member key");
        let registration =
            member_registration(Identity::agent("worker".to_string()), member.public_key());
        let admission = SignatureEnvelope {
            signature_scheme: SignatureScheme::Bls12_381MinPk,
            bls_public_key: root.public_key(),
            bls_signature: BlsSignature::new("not-a-real-signature".to_string()),
        };
        let gate = ClusterRoot::new(root.public_key());
        assert!(!gate.admits(&registration, &admission));
    }

    #[test]
    fn admission_claiming_an_unsupported_scheme_is_rejected() {
        let root = MasterKey::generate().expect("cluster root key");
        let member = MasterKey::generate().expect("member key");
        let registration =
            member_registration(Identity::agent("worker".to_string()), member.public_key());
        let mut admission = cluster_root_admission(&root, &registration);
        // A valid min-pk signature, but the envelope claims a different scheme.
        admission.signature_scheme = SignatureScheme::Bls12_381MinSig;
        let gate = ClusterRoot::new(root.public_key());
        assert!(!gate.admits(&registration, &admission));
    }
}
