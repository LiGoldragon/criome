//! The root founding ceremony's durable aggregate and its verification.
//!
//! Founding is a UNANIMOUS, owner-accepted establishment of a Criome's root: an
//! initiator builds a [`RootGenesis`] whose `anchor` — `blake3(rkyv(genesis))` —
//! commits to the ordered founding public keys (self-certifying identity), and
//! every cohort member's master key signs the [`RootFoundingStatement`] over that
//! anchor. The signatures ride ATTACHED to the anchor; they are never folded back
//! into the hash. [`RootFounding`] is the durable record: the genesis, its
//! anchor, and the accumulated founding signatures. It knows how to accept a
//! signature, whether the cohort is unanimous, and how to verify itself on boot.
//!
//! The judge (`ContractStore::evaluate`) never reads any of this — a founded root
//! is provenance/trust-anchor material, distinct from the per-operation quorum
//! evaluated against a Threshold contract.

use signal_criome::{
    BlsPublicKey, ContractParent, FoundingMember, FoundingSignature, GenesisDomainTag, Identity,
    IdentityRegistration, KeyPurpose, RootAnchorDigest, RootFoundingStatement, RootGenesis,
    SignatureScheme,
};

use crate::master_key::{FingerprintKey, VerifyBls};

/// A malformed genesis that cannot be founded. Every variant maps to the meta
/// reply `RootFoundingRejectionReason::MalformedGenesis`; the taxonomy is kept
/// distinct here so the refusal names the exact defect for the daemon log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FoundingError {
    #[error("root genesis names no founding keys")]
    EmptyCohort,
    #[error("root genesis contract is not parented at Root")]
    NonRootParent,
    #[error("root genesis anchor could not be encoded")]
    AnchorEncode,
}

/// The canonical signing bytes of a founding statement. A trait on the wire
/// [`RootFoundingStatement`] so the signer (which mints a founding signature) and
/// the verifier (which checks one) derive byte-for-byte the same preimage from
/// the one shared recipe.
pub trait FoundingStatementBytes {
    fn signing_bytes(&self) -> Result<Vec<u8>, FoundingError>;
}

impl FoundingStatementBytes for RootFoundingStatement {
    fn signing_bytes(&self) -> Result<Vec<u8>, FoundingError> {
        Ok(self
            .preimage_digest()
            .map_err(|_| FoundingError::AnchorEncode)?
            .as_str()
            .as_bytes()
            .to_vec())
    }
}

/// The durable founded-root record: the accepted genesis, its committed anchor,
/// and the founding signatures gathered so far. Persisted in the `root_founding`
/// table; on boot it is verified and adopted, never re-founded.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RootFounding {
    genesis: RootGenesis,
    anchor: RootAnchorDigest,
    signatures: Vec<FoundingSignature>,
}

impl RootFounding {
    /// Open a founding round over `genesis`: validate the genesis shape and
    /// compute its committed anchor. Identity exists the instant this succeeds;
    /// the returned record carries no signatures yet.
    pub fn found(genesis: RootGenesis) -> Result<Self, FoundingError> {
        if genesis.founding_member_vector().is_empty() {
            return Err(FoundingError::EmptyCohort);
        }
        if !matches!(genesis.contract.contract_parent(), ContractParent::Root) {
            return Err(FoundingError::NonRootParent);
        }
        let anchor = genesis.anchor().map_err(|_| FoundingError::AnchorEncode)?;
        Ok(Self {
            genesis,
            anchor,
            signatures: Vec::new(),
        })
    }

    /// Rebuild a founded record conveyed whole from the initiator: found the
    /// genesis, then attach every gathered signature. The caller checks
    /// [`Self::verify`] before persisting — `adopt` does not itself trust the
    /// signatures, it only reassembles the record so `verify` can judge it.
    pub fn adopt(
        genesis: RootGenesis,
        signatures: Vec<FoundingSignature>,
    ) -> Result<Self, FoundingError> {
        let mut founding = Self::found(genesis)?;
        for signature in signatures {
            founding.attach_signature(signature);
        }
        Ok(founding)
    }

    pub fn genesis(&self) -> &RootGenesis {
        &self.genesis
    }

    pub fn anchor(&self) -> &RootAnchorDigest {
        &self.anchor
    }

    pub fn signatures(&self) -> &[FoundingSignature] {
        self.signatures.as_slice()
    }

    /// The statement every founder signs: the committed anchor plus the domain
    /// tag, so a signature is domain-separated and bound to this exact cohort.
    pub fn statement(&self) -> RootFoundingStatement {
        RootFoundingStatement::new(self.anchor.clone(), self.genesis.genesis_domain_tag)
    }

    pub fn domain(&self) -> GenesisDomainTag {
        self.genesis.genesis_domain_tag
    }

    /// The founding member whose registered public key is `public_key`, if any —
    /// how a node finds its own seat in the cohort by its master key.
    pub fn member_by_key(&self, public_key: &BlsPublicKey) -> Option<&FoundingMember> {
        self.genesis
            .founding_member_vector()
            .iter()
            .find(|member| &member.bls_public_key == public_key)
    }

    fn member(&self, identity: &Identity) -> Option<&FoundingMember> {
        self.genesis
            .founding_member_vector()
            .iter()
            .find(|member| &member.identity == identity)
    }

    pub fn has_signature_from(&self, identity: &Identity) -> bool {
        self.signatures
            .iter()
            .any(|signature| &signature.identity == identity)
    }

    /// Attach `signature`, replacing an earlier one from the same signer. A
    /// non-member's signature is refused (returns `false`); a member votes once,
    /// so redelivery updates in place rather than double-counting.
    pub fn attach_signature(&mut self, signature: FoundingSignature) -> bool {
        if self.member(&signature.identity).is_none() {
            return false;
        }
        if let Some(existing) = self
            .signatures
            .iter_mut()
            .find(|held| held.identity == signature.identity)
        {
            *existing = signature;
        } else {
            self.signatures.push(signature);
        }
        true
    }

    /// Unanimity: every cohort member has attached a signature. Founding is the
    /// one place unanimity — not majority — is correct, so one missing member
    /// leaves the root un-founded by design.
    pub fn is_unanimous(&self) -> bool {
        self.genesis
            .founding_member_vector()
            .iter()
            .all(|member| self.has_signature_from(&member.identity))
    }

    /// Whether every attached signature is individually valid: its signer is a
    /// cohort member, its envelope names the implemented scheme and that member's
    /// key, and it verifies over the founding-statement preimage. Does NOT require
    /// unanimity — the gathering path and the boot path both use it.
    pub fn signatures_valid(&self) -> bool {
        let Ok(statement_bytes) = self.statement().signing_bytes() else {
            return false;
        };
        self.signatures
            .iter()
            .all(|signature| self.signature_valid(signature, &statement_bytes))
    }

    /// Whether a conveyed `signature` is fit to attach: its signer is a cohort
    /// member and its envelope verifies over this founding's statement preimage —
    /// the same per-signature gate [`Self::signatures_valid`] applies to each
    /// attached one. The initiator runs this on a peer-returned signature BEFORE
    /// counting it toward unanimity, so a forged conveyance (any bytes off the
    /// working socket) is rejected rather than accepted as presence.
    pub fn conveyed_signature_valid(&self, signature: &FoundingSignature) -> bool {
        let Ok(statement_bytes) = self.statement().signing_bytes() else {
            return false;
        };
        self.signature_valid(signature, &statement_bytes)
    }

    fn signature_valid(&self, signature: &FoundingSignature, statement_bytes: &[u8]) -> bool {
        let Some(member) = self.member(&signature.identity) else {
            return false;
        };
        // Only the implemented scheme is accepted; an envelope claiming another
        // scheme is refused, never verified as min-pk bytes (algorithm confusion).
        if !matches!(
            signature.signature_envelope.signature_scheme,
            SignatureScheme::Bls12_381MinPk
        ) {
            return false;
        }
        if signature.signature_envelope.bls_public_key != member.bls_public_key {
            return false;
        }
        member
            .bls_public_key
            .verify_bls(&signature.signature_envelope.bls_signature, statement_bytes)
    }

    /// The full founded-root gate run on boot: the stored anchor equals the anchor
    /// re-derived from the embedded genesis (tamper detection), every attached
    /// signature is valid, and the founding quorum is unanimous. A founded root
    /// that fails this is not adopted; the node does not re-found.
    pub fn verify(&self) -> bool {
        let Ok(recomputed) = self.genesis.anchor() else {
            return false;
        };
        recomputed == self.anchor && self.signatures_valid() && self.is_unanimous()
    }

    /// The `CriomeRoot` identity registrations seeding the registry from the
    /// founding cohort: each member's identity bound to its founding (master)
    /// public key. The founded cohort becomes the registry's trust anchor,
    /// superseding a single configured cluster-root seed.
    pub fn seed_registrations(&self) -> Vec<IdentityRegistration> {
        self.genesis
            .founding_member_vector()
            .iter()
            .map(|member| {
                IdentityRegistration::new(
                    member.identity.clone(),
                    member.bls_public_key.clone(),
                    member.bls_public_key.fingerprint(),
                    KeyPurpose::CriomeRoot,
                    None,
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_key::MasterKey;
    use signal_criome::{
        Contract, PolicyMember, ReplayNonce, RequiredSignatureThreshold, Rule, SignatureEnvelope,
        Threshold,
    };

    fn host(name: &str) -> Identity {
        Identity::host(name.to_string())
    }

    fn member(identity: &Identity, key: &MasterKey) -> FoundingMember {
        FoundingMember::new(identity.clone(), key.public_key())
    }

    /// A genesis whose root contract is a Threshold over the cohort as key
    /// members. The founding is unanimous regardless of this threshold — the
    /// threshold governs FUTURE ordinary changes, not founding.
    fn genesis(members: &[FoundingMember], nonce: &str) -> RootGenesis {
        let policy_members = members
            .iter()
            .map(|member| PolicyMember::KeyMember(member.identity.clone()))
            .collect();
        let root_contract = Contract::root(Rule::Threshold(Threshold::new(
            RequiredSignatureThreshold::new(members.len() as u64),
            policy_members,
        )));
        RootGenesis::new(
            root_contract,
            members.to_vec(),
            GenesisDomainTag::CriomeRootFoundingV1,
            ReplayNonce::new(nonce),
        )
    }

    fn sign(founding: &RootFounding, identity: &Identity, key: &MasterKey) -> FoundingSignature {
        let bytes = founding
            .statement()
            .signing_bytes()
            .expect("founding statement encodes");
        FoundingSignature::new(
            identity.clone(),
            SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: key.public_key(),
                bls_signature: key.sign(&bytes),
            },
        )
    }

    #[test]
    fn a_unanimous_cohort_verifies() {
        let alpha = host("alpha");
        let beta = host("beta");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let beta_key = MasterKey::generate().expect("beta key");
        let cohort = vec![member(&alpha, &alpha_key), member(&beta, &beta_key)];
        let mut founding = RootFounding::found(genesis(&cohort, "unanimous")).expect("found");
        assert!(!founding.is_unanimous(), "no signatures gathered yet");

        assert!(founding.attach_signature(sign(&founding, &alpha, &alpha_key)));
        assert!(
            !founding.is_unanimous(),
            "one of two members is short of unanimity"
        );
        assert!(!founding.verify(), "verify demands unanimity, not majority");

        assert!(founding.attach_signature(sign(&founding, &beta, &beta_key)));
        assert!(founding.is_unanimous(), "both cohort members signed");
        assert!(
            founding.verify(),
            "unanimous, anchor-consistent, valid sigs"
        );
    }

    #[test]
    fn a_forged_founding_signature_is_not_valid() {
        let alpha = host("alpha");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let cohort = vec![member(&alpha, &alpha_key)];
        let mut founding = RootFounding::found(genesis(&cohort, "forged")).expect("found");
        // A well-formed envelope that names alpha's key but signs garbage.
        let forged = FoundingSignature::new(
            alpha.clone(),
            SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: alpha_key.public_key(),
                bls_signature: alpha_key.sign(b"not the founding statement"),
            },
        );
        assert!(founding.attach_signature(forged));
        assert!(
            !founding.signatures_valid(),
            "a signature over other bytes does not verify against the statement"
        );
        assert!(!founding.verify());
    }

    #[test]
    fn conveyed_signature_valid_accepts_a_real_signature_and_rejects_a_forgery() {
        // The per-signature gate the initiator runs on a peer-returned signature
        // BEFORE attaching it: a genuine member signature is accepted; a well-formed
        // envelope over other bytes (a forgery) is rejected, so it can never be
        // counted toward unanimity as bare presence.
        let alpha = host("alpha");
        let beta = host("beta");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let beta_key = MasterKey::generate().expect("beta key");
        let cohort = vec![member(&alpha, &alpha_key), member(&beta, &beta_key)];
        let founding = RootFounding::found(genesis(&cohort, "conveyed")).expect("found");

        let genuine = sign(&founding, &beta, &beta_key);
        assert!(
            founding.conveyed_signature_valid(&genuine),
            "a genuine cohort-member signature over the statement is accepted"
        );

        let forged = FoundingSignature::new(
            beta.clone(),
            SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: beta_key.public_key(),
                bls_signature: beta_key.sign(b"not the founding statement"),
            },
        );
        assert!(
            !founding.conveyed_signature_valid(&forged),
            "a well-formed envelope over other bytes does not verify — rejected before attach"
        );
    }

    #[test]
    fn a_signature_claiming_another_scheme_is_refused() {
        let alpha = host("alpha");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let cohort = vec![member(&alpha, &alpha_key)];
        let mut founding = RootFounding::found(genesis(&cohort, "scheme")).expect("found");
        let mut signature = sign(&founding, &alpha, &alpha_key);
        // A valid min-pk signature, but the envelope claims a different scheme.
        signature.signature_envelope.signature_scheme = SignatureScheme::Bls12_381MinSig;
        assert!(founding.attach_signature(signature));
        assert!(!founding.signatures_valid());
    }

    #[test]
    fn a_non_member_signature_is_refused() {
        let alpha = host("alpha");
        let intruder = host("intruder");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let intruder_key = MasterKey::generate().expect("intruder key");
        let cohort = vec![member(&alpha, &alpha_key)];
        let mut founding = RootFounding::found(genesis(&cohort, "intruder")).expect("found");
        assert!(
            !founding.attach_signature(sign(&founding, &intruder, &intruder_key)),
            "a signer outside the cohort is not recorded"
        );
        assert!(founding.signatures().is_empty());
    }

    #[test]
    fn the_anchor_commits_to_the_founding_keys() {
        let alpha = host("alpha");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let other_key = MasterKey::generate().expect("other key");
        let first =
            RootFounding::found(genesis(&[member(&alpha, &alpha_key)], "n")).expect("found first");
        let second =
            RootFounding::found(genesis(&[member(&alpha, &other_key)], "n")).expect("found second");
        assert_ne!(
            first.anchor(),
            second.anchor(),
            "a different founding key yields a different self-certifying anchor"
        );
    }

    #[test]
    fn an_empty_cohort_or_non_root_parent_is_malformed() {
        assert!(matches!(
            RootFounding::found(genesis(&[], "empty")),
            Err(FoundingError::EmptyCohort)
        ));
        let alpha = host("alpha");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let cohort = vec![member(&alpha, &alpha_key)];
        let child_root = RootGenesis::new(
            Contract::child(
                Rule::SignedBy(alpha.clone()),
                signal_criome::ContractDigest::from_bytes(b"some-parent"),
            ),
            cohort,
            GenesisDomainTag::CriomeRootFoundingV1,
            ReplayNonce::new("child"),
        );
        assert!(matches!(
            RootFounding::found(child_root),
            Err(FoundingError::NonRootParent)
        ));
    }

    #[test]
    fn seed_registrations_bind_each_member_to_its_key() {
        let alpha = host("alpha");
        let alpha_key = MasterKey::generate().expect("alpha key");
        let founding =
            RootFounding::found(genesis(&[member(&alpha, &alpha_key)], "seed")).expect("found");
        let registrations = founding.seed_registrations();
        assert_eq!(registrations.len(), 1);
        assert_eq!(registrations[0].identity, alpha);
        assert_eq!(registrations[0].bls_public_key, alpha_key.public_key());
        assert_eq!(registrations[0].key_purpose, KeyPurpose::CriomeRoot);
        assert_eq!(
            registrations[0].public_key_fingerprint,
            alpha_key.fingerprint(),
            "the seeded fingerprint matches the one the node stamps on its own key"
        );
    }
}
