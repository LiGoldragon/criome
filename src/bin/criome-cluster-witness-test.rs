//! The criome-cluster authorization witness-test (designer report 704, Stage A).
//!
//! A runnable end-to-end prover for the 1-of-1 criome authorization gate against
//! a REAL `criome-daemon` over its Unix socket — the criome half of the spirit
//! gate (`spirit/tests/criome_gate_1of1.rs` proves the spirit side in-process;
//! this proves the criome decision through a deployed daemon, so a NixOS VM test
//! can run it against a systemd `criome` service). It mints real BLS key
//! material, seeds the daemon (RegisterIdentity x2 + AdmitContract), then proves:
//!
//!   (a) AUTHORIZED — evidence with one valid signer signature over the head's
//!       operation, against the 1-of-1 threshold-1 contract, yields
//!       `EvaluationDecision::Authorized`.
//!   (b) REJECTED — threshold-short evidence (zero operation signatures) yields a
//!       non-`Authorized` decision.
//!
//! Exit 0 only when both hold. Any socket fault, off-contract reply, or wrong
//! decision panics (nonzero exit), so the NixOS test's `succeed` fails honestly.
//!
//! Socket path: `CRIOME_SOCKET` (else `/run/criome/criome.sock`).

use std::path::PathBuf;

use criome::language::{AttestedMomentStatement, OperationStatement};
use criome::master_key::MasterKey;
use criome::transport::CriomeClient;
use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizationEvaluation, AuthorizedObjectKind,
    AuthorizedObjectReference, ComponentKind, Contract, ContractDigest, CriomeReply, CriomeRequest,
    EvaluationDecision, Evidence, Identity, IdentityRegistration, KeyPurpose, ObjectDigest,
    OperationDigest, PolicyMember, RequiredSignatureThreshold, Rule, SignatureEnvelope,
    SignatureScheme, StampedSignatureEnvelope, Threshold, TimeSignature, TimeWindow,
    TimestampNanos,
};

/// The 1-of-1 deploy-config trust material: one release-authorization signer and
/// one timekeeper, a single-member threshold-1 contract (criome's `k > n/2`
/// admits n=1, k=1). Mirrors `LocalCriomePolicy` in spirit's gate witness, but
/// drives `EvaluateAuthorization` directly rather than through spirit's engine.
struct WitnessPolicy {
    signer_identity: Identity,
    signer_key: MasterKey,
    timekeeper_identity: Identity,
    timekeeper_key: MasterKey,
}

impl WitnessPolicy {
    fn new() -> Self {
        Self {
            signer_identity: Identity::developer("spirit-local-signer".to_owned()),
            signer_key: MasterKey::generate().expect("signer key generates"),
            timekeeper_identity: Identity::cluster("spirit-local-timekeeper".to_owned()),
            timekeeper_key: MasterKey::generate().expect("timekeeper key generates"),
        }
    }

    fn registration(identity: &Identity, key: &MasterKey) -> IdentityRegistration {
        IdentityRegistration::new(
            identity.clone(),
            key.public_key(),
            key.fingerprint(),
            KeyPurpose::ReleaseAuthorization,
            None,
        )
    }

    fn contract() -> Contract {
        Contract::new(Rule::Threshold(Threshold::new(
            RequiredSignatureThreshold::new(1),
            vec![PolicyMember::KeyMember(Identity::developer(
                "spirit-local-signer".to_owned(),
            ))],
        )))
    }

    /// A timekeeper-signed attested moment over a valid (opens < closes) window.
    fn stamp(&self) -> AttestedMoment {
        let proposition = AttestedMomentProposition::new(
            TimeWindow {
                opens_at: TimestampNanos::new(10),
                closes_at: TimestampNanos::new(20),
            },
            RequiredSignatureThreshold::new(1),
            vec![self.timekeeper_identity.clone()],
        );
        let signature = TimeSignature {
            signer: self.timekeeper_identity.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.timekeeper_key.public_key(),
                signature: self.timekeeper_key.sign(
                    AttestedMomentStatement::new(&proposition)
                        .to_signing_bytes()
                        .expect("moment statement signs")
                        .as_slice(),
                ),
            },
        };
        AttestedMoment::new(proposition, vec![signature])
    }

    /// Evidence over `operation`, signed by `signer_count` distinct signers.
    /// `1` satisfies the threshold-1 contract; `0` is threshold-short.
    fn evidence(&self, operation: OperationDigest, signer_count: usize) -> Evidence {
        let stamp = self.stamp();
        let signatures: Vec<StampedSignatureEnvelope> = if signer_count == 0 {
            Vec::new()
        } else {
            let statement = OperationStatement::new(&self.signer_identity, &operation, &stamp)
                .to_signing_bytes()
                .expect("operation statement signs");
            vec![StampedSignatureEnvelope {
                stamp: stamp.clone(),
                envelope: SignatureEnvelope {
                    scheme: SignatureScheme::Bls12_381MinPk,
                    public_key: self.signer_key.public_key(),
                    signature: self.signer_key.sign(&statement),
                },
            }]
        };
        Evidence::new(
            ComponentKind::Spirit,
            operation,
            stamp,
            signatures,
            Vec::new(),
        )
    }
}

/// The witness run: a live criome socket client plus the 1-of-1 trust material.
/// Owns the whole proof so every step is a method on the data it acts through.
struct Witness {
    client: CriomeClient,
    policy: WitnessPolicy,
}

impl Witness {
    /// The synthetic spirit head digest `D` (32 bytes) — the content the gate
    /// authorizes. Deterministic so the run is reproducible.
    fn head_bytes() -> [u8; 32] {
        let mut bytes = [0u8; 32];
        let mut index = 0u8;
        while (index as usize) < bytes.len() {
            bytes[index as usize] = index.wrapping_mul(7).wrapping_add(13);
            index += 1;
        }
        bytes
    }

    fn from_environment() -> Self {
        let socket = std::env::var_os("CRIOME_SOCKET")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/run/criome/criome.sock"));
        eprintln!("criome-cluster-witness-test: socket={}", socket.display());
        Self {
            client: CriomeClient::new(&socket),
            policy: WitnessPolicy::new(),
        }
    }

    /// Seed the running criome daemon over the socket: register both identities
    /// and admit the contract. Returns the admitted contract digest.
    fn seed(&self) -> ContractDigest {
        for (identity, key) in [
            (&self.policy.signer_identity, &self.policy.signer_key),
            (
                &self.policy.timekeeper_identity,
                &self.policy.timekeeper_key,
            ),
        ] {
            let reply = self
                .client
                .send(CriomeRequest::RegisterIdentity(
                    WitnessPolicy::registration(identity, key),
                ))
                .expect("identity registration reaches criome over the socket");
            assert!(
                matches!(reply, CriomeReply::IdentityReceipt(_)),
                "identity registered, got {reply:?}"
            );
        }
        let reply = self
            .client
            .send(CriomeRequest::AdmitContract(WitnessPolicy::contract()))
            .expect("contract admission reaches criome over the socket");
        let CriomeReply::ContractAdmitted(admitted) = reply else {
            panic!("expected ContractAdmitted, got {reply:?}");
        };
        admitted.into_payload()
    }

    fn evaluate(&self, evaluation: AuthorizationEvaluation) -> EvaluationDecision {
        let reply = self
            .client
            .send(CriomeRequest::EvaluateAuthorization(evaluation))
            .expect("authorization evaluation reaches criome over the socket");
        let CriomeReply::AuthorizationEvaluated(evaluated) = reply else {
            panic!("expected AuthorizationEvaluated, got {reply:?}");
        };
        evaluated.decision
    }

    fn run(&self) {
        let contract = self.seed();
        eprintln!("criome-cluster-witness-test: seeded identities + 1-of-1 contract");

        let bytes = Self::head_bytes();
        let object = AuthorizedObjectReference {
            component: ComponentKind::Spirit,
            digest: ObjectDigest::from_bytes(&bytes),
            kind: AuthorizedObjectKind::Head,
        };
        let operation = OperationDigest::from_bytes(&bytes);

        // (a) AUTHORIZED — one valid signer signature satisfies threshold-1.
        let authorized = self.evaluate(AuthorizationEvaluation {
            contract: contract.clone(),
            object: object.clone(),
            evidence: self.policy.evidence(operation.clone(), 1),
        });
        assert!(
            matches!(authorized, EvaluationDecision::Authorized),
            "a satisfied 1-of-1 contract authorizes, got {authorized:?}"
        );
        eprintln!("criome-cluster-witness-test: PROOF (a) authorized head -> Authorized");

        // (b) REJECTED — threshold-short evidence (zero operation signatures).
        let rejected = self.evaluate(AuthorizationEvaluation {
            contract,
            object,
            evidence: self.policy.evidence(operation, 0),
        });
        assert!(
            !matches!(rejected, EvaluationDecision::Authorized),
            "threshold-short evidence must not authorize, got {rejected:?}"
        );
        eprintln!(
            "criome-cluster-witness-test: PROOF (b) threshold-short head -> {rejected:?} (not Authorized)"
        );

        println!("criome-cluster-witness-test: OK (authorized accepted, threshold-short rejected)");
    }
}

fn main() {
    Witness::from_environment().run();
}
