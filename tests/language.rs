use criome::language::{
    AdmissionError, ContractStore, EvaluationError, KeyRegistry, OperationStatement,
};
use criome::master_key::MasterKey;
use signal_criome::{
    AgreementFact, AgreementRule, BlsPublicKey, Contract, ContractAdmissionRejectionReason,
    ContractDigest, EvaluationDecision, EvaluationRejectionReason, Evidence, Identity,
    ObjectDigest, OperationDigest, PolicyMember, RequiredSignatureThreshold, Rule,
    SignatureEnvelope, SignatureScheme, Threshold, TimeSwitch, TimedRule, TimestampNanos,
};

struct Signer {
    identity: Identity,
    key: MasterKey,
}

impl Signer {
    fn developer(name: &str) -> Self {
        Self::new(Identity::developer(name.to_owned()))
    }

    fn cluster(name: &str) -> Self {
        Self::new(Identity::cluster(name.to_owned()))
    }

    fn new(identity: Identity) -> Self {
        Self {
            identity,
            key: MasterKey::generate().expect("test key"),
        }
    }

    fn identity(&self) -> Identity {
        self.identity.clone()
    }

    fn public_key(&self) -> BlsPublicKey {
        self.key.public_key()
    }

    fn sign_operation(&self, operation: &OperationDigest) -> SignatureEnvelope {
        SignatureEnvelope {
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: self.public_key(),
            signature: self
                .key
                .sign(&OperationStatement::new(&self.identity, operation).to_signing_bytes()),
        }
    }

    fn sign_reconciliation(&self, agreement: &AgreementRule) -> SignatureEnvelope {
        SignatureEnvelope {
            scheme: SignatureScheme::Bls12_381MinPk,
            public_key: self.public_key(),
            signature: self.key.sign(
                ReconciliationStatement::new(agreement)
                    .to_signing_bytes()
                    .as_slice(),
            ),
        }
    }
}

struct ReconciliationStatement<'a> {
    agreement: &'a AgreementRule,
}

impl<'a> ReconciliationStatement<'a> {
    fn new(agreement: &'a AgreementRule) -> Self {
        Self { agreement }
    }

    fn to_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"CRIOME-RECONCILIATION-V1".to_vec();
        self.agreement.divergence.as_str().encode_into(&mut bytes);
        self.agreement.resolution.as_str().encode_into(&mut bytes);
        self.agreement.resolver.encode_into(&mut bytes);
        bytes
    }
}

trait TestBytes {
    fn encode_into(&self, bytes: &mut Vec<u8>);
}

impl TestBytes for &str {
    fn encode_into(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&(self.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.as_bytes());
    }
}

impl TestBytes for Identity {
    fn encode_into(&self, bytes: &mut Vec<u8>) {
        let (tag, name) = match self {
            Identity::Persona(name) => (0u8, name.as_str()),
            Identity::Agent(name) => (1u8, name.as_str()),
            Identity::Host(name) => (2u8, name.as_str()),
            Identity::Developer(name) => (3u8, name.as_str()),
            Identity::Cluster(name) => (4u8, name.as_str()),
        };
        bytes.push(tag);
        name.encode_into(bytes);
    }
}

fn registry(signers: &[&Signer]) -> KeyRegistry {
    let mut registry = KeyRegistry::new();
    for signer in signers {
        registry.admit(signer.identity(), signer.public_key());
    }
    registry
}

fn moment(value: u64) -> TimestampNanos {
    TimestampNanos::new(value)
}

fn digest(value: &[u8]) -> ObjectDigest {
    ObjectDigest::from_bytes(value)
}

fn operation(value: &[u8]) -> OperationDigest {
    OperationDigest::from_bytes(value)
}

fn contract_digest(value: &[u8]) -> ContractDigest {
    ContractDigest::from_bytes(value)
}

fn evidence(operation: OperationDigest, observed_at: u64) -> Evidence {
    Evidence {
        operation,
        observed_at: moment(observed_at),
        signatures: Vec::new(),
        agreements: Vec::new(),
    }
}

fn signed_evidence(operation: OperationDigest, observed_at: u64, signers: &[&Signer]) -> Evidence {
    Evidence {
        operation: operation.clone(),
        observed_at: moment(observed_at),
        signatures: signers
            .iter()
            .map(|signer| signer.sign_operation(&operation))
            .collect(),
        agreements: Vec::new(),
    }
}

fn threshold(required: u64, members: Vec<PolicyMember>) -> Threshold {
    Threshold {
        required_signatures: RequiredSignatureThreshold::new(required),
        members,
    }
}

fn key_member(signer: &Signer) -> PolicyMember {
    PolicyMember::KeyMember(signer.identity())
}

fn object_member(digest: &ContractDigest) -> PolicyMember {
    PolicyMember::ObjectMember(digest.clone())
}

fn admitted(store: &mut ContractStore, contract: Contract) -> ContractDigest {
    store.admit(contract).expect("admit contract")
}

fn admission_reason(error: AdmissionError) -> ContractAdmissionRejectionReason {
    error.reason().expect("admission reason").clone()
}

#[test]
fn threshold_contract_accepts_only_enough_distinct_admitted_authorities() {
    let operator = Signer::developer("operator");
    let designer = Signer::developer("designer");
    let auditor = Signer::developer("auditor");
    let registry = registry(&[&operator, &designer, &auditor]);
    let operation = operation(b"merge policy");
    let mut store = ContractStore::new();
    let contract = Contract::new(Rule::Threshold(threshold(
        2,
        vec![
            key_member(&operator),
            key_member(&designer),
            key_member(&auditor),
        ],
    )));
    let digest = admitted(&mut store, contract);

    let one_signature = signed_evidence(operation.clone(), 10, &[&operator]);
    let duplicate_signature = Evidence {
        operation: operation.clone(),
        observed_at: moment(10),
        signatures: vec![
            operator.sign_operation(&operation),
            operator.sign_operation(&operation),
        ],
        agreements: Vec::new(),
    };
    let two_signatures = signed_evidence(operation, 10, &[&operator, &designer]);

    assert!(matches!(
        store.evaluate(&digest, &one_signature, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::QuorumShort(_)
        ))
    ));
    assert!(matches!(
        store.evaluate(&digest, &duplicate_signature, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::QuorumShort(_)
        ))
    ));
    assert_eq!(
        store.evaluate(&digest, &two_signatures, &registry),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn admission_rejects_duplicate_quorum_members_before_evaluation() {
    let operator = Signer::developer("operator");
    let mut store = ContractStore::new();
    let contract = Contract::new(Rule::Threshold(threshold(
        2,
        vec![key_member(&operator), key_member(&operator)],
    )));

    assert_eq!(
        admission_reason(store.admit(contract).expect_err("duplicate member")),
        ContractAdmissionRejectionReason::DuplicatePolicyMember
    );
}

#[test]
fn admission_rejects_dangling_object_references() {
    let mut store = ContractStore::new();
    let missing = contract_digest(b"missing policy");
    let contract = Contract::new(Rule::Threshold(threshold(1, vec![object_member(&missing)])));

    assert_eq!(
        admission_reason(store.admit(contract).expect_err("dangling reference")),
        ContractAdmissionRejectionReason::DanglingReference(missing)
    );
}

#[test]
fn object_members_reference_previously_admitted_contracts_by_digest() {
    let operator = Signer::developer("operator");
    let designer = Signer::developer("designer");
    let registry = registry(&[&operator, &designer]);
    let operation = operation(b"shared object member");
    let mut store = ContractStore::new();
    let operator_rule = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(operator.identity())),
    );
    let designer_rule = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(designer.identity())),
    );
    let parent = admitted(
        &mut store,
        Contract::new(Rule::Threshold(threshold(
            2,
            vec![object_member(&operator_rule), object_member(&designer_rule)],
        ))),
    );

    assert_eq!(
        store.evaluate(
            &parent,
            &signed_evidence(operation, 20, &[&operator, &designer]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn time_switch_changes_quorum_after_boundary() {
    let operator = Signer::developer("operator");
    let designer = Signer::developer("designer");
    let registry = registry(&[&operator, &designer]);
    let operation = operation(b"time switch");
    let mut store = ContractStore::new();
    let digest = admitted(
        &mut store,
        Contract::new(Rule::TimeSwitch(TimeSwitch {
            boundary: moment(100),
            before: threshold(1, vec![key_member(&operator), key_member(&designer)]),
            after: threshold(2, vec![key_member(&operator), key_member(&designer)]),
        })),
    );

    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(operation.clone(), 50, &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
    assert!(matches!(
        store.evaluate(
            &digest,
            &signed_evidence(operation.clone(), 150, &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::QuorumShort(_)
        ))
    ));
    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(operation, 150, &[&operator, &designer]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn active_after_rule_models_timelock_release() {
    let operator = Signer::developer("operator");
    let registry = registry(&[&operator]);
    let operation = operation(b"timelock");
    let mut store = ContractStore::new();
    let digest = admitted(
        &mut store,
        Contract::new(Rule::ActiveAfter(TimedRule {
            boundary: moment(100),
            signed_by: operator.identity(),
        })),
    );

    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(operation.clone(), 99, &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::OutsideTimeWindow
        ))
    );
    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(operation, 100, &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn agreement_rule_accepts_only_signed_matching_resolver_fact() {
    let resolver = Signer::cluster("model-governance-panel");
    let other = Signer::developer("single-reviewer");
    let registry = registry(&[&resolver, &other]);
    let operation = operation(b"fork agreement");
    let divergence = digest(b"network fork");
    let resolution = digest(b"chosen canonical branch");
    let agreement = AgreementRule {
        divergence: divergence.clone(),
        resolution: resolution.clone(),
        resolver: resolver.identity(),
    };
    let contract = Contract::new(Rule::Agreement(agreement.clone()));
    let mut store = ContractStore::new();
    let contract_digest = admitted(&mut store, contract);
    let matching_fact = AgreementFact {
        divergence: divergence.clone(),
        resolution: resolution.clone(),
        resolver: resolver.identity(),
        envelope: resolver.sign_reconciliation(&agreement),
    };
    let wrong_fact = AgreementFact {
        divergence,
        resolution: digest(b"other branch"),
        resolver: resolver.identity(),
        envelope: resolver.sign_reconciliation(&agreement),
    };
    let impostor_fact = AgreementFact {
        divergence: agreement.divergence.clone(),
        resolution,
        resolver: resolver.identity(),
        envelope: other.sign_reconciliation(&agreement),
    };

    let mut wrong = evidence(operation.clone(), 20);
    wrong.agreements.push(wrong_fact);
    let mut impostor = evidence(operation.clone(), 20);
    impostor.agreements.push(impostor_fact);
    let mut matching = evidence(operation, 20);
    matching.agreements.push(matching_fact);

    assert_eq!(
        store.evaluate(&contract_digest, &wrong, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::AgreementMissing
        ))
    );
    assert_eq!(
        store.evaluate(&contract_digest, &impostor, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::AgreementMissing
        ))
    );
    assert_eq!(
        store.evaluate(&contract_digest, &matching, &registry),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn explicit_policy_can_escalate_to_psyche() {
    let registry = KeyRegistry::new();
    let operation = operation(b"ambiguous contract");
    let mut store = ContractStore::new();
    let digest = admitted(&mut store, Contract::new(Rule::EscalateToPsyche));

    assert_eq!(
        store.evaluate(&digest, &evidence(operation, 10), &registry),
        Ok(EvaluationDecision::EscalateToPsyche)
    );
}

#[test]
fn all_composes_content_addressed_children_and_preserves_escalation() {
    let operator = Signer::developer("operator");
    let registry = registry(&[&operator]);
    let operation = operation(b"escalating all");
    let mut store = ContractStore::new();
    let signed = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(operator.identity())),
    );
    let escalation = admitted(&mut store, Contract::new(Rule::EscalateToPsyche));
    let parent = admitted(
        &mut store,
        Contract::new(Rule::All(vec![signed.clone(), escalation])),
    );

    assert!(matches!(
        store.evaluate(&parent, &evidence(operation.clone(), 10), &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::SignatureMissing(_)
        ))
    ));
    assert_eq!(
        store.evaluate(
            &parent,
            &signed_evidence(operation, 10, &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::EscalateToPsyche)
    );
}

#[test]
fn any_prefers_authorization_before_escalation() {
    let operator = Signer::developer("operator");
    let registry = registry(&[&operator]);
    let operation = operation(b"authorizing any");
    let mut store = ContractStore::new();
    let escalation = admitted(&mut store, Contract::new(Rule::EscalateToPsyche));
    let signed = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(operator.identity())),
    );
    let parent = admitted(
        &mut store,
        Contract::new(Rule::Any(vec![escalation, signed])),
    );

    assert_eq!(
        store.evaluate(&parent, &evidence(operation.clone(), 10), &registry),
        Ok(EvaluationDecision::EscalateToPsyche)
    );
    assert_eq!(
        store.evaluate(
            &parent,
            &signed_evidence(operation, 10, &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn missing_contract_is_evaluation_error_not_authorization_denial() {
    let store = ContractStore::new();
    let registry = KeyRegistry::new();
    let missing = contract_digest(b"not admitted");

    assert_eq!(
        store.evaluate(&missing, &evidence(operation(b"missing"), 1), &registry),
        Err(EvaluationError::MissingContract(missing))
    );
}

#[test]
fn contract_digest_is_content_stable_and_distinguishes_rules() {
    let first = Contract::new(Rule::SignedBy(Identity::developer("operator".to_owned())));
    let first_again = Contract::new(Rule::SignedBy(Identity::developer("operator".to_owned())));
    let second = Contract::new(Rule::SignedBy(Identity::developer("designer".to_owned())));

    assert_eq!(
        first.digest().expect("first digest"),
        first_again.digest().expect("first-again digest")
    );
    assert_ne!(
        first.digest().expect("first digest"),
        second.digest().expect("second digest")
    );
}

#[test]
fn schema_names_public_policy_surface() {
    let schema = include_str!("../schema/criome.language.schema");

    for construct in [
        "signal-criome",
        "Contract",
        "Rule",
        "PolicyMember",
        "Threshold",
        "Evidence",
        "SignatureEnvelope",
        "EvaluationDecision",
    ] {
        assert!(
            schema.contains(construct),
            "schema note should name {construct}"
        );
    }
}
