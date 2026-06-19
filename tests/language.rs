use criome::language::{
    AdmissionError, AttestedMomentStatement, ContractStore, EvaluationError, KeyRegistry,
    OperationStatement,
};
use criome::master_key::MasterKey;
use signal_criome::{
    AgreementFact, AgreementRule, AttestedMoment, AttestedMomentProposition, BlsPublicKey,
    ComponentKind, Contract, ContractAdmissionRejectionReason, ContractDigest, EvaluationDecision,
    EvaluationRejectionReason, Evidence, Identity, ObjectDigest, OperationDigest, PolicyMember,
    RequiredSignatureThreshold, Rule, SignatureEnvelope, SignatureScheme, StampedSignatureEnvelope,
    Threshold, TimeSignature, TimeSwitch, TimeWindow, TimedRule, TimestampNanos,
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

    fn sign_operation(
        &self,
        operation: &OperationDigest,
        stamp: &AttestedMoment,
    ) -> StampedSignatureEnvelope {
        StampedSignatureEnvelope {
            stamp: stamp.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.public_key(),
                signature: self.key.sign(
                    OperationStatement::new(&self.identity, operation, stamp)
                        .to_signing_bytes()
                        .expect("operation statement")
                        .as_slice(),
                ),
            },
        }
    }

    fn sign_moment(&self, proposition: &AttestedMomentProposition) -> TimeSignature {
        TimeSignature {
            signer: self.identity(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.public_key(),
                signature: self.key.sign(
                    AttestedMomentStatement::new(proposition)
                        .to_signing_bytes()
                        .expect("moment statement")
                        .as_slice(),
                ),
            },
        }
    }

    fn sign_reconciliation(
        &self,
        agreement: &AgreementRule,
        stamp: &AttestedMoment,
    ) -> StampedSignatureEnvelope {
        StampedSignatureEnvelope {
            stamp: stamp.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.public_key(),
                signature: self.key.sign(
                    ReconciliationStatement::new(agreement, stamp)
                        .to_signing_bytes()
                        .as_slice(),
                ),
            },
        }
    }
}

struct AttestedClock {
    authority: Signer,
}

impl AttestedClock {
    fn new() -> Self {
        Self {
            authority: Signer::cluster("timekeeper"),
        }
    }

    fn authority(&self) -> &Signer {
        &self.authority
    }

    fn moment(&self, opens_at: u64, closes_at: u64) -> AttestedMoment {
        self.moment_with_authorities(opens_at, closes_at, 1, vec![self.authority.identity()])
    }

    fn moment_with_authorities(
        &self,
        opens_at: u64,
        closes_at: u64,
        required_signatures: u64,
        authorities: Vec<Identity>,
    ) -> AttestedMoment {
        let proposition = AttestedMomentProposition::new(
            TimeWindow {
                opens_at: moment(opens_at),
                closes_at: moment(closes_at),
            },
            RequiredSignatureThreshold::new(required_signatures),
            authorities,
        );
        AttestedMoment::new(
            proposition.clone(),
            vec![self.authority.sign_moment(&proposition)],
        )
    }
}

struct ReconciliationStatement<'a> {
    agreement: &'a AgreementRule,
    stamp: &'a AttestedMoment,
}

impl<'a> ReconciliationStatement<'a> {
    fn new(agreement: &'a AgreementRule, stamp: &'a AttestedMoment) -> Self {
        Self { agreement, stamp }
    }

    fn to_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"CRIOME-RECONCILIATION-V1".to_vec();
        self.agreement.divergence.as_str().encode_into(&mut bytes);
        self.agreement.resolution.as_str().encode_into(&mut bytes);
        self.agreement.resolver.encode_into(&mut bytes);
        self.stamp
            .proposition
            .digest()
            .expect("stamp digest")
            .object_digest()
            .as_str()
            .encode_into(&mut bytes);
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

fn registry_with_clock(clock: &AttestedClock, signers: &[&Signer]) -> KeyRegistry {
    let mut registry = registry(signers);
    registry.admit(clock.authority().identity(), clock.authority().public_key());
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

fn evidence(operation: OperationDigest, stamp: AttestedMoment) -> Evidence {
    Evidence::new(
        ComponentKind::Spirit,
        operation,
        stamp,
        Vec::new(),
        Vec::new(),
    )
}

fn signed_evidence(
    operation: OperationDigest,
    stamp: AttestedMoment,
    signers: &[&Signer],
) -> Evidence {
    Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        stamp.clone(),
        signers
            .iter()
            .map(|signer| signer.sign_operation(&operation, &stamp))
            .collect(),
        Vec::new(),
    )
}

fn threshold(required: u64, members: Vec<PolicyMember>) -> Threshold {
    Threshold::new(RequiredSignatureThreshold::new(required), members)
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
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator, &designer, &auditor]);
    let operation = operation(b"merge policy");
    let stamp = clock.moment(10, 20);
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

    let one_signature = signed_evidence(operation.clone(), stamp.clone(), &[&operator]);
    let duplicate_signature = Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        stamp.clone(),
        vec![
            operator.sign_operation(&operation, &stamp),
            operator.sign_operation(&operation, &stamp),
        ],
        Vec::new(),
    );
    let two_signatures = signed_evidence(operation, stamp, &[&operator, &designer]);

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
fn invalid_time_attestation_rejects_before_policy_evaluation() {
    let operator = Signer::developer("operator");
    let other_timekeeper = Signer::cluster("backup-timekeeper");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator, &other_timekeeper]);
    let operation = operation(b"invalid time");
    let stamp = clock.moment_with_authorities(
        10,
        20,
        2,
        vec![clock.authority().identity(), other_timekeeper.identity()],
    );
    let mut store = ContractStore::new();
    let contract = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(operator.identity())),
    );
    let evidence = signed_evidence(operation, stamp, &[&operator]);

    assert!(matches!(
        store.evaluate(&contract, &evidence, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::TimeNotProven
        ))
    ));
}

#[test]
fn submajority_time_authority_rejects_attested_moment() {
    let operator = Signer::developer("operator");
    let first_timekeeper = Signer::cluster("first-timekeeper");
    let second_timekeeper = Signer::cluster("second-timekeeper");
    let third_timekeeper = Signer::cluster("third-timekeeper");
    let fourth_timekeeper = Signer::cluster("fourth-timekeeper");
    let fifth_timekeeper = Signer::cluster("fifth-timekeeper");
    let registry = registry(&[
        &operator,
        &first_timekeeper,
        &second_timekeeper,
        &third_timekeeper,
        &fourth_timekeeper,
        &fifth_timekeeper,
    ]);
    let proposition = AttestedMomentProposition::new(
        TimeWindow {
            opens_at: moment(10),
            closes_at: moment(20),
        },
        RequiredSignatureThreshold::new(2),
        vec![
            first_timekeeper.identity(),
            second_timekeeper.identity(),
            third_timekeeper.identity(),
            fourth_timekeeper.identity(),
            fifth_timekeeper.identity(),
        ],
    );
    let stamp = AttestedMoment::new(
        proposition.clone(),
        vec![
            first_timekeeper.sign_moment(&proposition),
            second_timekeeper.sign_moment(&proposition),
        ],
    );
    let operation = operation(b"submajority time authority");
    let mut store = ContractStore::new();
    let contract = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(operator.identity())),
    );
    let evidence = signed_evidence(operation, stamp, &[&operator]);

    assert_eq!(
        store.evaluate(&contract, &evidence, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::TimeNotProven
        ))
    );
}

#[test]
fn operation_signature_is_bound_to_the_attested_moment() {
    let operator = Signer::developer("operator");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator]);
    let operation = operation(b"moment-bound signature");
    let signed_moment = clock.moment(10, 20);
    let replayed_moment = clock.moment(20, 30);
    let mut store = ContractStore::new();
    let contract = admitted(
        &mut store,
        Contract::new(Rule::SignedBy(operator.identity())),
    );
    let evidence = Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        replayed_moment,
        vec![operator.sign_operation(&operation, &signed_moment)],
        Vec::new(),
    );

    assert!(matches!(
        store.evaluate(&contract, &evidence, &registry),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::SignatureMissing(_)
        ))
    ));
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
fn admission_rejects_submajority_thresholds_before_evaluation() {
    let operator = Signer::developer("operator");
    let designer = Signer::developer("designer");
    let auditor = Signer::developer("auditor");
    let reviewer = Signer::developer("reviewer");
    let maintainer = Signer::developer("maintainer");
    let mut store = ContractStore::new();
    let contract = Contract::new(Rule::Threshold(threshold(
        2,
        vec![
            key_member(&operator),
            key_member(&designer),
            key_member(&auditor),
            key_member(&reviewer),
            key_member(&maintainer),
        ],
    )));

    assert_eq!(
        admission_reason(store.admit(contract).expect_err("submajority threshold")),
        ContractAdmissionRejectionReason::ThresholdUnsatisfiable
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
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator, &designer]);
    let operation = operation(b"shared object member");
    let stamp = clock.moment(10, 20);
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
            &signed_evidence(operation, stamp, &[&operator, &designer]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn time_switch_changes_quorum_after_boundary() {
    let operator = Signer::developer("operator");
    let designer = Signer::developer("designer");
    let auditor = Signer::developer("auditor");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator, &designer, &auditor]);
    let operation = operation(b"time switch");
    let mut store = ContractStore::new();
    let digest = admitted(
        &mut store,
        Contract::new(Rule::TimeSwitch(TimeSwitch {
            boundary: moment(100),
            before: threshold(
                2,
                vec![
                    key_member(&operator),
                    key_member(&designer),
                    key_member(&auditor),
                ],
            ),
            after: threshold(
                3,
                vec![
                    key_member(&operator),
                    key_member(&designer),
                    key_member(&auditor),
                ],
            ),
        })),
    );

    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(
                operation.clone(),
                clock.moment(40, 50),
                &[&operator, &designer]
            ),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
    assert!(matches!(
        store.evaluate(
            &digest,
            &signed_evidence(
                operation.clone(),
                clock.moment(140, 150),
                &[&operator, &designer]
            ),
            &registry,
        ),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::QuorumShort(_)
        ))
    ));
    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(
                operation,
                clock.moment(140, 150),
                &[&operator, &designer, &auditor]
            ),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn active_after_rule_models_timelock_release() {
    let operator = Signer::developer("operator");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator]);
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
            &signed_evidence(operation.clone(), clock.moment(90, 99), &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::OutsideTimeWindow
        ))
    );
    assert_eq!(
        store.evaluate(
            &digest,
            &signed_evidence(operation, clock.moment(90, 100), &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn agreement_rule_accepts_only_signed_matching_resolver_fact() {
    let resolver = Signer::cluster("model-governance-panel");
    let other = Signer::developer("single-reviewer");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&resolver, &other]);
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
    let agreement_stamp = clock.moment(10, 20);
    let matching_fact = AgreementFact {
        divergence: divergence.clone(),
        resolution: resolution.clone(),
        resolver: resolver.identity(),
        signature: resolver.sign_reconciliation(&agreement, &agreement_stamp),
    };
    let wrong_fact = AgreementFact {
        divergence,
        resolution: digest(b"other branch"),
        resolver: resolver.identity(),
        signature: resolver.sign_reconciliation(&agreement, &agreement_stamp),
    };
    let impostor_fact = AgreementFact {
        divergence: agreement.divergence.clone(),
        resolution,
        resolver: resolver.identity(),
        signature: other.sign_reconciliation(&agreement, &agreement_stamp),
    };

    let wrong = Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        clock.moment(10, 20),
        Vec::new(),
        vec![wrong_fact],
    );
    let impostor = Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        clock.moment(10, 20),
        Vec::new(),
        vec![impostor_fact],
    );
    let matching = Evidence::new(
        ComponentKind::Spirit,
        operation,
        clock.moment(10, 20),
        Vec::new(),
        vec![matching_fact],
    );

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
fn agreement_signature_is_bound_to_its_attested_moment() {
    let resolver = Signer::cluster("model-governance-panel");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&resolver]);
    let operation = operation(b"stamp-bound agreement");
    let divergence = digest(b"network fork");
    let resolution = digest(b"chosen canonical branch");
    let agreement = AgreementRule {
        divergence: divergence.clone(),
        resolution: resolution.clone(),
        resolver: resolver.identity(),
    };
    let mut store = ContractStore::new();
    let contract_digest = admitted(
        &mut store,
        Contract::new(Rule::Agreement(agreement.clone())),
    );
    let signed_stamp = clock.moment(10, 20);
    let replayed_stamp = clock.moment(20, 30);
    let signed = resolver.sign_reconciliation(&agreement, &signed_stamp);
    let replayed_fact = AgreementFact {
        divergence: divergence.clone(),
        resolution: resolution.clone(),
        resolver: resolver.identity(),
        signature: StampedSignatureEnvelope {
            stamp: replayed_stamp,
            envelope: signed.envelope.clone(),
        },
    };
    let matching_fact = AgreementFact {
        divergence,
        resolution,
        resolver: resolver.identity(),
        signature: signed,
    };

    let replayed = Evidence::new(
        ComponentKind::Spirit,
        operation.clone(),
        clock.moment(10, 20),
        Vec::new(),
        vec![replayed_fact],
    );
    let matching = Evidence::new(
        ComponentKind::Spirit,
        operation,
        clock.moment(10, 20),
        Vec::new(),
        vec![matching_fact],
    );

    assert_eq!(
        store.evaluate(&contract_digest, &replayed, &registry),
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
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[]);
    let operation = operation(b"ambiguous contract");
    let mut store = ContractStore::new();
    let digest = admitted(&mut store, Contract::new(Rule::EscalateToPsyche));

    assert_eq!(
        store.evaluate(
            &digest,
            &evidence(operation, clock.moment(1, 10)),
            &registry
        ),
        Ok(EvaluationDecision::EscalateToPsyche)
    );
}

#[test]
fn all_composes_content_addressed_children_and_preserves_escalation() {
    let operator = Signer::developer("operator");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator]);
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
        store.evaluate(
            &parent,
            &evidence(operation.clone(), clock.moment(1, 10)),
            &registry
        ),
        Ok(EvaluationDecision::Rejected(
            EvaluationRejectionReason::SignatureMissing(_)
        ))
    ));
    assert_eq!(
        store.evaluate(
            &parent,
            &signed_evidence(operation, clock.moment(1, 10), &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::EscalateToPsyche)
    );
}

#[test]
fn any_prefers_authorization_before_escalation() {
    let operator = Signer::developer("operator");
    let clock = AttestedClock::new();
    let registry = registry_with_clock(&clock, &[&operator]);
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
        store.evaluate(
            &parent,
            &evidence(operation.clone(), clock.moment(1, 10)),
            &registry
        ),
        Ok(EvaluationDecision::EscalateToPsyche)
    );
    assert_eq!(
        store.evaluate(
            &parent,
            &signed_evidence(operation, clock.moment(1, 10), &[&operator]),
            &registry,
        ),
        Ok(EvaluationDecision::Authorized)
    );
}

#[test]
fn missing_contract_is_evaluation_error_not_authorization_denial() {
    let clock = AttestedClock::new();
    let store = ContractStore::new();
    let registry = registry_with_clock(&clock, &[]);
    let missing = contract_digest(b"not admitted");

    assert_eq!(
        store.evaluate(
            &missing,
            &evidence(operation(b"missing"), clock.moment(0, 1)),
            &registry
        ),
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
