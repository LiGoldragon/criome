//! Criome policy-language evaluator over the schema-emitted signal contract.
//!
//! Public policy nouns live in `signal-criome`: `Contract`, `Rule`,
//! `PolicyMember`, `Evidence`, and `EvaluationDecision` are wire/schema types.
//! This module owns only daemon-local runtime mechanics: admission into the
//! content-addressed store, key lookup, and handwritten evaluation over those
//! generated nouns.

use signal_criome::{
    AttestedMoment, AttestedMomentDigestError, AttestedMomentProposition, BlsPublicKey, Contract,
    ContractAdmissionRejectionReason, ContractDigest, EvaluationDecision,
    EvaluationRejectionReason, Evidence, Identity, ObjectDigest, OperationDigest, PolicyMember,
    QuorumShortfall, RequiredSignatureThreshold, Rule, SignatureScheme, Threshold, TimestampNanos,
};

use crate::master_key::VerifyBls;

/// Store of admitted content-addressed contracts.
///
/// Admission rejects dangling references and malformed quorum declarations before
/// a contract can become addressable by digest.
#[derive(Debug, Clone, Default)]
pub struct ContractStore {
    entries: Vec<ContractEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractEntry {
    digest: ContractDigest,
    contract: Contract,
}

/// Maps an admitted identity to its admitted BLS public key.
#[derive(Debug, Clone, Default)]
pub struct KeyRegistry {
    entries: Vec<KeyEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEntry {
    identity: Identity,
    public_key: BlsPublicKey,
}

/// The canonical statement signed by a `SignedBy` leaf.
pub struct OperationStatement<'a> {
    signer: &'a Identity,
    operation: &'a OperationDigest,
    observed_at: &'a AttestedMoment,
}

/// The canonical statement signed by a time authority for an attested moment.
pub struct AttestedMomentStatement<'a> {
    proposition: &'a AttestedMomentProposition,
}

/// Evaluation errors are distinct from authorization denial. A denial is a valid
/// `EvaluationDecision::Rejected`; an error means the store cannot resolve the
/// contract graph.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EvaluationError {
    #[error("referenced contract not present in store: {0:?}")]
    MissingContract(ContractDigest),
}

#[derive(Debug, thiserror::Error)]
pub enum StatementError {
    #[error("attested moment digest: {0}")]
    AttestedMomentDigest(#[from] AttestedMomentDigestError),
}

/// Contract admission failure.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error("contract admission rejected: {0:?}")]
    Rejected(ContractAdmissionRejectionReason),
    #[error("contract digest: {0}")]
    Digest(#[from] signal_criome::ContractDigestError),
}

impl AdmissionError {
    pub fn rejected(reason: ContractAdmissionRejectionReason) -> Self {
        Self::Rejected(reason)
    }

    pub fn reason(&self) -> Option<&ContractAdmissionRejectionReason> {
        match self {
            Self::Rejected(reason) => Some(reason),
            Self::Digest(_) => None,
        }
    }
}

impl ContractStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn admit(&mut self, contract: Contract) -> Result<ContractDigest, AdmissionError> {
        ContractAdmission::new(&contract).validate_against(self)?;
        let digest = contract.digest()?;
        if !self.contains(&digest) {
            self.entries.push(ContractEntry {
                digest: digest.clone(),
                contract,
            });
        }
        Ok(digest)
    }

    pub fn resolve(&self, digest: &ContractDigest) -> Result<&Contract, EvaluationError> {
        self.entries
            .iter()
            .find(|entry| &entry.digest == digest)
            .map(|entry| &entry.contract)
            .ok_or_else(|| EvaluationError::MissingContract(digest.clone()))
    }

    pub fn evaluate(
        &self,
        digest: &ContractDigest,
        evidence: &Evidence,
        registry: &KeyRegistry,
    ) -> Result<EvaluationDecision, EvaluationError> {
        let contract = self.resolve(digest)?;
        if let Some(reason) = evidence.observed_at.rejection_reason(registry) {
            return Ok(EvaluationDecision::Rejected(reason));
        }
        contract.rule().decide(evidence, self, registry)
    }

    fn contains(&self, digest: &ContractDigest) -> bool {
        self.entries.iter().any(|entry| &entry.digest == digest)
    }

    fn evaluate_all(
        &self,
        references: &[ContractDigest],
        evidence: &Evidence,
        registry: &KeyRegistry,
    ) -> Result<Vec<EvaluationDecision>, EvaluationError> {
        references
            .iter()
            .map(|digest| self.evaluate(digest, evidence, registry))
            .collect()
    }
}

impl KeyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Admit or replace the public key for `identity`.
    pub fn admit(&mut self, identity: Identity, public_key: BlsPublicKey) {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.identity == identity)
        {
            entry.public_key = public_key;
        } else {
            self.entries.push(KeyEntry {
                identity,
                public_key,
            });
        }
    }

    pub fn public_key(&self, identity: &Identity) -> Option<&BlsPublicKey> {
        self.entries
            .iter()
            .find(|entry| &entry.identity == identity)
            .map(|entry| &entry.public_key)
    }
}

impl<'a> OperationStatement<'a> {
    pub fn new(
        signer: &'a Identity,
        operation: &'a OperationDigest,
        observed_at: &'a AttestedMoment,
    ) -> Self {
        Self {
            signer,
            operation,
            observed_at,
        }
    }

    pub fn to_signing_bytes(&self) -> Result<Vec<u8>, StatementError> {
        let mut bytes = b"CRIOME-OPERATION-AUTHORIZATION-V1".to_vec();
        self.signer.encode_into(&mut bytes);
        self.operation.object_digest().encode_into(&mut bytes);
        self.observed_at
            .proposition
            .digest()?
            .object_digest()
            .encode_into(&mut bytes);
        Ok(bytes)
    }
}

impl<'a> AttestedMomentStatement<'a> {
    pub fn new(proposition: &'a AttestedMomentProposition) -> Self {
        Self { proposition }
    }

    pub fn to_signing_bytes(&self) -> Result<Vec<u8>, StatementError> {
        let mut bytes = b"CRIOME-ATTESTED-MOMENT-V1".to_vec();
        self.proposition
            .digest()?
            .object_digest()
            .encode_into(&mut bytes);
        Ok(bytes)
    }
}

struct ContractAdmission<'a> {
    contract: &'a Contract,
}

impl<'a> ContractAdmission<'a> {
    fn new(contract: &'a Contract) -> Self {
        Self { contract }
    }

    fn validate_against(&self, store: &ContractStore) -> Result<(), AdmissionError> {
        for reference in self.contract.rule().referenced_digests() {
            if !store.contains(&reference) {
                return Err(AdmissionError::rejected(
                    ContractAdmissionRejectionReason::DanglingReference(reference),
                ));
            }
        }
        self.contract.rule().validate_shape()
    }
}

trait ContractRule {
    fn rule(&self) -> &Rule;
}

impl ContractRule for Contract {
    fn rule(&self) -> &Rule {
        self.payload()
    }
}

trait RuleEvaluation {
    fn decide(
        &self,
        evidence: &Evidence,
        store: &ContractStore,
        registry: &KeyRegistry,
    ) -> Result<EvaluationDecision, EvaluationError>;

    fn referenced_digests(&self) -> Vec<ContractDigest>;

    fn validate_shape(&self) -> Result<(), AdmissionError>;
}

impl RuleEvaluation for Rule {
    fn decide(
        &self,
        evidence: &Evidence,
        store: &ContractStore,
        registry: &KeyRegistry,
    ) -> Result<EvaluationDecision, EvaluationError> {
        match self {
            Self::SignedBy(identity) => Ok(evidence.decision_for_signature(identity, registry)),
            Self::All(references) => {
                EvaluationDecision::all(store.evaluate_all(references, evidence, registry)?)
            }
            Self::Any(references) => {
                EvaluationDecision::any(store.evaluate_all(references, evidence, registry)?)
            }
            Self::Threshold(threshold) => threshold.decide(evidence, store, registry),
            Self::ActiveAfter(timed_rule) => {
                if evidence.observed_at.closes_at().into_u64() >= timed_rule.boundary.into_u64() {
                    Ok(evidence.decision_for_signature(&timed_rule.signed_by, registry))
                } else {
                    Ok(EvaluationDecision::Rejected(
                        EvaluationRejectionReason::OutsideTimeWindow,
                    ))
                }
            }
            Self::ActiveUntil(timed_rule) => {
                if evidence.observed_at.closes_at().into_u64() < timed_rule.boundary.into_u64() {
                    Ok(evidence.decision_for_signature(&timed_rule.signed_by, registry))
                } else {
                    Ok(EvaluationDecision::Rejected(
                        EvaluationRejectionReason::OutsideTimeWindow,
                    ))
                }
            }
            Self::TimeSwitch(time_switch) => time_switch
                .active_threshold(evidence)
                .decide(evidence, store, registry),
            Self::Agreement(agreement) => Ok(evidence.decision_for_agreement(agreement, registry)),
            Self::EscalateToPsyche => Ok(EvaluationDecision::EscalateToPsyche),
        }
    }

    fn referenced_digests(&self) -> Vec<ContractDigest> {
        match self {
            Self::All(references) | Self::Any(references) => references.clone(),
            Self::Threshold(threshold) => threshold.referenced_digests(),
            Self::TimeSwitch(time_switch) => {
                let mut references = time_switch.before.referenced_digests();
                references.extend(time_switch.after.referenced_digests());
                references
            }
            Self::SignedBy(_)
            | Self::ActiveAfter(_)
            | Self::ActiveUntil(_)
            | Self::Agreement(_)
            | Self::EscalateToPsyche => Vec::new(),
        }
    }

    fn validate_shape(&self) -> Result<(), AdmissionError> {
        match self {
            Self::All(references) if references.is_empty() => Err(AdmissionError::rejected(
                ContractAdmissionRejectionReason::EmptyConjunction,
            )),
            Self::Any(references) if references.is_empty() => Err(AdmissionError::rejected(
                ContractAdmissionRejectionReason::EmptyDisjunction,
            )),
            Self::Threshold(threshold) => threshold.validate_shape(),
            Self::TimeSwitch(time_switch) => {
                time_switch.before.validate_shape()?;
                time_switch.after.validate_shape()
            }
            _ => Ok(()),
        }
    }
}

trait ThresholdEvaluation {
    fn decide(
        &self,
        evidence: &Evidence,
        store: &ContractStore,
        registry: &KeyRegistry,
    ) -> Result<EvaluationDecision, EvaluationError>;

    fn referenced_digests(&self) -> Vec<ContractDigest>;

    fn validate_shape(&self) -> Result<(), AdmissionError>;
}

impl ThresholdEvaluation for Threshold {
    fn decide(
        &self,
        evidence: &Evidence,
        store: &ContractStore,
        registry: &KeyRegistry,
    ) -> Result<EvaluationDecision, EvaluationError> {
        let mut satisfied_members: Vec<PolicyMember> = Vec::new();
        for member in &self.members {
            if member.is_satisfied(evidence, store, registry)?
                && !satisfied_members.contains(member)
            {
                satisfied_members.push(member.clone());
            }
        }
        let required = self.required_signatures.into_u16();
        let satisfied = satisfied_members.len() as u16;
        Ok(if satisfied >= required {
            EvaluationDecision::Authorized
        } else {
            EvaluationDecision::Rejected(EvaluationRejectionReason::QuorumShort(QuorumShortfall {
                required: RequiredSignatureThreshold::new(required.into()),
                satisfied: RequiredSignatureThreshold::new(satisfied.into()),
            }))
        })
    }

    fn referenced_digests(&self) -> Vec<ContractDigest> {
        self.members
            .iter()
            .filter_map(|member| match member {
                PolicyMember::ObjectMember(digest) => Some(digest.clone()),
                PolicyMember::KeyMember(_) => None,
            })
            .collect()
    }

    fn validate_shape(&self) -> Result<(), AdmissionError> {
        if self.members.is_empty() {
            return Err(AdmissionError::rejected(
                ContractAdmissionRejectionReason::EmptyThreshold,
            ));
        }
        let required = self.required_signatures.into_u16();
        if required == 0 || required > self.members.len() as u16 {
            return Err(AdmissionError::rejected(
                ContractAdmissionRejectionReason::ThresholdUnsatisfiable,
            ));
        }
        let mut unique_members: Vec<&PolicyMember> = Vec::new();
        for member in &self.members {
            if unique_members.contains(&member) {
                return Err(AdmissionError::rejected(
                    ContractAdmissionRejectionReason::DuplicatePolicyMember,
                ));
            }
            unique_members.push(member);
        }
        Ok(())
    }
}

trait TimeSwitchEvaluation {
    fn active_threshold<'a>(&'a self, evidence: &Evidence) -> &'a Threshold;
}

impl TimeSwitchEvaluation for signal_criome::TimeSwitch {
    fn active_threshold<'a>(&'a self, evidence: &Evidence) -> &'a Threshold {
        if evidence.observed_at.closes_at().into_u64() < self.boundary.into_u64() {
            &self.before
        } else {
            &self.after
        }
    }
}

trait PolicyMemberEvaluation {
    fn is_satisfied(
        &self,
        evidence: &Evidence,
        store: &ContractStore,
        registry: &KeyRegistry,
    ) -> Result<bool, EvaluationError>;
}

impl PolicyMemberEvaluation for PolicyMember {
    fn is_satisfied(
        &self,
        evidence: &Evidence,
        store: &ContractStore,
        registry: &KeyRegistry,
    ) -> Result<bool, EvaluationError> {
        match self {
            Self::KeyMember(identity) => Ok(evidence.has_valid_signature_from(identity, registry)),
            Self::ObjectMember(digest) => {
                Ok(store.evaluate(digest, evidence, registry)?.is_authorized())
            }
        }
    }
}

trait EvidenceVerification {
    fn decision_for_signature(
        &self,
        identity: &Identity,
        registry: &KeyRegistry,
    ) -> EvaluationDecision;

    fn decision_for_agreement(
        &self,
        agreement: &signal_criome::AgreementRule,
        registry: &KeyRegistry,
    ) -> EvaluationDecision;

    fn has_valid_signature_from(&self, identity: &Identity, registry: &KeyRegistry) -> bool;

    fn has_valid_agreement_for(
        &self,
        agreement: &signal_criome::AgreementRule,
        registry: &KeyRegistry,
    ) -> bool;
}

impl EvidenceVerification for Evidence {
    fn decision_for_signature(
        &self,
        identity: &Identity,
        registry: &KeyRegistry,
    ) -> EvaluationDecision {
        if self.has_valid_signature_from(identity, registry) {
            EvaluationDecision::Authorized
        } else {
            EvaluationDecision::Rejected(EvaluationRejectionReason::SignatureMissing(
                identity.clone(),
            ))
        }
    }

    fn decision_for_agreement(
        &self,
        agreement: &signal_criome::AgreementRule,
        registry: &KeyRegistry,
    ) -> EvaluationDecision {
        if self.has_valid_agreement_for(agreement, registry) {
            EvaluationDecision::Authorized
        } else {
            EvaluationDecision::Rejected(EvaluationRejectionReason::AgreementMissing)
        }
    }

    fn has_valid_signature_from(&self, identity: &Identity, registry: &KeyRegistry) -> bool {
        let Some(admitted_key) = registry.public_key(identity) else {
            return false;
        };
        let Ok(statement) = OperationStatement::new(identity, &self.operation, &self.observed_at)
            .to_signing_bytes()
        else {
            return false;
        };
        self.signatures.iter().any(|envelope| {
            matches!(envelope.scheme, SignatureScheme::Bls12_381MinPk)
                && &envelope.public_key == admitted_key
                && admitted_key.verify_bls(&envelope.signature, &statement)
        })
    }

    fn has_valid_agreement_for(
        &self,
        agreement: &signal_criome::AgreementRule,
        registry: &KeyRegistry,
    ) -> bool {
        let Some(resolver_key) = registry.public_key(&agreement.resolver) else {
            return false;
        };
        let statement = agreement.reconciliation_bytes();
        self.agreements.iter().any(|fact| {
            agreement.matches(fact)
                && matches!(fact.envelope.scheme, SignatureScheme::Bls12_381MinPk)
                && &fact.envelope.public_key == resolver_key
                && resolver_key.verify_bls(&fact.envelope.signature, &statement)
        })
    }
}

trait AttestedMomentVerification {
    fn closes_at(&self) -> TimestampNanos;

    fn rejection_reason(&self, registry: &KeyRegistry) -> Option<EvaluationRejectionReason>;
}

impl AttestedMomentVerification for AttestedMoment {
    fn closes_at(&self) -> TimestampNanos {
        self.proposition.window.closes_at
    }

    fn rejection_reason(&self, registry: &KeyRegistry) -> Option<EvaluationRejectionReason> {
        let authorities = &self.proposition.authorities;
        let required = self.proposition.required_signatures.into_u16();
        if self.proposition.window.opens_at.into_u64()
            >= self.proposition.window.closes_at.into_u64()
            || required == 0
            || required > authorities.len() as u16
            || DuplicateIdentityScan::new(authorities).has_duplicates()
        {
            return Some(EvaluationRejectionReason::InvalidTimeAttestation);
        }
        let Ok(statement) = AttestedMomentStatement::new(&self.proposition).to_signing_bytes()
        else {
            return Some(EvaluationRejectionReason::InvalidTimeAttestation);
        };
        let mut satisfied: Vec<Identity> = Vec::new();
        for signature in &self.signatures {
            if !authorities.contains(&signature.signer) || satisfied.contains(&signature.signer) {
                continue;
            }
            let Some(admitted_key) = registry.public_key(&signature.signer) else {
                continue;
            };
            if matches!(signature.envelope.scheme, SignatureScheme::Bls12_381MinPk)
                && &signature.envelope.public_key == admitted_key
                && admitted_key.verify_bls(&signature.envelope.signature, &statement)
            {
                satisfied.push(signature.signer.clone());
            }
        }
        if satisfied.len() as u16 >= required {
            None
        } else {
            Some(EvaluationRejectionReason::TimeQuorumShort(
                QuorumShortfall {
                    required: RequiredSignatureThreshold::new(required.into()),
                    satisfied: RequiredSignatureThreshold::new((satisfied.len() as u16).into()),
                },
            ))
        }
    }
}

struct DuplicateIdentityScan<'a> {
    identities: &'a [Identity],
}

impl<'a> DuplicateIdentityScan<'a> {
    fn new(identities: &'a [Identity]) -> Self {
        Self { identities }
    }

    fn has_duplicates(&self) -> bool {
        let mut seen: Vec<&Identity> = Vec::new();
        for identity in self.identities {
            if seen.contains(&identity) {
                return true;
            }
            seen.push(identity);
        }
        false
    }
}

trait EvaluationDecisionLogic {
    fn is_authorized(&self) -> bool;

    fn all(decisions: Vec<EvaluationDecision>) -> Result<EvaluationDecision, EvaluationError>;

    fn any(decisions: Vec<EvaluationDecision>) -> Result<EvaluationDecision, EvaluationError>;
}

impl EvaluationDecisionLogic for EvaluationDecision {
    fn is_authorized(&self) -> bool {
        matches!(self, Self::Authorized)
    }

    fn all(decisions: Vec<EvaluationDecision>) -> Result<EvaluationDecision, EvaluationError> {
        let mut saw_escalation = false;
        for decision in decisions {
            match decision {
                Self::Authorized => {}
                Self::Rejected(reason) => return Ok(Self::Rejected(reason)),
                Self::EscalateToPsyche => saw_escalation = true,
            }
        }
        if saw_escalation {
            Ok(Self::EscalateToPsyche)
        } else {
            Ok(Self::Authorized)
        }
    }

    fn any(decisions: Vec<EvaluationDecision>) -> Result<EvaluationDecision, EvaluationError> {
        let mut saw_escalation = false;
        let mut last_rejection = None;
        for decision in decisions {
            match decision {
                Self::Authorized => return Ok(Self::Authorized),
                Self::Rejected(reason) => last_rejection = Some(reason),
                Self::EscalateToPsyche => saw_escalation = true,
            }
        }
        if saw_escalation {
            Ok(Self::EscalateToPsyche)
        } else {
            Ok(Self::Rejected(last_rejection.unwrap_or(
                EvaluationRejectionReason::QuorumShort(QuorumShortfall {
                    required: RequiredSignatureThreshold::new(1),
                    satisfied: RequiredSignatureThreshold::new(0),
                }),
            )))
        }
    }
}

trait AgreementRuleVerification {
    fn reconciliation_bytes(&self) -> Vec<u8>;

    fn matches(&self, fact: &signal_criome::AgreementFact) -> bool;
}

impl AgreementRuleVerification for signal_criome::AgreementRule {
    fn reconciliation_bytes(&self) -> Vec<u8> {
        let mut bytes = b"CRIOME-RECONCILIATION-V1".to_vec();
        self.divergence.encode_into(&mut bytes);
        self.resolution.encode_into(&mut bytes);
        self.resolver.encode_into(&mut bytes);
        bytes
    }

    fn matches(&self, fact: &signal_criome::AgreementFact) -> bool {
        self.divergence == fact.divergence
            && self.resolution == fact.resolution
            && self.resolver == fact.resolver
    }
}

trait CanonicalBytes {
    fn encode_into(&self, bytes: &mut Vec<u8>);
}

impl CanonicalBytes for Identity {
    fn encode_into(&self, bytes: &mut Vec<u8>) {
        let (tag, name) = match self {
            Identity::Persona(name) => (0u8, name.as_str()),
            Identity::Agent(name) => (1u8, name.as_str()),
            Identity::Host(name) => (2u8, name.as_str()),
            Identity::Developer(name) => (3u8, name.as_str()),
            Identity::Cluster(name) => (4u8, name.as_str()),
        };
        bytes.push(tag);
        bytes.extend_from_slice(&(name.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name.as_bytes());
    }
}

impl CanonicalBytes for ObjectDigest {
    fn encode_into(&self, bytes: &mut Vec<u8>) {
        let text = self.as_str();
        bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
    }
}
