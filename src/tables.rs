use std::path::{Path, PathBuf};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema_engine::{
    Engine, EngineOpen, EngineStoredValue, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan,
    RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_criome::{
    ActiveInterceptPolicies, ApprovalAuditSource, Attestation, AttestedMomentProposition,
    AuthorizationDenial, AuthorizationEvaluation, AuthorizationGrant, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, AuthorizedObjectReference, BlsPublicKey,
    Contract, ContractDigest, ContractOperationHead, ExpiryAction, Identity, IdentityReceipt,
    IdentityRegistration, IdentityRevocation, InterceptPolicies, InterceptPolicy,
    InterceptPolicyIdentifier, InterceptPolicyProposal, InterceptPolicyWindow, KeyPurpose,
    ObjectDigest, ParkedRequestAnswer, ParkedRequestDecision, ParkedRequestIdentifier,
    ParkedRequestOutcome, ParkedRequestQuery, ParkedRequestResolution, ParkedRequestSnapshot,
    ParkedSpiritRequest, ParkedSpiritRequests, PolicyOverlapMode, PrincipalName, PrincipalStatus,
    PublicKeyFingerprint, QuorumRoundIdentifier, QuorumVote, ReplayNonce, RootAnchorDigest,
    RootGenesis, SignatureSolicitationRoute, SignatureSubmission, SpiritAuthorizationContext,
    TimestampNanos,
};

use crate::Result;
use crate::founding::RootFounding;

// v5 re-serialises the `contracts` table for the parent-bearing `Contract`
// (`{ rule, parent }`, was the tuple `Contract(Rule)`) and adds the founding
// tables. The version is woven into every family's `SchemaHash`, so a store
// carrying pre-parent (`v4`) rows is REFUSED at open with a family-identity
// mismatch rather than silently mis-decoded: clean genesis, no re-digest
// migration. The founding ceremony then writes the first parent-bearing
// contracts into a fresh namespace. The test VMs hold nothing to preserve.
const CRIOME_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(6);
const IDENTITIES: TableName = TableName::new("identities");
const REVOCATIONS: TableName = TableName::new("revocations");
const ATTESTATIONS: TableName = TableName::new("attestations");
const AUTHORIZATION_STATES: TableName = TableName::new("authorization_requests");
const AUTHORIZATION_REPLAY_NONCES: TableName = TableName::new("authorization_replay_nonces");
const CONTRACTS: TableName = TableName::new("contracts");
const SIGNATURE_SOLICITATIONS: TableName = TableName::new("signature_solicitations");
const SUBMITTED_SIGNATURES: TableName = TableName::new("submitted_signatures");
const QUORUM_ROUNDS: TableName = TableName::new("quorum_rounds");
const CO_SIGNED_SUCCESSORS: TableName = TableName::new("co_signed_successors");
const CONTRACT_HEADS: TableName = TableName::new("contract_heads");
const ROOT_FOUNDING: TableName = TableName::new("root_founding");
const PENDING_FOUNDINGS: TableName = TableName::new("pending_foundings");
const INTERCEPT_POLICIES: TableName = TableName::new("intercept_policies");
const PARKED_SPIRIT_REQUESTS: TableName = TableName::new("parked_spirit_requests");
const ATTESTATION_NEXT_SLOT: TableName = TableName::new("attestation_next_slot");
const ATTESTATION_NEXT_SLOT_KEY: &str = "next";
const AUTHORIZATION_NEXT_SLOT: TableName = TableName::new("authorization_next_slot");
const AUTHORIZATION_NEXT_SLOT_KEY: &str = "next";
const INTERCEPT_POLICY_NEXT_SLOT: TableName = TableName::new("intercept_policy_next_slot");
const INTERCEPT_POLICY_NEXT_SLOT_KEY: &str = "next";
const PARKED_SPIRIT_REQUEST_NEXT_SLOT: TableName =
    TableName::new("parked_spirit_request_next_slot");
const PARKED_SPIRIT_REQUEST_NEXT_SLOT_KEY: &str = "next";
const IDENTITIES_FAMILY: &str = "criome-identity";
const REVOCATIONS_FAMILY: &str = "criome-revocation";
const ATTESTATIONS_FAMILY: &str = "criome-attestation";
const AUTHORIZATION_STATES_FAMILY: &str = "criome-authorization-state";
const AUTHORIZATION_REPLAY_NONCES_FAMILY: &str = "criome-authorization-replay-nonce";
const CONTRACTS_FAMILY: &str = "criome-contract";
const SIGNATURE_SOLICITATIONS_FAMILY: &str = "criome-signature-solicitation";
const SUBMITTED_SIGNATURES_FAMILY: &str = "criome-submitted-signature";
const QUORUM_ROUNDS_FAMILY: &str = "criome-quorum-round";
// This node's durable anti-equivocation ledger: the one successor it co-signed
// per `(contract, head)` state-point, and each contract's committed head. Both
// are the persistent projection of an in-memory map so the single-successor veto
// and the head cursor survive a restart (a cleared map would let a node co-sign a
// conflicting successor across the boot, or refuse a valid one as a stale-head
// conflict). Own family/hash, additive like `pending_foundings`.
const CO_SIGNED_SUCCESSORS_FAMILY: &str = "criome-co-signed-successor";
const CONTRACT_HEADS_FAMILY: &str = "criome-contract-head";
const ROOT_FOUNDING_FAMILY: &str = "criome-root-founding";
// A node founds at most one root, so the `root_founding` table is a singleton
// keyed under a constant slot.
const ROOT_FOUNDING_KEY: &str = "founded";
// Foundings this node is party to but has not yet founded: peer proposals
// awaiting an owner accept, and this node's own initiated gathering. Keyed by
// the founding's self-certifying anchor, so a node can be party to more than one
// candidate at a time without clobbering the single `root_founding` record.
const PENDING_FOUNDINGS_FAMILY: &str = "criome-pending-founding";
const INTERCEPT_POLICIES_FAMILY: &str = "criome-intercept-policy";
const PARKED_SPIRIT_REQUESTS_FAMILY: &str = "criome-parked-spirit-request";
const ATTESTATION_NEXT_SLOT_FAMILY: &str = "criome-attestation-slot";
const AUTHORIZATION_NEXT_SLOT_FAMILY: &str = "criome-authorization-slot";
const INTERCEPT_POLICY_NEXT_SLOT_FAMILY: &str = "criome-intercept-policy-slot";
const PARKED_SPIRIT_REQUEST_NEXT_SLOT_FAMILY: &str = "criome-parked-spirit-request-slot";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreLocation {
    path: PathBuf,
}

impl StoreLocation {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn from_environment() -> Self {
        match std::env::var_os("CRIOME_STORE") {
            Some(path) => Self::new(path),
            None => match std::env::var_os("PERSONA_STATE_PATH") {
                Some(path) => Self::new(path),
                None => Self::new("/tmp/criome.sema"),
            },
        }
    }

    pub fn as_path(&self) -> &Path {
        self.path.as_path()
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredIdentity {
    identity: Identity,
    public_key: BlsPublicKey,
    fingerprint: PublicKeyFingerprint,
    purpose: KeyPurpose,
    status: PrincipalStatus,
}

impl StoredIdentity {
    pub fn active(registration: IdentityRegistration) -> Self {
        Self {
            identity: registration.identity,
            public_key: registration.bls_public_key,
            fingerprint: registration.public_key_fingerprint,
            purpose: registration.key_purpose,
            status: PrincipalStatus::Active,
        }
    }

    pub fn revoked(mut self) -> Self {
        self.status = PrincipalStatus::Revoked;
        self
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn public_key(&self) -> &BlsPublicKey {
        &self.public_key
    }

    pub fn fingerprint(&self) -> &PublicKeyFingerprint {
        &self.fingerprint
    }

    pub const fn purpose(&self) -> KeyPurpose {
        self.purpose
    }

    pub const fn status(&self) -> PrincipalStatus {
        self.status
    }

    pub fn receipt(&self) -> IdentityReceipt {
        IdentityReceipt {
            identity: self.identity.clone(),
            principal_status: self.status,
        }
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRevocation {
    identity: Identity,
    fingerprint: PublicKeyFingerprint,
    reason: PrincipalName,
}

impl StoredRevocation {
    pub fn new(revocation: IdentityRevocation) -> Self {
        Self {
            identity: revocation.identity,
            fingerprint: revocation.public_key_fingerprint,
            reason: revocation.principal_name,
        }
    }

    pub fn identity(&self) -> &Identity {
        &self.identity
    }

    pub fn fingerprint(&self) -> &PublicKeyFingerprint {
        &self.fingerprint
    }

    pub fn reason(&self) -> &PrincipalName {
        &self.reason
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredAttestation {
    slot: u64,
    attestation: Attestation,
}

impl StoredAttestation {
    pub fn new(slot: u64, attestation: Attestation) -> Self {
        Self { slot, attestation }
    }

    pub const fn slot(&self) -> u64 {
        self.slot
    }

    pub fn attestation(&self) -> &Attestation {
        &self.attestation
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredAuthorizationState {
    state: AuthorizationStateRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationStateDraft {
    pub(crate) request_digest: ObjectDigest,
    pub(crate) status: AuthorizationStatus,
    pub(crate) missing_authorities: Vec<Identity>,
    pub(crate) grant: Option<AuthorizationGrant>,
    pub(crate) denial: Option<AuthorizationDenial>,
    pub(crate) parked_evaluation: Option<AuthorizationEvaluation>,
    pub(crate) signal_authorization: Option<signal_criome::SignalCallAuthorization>,
    pub(crate) replay_identity: Option<AuthorizationReplayIdentity>,
}

impl StoredAuthorizationState {
    pub fn new(state: AuthorizationStateRecord) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &AuthorizationStateRecord {
        &self.state
    }

    pub fn into_state(self) -> AuthorizationStateRecord {
        self.state
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredSignatureSolicitation {
    route: SignatureSolicitationRoute,
}

impl StoredSignatureSolicitation {
    pub fn new(route: SignatureSolicitationRoute) -> Self {
        Self { route }
    }

    pub fn route(&self) -> &SignatureSolicitationRoute {
        &self.route
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredSignatureSubmission {
    submission: SignatureSubmission,
}

/// A durable quorum-collection round: the withheld pending row for the
/// propose→gather→judge protocol. It holds the proposal (contract, object, and
/// the shared moment proposition) and every gathered `QuorumVote` (the
/// originator's self-vote plus each peer member's). The assembled `Evidence` is
/// derived from these votes at judge time — the votes are the source of truth,
/// so redelivery of a vote is idempotent.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredQuorumRound {
    round: QuorumRoundIdentifier,
    contract: ContractDigest,
    object: AuthorizedObjectReference,
    proposition: AttestedMomentProposition,
    votes: Vec<QuorumVote>,
}

impl StoredQuorumRound {
    pub fn open(
        round: QuorumRoundIdentifier,
        contract: ContractDigest,
        object: AuthorizedObjectReference,
        proposition: AttestedMomentProposition,
    ) -> Self {
        Self {
            round,
            contract,
            object,
            proposition,
            votes: Vec::new(),
        }
    }

    pub fn round(&self) -> &QuorumRoundIdentifier {
        &self.round
    }

    pub fn contract(&self) -> &ContractDigest {
        &self.contract
    }

    pub fn object(&self) -> &AuthorizedObjectReference {
        &self.object
    }

    pub fn proposition(&self) -> &AttestedMomentProposition {
        &self.proposition
    }

    pub fn votes(&self) -> &[QuorumVote] {
        self.votes.as_slice()
    }

    /// Record `vote`, replacing any earlier vote from the same member. A member
    /// votes once; redelivery updates in place rather than double-counting.
    pub fn record_vote(&mut self, vote: QuorumVote) {
        if let Some(existing) = self
            .votes
            .iter_mut()
            .find(|held| held.identity == vote.identity)
        {
            *existing = vote;
        } else {
            self.votes.push(vote);
        }
    }
}

/// This node's durable record of the one successor it co-signed for a
/// `(contract, head)` state-point — the persistent projection of the in-memory
/// anti-equivocation ledger. Persisting it means a node that co-signed successor
/// S1 from head H cannot, after a restart that clears the in-memory map, co-sign a
/// conflicting S2 from the same H: the single-successor veto survives the boot.
/// `head` rides alongside `object` so the loader rebuilds the exact state-point
/// key without parsing it back out of the stored key string.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredCoSignedSuccessor {
    contract: ContractDigest,
    head: ContractOperationHead,
    object: AuthorizedObjectReference,
}

impl StoredCoSignedSuccessor {
    pub fn new(
        contract: ContractDigest,
        head: ContractOperationHead,
        object: AuthorizedObjectReference,
    ) -> Self {
        Self {
            contract,
            head,
            object,
        }
    }

    pub fn contract(&self) -> &ContractDigest {
        &self.contract
    }

    pub fn head(&self) -> &ContractOperationHead {
        &self.head
    }

    pub fn object(&self) -> &AuthorizedObjectReference {
        &self.object
    }
}

/// This node's durable view of a contract's current committed head — the
/// persistent projection of the in-memory head cursor. Persisting it means the
/// head a commit advanced to survives a restart, so a successor from that head is
/// not later mistaken for a conflicting successor from genesis (which would wedge
/// the cluster exactly as a stale peer head does at runtime).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredContractHead {
    contract: ContractDigest,
    head: ContractOperationHead,
}

impl StoredContractHead {
    pub fn new(contract: ContractDigest, head: ContractOperationHead) -> Self {
        Self { contract, head }
    }

    pub fn contract(&self) -> &ContractDigest {
        &self.contract
    }

    pub fn head(&self) -> &ContractOperationHead {
        &self.head
    }
}

/// A founding this node is party to but has not yet founded: a peer's proposal
/// awaiting an owner accept, or this node's own initiated gathering. The full
/// `genesis` is retained so an owner can accept by anchor alone, and `initiator`
/// names the node to return this node's signature to on accept (this node itself
/// for a locally initiated founding, in which case no signature is conveyed).
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredPendingFounding {
    anchor: RootAnchorDigest,
    genesis: RootGenesis,
    initiator: Identity,
}

impl StoredPendingFounding {
    pub fn new(anchor: RootAnchorDigest, genesis: RootGenesis, initiator: Identity) -> Self {
        Self {
            anchor,
            genesis,
            initiator,
        }
    }

    pub fn anchor(&self) -> &RootAnchorDigest {
        &self.anchor
    }

    pub fn genesis(&self) -> &RootGenesis {
        &self.genesis
    }

    pub fn initiator(&self) -> &Identity {
        &self.initiator
    }

    pub fn into_parts(self) -> (RootAnchorDigest, RootGenesis, Identity) {
        (self.anchor, self.genesis, self.initiator)
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredContract {
    digest: ContractDigest,
    contract: Contract,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredInterceptPolicy {
    policy: InterceptPolicy,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkedSpiritRequestStatus {
    Parked,
    Resolved,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredParkedSpiritRequest {
    request: ParkedSpiritRequest,
    status: ParkedSpiritRequestStatus,
    resolution: Option<ParkedRequestResolution>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterceptPolicyStorageMode {
    Create,
    Replace,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterceptPolicyDraft {
    proposal: InterceptPolicyProposal,
    stored_at: TimestampNanos,
    mode: InterceptPolicyStorageMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterceptMatch {
    policy: InterceptPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorizationReplayIdentity {
    requester: Identity,
    nonce: ReplayNonce,
}

impl StoredSignatureSubmission {
    pub fn new(submission: SignatureSubmission) -> Self {
        Self { submission }
    }

    pub fn submission(&self) -> &SignatureSubmission {
        &self.submission
    }
}

impl AuthorizationReplayIdentity {
    pub fn new(requester: Identity, nonce: ReplayNonce) -> Self {
        Self { requester, nonce }
    }
}

impl StoredContract {
    pub fn new(digest: ContractDigest, contract: Contract) -> Self {
        Self { digest, contract }
    }

    pub fn digest(&self) -> &ContractDigest {
        &self.digest
    }

    pub fn contract(&self) -> &Contract {
        &self.contract
    }

    pub fn into_parts(self) -> (ContractDigest, Contract) {
        (self.digest, self.contract)
    }
}

impl StoredInterceptPolicy {
    pub fn new(policy: InterceptPolicy) -> Self {
        Self { policy }
    }

    pub fn from_draft(identifier: InterceptPolicyIdentifier, draft: InterceptPolicyDraft) -> Self {
        let expires_at = TimestampNanos::new(
            draft
                .stored_at
                .into_u64()
                .saturating_add(draft.proposal.policy_duration_nanos.into_u64()),
        );
        Self {
            policy: InterceptPolicy {
                intercept_policy_identifier: identifier,
                mentci_session_slot: draft.proposal.mentci_session_slot,
                intercept_target_selector: draft.proposal.intercept_target_selector,
                spirit_operation_names: draft.proposal.spirit_operation_names,
                intercept_policy_window: InterceptPolicyWindow {
                    starts_at: draft.stored_at,
                    expires_at,
                },
                expiry_action: draft.proposal.expiry_action,
                policy_priority: draft.proposal.policy_priority,
            },
        }
    }

    pub fn policy(&self) -> &InterceptPolicy {
        &self.policy
    }

    pub fn into_policy(self) -> InterceptPolicy {
        self.policy
    }

    pub fn identifier(&self) -> &InterceptPolicyIdentifier {
        &self.policy.intercept_policy_identifier
    }

    pub fn active_at(&self, now: TimestampNanos) -> bool {
        self.policy.intercept_policy_window.starts_at.into_u64() <= now.into_u64()
            && now.into_u64() < self.policy.intercept_policy_window.expires_at.into_u64()
    }

    pub fn matches_context(
        &self,
        context: &SpiritAuthorizationContext,
        now: TimestampNanos,
    ) -> bool {
        self.active_at(now)
            && self.policy.intercept_target_selector.payload() == &context.spirit_process_key
            && self
                .policy
                .spirit_operation_names
                .names()
                .iter()
                .any(|name| name == &context.spirit_operation_name)
    }

    pub fn same_priority_overlap(&self, other: &Self) -> bool {
        self.policy.policy_priority == other.policy.policy_priority
            && self.policy.intercept_target_selector == other.policy.intercept_target_selector
            && self.windows_overlap(other)
            && self.operation_names_overlap(other)
    }

    fn windows_overlap(&self, other: &Self) -> bool {
        self.policy.intercept_policy_window.starts_at.into_u64()
            < other.policy.intercept_policy_window.expires_at.into_u64()
            && other.policy.intercept_policy_window.starts_at.into_u64()
                < self.policy.intercept_policy_window.expires_at.into_u64()
    }

    fn operation_names_overlap(&self, other: &Self) -> bool {
        self.policy
            .spirit_operation_names
            .names()
            .iter()
            .any(|left| {
                other
                    .policy
                    .spirit_operation_names
                    .names()
                    .iter()
                    .any(|right| left == right)
            })
    }
}

impl StoredParkedSpiritRequest {
    pub fn parked(request: ParkedSpiritRequest) -> Self {
        Self {
            request,
            status: ParkedSpiritRequestStatus::Parked,
            resolution: None,
        }
    }

    pub fn request(&self) -> &ParkedSpiritRequest {
        &self.request
    }

    pub const fn status(&self) -> ParkedSpiritRequestStatus {
        self.status
    }

    pub fn resolution(&self) -> Option<&ParkedRequestResolution> {
        self.resolution.as_ref()
    }

    pub fn is_active(&self) -> bool {
        self.status == ParkedSpiritRequestStatus::Parked
    }

    pub fn matches_query(&self, query: &ParkedRequestQuery) -> bool {
        self.is_active()
            && query
                .optional_mentci_session_slot
                .as_ref()
                .is_none_or(|slot| slot == &self.request.mentci_session_slot)
            && query
                .optional_intercept_target_selector
                .as_ref()
                .is_none_or(|target| {
                    target.payload()
                        == &self.request.spirit_authorization_context.spirit_process_key
                })
    }

    pub fn resolve_manual(
        self,
        decision: ParkedRequestDecision,
        resolved_at: TimestampNanos,
    ) -> Self {
        let outcome = match decision {
            ParkedRequestDecision::Approve => ParkedRequestOutcome::Approved,
            ParkedRequestDecision::Reject => ParkedRequestOutcome::Rejected,
        };
        self.resolve(outcome, ApprovalAuditSource::Manual, resolved_at)
    }

    pub fn apply_expiry(self, now: TimestampNanos) -> Self {
        if !self.is_active() || now.into_u64() < self.request.expires_at.into_u64() {
            return self;
        }
        match self.request.expiry_action {
            ExpiryAction::AutoApprove => self.resolve(
                ParkedRequestOutcome::Approved,
                ApprovalAuditSource::Automatic,
                now,
            ),
            ExpiryAction::AutoReject => self.resolve(
                ParkedRequestOutcome::Rejected,
                ApprovalAuditSource::Automatic,
                now,
            ),
            ExpiryAction::LeaveParked => self,
        }
    }

    fn resolve(
        mut self,
        outcome: ParkedRequestOutcome,
        audit_source: ApprovalAuditSource,
        resolved_at: TimestampNanos,
    ) -> Self {
        self.status = ParkedSpiritRequestStatus::Resolved;
        self.resolution = Some(ParkedRequestResolution {
            parked_request_identifier: self.request.parked_request_identifier.clone(),
            intercept_policy_identifier: self.request.intercept_policy_identifier.clone(),
            parked_request_outcome: outcome,
            approval_audit_source: audit_source,
            timestamp_nanos: resolved_at,
        });
        self
    }
}

impl InterceptPolicyDraft {
    pub fn create(proposal: InterceptPolicyProposal, stored_at: TimestampNanos) -> Self {
        Self {
            proposal,
            stored_at,
            mode: InterceptPolicyStorageMode::Create,
        }
    }

    pub fn replace(proposal: InterceptPolicyProposal, stored_at: TimestampNanos) -> Self {
        Self {
            proposal,
            stored_at,
            mode: InterceptPolicyStorageMode::Replace,
        }
    }

    pub fn overlap_mode(&self) -> PolicyOverlapMode {
        match self.mode {
            InterceptPolicyStorageMode::Create => self.proposal.policy_overlap_mode,
            InterceptPolicyStorageMode::Replace => PolicyOverlapMode::ReplaceSamePriorityOverlap,
        }
    }
}

impl InterceptMatch {
    pub fn new(policy: InterceptPolicy) -> Self {
        Self { policy }
    }

    pub fn into_policy(self) -> InterceptPolicy {
        self.policy
    }
}

pub struct CriomeTables {
    engine: Engine,
    identities: TableReference<StoredIdentity>,
    revocations: TableReference<StoredRevocation>,
    attestations: TableReference<StoredAttestation>,
    authorization_states: TableReference<StoredAuthorizationState>,
    authorization_replay_nonces: TableReference<AuthorizationRequestSlot>,
    contracts: TableReference<StoredContract>,
    signature_solicitations: TableReference<StoredSignatureSolicitation>,
    submitted_signatures: TableReference<StoredSignatureSubmission>,
    quorum_rounds: TableReference<StoredQuorumRound>,
    co_signed_successors: TableReference<StoredCoSignedSuccessor>,
    contract_heads: TableReference<StoredContractHead>,
    root_founding: TableReference<RootFounding>,
    pending_foundings: TableReference<StoredPendingFounding>,
    intercept_policies: TableReference<StoredInterceptPolicy>,
    parked_spirit_requests: TableReference<StoredParkedSpiritRequest>,
    attestation_next_slot: TableReference<u64>,
    authorization_next_slot: TableReference<u64>,
    intercept_policy_next_slot: TableReference<u64>,
    parked_spirit_request_next_slot: TableReference<u64>,
}

impl CriomeTables {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(Self::engine_open(store))?;
        let identities =
            engine.register_table(Self::family_descriptor(IDENTITIES, IDENTITIES_FAMILY))?;
        let revocations =
            engine.register_table(Self::family_descriptor(REVOCATIONS, REVOCATIONS_FAMILY))?;
        let attestations =
            engine.register_table(Self::family_descriptor(ATTESTATIONS, ATTESTATIONS_FAMILY))?;
        let authorization_states = engine.register_table(Self::family_descriptor(
            AUTHORIZATION_STATES,
            AUTHORIZATION_STATES_FAMILY,
        ))?;
        let authorization_replay_nonces = engine.register_table(Self::family_descriptor(
            AUTHORIZATION_REPLAY_NONCES,
            AUTHORIZATION_REPLAY_NONCES_FAMILY,
        ))?;
        let contracts =
            engine.register_table(Self::family_descriptor(CONTRACTS, CONTRACTS_FAMILY))?;
        let signature_solicitations = engine.register_table(Self::family_descriptor(
            SIGNATURE_SOLICITATIONS,
            SIGNATURE_SOLICITATIONS_FAMILY,
        ))?;
        let submitted_signatures = engine.register_table(Self::family_descriptor(
            SUBMITTED_SIGNATURES,
            SUBMITTED_SIGNATURES_FAMILY,
        ))?;
        let quorum_rounds =
            engine.register_table(Self::family_descriptor(QUORUM_ROUNDS, QUORUM_ROUNDS_FAMILY))?;
        let co_signed_successors = engine.register_table(Self::family_descriptor(
            CO_SIGNED_SUCCESSORS,
            CO_SIGNED_SUCCESSORS_FAMILY,
        ))?;
        let contract_heads = engine.register_table(Self::family_descriptor(
            CONTRACT_HEADS,
            CONTRACT_HEADS_FAMILY,
        ))?;
        let root_founding =
            engine.register_table(Self::family_descriptor(ROOT_FOUNDING, ROOT_FOUNDING_FAMILY))?;
        let pending_foundings = engine.register_table(Self::family_descriptor(
            PENDING_FOUNDINGS,
            PENDING_FOUNDINGS_FAMILY,
        ))?;
        let intercept_policies = engine.register_table(Self::family_descriptor(
            INTERCEPT_POLICIES,
            INTERCEPT_POLICIES_FAMILY,
        ))?;
        let parked_spirit_requests = engine.register_table(Self::family_descriptor(
            PARKED_SPIRIT_REQUESTS,
            PARKED_SPIRIT_REQUESTS_FAMILY,
        ))?;
        let attestation_next_slot = engine.register_table(Self::family_descriptor(
            ATTESTATION_NEXT_SLOT,
            ATTESTATION_NEXT_SLOT_FAMILY,
        ))?;
        let authorization_next_slot = engine.register_table(Self::family_descriptor(
            AUTHORIZATION_NEXT_SLOT,
            AUTHORIZATION_NEXT_SLOT_FAMILY,
        ))?;
        let intercept_policy_next_slot = engine.register_table(Self::family_descriptor(
            INTERCEPT_POLICY_NEXT_SLOT,
            INTERCEPT_POLICY_NEXT_SLOT_FAMILY,
        ))?;
        let parked_spirit_request_next_slot = engine.register_table(Self::family_descriptor(
            PARKED_SPIRIT_REQUEST_NEXT_SLOT,
            PARKED_SPIRIT_REQUEST_NEXT_SLOT_FAMILY,
        ))?;
        Ok(Self {
            engine,
            identities,
            revocations,
            attestations,
            authorization_states,
            authorization_replay_nonces,
            contracts,
            signature_solicitations,
            submitted_signatures,
            quorum_rounds,
            co_signed_successors,
            contract_heads,
            root_founding,
            pending_foundings,
            intercept_policies,
            parked_spirit_requests,
            attestation_next_slot,
            authorization_next_slot,
            intercept_policy_next_slot,
            parked_spirit_request_next_slot,
        })
    }

    fn engine_open(store: &StoreLocation) -> EngineOpen {
        EngineOpen::new(store.as_path().to_path_buf(), CRIOME_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("criome"))
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!(
                "criome-{family}-v{}",
                CRIOME_SCHEMA_VERSION.value()
            )),
        )
    }

    pub fn put_identity(&self, identity: &StoredIdentity) -> Result<()> {
        let key = IdentityKey::new(identity.identity()).into_string();
        self.upsert(self.identities, key, identity.clone())?;
        Ok(())
    }

    pub fn identity(&self, identity: &Identity) -> Result<Option<StoredIdentity>> {
        let key = IdentityKey::new(identity).into_string();
        self.read_key(self.identities, key)
    }

    pub fn identities(&self) -> Result<Vec<StoredIdentity>> {
        self.read_all(self.identities)
    }

    pub fn put_revocation(&self, revocation: &StoredRevocation) -> Result<()> {
        let key = IdentityKey::new(revocation.identity()).into_string();
        self.upsert(self.revocations, key, revocation.clone())?;
        Ok(())
    }

    pub fn put_attestation(&self, attestation: Attestation) -> Result<StoredAttestation> {
        let slot = self.next_attestation_slot()?;
        let stored = StoredAttestation::new(slot.value(), attestation);
        self.upsert(self.attestations, stored.slot().to_string(), stored.clone())?;
        self.upsert(
            self.attestation_next_slot,
            ATTESTATION_NEXT_SLOT_KEY.to_owned(),
            slot.next_value(),
        )?;
        Ok(stored)
    }

    pub fn attestations(&self) -> Result<Vec<StoredAttestation>> {
        self.read_all(self.attestations)
    }

    pub fn put_authorization_state(&self, state: &StoredAuthorizationState) -> Result<()> {
        let key =
            AuthorizationSlotKey::new(&state.state().authorization_request_slot).into_string();
        self.upsert(self.authorization_states, key, state.clone())?;
        Ok(())
    }

    pub fn put_new_authorization_state(
        &self,
        draft: AuthorizationStateDraft,
    ) -> Result<StoredAuthorizationState> {
        let AuthorizationStateDraft {
            request_digest,
            status,
            missing_authorities,
            grant,
            denial,
            parked_evaluation,
            signal_authorization,
            replay_identity,
        } = draft;
        if let Some(replay_identity) = replay_identity.as_ref()
            && self.authorization_replay_slot(replay_identity)?.is_some()
        {
            return Err(crate::Error::AuthorizationReplayAttempted);
        }
        let slot = self.next_authorization_slot()?;
        let mut state = AuthorizationStateRecord::new(
            slot.request_slot(),
            request_digest,
            status,
            missing_authorities,
            grant,
            denial,
        );
        if let Some(evaluation) = parked_evaluation {
            state = state.with_parked_evaluation(evaluation);
        }
        if let Some(authorization) = signal_authorization {
            state = state.with_signal_authorization(authorization);
        }
        let stored = StoredAuthorizationState::new(state);
        let key =
            AuthorizationSlotKey::new(&stored.state().authorization_request_slot).into_string();
        self.upsert(self.authorization_states, key, stored.clone())?;
        if let Some(replay_identity) = replay_identity {
            let replay_key = AuthorizationReplayKey::new(&replay_identity).into_string();
            self.upsert(
                self.authorization_replay_nonces,
                replay_key,
                stored.state().authorization_request_slot.clone(),
            )?;
        }
        self.upsert(
            self.authorization_next_slot,
            AUTHORIZATION_NEXT_SLOT_KEY.to_owned(),
            slot.next_value(),
        )?;
        Ok(stored)
    }

    pub fn authorization_state(
        &self,
        slot: &AuthorizationRequestSlot,
    ) -> Result<Option<StoredAuthorizationState>> {
        let key = AuthorizationSlotKey::new(slot).into_string();
        self.read_key(self.authorization_states, key)
    }

    pub fn authorization_states(&self) -> Result<Vec<StoredAuthorizationState>> {
        self.read_all(self.authorization_states)
    }

    pub fn authorization_replay_slot(
        &self,
        replay_identity: &AuthorizationReplayIdentity,
    ) -> Result<Option<AuthorizationRequestSlot>> {
        let key = AuthorizationReplayKey::new(replay_identity).into_string();
        self.read_key(self.authorization_replay_nonces, key)
    }

    pub fn put_contract(&self, contract: &StoredContract) -> Result<()> {
        let key = ContractDigestKey::new(contract.digest()).into_string();
        self.upsert(self.contracts, key, contract.clone())?;
        Ok(())
    }

    pub fn contract(&self, digest: &ContractDigest) -> Result<Option<StoredContract>> {
        let key = ContractDigestKey::new(digest).into_string();
        self.read_key(self.contracts, key)
    }

    pub fn contracts(&self) -> Result<Vec<StoredContract>> {
        self.read_all(self.contracts)
    }

    pub fn put_signature_solicitation(
        &self,
        solicitation: &StoredSignatureSolicitation,
    ) -> Result<()> {
        let key = SignatureSolicitationKey::new(solicitation.route()).into_string();
        self.upsert(self.signature_solicitations, key, solicitation.clone())?;
        Ok(())
    }

    pub fn put_signature_submission(&self, submission: &StoredSignatureSubmission) -> Result<()> {
        let key = SignatureSubmissionKey::new(submission.submission()).into_string();
        self.upsert(self.submitted_signatures, key, submission.clone())?;
        Ok(())
    }

    pub fn put_quorum_round(&self, round: &StoredQuorumRound) -> Result<()> {
        let key = QuorumRoundKey::new(round.round()).into_string();
        self.upsert(self.quorum_rounds, key, round.clone())?;
        Ok(())
    }

    pub fn quorum_round(&self, round: &QuorumRoundIdentifier) -> Result<Option<StoredQuorumRound>> {
        let key = QuorumRoundKey::new(round).into_string();
        self.read_key(self.quorum_rounds, key)
    }

    /// Record the one successor this node co-signed for a `(contract, head)`
    /// state-point. Keyed by that state-point, so a later identical co-sign upserts
    /// the same row and a conflicting one is refused upstream before it reaches
    /// here.
    pub fn put_co_signed_successor(&self, record: &StoredCoSignedSuccessor) -> Result<()> {
        let key = StatePointKey::new(record.contract(), record.head()).into_string();
        self.upsert(self.co_signed_successors, key, record.clone())?;
        Ok(())
    }

    /// Every co-signed successor this node has recorded — the durable ledger the
    /// root rebuilds its in-memory anti-equivocation map from on boot.
    pub fn co_signed_successors(&self) -> Result<Vec<StoredCoSignedSuccessor>> {
        self.read_all(self.co_signed_successors)
    }

    /// Record a contract's current committed head, keyed by the contract.
    pub fn put_contract_head(&self, record: &StoredContractHead) -> Result<()> {
        let key = ContractDigestKey::new(record.contract()).into_string();
        self.upsert(self.contract_heads, key, record.clone())?;
        Ok(())
    }

    /// Every contract head this node has advanced — the durable cursor the root
    /// rebuilds its in-memory head map from on boot.
    pub fn contract_heads(&self) -> Result<Vec<StoredContractHead>> {
        self.read_all(self.contract_heads)
    }

    /// Persist (or update) this node's single founded root — the genesis, its
    /// anchor, and the founding signatures gathered so far.
    pub fn put_root_founding(&self, founding: &RootFounding) -> Result<()> {
        self.upsert(
            self.root_founding,
            ROOT_FOUNDING_KEY.to_owned(),
            founding.clone(),
        )?;
        Ok(())
    }

    /// The node's founded root, if it has founded (or is gathering) one.
    pub fn root_founding(&self) -> Result<Option<RootFounding>> {
        self.read_key(self.root_founding, ROOT_FOUNDING_KEY.to_owned())
    }

    /// Record (or update) a pending founding, keyed by its self-certifying
    /// anchor.
    pub fn put_pending_founding(&self, pending: &StoredPendingFounding) -> Result<()> {
        let key = RootAnchorKey::new(pending.anchor()).into_string();
        self.upsert(self.pending_foundings, key, pending.clone())?;
        Ok(())
    }

    /// The pending founding for `anchor`, if this node is party to one.
    pub fn pending_founding(
        &self,
        anchor: &RootAnchorDigest,
    ) -> Result<Option<StoredPendingFounding>> {
        let key = RootAnchorKey::new(anchor).into_string();
        self.read_key(self.pending_foundings, key)
    }

    /// Every founding awaiting an owner accept on this node.
    pub fn pending_foundings(&self) -> Result<Vec<StoredPendingFounding>> {
        self.read_all(self.pending_foundings)
    }

    pub fn put_intercept_policy(
        &self,
        draft: InterceptPolicyDraft,
    ) -> Result<StoredInterceptPolicy> {
        let policy_slot = self.next_intercept_policy_slot()?;
        let stored =
            StoredInterceptPolicy::from_draft(policy_slot.policy_identifier(), draft.clone());
        let overlapping = self.same_priority_overlaps(&stored)?;
        match draft.overlap_mode() {
            PolicyOverlapMode::RejectSamePriorityOverlap if !overlapping.is_empty() => {
                return Err(crate::Error::InterceptPolicyOverlapRejected);
            }
            PolicyOverlapMode::ReplaceSamePriorityOverlap => {
                for policy in overlapping {
                    self.retract_intercept_policy(policy.identifier())?;
                }
            }
            PolicyOverlapMode::RejectSamePriorityOverlap => {}
        }
        let key = InterceptPolicyKey::new(stored.identifier()).into_string();
        self.upsert(self.intercept_policies, key, stored.clone())?;
        self.upsert(
            self.intercept_policy_next_slot,
            INTERCEPT_POLICY_NEXT_SLOT_KEY.to_owned(),
            policy_slot.next_value(),
        )?;
        Ok(stored)
    }

    pub fn retract_intercept_policy(&self, identifier: &InterceptPolicyIdentifier) -> Result<()> {
        self.retract(
            self.intercept_policies,
            InterceptPolicyKey::new(identifier).into_string(),
        )
    }

    pub fn intercept_policies(&self) -> Result<Vec<StoredInterceptPolicy>> {
        self.read_all(self.intercept_policies)
    }

    pub fn active_intercept_policies(
        &self,
        now: TimestampNanos,
    ) -> Result<ActiveInterceptPolicies> {
        let mut policies: Vec<InterceptPolicy> = self
            .intercept_policies()?
            .into_iter()
            .filter(|policy| policy.active_at(now))
            .map(StoredInterceptPolicy::into_policy)
            .collect();
        policies.sort_by(|left, right| {
            InterceptPolicyKey::new(&left.intercept_policy_identifier)
                .into_string()
                .cmp(&InterceptPolicyKey::new(&right.intercept_policy_identifier).into_string())
        });
        Ok(ActiveInterceptPolicies::new(InterceptPolicies::new(
            policies,
        )))
    }

    pub fn matching_intercept_policy(
        &self,
        context: &SpiritAuthorizationContext,
        now: TimestampNanos,
    ) -> Result<Option<InterceptMatch>> {
        let mut policies: Vec<StoredInterceptPolicy> = self
            .intercept_policies()?
            .into_iter()
            .filter(|policy| policy.matches_context(context, now))
            .collect();
        policies.sort_by(|left, right| {
            right
                .policy()
                .policy_priority
                .into_u64()
                .cmp(&left.policy().policy_priority.into_u64())
                .then_with(|| {
                    InterceptPolicyKey::new(left.identifier())
                        .into_string()
                        .cmp(&InterceptPolicyKey::new(right.identifier()).into_string())
                })
        });
        Ok(policies
            .into_iter()
            .next()
            .map(StoredInterceptPolicy::into_policy)
            .map(InterceptMatch::new))
    }

    pub fn put_parked_spirit_request(
        &self,
        context: SpiritAuthorizationContext,
        now: TimestampNanos,
    ) -> Result<Option<StoredParkedSpiritRequest>> {
        let Some(policy) = self
            .matching_intercept_policy(&context, now)?
            .map(InterceptMatch::into_policy)
        else {
            return Ok(None);
        };
        let slot = self.next_parked_spirit_request_slot()?;
        let stored = StoredParkedSpiritRequest::parked(ParkedSpiritRequest {
            parked_request_identifier: slot.request_identifier(),
            intercept_policy_identifier: policy.intercept_policy_identifier,
            mentci_session_slot: policy.mentci_session_slot,
            spirit_authorization_context: context,
            parked_at: now,
            expires_at: policy.intercept_policy_window.expires_at,
            expiry_action: policy.expiry_action,
        });
        self.upsert(
            self.parked_spirit_requests,
            ParkedSpiritRequestKey::new(stored.request()).into_string(),
            stored.clone(),
        )?;
        self.upsert(
            self.parked_spirit_request_next_slot,
            PARKED_SPIRIT_REQUEST_NEXT_SLOT_KEY.to_owned(),
            slot.next_value(),
        )?;
        Ok(Some(stored))
    }

    pub fn parked_spirit_request(
        &self,
        identifier: &ParkedRequestIdentifier,
    ) -> Result<Option<StoredParkedSpiritRequest>> {
        self.read_key(
            self.parked_spirit_requests,
            ParkedSpiritRequestKey::from_identifier(identifier).into_string(),
        )
    }

    pub fn parked_spirit_requests(&self) -> Result<Vec<StoredParkedSpiritRequest>> {
        self.read_all(self.parked_spirit_requests)
    }

    pub fn parked_spirit_request_snapshot(
        &self,
        query: &ParkedRequestQuery,
        now: TimestampNanos,
    ) -> Result<ParkedRequestSnapshot> {
        self.apply_parked_spirit_expiry(now)?;
        let mut requests: Vec<ParkedSpiritRequest> = self
            .parked_spirit_requests()?
            .into_iter()
            .filter(|request| request.matches_query(query))
            .map(|stored| stored.request().clone())
            .collect();
        requests.sort_by(|left, right| {
            ParkedRequestSlot::sort_key(&left.parked_request_identifier).cmp(
                &ParkedRequestSlot::sort_key(&right.parked_request_identifier),
            )
        });
        Ok(ParkedRequestSnapshot::new(ParkedSpiritRequests::new(
            requests,
        )))
    }

    pub fn answer_parked_spirit_request(
        &self,
        answer: ParkedRequestAnswer,
        now: TimestampNanos,
    ) -> Result<ParkedRequestResolution> {
        self.apply_parked_spirit_expiry(now)?;
        let Some(stored) = self.parked_spirit_request(&answer.parked_request_identifier)? else {
            return Err(crate::Error::ParkedSpiritRequestMissing);
        };
        if !stored.is_active() {
            return stored
                .resolution()
                .cloned()
                .ok_or(crate::Error::ParkedSpiritRequestMissing);
        }
        let resolved = stored.resolve_manual(answer.parked_request_decision, now);
        let resolution = resolved
            .resolution()
            .cloned()
            .ok_or(crate::Error::ParkedSpiritRequestMissing)?;
        self.upsert(
            self.parked_spirit_requests,
            ParkedSpiritRequestKey::new(resolved.request()).into_string(),
            resolved,
        )?;
        Ok(resolution)
    }

    fn next_attestation_slot(&self) -> Result<AttestationSlot> {
        let stored = self.read_key(
            self.attestation_next_slot,
            ATTESTATION_NEXT_SLOT_KEY.to_owned(),
        )?;
        match stored {
            Some(next_slot) => Ok(AttestationSlot::new(next_slot)),
            None => Ok(AttestationSlot::after_records(&self.attestations()?)),
        }
    }

    fn next_authorization_slot(&self) -> Result<AuthorizationSlot> {
        let stored = self.read_key(
            self.authorization_next_slot,
            AUTHORIZATION_NEXT_SLOT_KEY.to_owned(),
        )?;
        match stored {
            Some(next_slot) => Ok(AuthorizationSlot::new(next_slot)),
            None => Ok(AuthorizationSlot::after_records(
                &self.authorization_states()?,
            )),
        }
    }

    fn next_intercept_policy_slot(&self) -> Result<InterceptPolicySlot> {
        let stored = self.read_key(
            self.intercept_policy_next_slot,
            INTERCEPT_POLICY_NEXT_SLOT_KEY.to_owned(),
        )?;
        match stored {
            Some(next_slot) => Ok(InterceptPolicySlot::new(next_slot)),
            None => Ok(InterceptPolicySlot::after_records(
                &self.intercept_policies()?,
            )),
        }
    }

    fn next_parked_spirit_request_slot(&self) -> Result<ParkedRequestSlot> {
        let stored = self.read_key(
            self.parked_spirit_request_next_slot,
            PARKED_SPIRIT_REQUEST_NEXT_SLOT_KEY.to_owned(),
        )?;
        match stored {
            Some(next_slot) => Ok(ParkedRequestSlot::new(next_slot)),
            None => Ok(ParkedRequestSlot::after_records(
                &self.parked_spirit_requests()?,
            )),
        }
    }

    fn same_priority_overlaps(
        &self,
        policy: &StoredInterceptPolicy,
    ) -> Result<Vec<StoredInterceptPolicy>> {
        Ok(self
            .intercept_policies()?
            .into_iter()
            .filter(|stored| stored.same_priority_overlap(policy))
            .collect())
    }

    fn apply_parked_spirit_expiry(&self, now: TimestampNanos) -> Result<()> {
        for request in self.parked_spirit_requests()? {
            let applied = request.clone().apply_expiry(now);
            if applied != request {
                self.upsert(
                    self.parked_spirit_requests,
                    ParkedSpiritRequestKey::new(applied.request()).into_string(),
                    applied,
                )?;
            }
        }
        Ok(())
    }

    fn upsert<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: String,
        record: RecordValue,
    ) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = RecordKey::new(key);
        let exists = !self
            .engine
            .match_records(QueryPlan::key(table, key.clone()))?
            .records()
            .is_empty();
        if exists {
            self.engine
                .mutate_keyed(KeyedMutation::new(table, key, record))?;
        } else {
            self.engine
                .assert_keyed(KeyedAssertion::new(table, key, record))?;
        }
        Ok(())
    }

    fn read_key<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: String,
    ) -> Result<Option<RecordValue>>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .engine
            .match_records(QueryPlan::key(table, RecordKey::new(key)))?
            .records()
            .first()
            .cloned())
    }

    fn read_all<RecordValue>(&self, table: TableReference<RecordValue>) -> Result<Vec<RecordValue>>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .engine
            .match_records(QueryPlan::all(table))?
            .records()
            .to_vec())
    }

    fn retract<RecordValue>(&self, table: TableReference<RecordValue>, key: String) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        self.engine
            .retract(Retraction::new(table, RecordKey::new(key)))?;
        Ok(())
    }
}

struct AttestationSlot {
    value: u64,
}

impl AttestationSlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_records(records: &[StoredAttestation]) -> Self {
        let value = records
            .iter()
            .map(StoredAttestation::slot)
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    const fn value(&self) -> u64 {
        self.value
    }

    const fn next_value(&self) -> u64 {
        self.value + 1
    }
}

struct AuthorizationSlot {
    value: u64,
}

struct InterceptPolicySlot {
    value: u64,
}

struct ParkedRequestSlot {
    value: u64,
}

impl AuthorizationSlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_records(records: &[StoredAuthorizationState]) -> Self {
        let value = records
            .iter()
            .filter_map(|record| {
                record
                    .state()
                    .authorization_request_slot
                    .as_str()
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn request_slot(&self) -> AuthorizationRequestSlot {
        AuthorizationRequestSlot::new(self.value.to_string())
    }

    const fn next_value(&self) -> u64 {
        self.value + 1
    }
}

impl InterceptPolicySlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_records(records: &[StoredInterceptPolicy]) -> Self {
        let value = records
            .iter()
            .filter_map(|record| record.identifier().as_str().parse::<u64>().ok())
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn policy_identifier(&self) -> InterceptPolicyIdentifier {
        InterceptPolicyIdentifier::new(self.value.to_string())
    }

    const fn next_value(&self) -> u64 {
        self.value + 1
    }
}

impl ParkedRequestSlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_records(records: &[StoredParkedSpiritRequest]) -> Self {
        let value = records
            .iter()
            .filter_map(|record| {
                record
                    .request()
                    .parked_request_identifier
                    .as_str()
                    .parse::<u64>()
                    .ok()
            })
            .max()
            .map_or(0, |slot| slot + 1);
        Self { value }
    }

    fn request_identifier(&self) -> ParkedRequestIdentifier {
        ParkedRequestIdentifier::new(self.value.to_string())
    }

    const fn next_value(&self) -> u64 {
        self.value + 1
    }

    fn sort_key(identifier: &ParkedRequestIdentifier) -> (u64, String) {
        match identifier.as_str().parse::<u64>() {
            Ok(value) => (value, String::new()),
            Err(_error) => (u64::MAX, identifier.as_str().to_owned()),
        }
    }
}

struct AuthorizationReplayKey {
    requester: String,
    nonce: String,
}

impl AuthorizationReplayKey {
    fn new(identity: &AuthorizationReplayIdentity) -> Self {
        Self {
            requester: IdentityKey::new(&identity.requester).into_string(),
            nonce: identity.nonce.as_str().to_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}::{}", self.requester, self.nonce)
    }
}

struct IdentityKey {
    kind: &'static str,
    name: String,
}

impl IdentityKey {
    fn new(identity: &Identity) -> Self {
        match identity {
            Identity::Persona(name) => Self::from_parts("Persona", name.as_str()),
            Identity::Agent(name) => Self::from_parts("Agent", name.as_str()),
            Identity::Host(name) => Self::from_parts("Host", name.as_str()),
            Identity::Developer(name) => Self::from_parts("Developer", name.as_str()),
            Identity::Cluster(name) => Self::from_parts("Cluster", name.as_str()),
        }
    }

    fn from_parts(kind: &'static str, name: &str) -> Self {
        Self {
            kind,
            name: name.to_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}:{}", self.kind, self.name)
    }
}

struct AuthorizationSlotKey {
    slot: String,
}

struct InterceptPolicyKey {
    identifier: String,
}

struct ParkedSpiritRequestKey {
    identifier: String,
}

impl AuthorizationSlotKey {
    fn new(slot: &AuthorizationRequestSlot) -> Self {
        Self {
            slot: slot.as_str().to_string(),
        }
    }

    fn into_string(self) -> String {
        self.slot
    }
}

impl InterceptPolicyKey {
    fn new(identifier: &InterceptPolicyIdentifier) -> Self {
        Self {
            identifier: identifier.as_str().to_owned(),
        }
    }

    fn into_string(self) -> String {
        self.identifier
    }
}

impl ParkedSpiritRequestKey {
    fn new(request: &ParkedSpiritRequest) -> Self {
        Self::from_identifier(&request.parked_request_identifier)
    }

    fn from_identifier(identifier: &ParkedRequestIdentifier) -> Self {
        Self {
            identifier: identifier.as_str().to_owned(),
        }
    }

    fn into_string(self) -> String {
        self.identifier
    }
}

struct ContractDigestKey {
    digest: String,
}

impl ContractDigestKey {
    fn new(digest: &ContractDigest) -> Self {
        Self {
            digest: digest.object_digest().payload().to_string(),
        }
    }

    fn into_string(self) -> String {
        self.digest
    }
}

struct SignatureSolicitationKey {
    request_slot: String,
    routed_to: String,
}

impl SignatureSolicitationKey {
    fn new(route: &SignatureSolicitationRoute) -> Self {
        Self {
            request_slot: route
                .signature_solicitation
                .authorization_request_slot
                .as_str()
                .to_string(),
            routed_to: IdentityKey::new(&route.identity).into_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}:{}", self.request_slot, self.routed_to)
    }
}

struct SignatureSubmissionKey {
    request_slot: String,
    signer: String,
}

impl SignatureSubmissionKey {
    fn new(submission: &SignatureSubmission) -> Self {
        Self {
            request_slot: submission.authorization_request_slot.as_str().to_string(),
            signer: IdentityKey::new(&submission.identity).into_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}:{}", self.request_slot, self.signer)
    }
}

struct QuorumRoundKey {
    round: String,
}

impl QuorumRoundKey {
    fn new(round: &QuorumRoundIdentifier) -> Self {
        Self {
            round: round.as_str().to_owned(),
        }
    }

    fn into_string(self) -> String {
        self.round
    }
}

/// The `(contract, head)` state-point key a co-signed successor is stored under —
/// the same `contract@head` shape the root's in-memory ledger keys on, so the two
/// stay in step.
struct StatePointKey {
    key: String,
}

impl StatePointKey {
    fn new(contract: &ContractDigest, head: &ContractOperationHead) -> Self {
        Self {
            key: format!("{}@{}", contract.as_str(), head.as_str()),
        }
    }

    fn into_string(self) -> String {
        self.key
    }
}

struct RootAnchorKey {
    anchor: String,
}

impl RootAnchorKey {
    fn new(anchor: &RootAnchorDigest) -> Self {
        Self {
            anchor: anchor.as_str().to_owned(),
        }
    }

    fn into_string(self) -> String {
        self.anchor
    }
}
