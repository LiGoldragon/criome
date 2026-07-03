use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_criome::{
    ActiveInterceptPolicies, Attestation, AuthorizationDenial, AuthorizationEvaluation,
    AuthorizationGrant, AuthorizationRequestSlot, AuthorizationStateRecord, AuthorizationStatus,
    Contract, ContractAdmissionRejectionReason, ContractDigest, Identity, IdentityRegistration,
    IdentityRevocation, InterceptPolicyCancellation, InterceptPolicyProposal, ObjectDigest,
    ParkedRequestAnswer, ParkedRequestQuery, ParkedRequestResolution, ParkedRequestSnapshot,
    QuorumRoundIdentifier, SignatureSolicitationRoute, SignatureSubmission,
    SpiritAuthorizationContext, TimestampNanos,
};

use crate::language::{AdmissionError, ContractStore};
use crate::tables::{
    AuthorizationReplayIdentity, AuthorizationStateDraft, CriomeTables, InterceptPolicyDraft,
    StoreLocation, StoredAttestation, StoredAuthorizationState, StoredContract, StoredIdentity,
    StoredInterceptPolicy, StoredParkedSpiritRequest, StoredQuorumRound, StoredRevocation,
    StoredSignatureSolicitation, StoredSignatureSubmission,
};

pub struct StoreKernel {
    tables: CriomeTables,
}

pub struct StoreIdentity {
    registration: IdentityRegistration,
}

pub struct StoreRevocation {
    revocation: IdentityRevocation,
}

pub struct LookupIdentity {
    identity: Identity,
}

pub struct ReadIdentitySnapshot;

pub struct StoreAttestation {
    attestation: Attestation,
}

pub struct StoreAuthorizationState {
    state: AuthorizationStateRecord,
}

pub struct CreateAuthorizationState {
    request_digest: ObjectDigest,
    status: AuthorizationStatus,
    missing_authorities: Vec<Identity>,
    grant: Option<AuthorizationGrant>,
    denial: Option<AuthorizationDenial>,
    parked_evaluation: Option<AuthorizationEvaluation>,
    signal_authorization: Option<signal_criome::SignalCallAuthorization>,
    replay_identity: Option<AuthorizationReplayIdentity>,
}

pub struct LookupAuthorizationState {
    request_slot: AuthorizationRequestSlot,
}

pub struct ReadAuthorizationSnapshot;

pub struct StoreContract {
    contract: Contract,
}

pub struct LookupContract {
    digest: ContractDigest,
}

pub struct ReadContractSnapshot;

pub struct StoreSignatureSolicitation {
    route: SignatureSolicitationRoute,
}

pub struct StoreSignatureSubmission {
    submission: SignatureSubmission,
}

pub struct StoreQuorumRound {
    round: StoredQuorumRound,
}

pub struct LookupQuorumRound {
    round: QuorumRoundIdentifier,
}

pub struct StoreInterceptPolicy {
    draft: InterceptPolicyDraft,
}

pub struct CancelInterceptPolicy {
    cancellation: InterceptPolicyCancellation,
}

pub struct ReadInterceptPolicies {
    now: TimestampNanos,
}

pub struct InterceptSpiritAuthorization {
    context: SpiritAuthorizationContext,
    now: TimestampNanos,
}

pub struct FetchParkedSpiritRequests {
    query: ParkedRequestQuery,
    now: TimestampNanos,
}

pub struct AnswerParkedSpiritRequest {
    answer: ParkedRequestAnswer,
    now: TimestampNanos,
}

pub struct ReadParkedSpiritRequestHistory;

#[derive(kameo::Reply)]
pub struct StoredIdentityReply {
    identity: StoredIdentity,
}

#[derive(kameo::Reply)]
pub struct LookupIdentityReply {
    identity: Option<StoredIdentity>,
}

#[derive(kameo::Reply)]
pub struct IdentitySnapshotReply {
    identities: Vec<StoredIdentity>,
}

#[derive(kameo::Reply)]
pub struct StoredAttestationReply {
    attestation: StoredAttestation,
}

#[derive(kameo::Reply)]
pub struct StoredAuthorizationStateReply {
    state: StoredAuthorizationState,
}

#[derive(kameo::Reply)]
pub struct AuthorizationStateCreationReply {
    outcome: AuthorizationStateCreationOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthorizationStateCreationOutcome {
    Created(Box<StoredAuthorizationState>),
    ReplayAttempted,
    StoreUnavailable(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ContractStorageOutcome {
    Stored(Box<StoredContract>),
    Rejected(ContractAdmissionRejectionReason),
    StoreUnavailable(String),
}

#[derive(kameo::Reply)]
pub struct LookupAuthorizationStateReply {
    state: Option<StoredAuthorizationState>,
}

#[derive(kameo::Reply)]
pub struct AuthorizationSnapshotReply {
    states: Vec<StoredAuthorizationState>,
}

#[derive(kameo::Reply)]
pub struct ContractStorageReply {
    outcome: ContractStorageOutcome,
}

#[derive(kameo::Reply)]
pub struct LookupContractReply {
    contract: Option<StoredContract>,
}

#[derive(kameo::Reply)]
pub struct ContractSnapshotReply {
    contracts: Vec<StoredContract>,
}

#[derive(kameo::Reply)]
pub struct StoredSignatureSolicitationReply {
    solicitation: StoredSignatureSolicitation,
}

#[derive(kameo::Reply)]
pub struct StoredSignatureSubmissionReply {
    submission: StoredSignatureSubmission,
}

#[derive(kameo::Reply)]
pub struct StoredQuorumRoundReply {
    round: StoredQuorumRound,
}

#[derive(kameo::Reply)]
pub struct LookupQuorumRoundReply {
    round: Option<StoredQuorumRound>,
}

#[derive(kameo::Reply)]
pub struct StoredInterceptPolicyReply {
    policy: StoredInterceptPolicy,
}

#[derive(kameo::Reply)]
pub struct InterceptPoliciesReply {
    policies: ActiveInterceptPolicies,
}

#[derive(kameo::Reply)]
pub struct InterceptedSpiritAuthorizationReply {
    request: Option<StoredParkedSpiritRequest>,
}

#[derive(kameo::Reply)]
pub struct ParkedSpiritRequestSnapshotReply {
    snapshot: ParkedRequestSnapshot,
}

#[derive(kameo::Reply)]
pub struct ParkedSpiritRequestResolutionReply {
    resolution: ParkedRequestResolution,
}

#[derive(kameo::Reply)]
pub struct ParkedSpiritRequestHistoryReply {
    requests: Vec<StoredParkedSpiritRequest>,
}

impl StoreIdentity {
    pub fn new(registration: IdentityRegistration) -> Self {
        Self { registration }
    }
}

impl StoreRevocation {
    pub fn new(revocation: IdentityRevocation) -> Self {
        Self { revocation }
    }
}

impl LookupIdentity {
    pub fn new(identity: Identity) -> Self {
        Self { identity }
    }
}

impl StoreAttestation {
    pub fn new(attestation: Attestation) -> Self {
        Self { attestation }
    }
}

impl StoreAuthorizationState {
    pub fn new(state: AuthorizationStateRecord) -> Self {
        Self { state }
    }
}

impl CreateAuthorizationState {
    pub fn signing(authorization: &signal_criome::SignalCallAuthorization) -> Self {
        Self {
            request_digest: authorization.request_digest.clone(),
            status: AuthorizationStatus::Signing,
            missing_authorities: Vec::new(),
            grant: None,
            denial: None,
            parked_evaluation: None,
            signal_authorization: Some(authorization.clone()),
            replay_identity: Some(AuthorizationReplayIdentity::new(
                authorization.requester.clone(),
                authorization.nonce.clone(),
            )),
        }
    }

    pub fn expired(authorization: &signal_criome::SignalCallAuthorization) -> Self {
        Self {
            request_digest: authorization.request_digest.clone(),
            status: AuthorizationStatus::Expired,
            missing_authorities: Vec::new(),
            grant: None,
            denial: None,
            parked_evaluation: None,
            signal_authorization: Some(authorization.clone()),
            replay_identity: Some(AuthorizationReplayIdentity::new(
                authorization.requester.clone(),
                authorization.nonce.clone(),
            )),
        }
    }

    pub fn parked_signal_authorization(
        authorization: signal_criome::SignalCallAuthorization,
    ) -> Self {
        Self {
            request_digest: authorization.request_digest.clone(),
            status: AuthorizationStatus::Parked,
            missing_authorities: Vec::new(),
            grant: None,
            denial: None,
            parked_evaluation: None,
            signal_authorization: Some(authorization.clone()),
            replay_identity: Some(AuthorizationReplayIdentity::new(
                authorization.requester,
                authorization.nonce,
            )),
        }
    }

    pub fn pending_signal_authorization(
        authorization: signal_criome::SignalCallAuthorization,
    ) -> Self {
        Self {
            request_digest: authorization.request_digest.clone(),
            status: AuthorizationStatus::Pending,
            missing_authorities: Vec::new(),
            grant: None,
            denial: None,
            parked_evaluation: None,
            signal_authorization: Some(authorization.clone()),
            replay_identity: Some(AuthorizationReplayIdentity::new(
                authorization.requester,
                authorization.nonce,
            )),
        }
    }

    pub fn parked(evaluation: AuthorizationEvaluation) -> Self {
        Self {
            request_digest: evaluation.object.digest.clone(),
            status: AuthorizationStatus::Parked,
            missing_authorities: Vec::new(),
            grant: None,
            denial: None,
            parked_evaluation: Some(evaluation),
            signal_authorization: None,
            replay_identity: None,
        }
    }

    fn into_draft(self) -> AuthorizationStateDraft {
        AuthorizationStateDraft {
            request_digest: self.request_digest,
            status: self.status,
            missing_authorities: self.missing_authorities,
            grant: self.grant,
            denial: self.denial,
            parked_evaluation: self.parked_evaluation,
            signal_authorization: self.signal_authorization,
            replay_identity: self.replay_identity,
        }
    }
}

impl LookupAuthorizationState {
    pub fn new(request_slot: AuthorizationRequestSlot) -> Self {
        Self { request_slot }
    }
}

impl StoreContract {
    pub fn new(contract: Contract) -> Self {
        Self { contract }
    }
}

impl LookupContract {
    pub fn new(digest: ContractDigest) -> Self {
        Self { digest }
    }
}

impl StoreSignatureSolicitation {
    pub fn new(route: SignatureSolicitationRoute) -> Self {
        Self { route }
    }
}

impl StoreSignatureSubmission {
    pub fn new(submission: SignatureSubmission) -> Self {
        Self { submission }
    }
}

impl StoreQuorumRound {
    pub fn new(round: StoredQuorumRound) -> Self {
        Self { round }
    }
}

impl LookupQuorumRound {
    pub fn new(round: QuorumRoundIdentifier) -> Self {
        Self { round }
    }
}

impl StoredQuorumRoundReply {
    pub fn into_round(self) -> StoredQuorumRound {
        self.round
    }
}

impl LookupQuorumRoundReply {
    pub fn into_round(self) -> Option<StoredQuorumRound> {
        self.round
    }
}

impl StoreInterceptPolicy {
    pub fn create(proposal: InterceptPolicyProposal, now: TimestampNanos) -> Self {
        Self {
            draft: InterceptPolicyDraft::create(proposal, now),
        }
    }

    pub fn replace(proposal: InterceptPolicyProposal, now: TimestampNanos) -> Self {
        Self {
            draft: InterceptPolicyDraft::replace(proposal, now),
        }
    }
}

impl CancelInterceptPolicy {
    pub fn new(cancellation: InterceptPolicyCancellation) -> Self {
        Self { cancellation }
    }
}

impl ReadInterceptPolicies {
    pub fn new(now: TimestampNanos) -> Self {
        Self { now }
    }
}

impl InterceptSpiritAuthorization {
    pub fn new(context: SpiritAuthorizationContext, now: TimestampNanos) -> Self {
        Self { context, now }
    }
}

impl FetchParkedSpiritRequests {
    pub fn new(query: ParkedRequestQuery, now: TimestampNanos) -> Self {
        Self { query, now }
    }
}

impl AnswerParkedSpiritRequest {
    pub fn new(answer: ParkedRequestAnswer, now: TimestampNanos) -> Self {
        Self { answer, now }
    }
}

impl StoredIdentityReply {
    pub fn into_identity(self) -> StoredIdentity {
        self.identity
    }
}

impl LookupIdentityReply {
    pub fn into_identity(self) -> Option<StoredIdentity> {
        self.identity
    }
}

impl IdentitySnapshotReply {
    pub fn into_identities(self) -> Vec<StoredIdentity> {
        self.identities
    }
}

impl StoredAttestationReply {
    pub fn into_attestation(self) -> StoredAttestation {
        self.attestation
    }
}

impl StoredAuthorizationStateReply {
    pub fn into_state(self) -> StoredAuthorizationState {
        self.state
    }
}

impl AuthorizationStateCreationReply {
    fn from_result(result: crate::Result<StoredAuthorizationState>) -> Self {
        let outcome = match result {
            Ok(state) => AuthorizationStateCreationOutcome::Created(Box::new(state)),
            Err(crate::Error::AuthorizationReplayAttempted) => {
                AuthorizationStateCreationOutcome::ReplayAttempted
            }
            Err(error) => AuthorizationStateCreationOutcome::StoreUnavailable(error.to_string()),
        };
        Self { outcome }
    }

    pub fn into_result(self) -> crate::Result<StoredAuthorizationState> {
        match self.outcome {
            AuthorizationStateCreationOutcome::Created(state) => Ok(*state),
            AuthorizationStateCreationOutcome::ReplayAttempted => {
                Err(crate::Error::AuthorizationReplayAttempted)
            }
            AuthorizationStateCreationOutcome::StoreUnavailable(error) => {
                Err(crate::Error::ActorCall(error))
            }
        }
    }
}

impl LookupAuthorizationStateReply {
    pub fn into_state(self) -> Option<StoredAuthorizationState> {
        self.state
    }
}

impl AuthorizationSnapshotReply {
    pub fn into_states(self) -> Vec<StoredAuthorizationState> {
        self.states
    }
}

impl ContractStorageReply {
    fn from_result(result: crate::Result<StoredContract>) -> Self {
        let outcome = match result {
            Ok(contract) => ContractStorageOutcome::Stored(Box::new(contract)),
            Err(crate::Error::ContractAdmissionRejected(reason)) => {
                ContractStorageOutcome::Rejected(reason)
            }
            Err(error) => ContractStorageOutcome::StoreUnavailable(error.to_string()),
        };
        Self { outcome }
    }

    pub fn into_result(self) -> crate::Result<StoredContract> {
        match self.outcome {
            ContractStorageOutcome::Stored(contract) => Ok(*contract),
            ContractStorageOutcome::Rejected(reason) => {
                Err(crate::Error::ContractAdmissionRejected(reason))
            }
            ContractStorageOutcome::StoreUnavailable(error) => Err(crate::Error::ActorCall(error)),
        }
    }
}

impl LookupContractReply {
    pub fn into_contract(self) -> Option<StoredContract> {
        self.contract
    }
}

impl ContractSnapshotReply {
    pub fn into_contracts(self) -> Vec<StoredContract> {
        self.contracts
    }
}

impl StoredSignatureSolicitationReply {
    pub fn into_solicitation(self) -> StoredSignatureSolicitation {
        self.solicitation
    }
}

impl StoredSignatureSubmissionReply {
    pub fn into_submission(self) -> StoredSignatureSubmission {
        self.submission
    }
}

impl StoredInterceptPolicyReply {
    pub fn into_policy(self) -> StoredInterceptPolicy {
        self.policy
    }
}

impl InterceptPoliciesReply {
    pub fn into_policies(self) -> ActiveInterceptPolicies {
        self.policies
    }
}

impl InterceptedSpiritAuthorizationReply {
    pub fn into_request(self) -> Option<StoredParkedSpiritRequest> {
        self.request
    }
}

impl ParkedSpiritRequestSnapshotReply {
    pub fn into_snapshot(self) -> ParkedRequestSnapshot {
        self.snapshot
    }
}

impl ParkedSpiritRequestResolutionReply {
    pub fn into_resolution(self) -> ParkedRequestResolution {
        self.resolution
    }
}

impl ParkedSpiritRequestHistoryReply {
    pub fn into_requests(self) -> Vec<StoredParkedSpiritRequest> {
        self.requests
    }
}

impl StoreKernel {
    fn open(location: StoreLocation) -> crate::Result<Self> {
        Ok(Self {
            tables: CriomeTables::open(&location)?,
        })
    }

    fn store_identity(&self, registration: IdentityRegistration) -> crate::Result<StoredIdentity> {
        let identity = StoredIdentity::active(registration);
        self.tables.put_identity(&identity)?;
        Ok(identity)
    }

    fn store_revocation(&self, revocation: IdentityRevocation) -> crate::Result<StoredIdentity> {
        let Some(identity) = self.tables.identity(&revocation.identity)? else {
            return Err(crate::Error::UnexpectedSignalFrame {
                got: "unknown identity during revocation".to_string(),
            });
        };
        let revoked = identity.revoked();
        self.tables
            .put_revocation(&StoredRevocation::new(revocation))?;
        self.tables.put_identity(&revoked)?;
        Ok(revoked)
    }

    fn lookup_identity(&self, identity: &Identity) -> crate::Result<Option<StoredIdentity>> {
        self.tables.identity(identity)
    }

    fn snapshot(&self) -> crate::Result<Vec<StoredIdentity>> {
        let mut identities = self.tables.identities()?;
        identities.sort_by(|left, right| {
            format!("{:?}", left.identity()).cmp(&format!("{:?}", right.identity()))
        });
        Ok(identities)
    }

    fn store_attestation(&self, attestation: Attestation) -> crate::Result<StoredAttestation> {
        self.tables.put_attestation(attestation)
    }

    fn store_authorization_state(
        &self,
        state: AuthorizationStateRecord,
    ) -> crate::Result<StoredAuthorizationState> {
        let stored = StoredAuthorizationState::new(state);
        self.tables.put_authorization_state(&stored)?;
        Ok(stored)
    }

    fn create_authorization_state(
        &self,
        state: CreateAuthorizationState,
    ) -> crate::Result<StoredAuthorizationState> {
        self.tables.put_new_authorization_state(state.into_draft())
    }

    fn authorization_state(
        &self,
        request_slot: &AuthorizationRequestSlot,
    ) -> crate::Result<Option<StoredAuthorizationState>> {
        self.tables.authorization_state(request_slot)
    }

    fn authorization_snapshot(&self) -> crate::Result<Vec<StoredAuthorizationState>> {
        let mut states = self.tables.authorization_states()?;
        states.sort_by(|left, right| {
            Self::authorization_slot_sort_key(&left.state().request_slot).cmp(
                &Self::authorization_slot_sort_key(&right.state().request_slot),
            )
        });
        Ok(states)
    }

    fn authorization_slot_sort_key(request_slot: &AuthorizationRequestSlot) -> (u64, String) {
        match request_slot.as_str().parse::<u64>() {
            Ok(value) => (value, String::new()),
            Err(_error) => (u64::MAX, request_slot.as_str().to_owned()),
        }
    }

    fn store_contract(&self, contract: Contract) -> crate::Result<StoredContract> {
        let mut snapshot = self.contract_snapshot_store()?;
        let digest = snapshot
            .admit(contract.clone())
            .map_err(Self::admission_error)?;
        let stored = StoredContract::new(digest, contract);
        self.tables.put_contract(&stored)?;
        Ok(stored)
    }

    fn lookup_contract(&self, digest: &ContractDigest) -> crate::Result<Option<StoredContract>> {
        self.tables.contract(digest)
    }

    fn contract_snapshot(&self) -> crate::Result<Vec<StoredContract>> {
        let mut contracts = self.tables.contracts()?;
        contracts.sort_by(|left, right| {
            left.digest()
                .object_digest()
                .payload()
                .cmp(right.digest().object_digest().payload())
        });
        Ok(contracts)
    }

    fn contract_snapshot_store(&self) -> crate::Result<ContractStore> {
        Ok(ContractStore::from_contracts(
            self.contract_snapshot()?
                .into_iter()
                .map(StoredContract::into_parts),
        ))
    }

    fn admission_error(error: AdmissionError) -> crate::Error {
        match error.reason() {
            Some(reason) => crate::Error::ContractAdmissionRejected(reason.clone()),
            None => crate::Error::UnexpectedSignalFrame {
                got: error.to_string(),
            },
        }
    }

    fn store_signature_solicitation(
        &self,
        route: SignatureSolicitationRoute,
    ) -> crate::Result<StoredSignatureSolicitation> {
        let stored = StoredSignatureSolicitation::new(route);
        self.tables.put_signature_solicitation(&stored)?;
        Ok(stored)
    }

    fn store_signature_submission(
        &self,
        submission: SignatureSubmission,
    ) -> crate::Result<StoredSignatureSubmission> {
        let stored = StoredSignatureSubmission::new(submission);
        self.tables.put_signature_submission(&stored)?;
        Ok(stored)
    }

    fn store_quorum_round(&self, round: StoredQuorumRound) -> crate::Result<StoredQuorumRound> {
        self.tables.put_quorum_round(&round)?;
        Ok(round)
    }

    fn quorum_round(
        &self,
        round: &QuorumRoundIdentifier,
    ) -> crate::Result<Option<StoredQuorumRound>> {
        self.tables.quorum_round(round)
    }

    fn store_intercept_policy(
        &self,
        draft: InterceptPolicyDraft,
    ) -> crate::Result<StoredInterceptPolicy> {
        self.tables.put_intercept_policy(draft)
    }

    fn cancel_intercept_policy(
        &self,
        cancellation: InterceptPolicyCancellation,
    ) -> crate::Result<()> {
        self.tables.retract_intercept_policy(cancellation.payload())
    }

    fn active_intercept_policies(
        &self,
        now: TimestampNanos,
    ) -> crate::Result<ActiveInterceptPolicies> {
        self.tables.active_intercept_policies(now)
    }

    fn intercept_spirit_authorization(
        &self,
        context: SpiritAuthorizationContext,
        now: TimestampNanos,
    ) -> crate::Result<Option<StoredParkedSpiritRequest>> {
        self.tables.put_parked_spirit_request(context, now)
    }

    fn parked_spirit_request_snapshot(
        &self,
        query: ParkedRequestQuery,
        now: TimestampNanos,
    ) -> crate::Result<ParkedRequestSnapshot> {
        self.tables.parked_spirit_request_snapshot(&query, now)
    }

    fn answer_parked_spirit_request(
        &self,
        answer: ParkedRequestAnswer,
        now: TimestampNanos,
    ) -> crate::Result<ParkedRequestResolution> {
        self.tables.answer_parked_spirit_request(answer, now)
    }

    fn parked_spirit_request_history(&self) -> crate::Result<Vec<StoredParkedSpiritRequest>> {
        self.tables.parked_spirit_requests()
    }
}

impl Actor for StoreKernel {
    type Args = StoreLocation;
    type Error = crate::Error;

    async fn on_start(
        location: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Self::open(location)
    }
}

impl Message<StoreIdentity> for StoreKernel {
    type Reply = crate::Result<StoredIdentityReply>;

    async fn handle(
        &mut self,
        message: StoreIdentity,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_identity(message.registration)
            .map(|identity| StoredIdentityReply { identity })
    }
}

impl Message<StoreRevocation> for StoreKernel {
    type Reply = crate::Result<StoredIdentityReply>;

    async fn handle(
        &mut self,
        message: StoreRevocation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_revocation(message.revocation)
            .map(|identity| StoredIdentityReply { identity })
    }
}

impl Message<LookupIdentity> for StoreKernel {
    type Reply = crate::Result<LookupIdentityReply>;

    async fn handle(
        &mut self,
        message: LookupIdentity,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.lookup_identity(&message.identity)
            .map(|identity| LookupIdentityReply { identity })
    }
}

impl Message<ReadIdentitySnapshot> for StoreKernel {
    type Reply = crate::Result<IdentitySnapshotReply>;

    async fn handle(
        &mut self,
        _message: ReadIdentitySnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.snapshot()
            .map(|identities| IdentitySnapshotReply { identities })
    }
}

impl Message<StoreAttestation> for StoreKernel {
    type Reply = crate::Result<StoredAttestationReply>;

    async fn handle(
        &mut self,
        message: StoreAttestation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_attestation(message.attestation)
            .map(|attestation| StoredAttestationReply { attestation })
    }
}

impl Message<StoreAuthorizationState> for StoreKernel {
    type Reply = crate::Result<StoredAuthorizationStateReply>;

    async fn handle(
        &mut self,
        message: StoreAuthorizationState,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_authorization_state(message.state)
            .map(|state| StoredAuthorizationStateReply { state })
    }
}

impl Message<CreateAuthorizationState> for StoreKernel {
    type Reply = AuthorizationStateCreationReply;

    async fn handle(
        &mut self,
        message: CreateAuthorizationState,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        AuthorizationStateCreationReply::from_result(self.create_authorization_state(message))
    }
}

impl Message<LookupAuthorizationState> for StoreKernel {
    type Reply = crate::Result<LookupAuthorizationStateReply>;

    async fn handle(
        &mut self,
        message: LookupAuthorizationState,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.authorization_state(&message.request_slot)
            .map(|state| LookupAuthorizationStateReply { state })
    }
}

impl Message<ReadAuthorizationSnapshot> for StoreKernel {
    type Reply = crate::Result<AuthorizationSnapshotReply>;

    async fn handle(
        &mut self,
        _message: ReadAuthorizationSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.authorization_snapshot()
            .map(|states| AuthorizationSnapshotReply { states })
    }
}

impl Message<StoreContract> for StoreKernel {
    type Reply = ContractStorageReply;

    async fn handle(
        &mut self,
        message: StoreContract,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        ContractStorageReply::from_result(self.store_contract(message.contract))
    }
}

impl Message<LookupContract> for StoreKernel {
    type Reply = crate::Result<LookupContractReply>;

    async fn handle(
        &mut self,
        message: LookupContract,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.lookup_contract(&message.digest)
            .map(|contract| LookupContractReply { contract })
    }
}

impl Message<ReadContractSnapshot> for StoreKernel {
    type Reply = crate::Result<ContractSnapshotReply>;

    async fn handle(
        &mut self,
        _message: ReadContractSnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.contract_snapshot()
            .map(|contracts| ContractSnapshotReply { contracts })
    }
}

impl Message<StoreSignatureSolicitation> for StoreKernel {
    type Reply = crate::Result<StoredSignatureSolicitationReply>;

    async fn handle(
        &mut self,
        message: StoreSignatureSolicitation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_signature_solicitation(message.route)
            .map(|solicitation| StoredSignatureSolicitationReply { solicitation })
    }
}

impl Message<StoreSignatureSubmission> for StoreKernel {
    type Reply = crate::Result<StoredSignatureSubmissionReply>;

    async fn handle(
        &mut self,
        message: StoreSignatureSubmission,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_signature_submission(message.submission)
            .map(|submission| StoredSignatureSubmissionReply { submission })
    }
}

impl Message<StoreQuorumRound> for StoreKernel {
    type Reply = crate::Result<StoredQuorumRoundReply>;

    async fn handle(
        &mut self,
        message: StoreQuorumRound,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_quorum_round(message.round)
            .map(|round| StoredQuorumRoundReply { round })
    }
}

impl Message<LookupQuorumRound> for StoreKernel {
    type Reply = crate::Result<LookupQuorumRoundReply>;

    async fn handle(
        &mut self,
        message: LookupQuorumRound,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.quorum_round(&message.round)
            .map(|round| LookupQuorumRoundReply { round })
    }
}

impl Message<StoreInterceptPolicy> for StoreKernel {
    type Reply = crate::Result<StoredInterceptPolicyReply>;

    async fn handle(
        &mut self,
        message: StoreInterceptPolicy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.store_intercept_policy(message.draft)
            .map(|policy| StoredInterceptPolicyReply { policy })
    }
}

impl Message<CancelInterceptPolicy> for StoreKernel {
    type Reply = crate::Result<()>;

    async fn handle(
        &mut self,
        message: CancelInterceptPolicy,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.cancel_intercept_policy(message.cancellation)
    }
}

impl Message<ReadInterceptPolicies> for StoreKernel {
    type Reply = crate::Result<InterceptPoliciesReply>;

    async fn handle(
        &mut self,
        message: ReadInterceptPolicies,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.active_intercept_policies(message.now)
            .map(|policies| InterceptPoliciesReply { policies })
    }
}

impl Message<InterceptSpiritAuthorization> for StoreKernel {
    type Reply = crate::Result<InterceptedSpiritAuthorizationReply>;

    async fn handle(
        &mut self,
        message: InterceptSpiritAuthorization,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.intercept_spirit_authorization(message.context, message.now)
            .map(|request| InterceptedSpiritAuthorizationReply { request })
    }
}

impl Message<FetchParkedSpiritRequests> for StoreKernel {
    type Reply = crate::Result<ParkedSpiritRequestSnapshotReply>;

    async fn handle(
        &mut self,
        message: FetchParkedSpiritRequests,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.parked_spirit_request_snapshot(message.query, message.now)
            .map(|snapshot| ParkedSpiritRequestSnapshotReply { snapshot })
    }
}

impl Message<AnswerParkedSpiritRequest> for StoreKernel {
    type Reply = crate::Result<ParkedSpiritRequestResolutionReply>;

    async fn handle(
        &mut self,
        message: AnswerParkedSpiritRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.answer_parked_spirit_request(message.answer, message.now)
            .map(|resolution| ParkedSpiritRequestResolutionReply { resolution })
    }
}

impl Message<ReadParkedSpiritRequestHistory> for StoreKernel {
    type Reply = crate::Result<ParkedSpiritRequestHistoryReply>;

    async fn handle(
        &mut self,
        _message: ReadParkedSpiritRequestHistory,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.parked_spirit_request_history()
            .map(|requests| ParkedSpiritRequestHistoryReply { requests })
    }
}
