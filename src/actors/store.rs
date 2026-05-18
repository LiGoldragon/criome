use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_criome::{
    Attestation, AuthorizationDenial, AuthorizationGrant, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, Identity, IdentityRegistration,
    IdentityRevocation, ObjectDigest, PrincipalStatus, SignatureSolicitationRoute,
    SignatureSubmission,
};

use crate::tables::{
    AuthorizationReplayIdentity, CriomeTables, StoreLocation, StoredAttestation,
    StoredAuthorizationState, StoredIdentity, StoredRevocation, StoredSignatureSolicitation,
    StoredSignatureSubmission,
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
    replay_identity: AuthorizationReplayIdentity,
}

pub struct LookupAuthorizationState {
    request_slot: AuthorizationRequestSlot,
}

pub struct ReadAuthorizationSnapshot;

pub struct StoreSignatureSolicitation {
    route: SignatureSolicitationRoute,
}

pub struct StoreSignatureSubmission {
    submission: SignatureSubmission,
}

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

#[derive(kameo::Reply)]
pub struct LookupAuthorizationStateReply {
    state: Option<StoredAuthorizationState>,
}

#[derive(kameo::Reply)]
pub struct AuthorizationSnapshotReply {
    states: Vec<StoredAuthorizationState>,
}

#[derive(kameo::Reply)]
pub struct StoredSignatureSolicitationReply {
    solicitation: StoredSignatureSolicitation,
}

#[derive(kameo::Reply)]
pub struct StoredSignatureSubmissionReply {
    submission: StoredSignatureSubmission,
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
            replay_identity: AuthorizationReplayIdentity::new(
                authorization.requester.clone(),
                authorization.nonce.clone(),
            ),
        }
    }

    pub fn expired(authorization: &signal_criome::SignalCallAuthorization) -> Self {
        Self {
            request_digest: authorization.request_digest.clone(),
            status: AuthorizationStatus::Expired,
            missing_authorities: Vec::new(),
            grant: None,
            denial: None,
            replay_identity: AuthorizationReplayIdentity::new(
                authorization.requester.clone(),
                authorization.nonce.clone(),
            ),
        }
    }
}

impl LookupAuthorizationState {
    pub fn new(request_slot: AuthorizationRequestSlot) -> Self {
        Self { request_slot }
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
        self.tables.put_new_authorization_state(
            state.request_digest,
            state.status,
            state.missing_authorities,
            state.grant,
            state.denial,
            state.replay_identity,
        )
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
            left.state()
                .request_slot
                .as_str()
                .cmp(right.state().request_slot.as_str())
        });
        Ok(states)
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

pub fn active_status(identity: &StoredIdentity) -> bool {
    identity.status() == PrincipalStatus::Active
}
