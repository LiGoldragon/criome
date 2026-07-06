use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::message::{Context, Message};
use meta_signal_criome::{
    AuthorizationApproval, AuthorizationApprovalDecision, AuthorizationApprovalRecorded,
    ConfigurationRejectionReason, OperationKind, PendingFounding, RequestUnimplemented,
    RootFoundingAcceptance, RootFoundingAccepted, RootFoundingInitiation, RootFoundingObservation,
    RootFoundingRejectionReason, RootFoundingState, RootFoundingStatus, UnimplementedReason,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizationAttestationRequest,
    AuthorizationDenial, AuthorizationDenialReason, AuthorizationDenialSource,
    AuthorizationEvaluated, AuthorizationEvaluation, AuthorizationMode,
    AuthorizationObservationToken, AuthorizationPending, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, AuthorizedObjectReference,
    AuthorizedObjectUpdate, AuthorizedObjectUpdateToken, BlsPublicKey, ContractAdmissionRejected,
    ContractAdmitted, ContractFound, ContractMissing, ContractOperationHead,
    CriomeDaemonConfiguration, CriomeReply, CriomeRequest, EvaluationDecision, Evidence,
    FoundedRoot, FoundingConveyance, FoundingConveyanceOutcome, FoundingConveyanceReceipt,
    FoundingProposal, FoundingSignature, FoundingSignatureReturn, Identity, IdentityRegistration,
    IdentitySubscriptionToken, InterceptPolicyCancellation, InterceptPolicyProposal, KeyPurpose,
    OperationDigest, ParkedAuthorization, ParkedAuthorizationObservation,
    ParkedAuthorizationSnapshot, ParkedRequestAnswer, ParkedRequestDecision,
    ParkedRequestIdentifier, ParkedRequestQuery, ParkedSpiritRequest, PolicyMember, QuorumConflict,
    QuorumProposal, QuorumRoundIdentifier, QuorumRoundState, QuorumRoundStatus, QuorumVote,
    QuorumVoteSolicitation, RejectionReason, RequiredSignatureThreshold, RootAnchorDigest,
    RoundPhase, Rule, SignalCallAuthorization, SpiritAuthorizationContext,
    StampedSignatureEnvelope, TimeSignature, TimestampNanos,
};
use tokio::sync::broadcast;

use crate::actors::{
    CriomeActorReply, actor_reply, authorization, registry, rejection, signer, store, subscription,
    verifier,
};
use crate::admission::ClusterRoot;
use crate::conveyance::{NoConveyance, PeerConveyance};
use crate::founding::RootFounding;
use crate::language::{ContractStore, EvaluationError, KeyRegistry};
use crate::master_key::MasterKey;
use crate::master_key::{SystemClock, WindowAdmission};
use crate::tables::{
    StoredCoSignedSuccessor, StoredContractHead, StoredPendingFounding, StoredQuorumRound,
};
use crate::{Error, Result, StoreLocation};

pub struct CriomeRoot {
    registry: ActorRef<registry::IdentityRegistry>,
    signer: ActorRef<signer::AttestationSigner>,
    verifier: ActorRef<verifier::AttestationVerifier>,
    authorization: ActorRef<authorization::AuthorizationCoordinator>,
    subscription: ActorRef<subscription::SubscriptionRegistry>,
    store: ActorRef<store::StoreKernel>,
    authorization_mode: AuthorizationMode,
    configuration_generation: u64,
    authorization_updates: broadcast::Sender<AuthorizationStateRecord>,
    /// The identity this node casts its quorum votes as — the same identity its
    /// master key is registered under, so a peer's registry verifies its votes.
    node_identity: Identity,
    /// How this node conveys solicitations and votes to peer members' criomes.
    conveyance: Arc<dyn PeerConveyance>,
    /// This node's own clock, consulted by the peer witness-clock re-check so a
    /// solicited peer independently refuses a window its clock is not inside —
    /// the same gate the signer enforces before time-signing.
    clock: SystemClock,
    /// The Request-phase rounds this node originated (proposed). Only the
    /// originator drives the commit round when round 1 reaches a majority; a peer
    /// whose round-1 round reaches a majority through conveyed round-1 evidence
    /// does not re-drive. Keyed by the round identifier's canonical text.
    originated_request_rounds: HashSet<String>,
    /// One honest successor per state-point: the successor this node has co-signed
    /// for each `(contract, head)`, keyed by [`Self::state_point_key`]. A
    /// conflicting second successor from the same head is refused with
    /// `QuorumConflict` (the pluggable phase-2 commutative-merge seam lives in
    /// [`Self::successors_conflict`]).
    co_signed_successors: HashMap<String, AuthorizedObjectReference>,
    /// This node's view of each contract's current head — the state-point a change
    /// advances from — keyed by the contract digest's text. Absent ⇒ the
    /// contract's genesis head; advanced when a successor commits on round 2.
    contract_heads: HashMap<String, ContractOperationHead>,
}

pub struct Arguments {
    pub store: StoreLocation,
    pub cluster_root: Option<BlsPublicKey>,
    pub authorization_mode: AuthorizationMode,
    /// The identity this criome signs attestations as. A single-node deployment
    /// keeps the historical `Host("criome")`; a multi-node cluster gives each
    /// node a distinct identity so peers cross-verify by registered key.
    pub node_identity: Identity,
    /// How this node conveys quorum solicitations and votes to peer members.
    /// Defaults to the unarmed [`NoConveyance`]; a deployment supplies a
    /// router-mediated or direct-dial conveyance.
    pub conveyance: Arc<dyn PeerConveyance>,
    /// This node's clock. The peer witness-clock re-check reads it, and the same
    /// clock is handed to the signer; a pinned clock makes the gate deterministic
    /// under test. Defaults to the real wall clock.
    pub clock: SystemClock,
}

pub struct SubmitRequest {
    request: CriomeRequest,
}

pub struct SubmitMetaRequest {
    request: meta_signal_criome::Input,
}

pub struct OpenAuthorizationObservation {
    request_slot: AuthorizationRequestSlot,
}

pub struct ReadTopology;

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct CriomeTopology {
    registry: bool,
    signer: bool,
    verifier: bool,
    authorization: bool,
    subscription: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct CriomeMetaActorReply {
    reply: meta_signal_criome::Output,
}

pub struct AuthorizationObservationOpened {
    token: AuthorizationObservationToken,
    snapshot: signal_criome::AuthorizationObservationSnapshot,
    updates: broadcast::Receiver<AuthorizationStateRecord>,
}

impl Arguments {
    pub fn new(store: StoreLocation) -> Self {
        Self {
            store,
            cluster_root: None,
            authorization_mode: AuthorizationMode::Quorum,
            node_identity: Self::default_node_identity(),
            conveyance: Arc::new(NoConveyance),
            clock: SystemClock::system(),
        }
    }

    /// Arm this node's peer conveyance (router-mediated or direct-dial). Absent, the
    /// node self-votes but originates no solicitation.
    pub fn with_peer_conveyance(mut self, conveyance: Arc<dyn PeerConveyance>) -> Self {
        self.conveyance = conveyance;
        self
    }

    /// The historical single-node signing identity, used when a deployment does
    /// not configure a distinct per-node identity.
    pub fn default_node_identity() -> Identity {
        Identity::host("criome".to_string())
    }
}

impl SubmitRequest {
    pub fn new(request: CriomeRequest) -> Self {
        Self { request }
    }
}

impl SubmitMetaRequest {
    pub fn new(request: meta_signal_criome::Input) -> Self {
        Self { request }
    }
}

impl OpenAuthorizationObservation {
    pub fn new(request_slot: AuthorizationRequestSlot) -> Self {
        Self { request_slot }
    }
}

impl AuthorizationObservationOpened {
    pub fn token(&self) -> &AuthorizationObservationToken {
        &self.token
    }

    pub fn snapshot(&self) -> &signal_criome::AuthorizationObservationSnapshot {
        &self.snapshot
    }

    pub fn into_updates(self) -> broadcast::Receiver<AuthorizationStateRecord> {
        self.updates
    }
}

impl CriomeMetaActorReply {
    pub fn new(reply: meta_signal_criome::Output) -> Self {
        Self { reply }
    }

    pub fn into_reply(self) -> meta_signal_criome::Output {
        self.reply
    }
}

impl CriomeTopology {
    fn complete() -> Self {
        Self {
            registry: true,
            signer: true,
            verifier: true,
            authorization: true,
            subscription: true,
        }
    }

    pub const fn registry(&self) -> bool {
        self.registry
    }

    pub const fn signer(&self) -> bool {
        self.signer
    }

    pub const fn verifier(&self) -> bool {
        self.verifier
    }

    pub const fn authorization(&self) -> bool {
        self.authorization
    }

    pub const fn subscription(&self) -> bool {
        self.subscription
    }
}

impl CriomeRoot {
    #[allow(clippy::too_many_arguments)]
    fn new(
        registry: ActorRef<registry::IdentityRegistry>,
        signer: ActorRef<signer::AttestationSigner>,
        verifier: ActorRef<verifier::AttestationVerifier>,
        authorization: ActorRef<authorization::AuthorizationCoordinator>,
        subscription: ActorRef<subscription::SubscriptionRegistry>,
        store: ActorRef<store::StoreKernel>,
        authorization_mode: AuthorizationMode,
        node_identity: Identity,
        conveyance: Arc<dyn PeerConveyance>,
        clock: SystemClock,
    ) -> Self {
        let (authorization_updates, _updates) = broadcast::channel(128);
        Self {
            registry,
            signer,
            verifier,
            authorization,
            subscription,
            store,
            authorization_mode,
            configuration_generation: 0,
            authorization_updates,
            node_identity,
            conveyance,
            clock,
            originated_request_rounds: HashSet::new(),
            co_signed_successors: HashMap::new(),
            contract_heads: HashMap::new(),
        }
    }

    pub async fn start(arguments: Arguments) -> Result<ActorRef<Self>> {
        let actor_reference = Self::spawn(arguments);
        actor_reference.wait_for_startup().await;
        if !actor_reference.is_alive() {
            return Err(Error::Startup(
                "criome root failed to start (see the daemon log for the typed startup error)"
                    .to_string(),
            ));
        }
        Ok(actor_reference)
    }

    pub async fn stop(actor_reference: ActorRef<Self>) -> Result<()> {
        actor_reference
            .stop_gracefully()
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        actor_reference.wait_for_shutdown().await;
        Ok(())
    }

    async fn submit(&mut self, request: CriomeRequest) -> CriomeReply {
        match request {
            CriomeRequest::Sign(request) => {
                self.ask_signer(signer::SignContent::new(request)).await
            }
            CriomeRequest::VerifyAttestation(request) => {
                self.ask_verifier(verifier::VerifyAttestation::new(request))
                    .await
            }
            CriomeRequest::RegisterIdentity(request) => {
                self.ask_registry(registry::RegisterIdentity::new(request))
                    .await
            }
            CriomeRequest::RevokeIdentity(request) => {
                self.ask_registry(registry::RevokeIdentity::new(request))
                    .await
            }
            CriomeRequest::LookupIdentity(request) => {
                self.ask_registry(registry::LookupIdentity::new(request))
                    .await
            }
            CriomeRequest::AttestArchive(request) => {
                self.ask_signer(signer::AttestArchive::new(request)).await
            }
            CriomeRequest::AttestChannelGrant(request) => {
                self.ask_signer(signer::AttestChannelGrant::new(request))
                    .await
            }
            CriomeRequest::AttestAuthorization(request) => {
                let AuthorizationAttestationRequest {
                    authorization_content,
                    source,
                    audit_context,
                } = request;
                self.ask_signer(signer::AttestAuthorization::new(
                    authorization_content,
                    source,
                    audit_context,
                ))
                .await
            }
            CriomeRequest::AuthorizeSignalCall(request) => {
                if let Some(pending) = self.intercept_signal_authorization(request.clone()).await {
                    return pending;
                }
                if self.authorization_mode == AuthorizationMode::AutoApprove {
                    self.auto_approve_signal_call(request).await
                } else if self.authorization_mode == AuthorizationMode::ClientApproval {
                    self.park_signal_authorization(request).await
                } else {
                    self.ask_authorization(authorization::AuthorizeSignalCall::new(request))
                        .await
                }
            }
            CriomeRequest::ObserveAuthorization(request) => {
                self.ask_authorization(authorization::ObserveAuthorization::new(request))
                    .await
            }
            CriomeRequest::ObserveParkedAuthorizations(request) => {
                self.parked_authorization_snapshot(request).await
            }
            CriomeRequest::VerifyAuthorization(request) => {
                self.ask_authorization(authorization::VerifyAuthorization::new(request))
                    .await
            }
            CriomeRequest::RouteSignatureRequest(request) => {
                self.ask_authorization(authorization::RouteSignatureRequest::new(request))
                    .await
            }
            CriomeRequest::SubmitSignature(request) => {
                self.ask_authorization(authorization::SubmitSignature::new(request))
                    .await
            }
            CriomeRequest::RejectAuthorization(request) => {
                if self.authorization_mode == AuthorizationMode::ClientApproval {
                    return rejection(RejectionReason::MalformedRequest);
                }
                self.ask_authorization(authorization::RejectAuthorization::new(request))
                    .await
            }
            CriomeRequest::AdmitContract(contract) => self.admit_contract(contract).await,
            CriomeRequest::LookupContract(digest) => self.lookup_contract(digest).await,
            CriomeRequest::EvaluateAuthorization(evaluation) => {
                self.evaluate_authorization(evaluation).await
            }
            CriomeRequest::ObserveAuthorizedObjects(request) => {
                self.ask_subscription(subscription::OpenAuthorizedObjectSubscription {
                    token: AuthorizedObjectUpdateToken {
                        subscriber: request.subscriber,
                        interest: request.interest,
                    },
                })
                .await
            }
            CriomeRequest::AuthorizedObjectUpdateRetraction(token) => {
                self.ask_subscription(subscription::CloseAuthorizedObjectSubscription { token })
                    .await
            }
            CriomeRequest::ScheduleContractTimeCheck(check) => {
                self.ask_subscription(subscription::ScheduleContractTimeCheck::new(check))
                    .await
            }
            CriomeRequest::RunDueContractChecks(stamp) => {
                self.ask_subscription(subscription::RunDueContractChecks::new(stamp))
                    .await
            }
            CriomeRequest::SubscribeIdentityUpdates(request) => {
                let token = IdentitySubscriptionToken::new(request.into_payload());
                self.ask_subscription(subscription::OpenIdentitySubscription { token })
                    .await
            }
            CriomeRequest::IdentitySubscriptionRetraction(token) => {
                self.ask_subscription(subscription::CloseIdentitySubscription { token })
                    .await
            }
            CriomeRequest::AuthorizationObservationRetraction(token) => {
                self.ask_authorization(authorization::CloseAuthorizationObservation::new(token))
                    .await
            }
            CriomeRequest::ProposeQuorumAuthorization(proposal) => {
                self.propose_quorum_authorization(proposal).await
            }
            CriomeRequest::SolicitQuorumVote(solicitation) => {
                self.solicit_quorum_vote(solicitation).await
            }
            CriomeRequest::SubmitQuorumVote(vote) => self.submit_quorum_vote(vote).await,
            CriomeRequest::ObserveQuorumRound(query) => {
                self.observe_quorum_round(query.into_payload()).await
            }
            CriomeRequest::ObserveNodePublicKey(_observation) => {
                self.observe_node_public_key().await
            }
            CriomeRequest::ConveyFounding(conveyance) => self.convey_founding(conveyance).await,
        }
    }

    async fn open_authorization_observation(
        &self,
        request_slot: AuthorizationRequestSlot,
    ) -> Result<AuthorizationObservationOpened> {
        let token = AuthorizationObservationToken::new(request_slot.clone());
        let updates = self.authorization_updates.subscribe();
        let states = self
            .lookup_authorization_state(request_slot)
            .await?
            .map(crate::tables::StoredAuthorizationState::into_state)
            .into_iter()
            .collect();
        Ok(AuthorizationObservationOpened {
            token,
            snapshot: signal_criome::AuthorizationObservationSnapshot::from_states(states),
            updates,
        })
    }

    async fn submit_meta(
        &mut self,
        request: meta_signal_criome::Input,
    ) -> meta_signal_criome::Output {
        match request {
            meta_signal_criome::Input::Configure(configuration) => {
                self.configure(configuration).await
            }
            meta_signal_criome::Input::CreateInterceptPolicy(request) => {
                self.create_intercept_policy_meta(request).await
            }
            meta_signal_criome::Input::ReplaceInterceptPolicy(request) => {
                self.replace_intercept_policy_meta(request).await
            }
            meta_signal_criome::Input::CancelInterceptPolicy(request) => {
                self.cancel_intercept_policy_meta(request).await
            }
            meta_signal_criome::Input::ListInterceptPolicies(request) => {
                self.list_intercept_policies_meta(request).await
            }
            meta_signal_criome::Input::ObserveInterceptPolicies(request) => {
                self.observe_intercept_policies_meta(request).await
            }
            meta_signal_criome::Input::RetractInterceptPolicyObservation(token) => {
                meta_signal_criome::Output::InterceptPolicyObservationRetracted(token)
            }
            meta_signal_criome::Input::FetchParkedRequests(request) => {
                self.fetch_parked_requests_meta(request).await
            }
            meta_signal_criome::Input::AnswerParkedRequest(request) => {
                self.answer_parked_request_meta(request).await
            }
            meta_signal_criome::Input::ObserveParkedAuthorizations(request) => {
                meta_signal_criome::Output::ParkedAuthorizationSnapshot(
                    self.read_parked_authorization_snapshot(request).await,
                )
            }
            meta_signal_criome::Input::SubmitAuthorizationApproval(approval) => {
                self.record_authorization_approval(approval).await
            }
            meta_signal_criome::Input::AcceptRootFounding(acceptance) => {
                self.accept_root_founding(acceptance).await
            }
            meta_signal_criome::Input::InitiateRootFounding(initiation) => {
                self.initiate_root_founding(initiation).await
            }
            meta_signal_criome::Input::ObserveRootFounding(observation) => {
                self.observe_root_founding(observation).await
            }
        }
    }

    async fn intercept_signal_authorization(
        &self,
        authorization: SignalCallAuthorization,
    ) -> Option<CriomeReply> {
        let context = authorization.spirit_context()?.clone();
        match self
            .intercept_spirit_authorization(context, self.timestamp())
            .await
        {
            Ok(Some(_parked)) => {
                match self
                    .create_authorization_state(
                        store::CreateAuthorizationState::pending_signal_authorization(
                            authorization,
                        ),
                    )
                    .await
                {
                    Ok(stored) => {
                        let state = stored.state();
                        let request_slot = state.request_slot.clone();
                        self.publish_authorization_update(state.clone());
                        Some(CriomeReply::AuthorizationPending(
                            AuthorizationPending::new(
                                request_slot.clone(),
                                state.request_digest.clone(),
                                Vec::new(),
                                AuthorizationObservationToken::new(request_slot),
                            ),
                        ))
                    }
                    Err(Error::AuthorizationReplayAttempted) => {
                        Some(rejection(RejectionReason::ReplayAttempted))
                    }
                    Err(_error) => Some(rejection(RejectionReason::MalformedRequest)),
                }
            }
            Ok(None) => None,
            Err(_error) => Some(rejection(RejectionReason::MalformedRequest)),
        }
    }

    fn unimplemented_meta_request(operation: OperationKind) -> meta_signal_criome::Output {
        meta_signal_criome::Output::request_unimplemented(RequestUnimplemented {
            operation,
            reason: UnimplementedReason::DependencyNotReady,
        })
    }

    fn timestamp(&self) -> TimestampNanos {
        SystemClock::system().timestamp()
    }

    async fn configure(
        &mut self,
        configuration: CriomeDaemonConfiguration,
    ) -> meta_signal_criome::Output {
        if configuration.socket_path.payload().is_empty()
            || configuration.store_path.payload().is_empty()
        {
            return meta_signal_criome::Output::configuration_rejected(
                ConfigurationRejectionReason::MalformedConfiguration,
            );
        }
        self.authorization_mode = *configuration.authorization_mode();
        let cluster_root = configuration.cluster_root().cloned().map(ClusterRoot::new);
        let _ = self
            .registry
            .ask(registry::ConfigureClusterRoot::new(cluster_root))
            .await;
        self.configuration_generation += 1;
        meta_signal_criome::Output::configured(meta_signal_criome::ConfigurationGeneration::new(
            self.configuration_generation,
        ))
    }

    async fn create_intercept_policy(
        &self,
        proposal: InterceptPolicyProposal,
        now: TimestampNanos,
    ) -> Result<crate::tables::StoredInterceptPolicy> {
        let reply = self
            .store
            .ask(store::StoreInterceptPolicy::create(proposal, now))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_policy())
    }

    async fn replace_intercept_policy(
        &self,
        proposal: InterceptPolicyProposal,
        now: TimestampNanos,
    ) -> Result<crate::tables::StoredInterceptPolicy> {
        let reply = self
            .store
            .ask(store::StoreInterceptPolicy::replace(proposal, now))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_policy())
    }

    async fn cancel_intercept_policy(
        &self,
        cancellation: InterceptPolicyCancellation,
    ) -> Result<()> {
        self.store
            .ask(store::CancelInterceptPolicy::new(cancellation))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(())
    }

    async fn active_intercept_policies(
        &self,
        now: TimestampNanos,
    ) -> Result<signal_criome::ActiveInterceptPolicies> {
        let reply = self
            .store
            .ask(store::ReadInterceptPolicies::new(now))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_policies())
    }

    #[allow(dead_code)]
    async fn intercept_spirit_authorization(
        &self,
        context: SpiritAuthorizationContext,
        now: TimestampNanos,
    ) -> Result<Option<crate::tables::StoredParkedSpiritRequest>> {
        let reply = self
            .store
            .ask(store::InterceptSpiritAuthorization::new(context, now))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_request())
    }

    async fn parked_spirit_requests(
        &self,
        query: ParkedRequestQuery,
        now: TimestampNanos,
    ) -> Result<signal_criome::ParkedRequestSnapshot> {
        let reply = self
            .store
            .ask(store::FetchParkedSpiritRequests::new(query, now))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_snapshot())
    }

    async fn answer_parked_spirit_request(
        &self,
        answer: ParkedRequestAnswer,
        now: TimestampNanos,
    ) -> Result<signal_criome::ParkedRequestResolution> {
        let reply = self
            .store
            .ask(store::AnswerParkedSpiritRequest::new(answer, now))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_resolution())
    }

    async fn create_intercept_policy_meta(
        &self,
        proposal: InterceptPolicyProposal,
    ) -> meta_signal_criome::Output {
        match self
            .create_intercept_policy(proposal, self.timestamp())
            .await
        {
            Ok(policy) => {
                meta_signal_criome::Output::intercept_policy_created(policy.into_policy())
            }
            Err(_error) => Self::unimplemented_meta_request(OperationKind::CreateInterceptPolicy),
        }
    }

    async fn replace_intercept_policy_meta(
        &self,
        proposal: InterceptPolicyProposal,
    ) -> meta_signal_criome::Output {
        match self
            .replace_intercept_policy(proposal, self.timestamp())
            .await
        {
            Ok(policy) => {
                meta_signal_criome::Output::intercept_policy_replaced(policy.into_policy())
            }
            Err(_error) => Self::unimplemented_meta_request(OperationKind::ReplaceInterceptPolicy),
        }
    }

    async fn cancel_intercept_policy_meta(
        &self,
        cancellation: InterceptPolicyCancellation,
    ) -> meta_signal_criome::Output {
        let identifier = cancellation.payload().clone();
        match self.cancel_intercept_policy(cancellation).await {
            Ok(()) => meta_signal_criome::Output::intercept_policy_cancelled(identifier),
            Err(_error) => Self::unimplemented_meta_request(OperationKind::CancelInterceptPolicy),
        }
    }

    async fn list_intercept_policies_meta(
        &self,
        _request: meta_signal_criome::InterceptPolicyObservation,
    ) -> meta_signal_criome::Output {
        match self.active_intercept_policies(self.timestamp()).await {
            Ok(policies) => meta_signal_criome::Output::intercept_policies_listed(policies),
            Err(_error) => Self::unimplemented_meta_request(OperationKind::ListInterceptPolicies),
        }
    }

    async fn observe_intercept_policies_meta(
        &self,
        _request: meta_signal_criome::InterceptPolicyObservation,
    ) -> meta_signal_criome::Output {
        match self.active_intercept_policies(self.timestamp()).await {
            Ok(policies) => {
                meta_signal_criome::Output::intercept_policy_observation_opened(policies)
            }
            Err(_error) => {
                Self::unimplemented_meta_request(OperationKind::ObserveInterceptPolicies)
            }
        }
    }

    async fn fetch_parked_requests_meta(
        &self,
        query: ParkedRequestQuery,
    ) -> meta_signal_criome::Output {
        match self.parked_spirit_requests(query, self.timestamp()).await {
            Ok(snapshot) => meta_signal_criome::Output::parked_requests_fetched(snapshot),
            Err(_error) => Self::unimplemented_meta_request(OperationKind::FetchParkedRequests),
        }
    }

    async fn answer_parked_request_meta(
        &self,
        answer: ParkedRequestAnswer,
    ) -> meta_signal_criome::Output {
        let parked_request = self
            .parked_spirit_request(&answer.identifier)
            .await
            .ok()
            .flatten()
            .map(|stored| stored.request().clone());
        let decision = answer.decision;
        match self
            .answer_parked_spirit_request(answer, self.timestamp())
            .await
        {
            Ok(resolution) => {
                if let Some(request) = parked_request.as_ref() {
                    self.apply_parked_spirit_request_authorization_resolution(request, decision)
                        .await;
                }
                meta_signal_criome::Output::parked_request_answered(resolution)
            }
            Err(_error) => Self::unimplemented_meta_request(OperationKind::AnswerParkedRequest),
        }
    }

    async fn parked_spirit_request(
        &self,
        identifier: &ParkedRequestIdentifier,
    ) -> Result<Option<crate::tables::StoredParkedSpiritRequest>> {
        let reply = self
            .store
            .ask(store::ReadParkedSpiritRequestHistory)
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply
            .into_requests()
            .into_iter()
            .find(|request| &request.request().identifier == identifier))
    }

    async fn apply_parked_spirit_request_authorization_resolution(
        &self,
        request: &ParkedSpiritRequest,
        decision: ParkedRequestDecision,
    ) {
        let Some(state) = self
            .parked_spirit_request_authorization_state(request)
            .await
        else {
            return;
        };
        let Some(authorization) = state.signal_authorization().cloned() else {
            return;
        };
        let decision = match decision {
            ParkedRequestDecision::Approve => AuthorizationApprovalDecision::Approve,
            ParkedRequestDecision::Reject => AuthorizationApprovalDecision::Reject,
        };
        self.apply_signal_authorization_approval(state, decision, authorization)
            .await;
    }

    async fn parked_spirit_request_authorization_state(
        &self,
        request: &ParkedSpiritRequest,
    ) -> Option<AuthorizationStateRecord> {
        let request_digest = signal_criome::ObjectDigest::from_bytes(
            request.context.raw_payload.as_str().as_bytes(),
        );
        let reply = self
            .store
            .ask(store::ReadAuthorizationSnapshot)
            .await
            .ok()?;
        reply
            .into_states()
            .into_iter()
            .map(crate::tables::StoredAuthorizationState::into_state)
            .find(|state| {
                matches!(
                    state.status,
                    AuthorizationStatus::Pending | AuthorizationStatus::Parked
                ) && state.request_digest == request_digest
                    && state
                        .signal_authorization()
                        .and_then(SignalCallAuthorization::spirit_context)
                        == Some(&request.context)
            })
    }

    async fn evaluate_authorization(&self, evaluation: AuthorizationEvaluation) -> CriomeReply {
        if &evaluation.object.digest != evaluation.evidence.operation.object_digest() {
            return rejection(RejectionReason::MalformedRequest);
        }

        if self.authorization_mode == AuthorizationMode::AutoApprove {
            return self
                .record_evaluation_decision(evaluation, EvaluationDecision::Authorized)
                .await;
        }

        if self.authorization_mode == AuthorizationMode::ClientApproval {
            return self.park_authorization(evaluation).await;
        }

        match (self.key_registry().await, self.contract_store().await) {
            (Some(registry), Some(store)) => {
                match store.evaluate(&evaluation.contract, &evaluation.evidence, &registry) {
                    Ok(decision) => self.record_evaluation_decision(evaluation, decision).await,
                    Err(EvaluationError::MissingContract(digest)) => {
                        CriomeReply::ContractMissing(ContractMissing::new(digest))
                    }
                }
            }
            _ => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn park_authorization(&self, evaluation: AuthorizationEvaluation) -> CriomeReply {
        match self
            .create_authorization_state(store::CreateAuthorizationState::parked(evaluation))
            .await
        {
            Ok(stored) => {
                let state = stored.state();
                self.publish_authorization_update(state.clone());
                CriomeReply::AuthorizationPending(AuthorizationPending::new(
                    state.request_slot.clone(),
                    state.request_digest.clone(),
                    Vec::new(),
                    AuthorizationObservationToken::new(state.request_slot.clone()),
                ))
            }
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn park_signal_authorization(
        &self,
        authorization: SignalCallAuthorization,
    ) -> CriomeReply {
        match self
            .create_authorization_state(
                store::CreateAuthorizationState::parked_signal_authorization(authorization),
            )
            .await
        {
            Ok(stored) => {
                let state = stored.state();
                self.publish_authorization_update(state.clone());
                CriomeReply::AuthorizationPending(AuthorizationPending::new(
                    state.request_slot.clone(),
                    state.request_digest.clone(),
                    Vec::new(),
                    AuthorizationObservationToken::new(state.request_slot.clone()),
                ))
            }
            Err(Error::AuthorizationReplayAttempted) => rejection(RejectionReason::ReplayAttempted),
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn parked_authorization_snapshot(
        &self,
        request: ParkedAuthorizationObservation,
    ) -> CriomeReply {
        CriomeReply::ParkedAuthorizationSnapshot(
            self.read_parked_authorization_snapshot(request).await,
        )
    }

    async fn record_evaluation_decision(
        &self,
        evaluation: AuthorizationEvaluation,
        decision: EvaluationDecision,
    ) -> CriomeReply {
        if decision == EvaluationDecision::Authorized {
            self.publish_authorized_object_update(AuthorizedObjectUpdate {
                object: evaluation.object,
                contract: evaluation.contract.clone(),
                decision: decision.clone(),
                stamp: evaluation.evidence.stamp.clone(),
            })
            .await;
        }
        CriomeReply::AuthorizationEvaluated(AuthorizationEvaluated {
            contract: evaluation.contract,
            decision,
        })
    }

    async fn record_authorization_approval(
        &self,
        approval: AuthorizationApproval,
    ) -> meta_signal_criome::Output {
        let AuthorizationApproval {
            request_slot,
            decision,
        } = approval;
        let recorded_decision = match self
            .lookup_authorization_state(request_slot.clone())
            .await
            .ok()
            .flatten()
            .map(crate::tables::StoredAuthorizationState::into_state)
        {
            Some(state) => {
                self.apply_authorization_approval(state, decision).await;
                decision
            }
            None => {
                return meta_signal_criome::Output::request_unimplemented(RequestUnimplemented {
                    operation: OperationKind::SubmitAuthorizationApproval,
                    reason: UnimplementedReason::DependencyNotReady,
                });
            }
        };

        meta_signal_criome::Output::authorization_approval_recorded(AuthorizationApprovalRecorded {
            request_slot,
            decision: recorded_decision,
        })
    }

    async fn apply_authorization_approval(
        &self,
        state: AuthorizationStateRecord,
        decision: AuthorizationApprovalDecision,
    ) {
        if decision == AuthorizationApprovalDecision::Defer {
            return;
        }
        if let Some(authorization) = state.signal_authorization().cloned() {
            self.apply_signal_authorization_approval(state, decision, authorization)
                .await;
            return;
        }
        let Some(evaluation) = state.parked_evaluation().cloned() else {
            return;
        };
        if decision == AuthorizationApprovalDecision::Approve {
            self.publish_authorized_object_update(AuthorizedObjectUpdate {
                object: evaluation.object.clone(),
                contract: evaluation.contract.clone(),
                decision: EvaluationDecision::Authorized,
                stamp: evaluation.evidence.stamp.clone(),
            })
            .await;
        }
        let denial =
            (decision == AuthorizationApprovalDecision::Reject).then_some(AuthorizationDenial {
                source: AuthorizationDenialSource::Policy,
                reason: AuthorizationDenialReason::PolicyRefused,
            });
        let status = match decision {
            AuthorizationApprovalDecision::Approve => AuthorizationStatus::Granted,
            AuthorizationApprovalDecision::Reject => AuthorizationStatus::Denied,
            AuthorizationApprovalDecision::Defer => AuthorizationStatus::Parked,
        };
        let state = AuthorizationStateRecord::new(
            state.request_slot,
            state.request_digest,
            status,
            Vec::new(),
            None,
            denial,
        )
        .with_parked_evaluation(evaluation);
        self.store_authorization_update(state).await;
    }

    async fn apply_signal_authorization_approval(
        &self,
        state: AuthorizationStateRecord,
        decision: AuthorizationApprovalDecision,
        authorization: SignalCallAuthorization,
    ) {
        let denial =
            (decision == AuthorizationApprovalDecision::Reject).then_some(AuthorizationDenial {
                source: AuthorizationDenialSource::Policy,
                reason: AuthorizationDenialReason::PolicyRefused,
            });
        if decision == AuthorizationApprovalDecision::Reject {
            let state = AuthorizationStateRecord::new(
                state.request_slot,
                state.request_digest,
                AuthorizationStatus::Denied,
                Vec::new(),
                None,
                denial,
            )
            .with_signal_authorization(authorization);
            self.store_authorization_update(state).await;
            return;
        }

        let request_slot = state.request_slot.clone();
        let request_digest = state.request_digest.clone();
        let reply = self
            .ask_signer(signer::SignAuthorizationGrant::new(
                request_slot.clone(),
                authorization.clone(),
            ))
            .await;
        let CriomeReply::AuthorizationGranted(grant) = reply else {
            return;
        };
        let state = AuthorizationStateRecord::new(
            request_slot,
            request_digest,
            AuthorizationStatus::Granted,
            Vec::new(),
            Some(grant),
            None,
        )
        .with_signal_authorization(authorization);
        self.store_authorization_update(state).await;
    }

    async fn auto_approve_signal_call(
        &self,
        authorization: SignalCallAuthorization,
    ) -> CriomeReply {
        let stored = match self
            .create_authorization_state(store::CreateAuthorizationState::signing(&authorization))
            .await
        {
            Ok(stored) => stored,
            Err(Error::AuthorizationReplayAttempted) => {
                return rejection(RejectionReason::ReplayAttempted);
            }
            Err(_error) => return rejection(RejectionReason::MalformedRequest),
        };
        let state = stored.into_state();
        let request_slot = state.request_slot.clone();
        let request_digest = state.request_digest.clone();
        let reply = self
            .ask_signer(signer::SignAuthorizationGrant::new(
                request_slot.clone(),
                authorization,
            ))
            .await;
        let CriomeReply::AuthorizationGranted(grant) = reply else {
            return reply;
        };
        let granted_state = AuthorizationStateRecord::new(
            request_slot,
            request_digest,
            AuthorizationStatus::Granted,
            Vec::new(),
            Some(grant.clone()),
            None,
        );
        if !self.store_authorization_update(granted_state).await {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::AuthorizationGranted(grant)
    }

    async fn create_authorization_state(
        &self,
        state: store::CreateAuthorizationState,
    ) -> Result<crate::tables::StoredAuthorizationState> {
        let reply = self
            .store
            .ask(state)
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        reply.into_result()
    }

    async fn lookup_authorization_state(
        &self,
        request_slot: AuthorizationRequestSlot,
    ) -> Result<Option<crate::tables::StoredAuthorizationState>> {
        let reply = self
            .store
            .ask(store::LookupAuthorizationState::new(request_slot))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_state())
    }

    async fn store_authorization_update(&self, state: AuthorizationStateRecord) -> bool {
        if self
            .store
            .ask(store::StoreAuthorizationState::new(state.clone()))
            .await
            .is_err()
        {
            return false;
        }
        self.publish_authorization_update(state);
        true
    }

    async fn read_parked_authorization_snapshot(
        &self,
        _request: ParkedAuthorizationObservation,
    ) -> ParkedAuthorizationSnapshot {
        let parked = match self.store.ask(store::ReadAuthorizationSnapshot).await {
            Ok(reply) => reply
                .into_states()
                .into_iter()
                .filter_map(|stored| {
                    let state = stored.into_state();
                    if state.status != AuthorizationStatus::Parked {
                        return None;
                    }
                    if let Some(evaluation) = state.parked_evaluation().cloned() {
                        return Some(ParkedAuthorization::from_evaluation(
                            state.request_slot,
                            evaluation,
                        ));
                    }
                    state.signal_authorization().cloned().map(|authorization| {
                        ParkedAuthorization::from_signal_authorization(
                            state.request_slot,
                            authorization,
                        )
                    })
                })
                .collect(),
            Err(_error) => Vec::new(),
        };
        ParkedAuthorizationSnapshot::from_parked(parked)
    }

    async fn ask_registry<M>(&self, message: M) -> CriomeReply
    where
        registry::IdentityRegistry: kameo::message::Message<M, Reply = CriomeActorReply>,
        M: Send + 'static,
    {
        self.registry
            .ask(message)
            .await
            .map(CriomeActorReply::into_reply)
            .unwrap_or_else(|_error| rejection(RejectionReason::MalformedRequest))
    }

    async fn ask_signer<M>(&self, message: M) -> CriomeReply
    where
        signer::AttestationSigner: kameo::message::Message<M, Reply = CriomeActorReply>,
        M: Send + 'static,
    {
        self.signer
            .ask(message)
            .await
            .map(CriomeActorReply::into_reply)
            .unwrap_or_else(|_error| rejection(RejectionReason::MalformedRequest))
    }

    async fn ask_verifier<M>(&self, message: M) -> CriomeReply
    where
        verifier::AttestationVerifier: kameo::message::Message<M, Reply = CriomeActorReply>,
        M: Send + 'static,
    {
        self.verifier
            .ask(message)
            .await
            .map(CriomeActorReply::into_reply)
            .unwrap_or_else(|_error| rejection(RejectionReason::MalformedRequest))
    }

    async fn ask_authorization<M>(&self, message: M) -> CriomeReply
    where
        authorization::AuthorizationCoordinator:
            kameo::message::Message<M, Reply = CriomeActorReply>,
        M: Send + 'static,
    {
        self.authorization
            .ask(message)
            .await
            .map(CriomeActorReply::into_reply)
            .unwrap_or_else(|_error| rejection(RejectionReason::MalformedRequest))
    }

    async fn ask_subscription<M>(&self, message: M) -> CriomeReply
    where
        subscription::SubscriptionRegistry: kameo::message::Message<M, Reply = CriomeActorReply>,
        M: Send + 'static,
    {
        self.subscription
            .ask(message)
            .await
            .map(CriomeActorReply::into_reply)
            .unwrap_or_else(|_error| rejection(RejectionReason::MalformedRequest))
    }

    async fn admit_contract(&self, contract: signal_criome::Contract) -> CriomeReply {
        match self.store.ask(store::StoreContract::new(contract)).await {
            Ok(reply) => match reply.into_result() {
                Ok(contract) => {
                    let (digest, _contract) = contract.into_parts();
                    CriomeReply::ContractAdmitted(ContractAdmitted::new(digest))
                }
                Err(Error::ContractAdmissionRejected(reason)) => {
                    CriomeReply::ContractAdmissionRejected(ContractAdmissionRejected::new(reason))
                }
                Err(_) => rejection(RejectionReason::MalformedRequest),
            },
            Err(_) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn lookup_contract(&self, digest: signal_criome::ContractDigest) -> CriomeReply {
        match self
            .store
            .ask(store::LookupContract::new(digest.clone()))
            .await
        {
            Ok(reply) => match reply.into_contract() {
                Some(stored) => {
                    let (digest, contract) = stored.into_parts();
                    CriomeReply::ContractFound(ContractFound { digest, contract })
                }
                None => CriomeReply::ContractMissing(ContractMissing::new(digest)),
            },
            Err(_) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn publish_authorized_object_update(&self, update: AuthorizedObjectUpdate) {
        let _ = self
            .subscription
            .ask(subscription::PublishAuthorizedObjectUpdate::new(update))
            .await;
    }

    fn publish_authorization_update(&self, state: AuthorizationStateRecord) {
        let _ = self.authorization_updates.send(state);
    }

    async fn contract_store(&self) -> Option<ContractStore> {
        let contracts = self
            .store
            .ask(store::ReadContractSnapshot)
            .await
            .ok()?
            .into_contracts()
            .into_iter()
            .map(crate::tables::StoredContract::into_parts);
        Some(ContractStore::from_contracts(contracts))
    }

    async fn key_registry(&self) -> Option<KeyRegistry> {
        let reply = self
            .registry
            .ask(registry::ReadIdentitySnapshot)
            .await
            .ok()?
            .into_reply();
        let CriomeReply::IdentitySnapshot(snapshot) = reply else {
            return None;
        };
        let mut key_registry = KeyRegistry::new();
        for identity in snapshot.into_identities() {
            match self
                .registry
                .ask(registry::ResolveIdentity::new(identity.identity))
                .await
            {
                Ok(lookup) => {
                    let Some(stored) = lookup.into_identity() else {
                        continue;
                    };
                    key_registry.admit(stored.identity().clone(), stored.public_key().clone());
                }
                Err(_) => return None,
            }
        }
        Some(key_registry)
    }

    // ─── Quorum collection (propose → gather → judge → commit) ────────────────
    //
    // The genuinely new consensus core. An originating node proposes an
    // operation under an admitted Threshold contract, casts its own BLS vote,
    // solicits each peer member's vote across the conveyance, collects the stamped
    // signatures into a durable round, and feeds the assembled Evidence to the
    // EXISTING majority-judge (`ContractStore::evaluate`, reused unchanged). A
    // round is WITHHELD (`Gathering`) until the judge returns `Authorized`; an
    // unreachable peer leaves it pending forever. Below-majority Evidence is
    // refused fail-closed by the same judge.

    /// Propose (originator). Derive the moment from the contract's members, cast
    /// the self-vote, open the durable round, solicit each peer, and return the
    /// withheld round state.
    async fn propose_quorum_authorization(&mut self, proposal: QuorumProposal) -> CriomeReply {
        let QuorumProposal {
            round,
            // The round is phase-aware: round 1 (Request) and round 2 (Commit) over
            // the same object occupy DISTINCT durable rounds (`for_phase`), so their
            // signatures are never interchangeable. An external propose opens the
            // Request round; the originator drives the Commit round itself.
            phase,
            contract,
            object,
            window,
        } = proposal;
        // Round-id bound to the change's fingerprint AND phase: the round key MUST
        // be the one derived from the operation digest and phase, so two distinct
        // operations (or the two rounds of one operation) can never share a round
        // and a colliding proposal cannot clobber an unrelated in-flight round
        // (audit S1). Enforced at every round-creation ingress; `submit_quorum_vote`
        // inherits it via the round key.
        if round != QuorumRoundIdentifier::for_phase(&object.digest, phase) {
            return rejection(RejectionReason::MalformedRequest);
        }
        let Some(store) = self.contract_store().await else {
            return rejection(RejectionReason::MalformedRequest);
        };
        let Some((required, members)) = self.quorum_members(&store, &contract) else {
            return rejection(RejectionReason::MalformedRequest);
        };
        let proposition = AttestedMomentProposition::new(window, required, members);
        // Non-double-signing guard (reconciled with the clock gate the cast below
        // enforces): this node co-signs at most one successor per (contract, head).
        if let Some(conflict) = self.check_successor_conflict(&contract, &object) {
            return conflict;
        }
        // Durable-first veto: commit this node's single-successor veto row BEFORE it
        // casts (and later solicits) its vote, so a failed veto write aborts the
        // vote instead of emitting one whose anti-equivocation veto is only
        // best-effort.
        if let Err(reply) = self.record_co_sign(&contract, &object).await {
            return reply;
        }
        let self_vote = match self
            .cast_quorum_vote(&round, &object, &proposition, phase)
            .await
        {
            Ok(vote) => vote,
            Err(reply) => return reply,
        };
        let mut stored = StoredQuorumRound::open(round.clone(), contract, object, proposition);
        stored.record_vote(self_vote);
        if self.persist_quorum_round(stored.clone()).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        // Remember that THIS node originated the Request round: only the originator
        // drives the Commit round once round 1 reaches a majority.
        if phase == RoundPhase::Request {
            self.originated_request_rounds
                .insert(round.as_str().to_string());
        }
        self.solicit_peers(&stored, phase);
        CriomeReply::QuorumRoundOpened(self.round_state(&stored).await)
    }

    /// Peer vote. Independently re-validate the solicitation (contract admitted
    /// here, this node is a member), cast this node's vote, convey it back to the
    /// originator across the conveyance, and record it locally for idempotent redial.
    async fn solicit_quorum_vote(&mut self, solicitation: QuorumVoteSolicitation) -> CriomeReply {
        let QuorumVoteSolicitation {
            round,
            // Phase-aware: a Request solicitation opens round 1; a Commit
            // solicitation opens round 2, gated on an independently verified
            // round-1 majority below.
            phase,
            contract,
            object,
            proposition,
            originator,
        } = solicitation;
        // Same round-id ⇄ (operation-digest, phase) binding the originator
        // enforced, so a dishonest originator cannot make this peer open a round
        // under a round key that is not the one its operation and phase dictate
        // (audit S1).
        if round != QuorumRoundIdentifier::for_phase(&object.digest, phase) {
            return rejection(RejectionReason::MalformedRequest);
        }
        let Some(store) = self.contract_store().await else {
            return rejection(RejectionReason::MalformedRequest);
        };
        let Some((required, members)) = self.quorum_members(&store, &contract) else {
            return rejection(RejectionReason::MalformedRequest);
        };
        if !members.contains(&self.node_identity) {
            return rejection(RejectionReason::UnknownIdentity);
        }
        // Independent re-validation: the moment this node time-attests must name
        // the FULL contract member set with the contract's majority threshold, so
        // a dishonest originator cannot make the peer time-sign a degenerate
        // moment (e.g. a self-only time authority) that weakens the time-quorum.
        if !self.proposition_matches_members(&proposition, required, &members) {
            return rejection(RejectionReason::MalformedRequest);
        }
        // Independent witness-clock re-check: a solicited peer time-signs a window
        // only when its OWN clock places the present inside it. This joins the
        // member-set guard so an honest peer refuses a window it is not inside on
        // its own clock — the same gate the signer enforces (defence in depth), so
        // a proposer's convenient window is refused independently by every honest
        // peer, not merely on the originator's say-so.
        match self.clock.admits_window(&proposition.window) {
            WindowAdmission::Inside => {}
            WindowAdmission::OutsideTimeWindow => {
                return rejection(RejectionReason::MalformedRequest);
            }
        }
        // Non-double-signing guard, reconciled with the clock gate above: at most
        // one honest successor per (contract, head). Having co-signed successor S1
        // from this head (in either round), refuse any different successor from the
        // same head, answering the loser with the typed QuorumConflict reply.
        if let Some(conflict) = self.check_successor_conflict(&contract, &object) {
            return conflict;
        }
        // Round-2 (Commit) independent verification: this node re-runs the reused
        // judge on the round-1 evidence it holds locally and co-signs the commit
        // only when a REAL round-1 majority for the same object is Authorized. A
        // forged or short round-1 never judges Authorized, so it cannot be
        // committed; the window is enforced by the clock gate above (both rounds
        // share the window).
        if phase == RoundPhase::Commit && !self.round_one_verified(&object).await {
            return rejection(RejectionReason::MalformedRequest);
        }
        // Durable-first veto: persist this peer's single-successor veto row BEFORE it
        // casts and conveys its vote to the originator, so a failed veto write aborts
        // the vote instead of conveying one whose anti-equivocation veto is only
        // best-effort.
        if let Err(reply) = self.record_co_sign(&contract, &object).await {
            return reply;
        }
        let vote = match self
            .cast_quorum_vote(&round, &object, &proposition, phase)
            .await
        {
            Ok(vote) => vote,
            Err(reply) => return reply,
        };
        let mut stored = StoredQuorumRound::open(round, contract, object, proposition);
        stored.record_vote(vote.clone());
        let _ = self.persist_quorum_round(stored.clone()).await;
        self.conveyance
            .convey(&originator, CriomeRequest::submit_quorum_vote(vote));
        CriomeReply::QuorumVoteSolicited(self.round_state(&stored).await)
    }

    /// A vote arrived. Record it into the round, re-judge, and act on a true
    /// majority by phase: a Request-round majority DRIVES the commit round (on the
    /// originator only) — real approval is still withheld; a Commit-round majority
    /// is the real approval, so it publishes the authorized-object update and
    /// advances this node's head.
    async fn submit_quorum_vote(&mut self, vote: QuorumVote) -> CriomeReply {
        let Some(mut stored) = self.stored_quorum_round(&vote.round).await else {
            return rejection(RejectionReason::MalformedRequest);
        };
        // Drop votes from non-members of the admitted contract at ingress. The
        // judge already refuses to COUNT a non-member's signature, but an
        // unadmitted voter's row would still accumulate in the round (a storage
        // lever the audit flagged, S1); a member set the vote is not part of has
        // no business extending this round.
        let Some(store) = self.contract_store().await else {
            return rejection(RejectionReason::MalformedRequest);
        };
        let Some((_required, members)) = self.quorum_members(&store, stored.contract()) else {
            return rejection(RejectionReason::MalformedRequest);
        };
        if !members.contains(&vote.voter) {
            return rejection(RejectionReason::UnknownIdentity);
        }
        let phase = vote.phase;
        stored.record_vote(vote);
        if self.persist_quorum_round(stored.clone()).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        let state = self.round_state(&stored).await;
        if state.status == QuorumRoundStatus::Authorized {
            match phase {
                RoundPhase::Request => {
                    // Round 1 reached a majority. Only the ORIGINATOR drives round 2:
                    // a peer whose round-1 round reached a majority through the
                    // conveyed round-1 evidence does not re-drive. Real approval is
                    // withheld until the commit round itself reaches a majority.
                    if self
                        .originated_request_rounds
                        .contains(stored.round().as_str())
                    {
                        self.drive_commit_round(&stored).await;
                    }
                }
                RoundPhase::Commit => {
                    // Round 2 reached a majority — real approval lands here. Publish
                    // the authorized-object update so subscribers converge, and
                    // advance this node's head to the committed successor.
                    let stamp = self.assemble_evidence(&stored).stamp;
                    self.publish_authorized_object_update(AuthorizedObjectUpdate {
                        object: stored.object().clone(),
                        contract: stored.contract().clone(),
                        decision: EvaluationDecision::Authorized,
                        stamp,
                    })
                    .await;
                    self.advance_head(stored.contract(), stored.object()).await;
                }
            }
        }
        CriomeReply::QuorumVoteAccepted(state)
    }

    /// Read a round's current withheld/authorized state.
    async fn observe_quorum_round(&self, round: QuorumRoundIdentifier) -> CriomeReply {
        match self.stored_quorum_round(&round).await {
            Some(stored) => CriomeReply::QuorumRoundObserved(self.round_state(&stored).await),
            None => rejection(RejectionReason::MalformedRequest),
        }
    }

    /// The withheld-until-authorized rule made concrete: assemble the Evidence
    /// from the gathered votes and hand it to the reused majority-judge. Only an
    /// `Authorized` verdict carries the Evidence; anything short stays `Gathering`.
    async fn round_state(&self, stored: &StoredQuorumRound) -> QuorumRoundState {
        let evidence = self.assemble_evidence(stored);
        let gathered = RequiredSignatureThreshold::new(stored.votes().len() as u64);
        let store = self.contract_store().await;
        let registry = self.key_registry().await;
        let required = store
            .as_ref()
            .and_then(|store| self.quorum_members(store, stored.contract()))
            .map(|(required, _members)| required)
            .unwrap_or_else(|| RequiredSignatureThreshold::new(0));
        let authorized = match (&store, &registry) {
            (Some(store), Some(registry)) => matches!(
                store.evaluate(stored.contract(), &evidence, registry),
                Ok(EvaluationDecision::Authorized)
            ),
            _ => false,
        };
        let (status, authorized_evidence) = if authorized {
            (QuorumRoundStatus::Authorized, Some(evidence))
        } else {
            (QuorumRoundStatus::Gathering, None)
        };
        // The round's phase is carried by its votes — a round key is phase-specific
        // (`for_phase`), so every vote in a round shares the round's phase.
        let phase = stored
            .votes()
            .first()
            .map_or(RoundPhase::Request, |vote| vote.phase);
        QuorumRoundState {
            round: stored.round().clone(),
            phase,
            contract: stored.contract().clone(),
            status,
            gathered,
            required,
            authorized_evidence,
        }
    }

    /// Wrap the gathered votes into the `Evidence` the judge consumes: one shared
    /// `AttestedMoment` carrying every member's time signature, and one stamped
    /// operation signature per member.
    fn assemble_evidence(&self, stored: &StoredQuorumRound) -> Evidence {
        let stamp = AttestedMoment::new(
            stored.proposition().clone(),
            stored
                .votes()
                .iter()
                .map(|vote| TimeSignature {
                    signer: vote.voter.clone(),
                    envelope: vote.time_signature.clone(),
                })
                .collect(),
        );
        let evidence_signatures = stored
            .votes()
            .iter()
            .map(|vote| StampedSignatureEnvelope {
                stamp: stamp.clone(),
                envelope: vote.operation_signature.clone(),
            })
            .collect();
        let operation = OperationDigest::new(stored.object().digest.clone());
        Evidence::new(
            stored.object().component,
            operation,
            stamp,
            evidence_signatures,
            Vec::new(),
        )
    }

    /// Cast this node's `phase` vote and time attestation over `object` under
    /// `proposition`. The signer's witness-clock gate refuses the time-signature
    /// (failing the whole vote) when this node's clock is not inside the window, so
    /// BOTH rounds are independently clock-gated as they are cast.
    async fn cast_quorum_vote(
        &self,
        round: &QuorumRoundIdentifier,
        object: &AuthorizedObjectReference,
        proposition: &AttestedMomentProposition,
        phase: RoundPhase,
    ) -> std::result::Result<QuorumVote, CriomeReply> {
        let operation = OperationDigest::new(object.digest.clone());
        let signatures = self
            .signer
            .ask(signer::SignQuorumVote::new(operation, proposition.clone()))
            .await
            .map_err(|_error| rejection(RejectionReason::MalformedRequest))?;
        Ok(QuorumVote {
            round: round.clone(),
            phase,
            voter: self.node_identity.clone(),
            operation_signature: signatures.operation_signature,
            time_signature: signatures.time_signature,
        })
    }

    /// Solicit every peer member (contract members other than this node) across
    /// the conveyance in the given `phase`. Best-effort: an unreachable peer leaves the
    /// round pending.
    fn solicit_peers(&self, stored: &StoredQuorumRound, phase: RoundPhase) {
        for peer in stored.proposition().authorities() {
            if peer == &self.node_identity {
                continue;
            }
            let solicitation = QuorumVoteSolicitation {
                round: stored.round().clone(),
                phase,
                contract: stored.contract().clone(),
                object: stored.object().clone(),
                proposition: stored.proposition().clone(),
                originator: self.node_identity.clone(),
            };
            self.conveyance
                .convey(peer, CriomeRequest::solicit_quorum_vote(solicitation));
        }
    }

    /// Round 1 reached a majority on the originator: drive round 2 (Commit). The
    /// initiator casts its OWN commit vote (its round 1 is Authorized and the clock
    /// gate re-checks the window as it signs), then conveys to each peer IN ORDER
    /// over the conveyance: the round-1 evidence — the gathered Request votes — THEN the
    /// commit solicitation THEN its own commit vote. So every round-2 signer holds
    /// a real round-1 majority to re-judge before it co-signs, and its commit round
    /// reaches the SAME majority the initiator gathers — both nodes advance the SAME
    /// head (the ordered conveyance closes the race the best-effort conveyance would
    /// otherwise open). Round 2 assembles a majority-of-the-total; it need not be a
    /// subset of the round-1 signers. Real approval lands only when the commit round
    /// itself reaches a majority (`submit_quorum_vote`). Two rounds — no third.
    async fn drive_commit_round(&mut self, request_round: &StoredQuorumRound) {
        let contract = request_round.contract().clone();
        let object = request_round.object().clone();
        let proposition = request_round.proposition().clone();
        let commit_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Commit);

        // Drive the commit round at most once. A redelivered round-1 vote can bring
        // an already-Authorized round-1 round through here again; if this node has
        // already cast its commit vote, re-driving would re-broadcast and could
        // clobber commit votes already gathered from peers, so stop here.
        let existing_commit = self.stored_quorum_round(&commit_round).await;
        if existing_commit.as_ref().is_some_and(|stored| {
            stored
                .votes()
                .iter()
                .any(|vote| vote.voter == self.node_identity)
        }) {
            return;
        }

        // Cast this node's OWN commit vote FIRST, so it can be conveyed to each
        // peer alongside the solicitation. Its round-1 round is Authorized (it is
        // the driver), so its independent verification is already satisfied; the
        // single-successor guard sees the same successor it co-signed in round 1
        // (idempotent), and the clock gate re-checks the window as it signs. If the
        // gate refuses, nothing is conveyed — the commit does not proceed at all.
        if self.check_successor_conflict(&contract, &object).is_some() {
            return;
        }
        // Durable-first veto: commit this driver's single-successor veto row BEFORE it
        // casts and conveys its own commit vote, so a failed veto write aborts the
        // commit rather than conveying a vote whose anti-equivocation veto is only
        // best-effort. (Idempotent: round 1 already co-signed this same successor, so
        // the row is already durable and this is a no-op re-write.)
        if self.record_co_sign(&contract, &object).await.is_err() {
            return;
        }
        let self_vote = match self
            .cast_quorum_vote(&commit_round, &object, &proposition, RoundPhase::Commit)
            .await
        {
            Ok(vote) => vote,
            Err(_reply) => return,
        };

        // Convey the round-1 evidence (its votes), the commit solicitation, and
        // this node's own commit vote to each peer IN ORDER over the conveyance: the
        // evidence lands first (the peer's round-1 round judges Authorized), then
        // the solicitation (the peer casts its own commit vote), then this node's
        // commit vote — which brings the peer's commit round to the SAME majority
        // this node gathers, so the peer advances the SAME head rather than leaving
        // it stale (a stale peer head refuses the next successor as a false
        // QuorumConflict and wedges the cluster). The ordered conveyance closes the
        // race the best-effort conveyance would otherwise open.
        let commit_solicitation = CriomeRequest::solicit_quorum_vote(QuorumVoteSolicitation {
            round: commit_round.clone(),
            phase: RoundPhase::Commit,
            contract: contract.clone(),
            object: object.clone(),
            proposition: proposition.clone(),
            originator: self.node_identity.clone(),
        });
        for peer in proposition.authorities() {
            if peer == &self.node_identity {
                continue;
            }
            let mut sequence: Vec<CriomeRequest> = request_round
                .votes()
                .iter()
                .map(|vote| CriomeRequest::submit_quorum_vote(vote.clone()))
                .collect();
            sequence.push(commit_solicitation.clone());
            sequence.push(CriomeRequest::submit_quorum_vote(self_vote.clone()));
            self.conveyance.convey_ordered(peer, sequence);
        }

        // Persist this node's own commit vote, preserving any commit votes a peer
        // conveyed ahead of it rather than opening a fresh round over them.
        let mut stored = existing_commit.unwrap_or_else(|| {
            StoredQuorumRound::open(commit_round, contract, object, proposition)
        });
        stored.record_vote(self_vote);
        let _ = self.persist_quorum_round(stored).await;
    }

    /// A round-2 (Commit) signer's independent check: a REAL round-1 (Request)
    /// majority for the same object is held locally — the reused judge returns
    /// `Authorized` over the round-1 evidence this node gathered. A forged or short
    /// round-1 never reaches `Authorized`, so it cannot be committed.
    async fn round_one_verified(&self, object: &AuthorizedObjectReference) -> bool {
        let request_round = QuorumRoundIdentifier::for_phase(&object.digest, RoundPhase::Request);
        match self.stored_quorum_round(&request_round).await {
            Some(stored) => self.round_state(&stored).await.status == QuorumRoundStatus::Authorized,
            None => false,
        }
    }

    /// This node's view of `contract`'s current head — the state-point a change
    /// advances from. Absent from the ledger ⇒ the contract's genesis head
    /// (nothing has committed on this node yet).
    fn state_head(&self, contract: &signal_criome::ContractDigest) -> ContractOperationHead {
        self.contract_heads
            .get(contract.as_str())
            .cloned()
            .unwrap_or_else(|| ContractOperationHead::new(format!("genesis:{}", contract.as_str())))
    }

    /// The state-point key `(contract, head)` under which one co-signed successor
    /// is tracked. Mirrors the string-keyed identity convention in `tables.rs`.
    fn state_point_key(
        contract: &signal_criome::ContractDigest,
        head: &ContractOperationHead,
    ) -> String {
        format!("{}@{}", contract.as_str(), head.as_str())
    }

    /// The pluggable conflict predicate (the phase-2 commutative-merge seam): TODAY
    /// two successors from the same head conflict iff they are different objects. A
    /// later compatible/commutative-merge predicate slots in here to let
    /// order-independent changes both commit.
    fn successors_conflict(
        &self,
        existing: &AuthorizedObjectReference,
        proposed: &AuthorizedObjectReference,
    ) -> bool {
        existing.digest != proposed.digest
    }

    /// Non-double-signing guard: at most one honest successor per (contract, head).
    /// Returns the typed `QuorumConflict` refusal when this node has already
    /// co-signed a CONFLICTING successor from the same head; `None` when the
    /// successor is new or identical (idempotent re-co-sign) and may proceed.
    fn check_successor_conflict(
        &self,
        contract: &signal_criome::ContractDigest,
        object: &AuthorizedObjectReference,
    ) -> Option<CriomeReply> {
        let head = self.state_head(contract);
        let existing = self
            .co_signed_successors
            .get(&Self::state_point_key(contract, &head))?;
        self.successors_conflict(existing, object).then(|| {
            CriomeReply::quorum_conflict(QuorumConflict::new(
                contract.clone(),
                head.clone(),
                existing.clone(),
            ))
        })
    }

    /// Durably record `object` as the one successor this node has co-signed for
    /// `contract`'s current head, BEFORE the caller casts or conveys its vote.
    /// Idempotent: a later identical co-sign keeps the first record (and needs no
    /// re-write); a conflicting one is refused earlier by
    /// [`Self::check_successor_conflict`].
    ///
    /// The veto row is a HARD guarantee, not best-effort: the durable write happens
    /// first, and a failure returns a rejection so the caller aborts the vote. A
    /// vote is therefore never cast or conveyed without its veto row already on
    /// disk — a crash after the vote is emitted still finds the veto on the next
    /// boot (rebuilt by `on_start`), so the node refuses a conflicting successor
    /// from the same head rather than reopening the F2 equivocation window. The
    /// only failure mode this admits is failing to emit (the round times out) —
    /// safe and live-degraded, never equivocation.
    async fn record_co_sign(
        &mut self,
        contract: &signal_criome::ContractDigest,
        object: &AuthorizedObjectReference,
    ) -> std::result::Result<(), CriomeReply> {
        let head = self.state_head(contract);
        let key = Self::state_point_key(contract, &head);
        if self.co_signed_successors.contains_key(&key) {
            return Ok(());
        }
        // Durable-first: commit the veto row before touching the in-memory map and
        // before the caller emits, so the durable ledger never lags the vote. A
        // failed write aborts the vote with a rejection instead of leaving a vote
        // whose anti-equivocation veto is only best-effort.
        self.persist_co_signed_successor(StoredCoSignedSuccessor::new(
            contract.clone(),
            head,
            object.clone(),
        ))
        .await
        .map_err(|_error| rejection(RejectionReason::MalformedRequest))?;
        self.co_signed_successors.insert(key, object.clone());
        Ok(())
    }

    /// Advance this node's head for `contract` to the just-committed successor, so
    /// a later change is a fresh state-point rather than a conflict. The head is
    /// persisted so it survives a restart — otherwise a reboot to genesis would
    /// mistake the next successor for a conflict from genesis and wedge the cluster.
    async fn advance_head(
        &mut self,
        contract: &signal_criome::ContractDigest,
        object: &AuthorizedObjectReference,
    ) {
        let head = ContractOperationHead::new(object.digest.as_str().to_string());
        self.contract_heads
            .insert(contract.as_str().to_string(), head.clone());
        let _ = self
            .persist_contract_head(StoredContractHead::new(contract.clone(), head))
            .await;
    }

    /// Persist one co-signed-successor ledger row through the store's single-writer
    /// kernel.
    async fn persist_co_signed_successor(&self, record: StoredCoSignedSuccessor) -> Result<()> {
        self.store
            .ask(store::StoreCoSignedSuccessor::new(record))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(())
    }

    /// Persist one contract-head cursor row through the store's single-writer
    /// kernel.
    async fn persist_contract_head(&self, record: StoredContractHead) -> Result<()> {
        self.store
            .ask(store::StoreContractHead::new(record))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(())
    }

    /// Rebuild the in-memory co-signed-successor ledger from its durable rows on
    /// boot, re-deriving each state-point key so the reconstructed map keys exactly
    /// as the live one does. The single-successor veto is thereby the same before
    /// and after a restart.
    async fn reconstruct_co_signed_successors(
        store: &ActorRef<store::StoreKernel>,
    ) -> HashMap<String, AuthorizedObjectReference> {
        store
            .ask(store::ReadCoSignedSuccessors)
            .await
            .map(store::CoSignedSuccessorsReply::into_records)
            .unwrap_or_default()
            .into_iter()
            .map(|record| {
                (
                    Self::state_point_key(record.contract(), record.head()),
                    record.object().clone(),
                )
            })
            .collect()
    }

    /// Rebuild the in-memory head cursor from its durable rows on boot, so a node
    /// resumes at the head each commit advanced it to rather than snapping back to
    /// genesis.
    async fn reconstruct_contract_heads(
        store: &ActorRef<store::StoreKernel>,
    ) -> HashMap<String, ContractOperationHead> {
        store
            .ask(store::ReadContractHeads)
            .await
            .map(store::ContractHeadsReply::into_records)
            .unwrap_or_default()
            .into_iter()
            .map(|record| {
                (
                    record.contract().as_str().to_string(),
                    record.head().clone(),
                )
            })
            .collect()
    }

    /// The `KeyMember` identities and required threshold of an admitted Threshold
    /// contract. Returns `None` for a missing contract or a non-Threshold rule —
    /// quorum collection governs only Threshold contracts.
    fn quorum_members(
        &self,
        store: &ContractStore,
        contract: &signal_criome::ContractDigest,
    ) -> Option<(RequiredSignatureThreshold, Vec<Identity>)> {
        let Rule::Threshold(threshold) = store.resolve(contract).ok()?.rule() else {
            return None;
        };
        let members = threshold
            .members()
            .iter()
            .filter_map(|member| match member {
                PolicyMember::KeyMember(identity) => Some(identity.clone()),
                PolicyMember::ObjectMember(_) => None,
            })
            .collect();
        Some((threshold.required_signatures, members))
    }

    /// Whether a solicited moment proposition names exactly this contract's member
    /// set (as a set) with its majority threshold — the peer's guard against
    /// time-attesting a moment weaker than the operation quorum it authorizes.
    fn proposition_matches_members(
        &self,
        proposition: &AttestedMomentProposition,
        required: RequiredSignatureThreshold,
        members: &[Identity],
    ) -> bool {
        let authorities = proposition.authorities();
        proposition.required_signatures.into_u16() == required.into_u16()
            && authorities.len() == members.len()
            && members.iter().all(|member| authorities.contains(member))
    }

    async fn persist_quorum_round(&self, round: StoredQuorumRound) -> Result<StoredQuorumRound> {
        let reply = self
            .store
            .ask(store::StoreQuorumRound::new(round))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_round())
    }

    async fn stored_quorum_round(
        &self,
        round: &QuorumRoundIdentifier,
    ) -> Option<StoredQuorumRound> {
        self.store
            .ask(store::LookupQuorumRound::new(round.clone()))
            .await
            .ok()?
            .into_round()
    }

    // ─── Root founding ceremony (observe key → owner accept → persist → seed) ──
    //
    // Founding is UNANIMOUS and owner-accepted on the meta socket, with NO
    // auto-approval: a node's master key emits a `FoundingSignature` ONLY on an
    // explicit owner accept, and ONLY for the exact self-certifying cohort. The
    // founded root is persisted with its attached signatures and, on unanimity,
    // seeds the registry as the trust anchor; on reboot it is verified and adopted
    // (`on_start`), never re-founded.
    //
    // The peer signatures a multi-node cohort needs ride node-to-node over the
    // EXISTING router conveyance; `RootFounding::attach_signature` is the accumulation
    // seam a conveyed peer signature feeds. The live 2-node gather over the conveyance
    // is the live-proof worker's bead (.15); today the single-node cohort founds
    // end-to-end and the unanimity/verification logic is proven at the unit level.

    /// This node's Criome master public key, read from the signer that owns the
    /// master key. Backs both the public read-op and the founding member match.
    async fn node_public_key(&self) -> Option<BlsPublicKey> {
        self.signer
            .ask(signer::ReadNodePublicKey)
            .await
            .ok()
            .map(|reply| reply.public_key)
    }

    /// Public-socket read-op: expose this node's Criome master public key so a
    /// client can enroll it into a founding cohort out-of-band.
    async fn observe_node_public_key(&self) -> CriomeReply {
        match self.node_public_key().await {
            Some(public_key) => CriomeReply::node_public_key(public_key),
            None => rejection(RejectionReason::MalformedRequest),
        }
    }

    /// Owner-only meta-op — the explicit action that founds a root. No
    /// auto-approval anywhere: the master key signs the founding statement ONLY
    /// here and ONLY when the presented anchor equals the cohort's self-certifying
    /// anchor. This node's signature is recorded into the durable founding round;
    /// on unanimity the registry is seeded from the cohort. A second accept for a
    /// different anchor — or any accept after founding is unanimous — is refused.
    async fn accept_root_founding(
        &self,
        acceptance: RootFoundingAcceptance,
    ) -> meta_signal_criome::Output {
        let RootFoundingAcceptance { anchor, cohort } = acceptance;
        // A malformed genesis (empty cohort, non-root parent, un-encodable anchor)
        // is refused before any signing.
        let candidate = match RootFounding::found(cohort) {
            Ok(founding) => founding,
            Err(_) => return Self::reject_founding(RootFoundingRejectionReason::MalformedGenesis),
        };
        // The owner's stated anchor must equal the cohort's self-certifying
        // anchor: the node founds ONLY the exact cohort it was handed.
        if candidate.anchor() != &anchor {
            return Self::reject_founding(RootFoundingRejectionReason::CohortMismatch);
        }
        // A unanimous (founded) root is immutable, and a node commits to one root:
        // any accept once founded — or for a different anchor — is refused. A
        // still-gathering record for the SAME anchor is resumed so this node's
        // signature accumulates alongside any peers' already gathered.
        let mut founding = match self.stored_root_founding().await {
            Some(existing) if existing.is_unanimous() => {
                return Self::reject_founding(RootFoundingRejectionReason::AlreadyFounded);
            }
            Some(existing) if existing.anchor() != &anchor => {
                return Self::reject_founding(RootFoundingRejectionReason::AlreadyFounded);
            }
            Some(existing) => existing,
            None => candidate,
        };
        // This node must hold a seat in the cohort by its master key; without one
        // it has no founding authority over this root.
        let Some(public_key) = self.node_public_key().await else {
            return Self::reject_founding(RootFoundingRejectionReason::MalformedGenesis);
        };
        let Some(member) = founding.member_by_key(&public_key) else {
            return Self::reject_founding(RootFoundingRejectionReason::ManagerAuthorityRequired);
        };
        let signer_identity = member.identity.clone();
        // The master key signs the founding statement — this node's willing
        // establishment, minted only because the owner explicitly accepted.
        let Some(envelope) = self.sign_founding_statement(founding.statement()).await else {
            return Self::reject_founding(RootFoundingRejectionReason::MalformedGenesis);
        };
        let signature = FoundingSignature::new(signer_identity, envelope);
        founding.attach_signature(signature.clone());
        if self.persist_root_founding(founding.clone()).await.is_err() {
            return Self::reject_founding(RootFoundingRejectionReason::MalformedGenesis);
        }
        // Return this node's signature to the founding's initiator when a PEER
        // initiated it: a pending record naming another node is a proposal this node
        // was asked to join, so its signature is conveyed back for the initiator to
        // accumulate. A locally initiated founding names this node as its own
        // initiator, so its signatures accumulate here directly with nothing to convey.
        if let Some(pending) = self.pending_founding(&anchor).await
            && pending.initiator() != &self.node_identity
        {
            self.conveyance.convey(
                pending.initiator(),
                CriomeRequest::convey_founding(FoundingConveyance::Signature(
                    FoundingSignatureReturn {
                        anchor: anchor.clone(),
                        signature: signature.clone(),
                    },
                )),
            );
        }
        // Unanimity here means this node holds every cohort member's signature (it is
        // the initiator gathering them): seed the registry from the founded cohort and
        // distribute the finished root to every peer so each adopts the SAME anchor.
        if founding.is_unanimous() {
            self.on_founding_unanimous(&founding).await;
        }
        meta_signal_criome::Output::root_founding_accepted(RootFoundingAccepted::new(
            anchor, signature,
        ))
    }

    fn reject_founding(reason: RootFoundingRejectionReason) -> meta_signal_criome::Output {
        meta_signal_criome::Output::root_founding_rejected(reason)
    }

    async fn sign_founding_statement(
        &self,
        statement: signal_criome::RootFoundingStatement,
    ) -> Option<signal_criome::SignatureEnvelope> {
        self.signer
            .ask(signer::SignFoundingStatement::new(statement))
            .await
            .ok()
            .map(|reply| reply.envelope)
    }

    async fn stored_root_founding(&self) -> Option<RootFounding> {
        self.store
            .ask(store::ReadRootFounding)
            .await
            .ok()?
            .into_founding()
    }

    async fn persist_root_founding(&self, founding: RootFounding) -> Result<RootFounding> {
        let reply = self
            .store
            .ask(store::StoreRootFounding::new(founding))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_founding())
    }

    /// Seed the identity registry from the founded cohort: each founding member's
    /// identity bound to its master key, registered directly (bypassing the
    /// cluster-root gate, exactly as `on_start` seeds this node's own identity) so
    /// the founded cohort becomes the registry's trust anchor. Idempotent by
    /// identity — re-seeding the node's own key is a no-op overwrite.
    async fn seed_founding_registry(&self, founding: &RootFounding) {
        for registration in founding.seed_registrations() {
            let _ = self
                .store
                .ask(store::StoreIdentity::new(registration))
                .await;
        }
    }

    // ─── Cross-node founding conveyance (proposal → signatures → founded) ─────
    //
    // The wire that lets a multi-node cohort assemble a UNANIMOUS root across
    // peers' working sockets, riding the SAME router conveyance the quorum path uses
    // (no new lane). A founding moves in three conveyances between cohort
    // criomes: the initiator conveys a Proposal to each peer; each peer's owner
    // accepts and its criome conveys that Signature back; when the initiator has
    // gathered every member's signature it distributes the finished Founded root
    // to every peer, which each verify and persist. No auto-approval anywhere:
    // every signature is minted only by an explicit owner AcceptRootFounding.
    //
    // PHASE-2 HOOK (root rotation/mutation, deliberately NOT built here): a
    // founded root is immutable within this design. Rotating to a successor root
    // would ride a new FoundingConveyance movement (a rotation proposal carrying
    // the successor genesis and a link back to the current anchor) gathered the
    // same unanimous way; `RootFounding`/the `root_founding` singleton would then
    // hold a chain of founded anchors rather than the single record it holds today.

    /// A founding conveyance arrived on the working socket. Route it by movement:
    /// a proposal is stored pending (never signed on receipt), a returned
    /// signature is accumulated toward unanimity, and a finished root is verified
    /// and adopted.
    async fn convey_founding(&self, conveyance: FoundingConveyance) -> CriomeReply {
        match conveyance {
            FoundingConveyance::Proposal(proposal) => {
                self.receive_founding_proposal(proposal).await
            }
            FoundingConveyance::Signature(signature_return) => {
                self.receive_founding_signature(signature_return).await
            }
            FoundingConveyance::Founded(founded) => self.receive_founded_root(founded).await,
        }
    }

    /// Peer role: an initiator proposed this cohort. Validate the genesis, derive
    /// its self-certifying anchor, and store it pending an explicit owner accept —
    /// this node does NOT sign on receipt (no auto-approval). A malformed proposal
    /// is refused, never stored.
    async fn receive_founding_proposal(&self, proposal: FoundingProposal) -> CriomeReply {
        let FoundingProposal { genesis, initiator } = proposal;
        let founding = match RootFounding::found(genesis) {
            Ok(founding) => founding,
            Err(_) => return rejection(RejectionReason::MalformedRequest),
        };
        let anchor = founding.anchor().clone();
        let pending =
            StoredPendingFounding::new(anchor.clone(), founding.genesis().clone(), initiator);
        if self.store_pending_founding(pending).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        Self::founding_conveyed(anchor, FoundingConveyanceOutcome::ProposalPending)
    }

    /// Initiator role: a peer returned its accepted signature. Resume this node's
    /// gathering for the anchor — the singleton `root_founding` if this node has
    /// already accepted, else found afresh from the pending cohort it initiated (a
    /// peer signature can arrive before this node's own accept). Accumulate the
    /// signature; on unanimity, seed and distribute the finished root.
    async fn receive_founding_signature(
        &self,
        signature_return: FoundingSignatureReturn,
    ) -> CriomeReply {
        let FoundingSignatureReturn { anchor, signature } = signature_return;
        let mut founding = match self.stored_root_founding().await {
            Some(existing) if existing.anchor() == &anchor => existing,
            Some(_) => return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused),
            None => match self.pending_founding(&anchor).await {
                Some(pending) => {
                    let (_, genesis, _) = pending.into_parts();
                    match RootFounding::found(genesis) {
                        Ok(founding) => founding,
                        Err(_) => {
                            return Self::founding_conveyed(
                                anchor,
                                FoundingConveyanceOutcome::Refused,
                            );
                        }
                    }
                }
                None => return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused),
            },
        };
        // Verify the conveyed signature against the member's key over the founding
        // statement BEFORE attaching it. `attach_signature` checks membership only,
        // and `is_unanimous` counts presence — so without this gate a garbage
        // `FoundingSignatureReturn` (any bytes a co-resident process or a malicious
        // cohort peer puts on the working socket) would be accepted and drive a
        // false unanimity. This mirrors the `verify()` the distributed-root path
        // already runs before persisting.
        if !founding.conveyed_signature_valid(&signature) {
            return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused);
        }
        // attach_signature refuses a non-member; a member's redelivery updates in
        // place rather than double-counting.
        if !founding.attach_signature(signature) {
            return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused);
        }
        if self.persist_root_founding(founding.clone()).await.is_err() {
            return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused);
        }
        // Gate the seed-and-distribute on every attached signature VERIFYING, not
        // bare presence: unanimity of validly-signed members, never unanimity of
        // rows.
        if founding.is_unanimous() && founding.signatures_valid() {
            self.on_founding_unanimous(&founding).await;
            return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::RootFounded);
        }
        Self::founding_conveyed(anchor, FoundingConveyanceOutcome::SignatureAccumulated)
    }

    /// Peer role: the initiator distributed the finished root. Trust nothing on the
    /// wire — reassemble it and adopt ONLY a root that `verify`s (the anchor matches
    /// the embedded genesis, every attached signature is valid, and the cohort is
    /// unanimous), persisting it and seeding the registry along the same durable
    /// path `on_start` reboot adoption uses. A tampered or short distribution is
    /// refused.
    async fn receive_founded_root(&self, founded: FoundedRoot) -> CriomeReply {
        let FoundedRoot {
            genesis,
            signatures,
        } = founded;
        let founding = match RootFounding::adopt(genesis, signatures) {
            Ok(founding) => founding,
            Err(_) => return rejection(RejectionReason::MalformedRequest),
        };
        let anchor = founding.anchor().clone();
        if !founding.verify() {
            return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused);
        }
        if self.persist_root_founding(founding.clone()).await.is_err() {
            return Self::founding_conveyed(anchor, FoundingConveyanceOutcome::Refused);
        }
        self.seed_founding_registry(&founding).await;
        Self::founding_conveyed(anchor, FoundingConveyanceOutcome::RootFounded)
    }

    fn founding_conveyed(
        anchor: RootAnchorDigest,
        outcome: FoundingConveyanceOutcome,
    ) -> CriomeReply {
        CriomeReply::founding_conveyed(FoundingConveyanceReceipt { anchor, outcome })
    }

    /// The initiator has gathered every cohort member's signature. Seed this node's
    /// registry from the founded cohort (the same trust-anchor path reboot adoption
    /// uses) and distribute the finished root to every peer so each verifies and
    /// persists the SAME anchor.
    async fn on_founding_unanimous(&self, founding: &RootFounding) {
        self.seed_founding_registry(founding).await;
        self.distribute_founded(founding);
    }

    /// Convey the finished unanimous root to every cohort member other than this
    /// node, over the same best-effort conveyance.
    fn distribute_founded(&self, founding: &RootFounding) {
        let founded = FoundedRoot {
            genesis: founding.genesis().clone(),
            signatures: founding.signatures().to_vec(),
        };
        for member in founding.genesis().founding_keys() {
            if member.identity == self.node_identity {
                continue;
            }
            self.conveyance.convey(
                &member.identity,
                CriomeRequest::convey_founding(FoundingConveyance::Founded(founded.clone())),
            );
        }
    }

    /// Owner-only meta-op: initiate a multi-node founding on THIS node. Validate the
    /// cohort, record this node's own gathering as pending (initiator = self, so a
    /// peer signature arriving before this owner accepts can resume it and the
    /// accept-by-anchor resolves the cohort here), and convey a Proposal to each
    /// peer over the conveyance. No signing yet — founding stays owner-accepted with no
    /// auto-approval, so the operator must still explicitly accept on this node.
    async fn initiate_root_founding(
        &self,
        initiation: RootFoundingInitiation,
    ) -> meta_signal_criome::Output {
        let founding = match RootFounding::found(initiation.into_payload()) {
            Ok(founding) => founding,
            Err(_) => return Self::reject_founding(RootFoundingRejectionReason::MalformedGenesis),
        };
        let anchor = founding.anchor().clone();
        let pending = StoredPendingFounding::new(
            anchor,
            founding.genesis().clone(),
            self.node_identity.clone(),
        );
        if self.store_pending_founding(pending).await.is_err() {
            return Self::reject_founding(RootFoundingRejectionReason::MalformedGenesis);
        }
        for member in founding.genesis().founding_keys() {
            if member.identity == self.node_identity {
                continue;
            }
            self.conveyance.convey(
                &member.identity,
                CriomeRequest::convey_founding(FoundingConveyance::Proposal(FoundingProposal {
                    genesis: founding.genesis().clone(),
                    initiator: self.node_identity.clone(),
                })),
            );
        }
        meta_signal_criome::Output::root_founding_status(self.root_founding_status().await)
    }

    /// Owner-only meta-op: this node's founding state and its pending-founding queue,
    /// so the operator knows what awaits an accept and which anchor to accept.
    async fn observe_root_founding(
        &self,
        _observation: RootFoundingObservation,
    ) -> meta_signal_criome::Output {
        meta_signal_criome::Output::root_founding_status(self.root_founding_status().await)
    }

    async fn root_founding_status(&self) -> RootFoundingStatus {
        let state = match self.stored_root_founding().await {
            Some(founding) if founding.is_unanimous() => RootFoundingState::Founded,
            Some(_) => RootFoundingState::Gathering,
            None => RootFoundingState::Unfounded,
        };
        let pending = self
            .pending_foundings()
            .await
            .into_iter()
            .map(|stored| {
                let (anchor, cohort, initiator) = stored.into_parts();
                PendingFounding {
                    anchor,
                    cohort,
                    initiator,
                }
            })
            .collect();
        RootFoundingStatus { state, pending }
    }

    async fn store_pending_founding(
        &self,
        pending: StoredPendingFounding,
    ) -> Result<StoredPendingFounding> {
        let reply = self
            .store
            .ask(store::StorePendingFounding::new(pending))
            .await
            .map_err(|error| Error::ActorCall(error.to_string()))?;
        Ok(reply.into_pending())
    }

    async fn pending_founding(&self, anchor: &RootAnchorDigest) -> Option<StoredPendingFounding> {
        self.store
            .ask(store::ReadPendingFounding::new(anchor.clone()))
            .await
            .ok()?
            .into_pending()
    }

    async fn pending_foundings(&self) -> Vec<StoredPendingFounding> {
        self.store
            .ask(store::ReadPendingFoundings)
            .await
            .map(store::PendingFoundingsReply::into_pendings)
            .unwrap_or_default()
    }
}

impl Actor for CriomeRoot {
    type Args = Arguments;
    type Error = Error;

    async fn on_start(
        arguments: Self::Args,
        actor_reference: ActorRef<Self>,
    ) -> std::result::Result<Self, Self::Error> {
        let master_key_path = arguments.store.as_path().with_extension("masterkey");
        let master_key = MasterKey::load_or_generate(&master_key_path)?;
        let criome_identity = arguments.node_identity;
        let node_identity = criome_identity.clone();
        let conveyance = arguments.conveyance;
        let cluster_root = arguments.cluster_root.map(ClusterRoot::new);

        let store = store::StoreKernel::supervise(&actor_reference, arguments.store)
            .spawn()
            .await;
        let registry = registry::IdentityRegistry::supervise(
            &actor_reference,
            registry::Arguments {
                store: store.clone(),
                cluster_root,
            },
        )
        .spawn()
        .await;
        // Reconcile the master key against any already-registered criome identity.
        // A restored/migrated store whose adjacent key file was regenerated or
        // copied from another host would otherwise mint attestations its own
        // verifier rejects; fail loudly instead of starting unhealthy.
        let master_public_key = master_key.public_key();
        let existing = registry
            .ask(registry::ResolveIdentity::new(criome_identity.clone()))
            .await
            .map_err(|error| Error::Startup(format!("resolve criome identity: {error}")))?
            .into_identity();
        match existing {
            Some(record) => {
                if record.public_key() != &master_public_key {
                    return Err(Error::Startup(format!(
                        "criome master key does not match the registered {criome_identity:?} \
                         identity key; refusing to start (restored store with a mismatched key?)"
                    )));
                }
            }
            None => {
                // criome's own identity is its self-owned authority; register it
                // directly to the store, bypassing the cluster-root gate (which
                // governs externally-submitted keys via RegisterIdentity).
                store
                    .ask(store::StoreIdentity::new(IdentityRegistration::new(
                        criome_identity.clone(),
                        master_public_key,
                        master_key.fingerprint(),
                        KeyPurpose::CriomeRoot,
                        None,
                    )))
                    .await
                    .map_err(|error| {
                        Error::Startup(format!("register criome identity: {error}"))
                    })?;
            }
        }
        // Reboot founding verification — NEVER re-found. If this node already
        // founded a root, verify its anchor and its attached founding-quorum
        // signatures (`RootFounding::verify`) and adopt it by re-seeding the
        // registry from the founded cohort, closing the "haywire trust on every
        // boot" hazard: an operational Criome trusts its verified founded anchor,
        // not a fresh bootstrap. A gathering (not-yet-unanimous) or unverifiable
        // record is left as-is; the node never spontaneously re-founds.
        if let Ok(reply) = store.ask(store::ReadRootFounding).await
            && let Some(founding) = reply.into_founding()
            && founding.verify()
        {
            for registration in founding.seed_registrations() {
                let _ = store.ask(store::StoreIdentity::new(registration)).await;
            }
        }
        let signer = signer::AttestationSigner::supervise(
            &actor_reference,
            signer::Arguments {
                registry: registry.clone(),
                store: store.clone(),
                master_key,
                criome_identity,
                clock: arguments.clock,
            },
        )
        .spawn()
        .await;
        let verifier = verifier::AttestationVerifier::supervise(
            &actor_reference,
            verifier::Arguments {
                registry: registry.clone(),
            },
        )
        .spawn()
        .await;
        let authorization = authorization::AuthorizationCoordinator::supervise(
            &actor_reference,
            authorization::Arguments {
                store: store.clone(),
            },
        )
        .spawn()
        .await;
        let subscription = subscription::SubscriptionRegistry::supervise(
            &actor_reference,
            subscription::Arguments {
                registry: registry.clone(),
            },
        )
        .spawn()
        .await;

        // Rebuild the anti-equivocation ledger and head cursor from durable state
        // BEFORE the actor takes ownership of the store, so a restarted node resumes
        // with the same single-successor veto and head it held before the boot.
        let co_signed_successors = Self::reconstruct_co_signed_successors(&store).await;
        let contract_heads = Self::reconstruct_contract_heads(&store).await;

        let mut root = Self::new(
            registry,
            signer,
            verifier,
            authorization,
            subscription,
            store,
            arguments.authorization_mode,
            node_identity,
            conveyance,
            arguments.clock,
        );
        root.co_signed_successors = co_signed_successors;
        root.contract_heads = contract_heads;
        Ok(root)
    }
}

impl Message<SubmitRequest> for CriomeRoot {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: SubmitRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.submit(message.request).await)
    }
}

impl Message<SubmitMetaRequest> for CriomeRoot {
    type Reply = CriomeMetaActorReply;

    async fn handle(
        &mut self,
        message: SubmitMetaRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        CriomeMetaActorReply::new(self.submit_meta(message.request).await)
    }
}

impl Message<OpenAuthorizationObservation> for CriomeRoot {
    type Reply = Result<AuthorizationObservationOpened>;

    async fn handle(
        &mut self,
        message: OpenAuthorizationObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.open_authorization_observation(message.request_slot)
            .await
    }
}

impl Message<ReadTopology> for CriomeRoot {
    type Reply = CriomeTopology;

    async fn handle(
        &mut self,
        _message: ReadTopology,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        CriomeTopology::complete()
    }
}
