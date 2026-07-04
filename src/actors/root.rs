use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::message::{Context, Message};
use meta_signal_criome::{
    AuthorizationApproval, AuthorizationApprovalDecision, AuthorizationApprovalRecorded,
    ConfigurationRejectionReason, OperationKind, RequestUnimplemented, RootFoundingAcceptance,
    RootFoundingAccepted, RootFoundingRejectionReason, UnimplementedReason,
};
use std::sync::Arc;

use signal_criome::{
    AttestedMoment, AttestedMomentProposition, AuthorizationAttestationRequest,
    AuthorizationDenial, AuthorizationDenialReason, AuthorizationDenialSource,
    AuthorizationEvaluated, AuthorizationEvaluation, AuthorizationMode,
    AuthorizationObservationToken, AuthorizationPending, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, AuthorizedObjectReference,
    AuthorizedObjectUpdate, AuthorizedObjectUpdateToken, BlsPublicKey, ContractAdmissionRejected,
    ContractAdmitted, ContractFound, ContractMissing, CriomeDaemonConfiguration, CriomeReply,
    CriomeRequest, EvaluationDecision, Evidence, FoundingSignature, Identity, IdentityRegistration,
    IdentitySubscriptionToken, InterceptPolicyCancellation, InterceptPolicyProposal, KeyPurpose,
    OperationDigest, ParkedAuthorization, ParkedAuthorizationObservation,
    ParkedAuthorizationSnapshot, ParkedRequestAnswer, ParkedRequestDecision,
    ParkedRequestIdentifier, ParkedRequestQuery, ParkedSpiritRequest, PolicyMember, QuorumProposal,
    QuorumRoundIdentifier, QuorumRoundState, QuorumRoundStatus, QuorumVote, QuorumVoteSolicitation,
    RejectionReason, RequiredSignatureThreshold, RoundPhase, Rule, SignalCallAuthorization,
    SpiritAuthorizationContext, StampedSignatureEnvelope, TimeSignature, TimestampNanos,
};

use crate::actors::{
    CriomeActorReply, actor_reply, authorization, registry, rejection, signer, store, subscription,
    verifier,
};
use crate::admission::ClusterRoot;
use crate::founding::RootFounding;
use crate::language::{ContractStore, EvaluationError, KeyRegistry};
use crate::master_key::MasterKey;
use crate::master_key::{SystemClock, WindowAdmission};
use crate::tables::StoredQuorumRound;
use crate::voice::{QuorumVoice, SilentVoice};
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
    /// The identity this node casts its quorum votes as — the same identity its
    /// master key is registered under, so a peer's registry verifies its votes.
    node_identity: Identity,
    /// How this node conveys solicitations and votes to peer members' criomes.
    voice: Arc<dyn QuorumVoice>,
    /// This node's own clock, consulted by the peer witness-clock re-check so a
    /// solicited peer independently refuses a window its clock is not inside —
    /// the same gate the signer enforces before time-signing.
    clock: SystemClock,
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
    /// Defaults to the unarmed [`SilentVoice`]; a deployment supplies a
    /// router-mediated or direct-dial voice.
    pub voice: Arc<dyn QuorumVoice>,
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

impl Arguments {
    pub fn new(store: StoreLocation) -> Self {
        Self {
            store,
            cluster_root: None,
            authorization_mode: AuthorizationMode::Quorum,
            node_identity: Self::default_node_identity(),
            voice: Arc::new(SilentVoice),
            clock: SystemClock::system(),
        }
    }

    /// Arm this node's quorum voice (router-mediated or direct-dial). Absent, the
    /// node self-votes but originates no solicitation.
    pub fn with_voice(mut self, voice: Arc<dyn QuorumVoice>) -> Self {
        self.voice = voice;
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
        voice: Arc<dyn QuorumVoice>,
        clock: SystemClock,
    ) -> Self {
        Self {
            registry,
            signer,
            verifier,
            authorization,
            subscription,
            store,
            authorization_mode,
            configuration_generation: 0,
            node_identity,
            voice,
            clock,
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
        }
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
        let _ = self
            .store
            .ask(store::StoreAuthorizationState::new(state))
            .await;
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
            let _ = self
                .store
                .ask(store::StoreAuthorizationState::new(state))
                .await;
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
        let _ = self
            .store
            .ask(store::StoreAuthorizationState::new(state))
            .await;
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
        if self
            .store
            .ask(store::StoreAuthorizationState::new(granted_state))
            .await
            .is_err()
        {
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
    // solicits each peer member's vote across the voice, collects the stamped
    // signatures into a durable round, and feeds the assembled Evidence to the
    // EXISTING majority-judge (`ContractStore::evaluate`, reused unchanged). A
    // round is WITHHELD (`Gathering`) until the judge returns `Authorized`; an
    // unreachable peer leaves it pending forever. Below-majority Evidence is
    // refused fail-closed by the same judge.

    /// Propose (originator). Derive the moment from the contract's members, cast
    /// the self-vote, open the durable round, solicit each peer, and return the
    /// withheld round state.
    async fn propose_quorum_authorization(&self, proposal: QuorumProposal) -> CriomeReply {
        let QuorumProposal {
            round,
            // Single-gather round today: the round key is pinned to
            // `RoundPhase::Request` via `for_operation` below, so the proposal's
            // phase carries no extra choice yet. The two-round worker (.12/.13)
            // makes this phase-aware and must reconcile the round-key binding.
            phase: _,
            contract,
            object,
            window,
        } = proposal;
        // Round-id bound to the change's fingerprint: the round key MUST be the
        // one derived from the operation digest, so two distinct operations can
        // never share a round and a colliding proposal cannot clobber an
        // unrelated in-flight round (audit S1). Enforced at every round-creation
        // ingress; `submit_quorum_vote` inherits it via the round key.
        if round != QuorumRoundIdentifier::for_operation(&object.digest) {
            return rejection(RejectionReason::MalformedRequest);
        }
        let Some(store) = self.contract_store().await else {
            return rejection(RejectionReason::MalformedRequest);
        };
        let Some((required, members)) = self.quorum_members(&store, &contract) else {
            return rejection(RejectionReason::MalformedRequest);
        };
        let proposition = AttestedMomentProposition::new(window, required, members);
        let self_vote = match self.cast_quorum_vote(&round, &object, &proposition).await {
            Ok(vote) => vote,
            Err(reply) => return reply,
        };
        let mut stored = StoredQuorumRound::open(round, contract, object, proposition);
        stored.record_vote(self_vote);
        if self.persist_quorum_round(stored.clone()).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        self.solicit_peers(&stored);
        CriomeReply::QuorumRoundOpened(self.round_state(&stored).await)
    }

    /// Peer vote. Independently re-validate the solicitation (contract admitted
    /// here, this node is a member), cast this node's vote, convey it back to the
    /// originator across the voice, and record it locally for idempotent redial.
    async fn solicit_quorum_vote(&self, solicitation: QuorumVoteSolicitation) -> CriomeReply {
        let QuorumVoteSolicitation {
            round,
            // See `propose_quorum_authorization`: single Request-phase gather
            // today; the two-round worker threads the phase here.
            phase: _,
            contract,
            object,
            proposition,
            originator,
        } = solicitation;
        // Same round-id ⇄ operation-digest binding the originator enforced, so a
        // dishonest originator cannot make this peer open a round under a round
        // key that is not the one its operation dictates (audit S1).
        if round != QuorumRoundIdentifier::for_operation(&object.digest) {
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
        let vote = match self.cast_quorum_vote(&round, &object, &proposition).await {
            Ok(vote) => vote,
            Err(reply) => return reply,
        };
        let mut stored = StoredQuorumRound::open(round, contract, object, proposition);
        stored.record_vote(vote.clone());
        let _ = self.persist_quorum_round(stored.clone()).await;
        self.voice
            .convey(&originator, CriomeRequest::submit_quorum_vote(vote));
        CriomeReply::QuorumVoteSolicited(self.round_state(&stored).await)
    }

    /// A peer's vote arrived. Record it into the round, re-judge, and — on a true
    /// majority — publish the authorized-object update so subscribers converge.
    async fn submit_quorum_vote(&self, vote: QuorumVote) -> CriomeReply {
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
        stored.record_vote(vote);
        if self.persist_quorum_round(stored.clone()).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        let state = self.round_state(&stored).await;
        if state.status == QuorumRoundStatus::Authorized {
            let stamp = self.assemble_evidence(&stored).stamp;
            self.publish_authorized_object_update(AuthorizedObjectUpdate {
                object: stored.object().clone(),
                contract: stored.contract().clone(),
                decision: EvaluationDecision::Authorized,
                stamp,
            })
            .await;
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
        QuorumRoundState {
            round: stored.round().clone(),
            phase: RoundPhase::Request,
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

    /// Cast this node's vote and time attestation over `object` under `proposition`.
    async fn cast_quorum_vote(
        &self,
        round: &QuorumRoundIdentifier,
        object: &AuthorizedObjectReference,
        proposition: &AttestedMomentProposition,
    ) -> std::result::Result<QuorumVote, CriomeReply> {
        let operation = OperationDigest::new(object.digest.clone());
        let signatures = self
            .signer
            .ask(signer::SignQuorumVote::new(operation, proposition.clone()))
            .await
            .map_err(|_error| rejection(RejectionReason::MalformedRequest))?;
        Ok(QuorumVote {
            round: round.clone(),
            phase: RoundPhase::Request,
            voter: self.node_identity.clone(),
            operation_signature: signatures.operation_signature,
            time_signature: signatures.time_signature,
        })
    }

    /// Solicit every peer member (contract members other than this node) across
    /// the voice. Best-effort: an unreachable peer leaves the round pending.
    fn solicit_peers(&self, stored: &StoredQuorumRound) {
        for peer in stored.proposition().authorities() {
            if peer == &self.node_identity {
                continue;
            }
            let solicitation = QuorumVoteSolicitation {
                round: stored.round().clone(),
                phase: RoundPhase::Request,
                contract: stored.contract().clone(),
                object: stored.object().clone(),
                proposition: stored.proposition().clone(),
                originator: self.node_identity.clone(),
            };
            self.voice
                .convey(peer, CriomeRequest::solicit_quorum_vote(solicitation));
        }
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
    // EXISTING router voice; `RootFounding::attach_signature` is the accumulation
    // seam a conveyed peer signature feeds. The live 2-node gather over the voice
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
        if founding.is_unanimous() {
            self.seed_founding_registry(&founding).await;
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
        let voice = arguments.voice;
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

        Ok(Self::new(
            registry,
            signer,
            verifier,
            authorization,
            subscription,
            store,
            arguments.authorization_mode,
            node_identity,
            voice,
            arguments.clock,
        ))
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
