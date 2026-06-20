use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::message::{Context, Message};
use meta_signal_criome::{
    AuthorizationApproval, AuthorizationApprovalDecision, AuthorizationApprovalRecorded,
};
use signal_criome::{
    AuthorizationAttestationRequest, AuthorizationDenial, AuthorizationDenialReason,
    AuthorizationDenialSource, AuthorizationEvaluated, AuthorizationEvaluation, AuthorizationMode,
    AuthorizationObservationToken, AuthorizationPending, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, AuthorizedObjectUpdate,
    AuthorizedObjectUpdateToken, BlsPublicKey, ContractAdmissionRejected, ContractAdmitted,
    ContractFound, ContractMissing, CriomeDaemonConfiguration, CriomeReply, CriomeRequest,
    EvaluationDecision, Identity, IdentityRegistration, IdentitySubscriptionToken, KeyPurpose,
    ParkedAuthorization, ParkedAuthorizationObservation, ParkedAuthorizationSnapshot,
    RejectionReason,
};

use crate::actors::{
    CriomeActorReply, actor_reply, authorization, registry, rejection, signer, store, subscription,
    verifier,
};
use crate::admission::ClusterRoot;
use crate::language::{ContractStore, EvaluationError, KeyRegistry};
use crate::master_key::MasterKey;
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
}

pub struct Arguments {
    pub store: StoreLocation,
    pub cluster_root: Option<BlsPublicKey>,
    pub authorization_mode: AuthorizationMode,
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
        }
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
    fn new(
        registry: ActorRef<registry::IdentityRegistry>,
        signer: ActorRef<signer::AttestationSigner>,
        verifier: ActorRef<verifier::AttestationVerifier>,
        authorization: ActorRef<authorization::AuthorizationCoordinator>,
        subscription: ActorRef<subscription::SubscriptionRegistry>,
        store: ActorRef<store::StoreKernel>,
        authorization_mode: AuthorizationMode,
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
                self.ask_authorization(authorization::AuthorizeSignalCall::new(request))
                    .await
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
            meta_signal_criome::Input::ObserveParkedAuthorizations(request) => {
                meta_signal_criome::Output::ParkedAuthorizationSnapshot(
                    self.read_parked_authorization_snapshot(request).await,
                )
            }
            meta_signal_criome::Input::SubmitAuthorizationApproval(approval) => {
                self.record_authorization_approval(approval).await
            }
        }
    }

    async fn configure(
        &mut self,
        configuration: CriomeDaemonConfiguration,
    ) -> meta_signal_criome::Output {
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
            None => AuthorizationApprovalDecision::Reject,
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
                    state
                        .parked_evaluation()
                        .cloned()
                        .map(|evaluation| ParkedAuthorization {
                            request_slot: state.request_slot,
                            evaluation,
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
        let criome_identity = Identity::host("criome".to_string());
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
        let signer = signer::AttestationSigner::supervise(
            &actor_reference,
            signer::Arguments {
                registry: registry.clone(),
                store: store.clone(),
                master_key,
                criome_identity,
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
