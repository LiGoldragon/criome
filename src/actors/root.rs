use kameo::actor::{Actor, ActorRef, Spawn};
use kameo::message::{Context, Message};
use signal_criome::{
    AuthorizationAttestationRequest, BlsPublicKey, CriomeReply, CriomeRequest, Identity,
    IdentityRegistration, IdentitySubscriptionToken, KeyPurpose, RejectionReason,
};

use crate::actors::{
    CriomeActorReply, actor_reply, authorization, registry, rejection, signer, store, subscription,
    verifier,
};
use crate::admission::ClusterRoot;
use crate::master_key::MasterKey;
use crate::{Error, Result, StoreLocation};

pub struct CriomeRoot {
    registry: ActorRef<registry::IdentityRegistry>,
    signer: ActorRef<signer::AttestationSigner>,
    verifier: ActorRef<verifier::AttestationVerifier>,
    authorization: ActorRef<authorization::AuthorizationCoordinator>,
    subscription: ActorRef<subscription::SubscriptionRegistry>,
}

pub struct Arguments {
    pub store: StoreLocation,
    pub cluster_root: Option<BlsPublicKey>,
}

pub struct SubmitRequest {
    request: CriomeRequest,
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

impl Arguments {
    pub fn new(store: StoreLocation) -> Self {
        Self {
            store,
            cluster_root: None,
        }
    }
}

impl SubmitRequest {
    pub fn new(request: CriomeRequest) -> Self {
        Self { request }
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
    ) -> Self {
        Self {
            registry,
            signer,
            verifier,
            authorization,
            subscription,
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

    async fn submit(&self, request: CriomeRequest) -> CriomeReply {
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
                    .ask(store::StoreIdentity::new(IdentityRegistration {
                        identity: criome_identity.clone(),
                        public_key: master_public_key,
                        fingerprint: master_key.fingerprint(),
                        purpose: KeyPurpose::CriomeRoot,
                        admission: None,
                    }))
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
