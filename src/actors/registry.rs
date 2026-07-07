use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    BlsPublicKey, CriomeReply, Identity, IdentityLookup, IdentityReceipt, IdentityRegistration,
    IdentityRevocation, IdentitySnapshot, PrincipalStatus, RejectionReason,
};

use crate::actors::{CriomeActorReply, actor_reply, rejection, store};
use crate::admission::ClusterRoot;
use crate::tables::StoredIdentity;

pub struct IdentityRegistry {
    store: ActorRef<store::StoreKernel>,
    cluster_root: Option<ClusterRoot>,
}

pub struct RegisterIdentity {
    registration: IdentityRegistration,
}

pub struct RevokeIdentity {
    revocation: IdentityRevocation,
}

pub struct LookupIdentity {
    lookup: IdentityLookup,
}

pub struct ReadIdentitySnapshot;

pub struct ResolveIdentity {
    identity: Identity,
}

pub struct ConfigureClusterRoot {
    cluster_root: Option<ClusterRoot>,
}

#[derive(Clone)]
pub struct Arguments {
    pub store: ActorRef<store::StoreKernel>,
    pub cluster_root: Option<ClusterRoot>,
}

#[derive(kameo::Reply)]
pub struct RegistryLookup {
    identity: Option<StoredIdentity>,
}

#[derive(kameo::Reply)]
pub struct RegistrySnapshot {
    identities: Vec<StoredIdentity>,
}

impl RegisterIdentity {
    pub fn new(registration: IdentityRegistration) -> Self {
        Self { registration }
    }
}

impl RevokeIdentity {
    pub fn new(revocation: IdentityRevocation) -> Self {
        Self { revocation }
    }
}

impl LookupIdentity {
    pub fn new(lookup: IdentityLookup) -> Self {
        Self { lookup }
    }
}

impl ResolveIdentity {
    pub fn new(identity: Identity) -> Self {
        Self { identity }
    }
}

impl ConfigureClusterRoot {
    pub fn new(cluster_root: Option<ClusterRoot>) -> Self {
        Self { cluster_root }
    }
}

impl RegistryLookup {
    pub fn into_identity(self) -> Option<StoredIdentity> {
        self.identity
    }
}

impl RegistrySnapshot {
    pub fn into_identities(self) -> Vec<StoredIdentity> {
        self.identities
    }
}

impl IdentityRegistry {
    fn new(store: ActorRef<store::StoreKernel>, cluster_root: Option<ClusterRoot>) -> Self {
        Self {
            store,
            cluster_root,
        }
    }

    async fn register(&self, registration: IdentityRegistration) -> CriomeReply {
        // Cluster-root admission gate (Spirit ermr): when a cluster root is
        // configured, a key is admitted only with a valid cluster-root signature
        // over the registration statement. Dev/virgin daemons (no configured
        // root) skip the gate.
        if let Some(root) = &self.cluster_root {
            match registration.optional_signature_envelope() {
                Some(admission) if root.admits(&registration, admission) => {}
                _ => return rejection(RejectionReason::UnauthorizedRegistration),
            }
        }
        match self.lookup_stored(registration.identity.clone()).await {
            Ok(Some(existing)) if existing.status() == PrincipalStatus::Active => {
                rejection(RejectionReason::DuplicateIdentity)
            }
            Ok(_) => match self
                .store
                .ask(store::StoreIdentity::new(registration))
                .await
            {
                Ok(reply) => CriomeReply::IdentityReceipt(reply.into_identity().receipt()),
                Err(_error) => rejection(RejectionReason::MalformedRequest),
            },
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn revoke(&self, revocation: IdentityRevocation) -> CriomeReply {
        match self
            .store
            .ask(store::StoreRevocation::new(revocation))
            .await
        {
            Ok(reply) => CriomeReply::IdentityReceipt(reply.into_identity().receipt()),
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn lookup(&self, lookup: IdentityLookup) -> CriomeReply {
        match self.lookup_stored(lookup.into_payload()).await {
            Ok(Some(identity)) => CriomeReply::IdentityReceipt(identity.receipt()),
            Ok(None) => rejection(RejectionReason::UnknownIdentity),
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn snapshot(&self) -> CriomeReply {
        match self.snapshot_records().await {
            Ok(records) => CriomeReply::IdentitySnapshot(IdentitySnapshot::from_identities(
                records
                    .into_iter()
                    .map(|identity| IdentityReceipt {
                        identity: identity.identity().clone(),
                        principal_status: identity.status(),
                    })
                    .collect(),
            )),
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    pub async fn lookup_stored(&self, identity: Identity) -> crate::Result<Option<StoredIdentity>> {
        let reply = self
            .store
            .ask(store::LookupIdentity::new(identity))
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(reply.into_identity())
    }

    pub async fn snapshot_records(&self) -> crate::Result<Vec<StoredIdentity>> {
        let reply = self
            .store
            .ask(store::ReadIdentitySnapshot)
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(reply.into_identities())
    }

    pub async fn active_public_key(
        &self,
        identity: Identity,
    ) -> crate::Result<Option<BlsPublicKey>> {
        let Some(stored) = self.lookup_stored(identity).await? else {
            return Ok(None);
        };
        if stored.status() == PrincipalStatus::Active {
            Ok(Some(stored.public_key().clone()))
        } else {
            Ok(None)
        }
    }

    fn configure_cluster_root(&mut self, cluster_root: Option<ClusterRoot>) {
        self.cluster_root = cluster_root;
    }
}

impl Actor for IdentityRegistry {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(arguments.store, arguments.cluster_root))
    }
}

impl Message<RegisterIdentity> for IdentityRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: RegisterIdentity,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.register(message.registration).await)
    }
}

impl Message<RevokeIdentity> for IdentityRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: RevokeIdentity,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.revoke(message.revocation).await)
    }
}

impl Message<LookupIdentity> for IdentityRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: LookupIdentity,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.lookup(message.lookup).await)
    }
}

impl Message<ReadIdentitySnapshot> for IdentityRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        _message: ReadIdentitySnapshot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.snapshot().await)
    }
}

impl Message<ResolveIdentity> for IdentityRegistry {
    type Reply = crate::Result<RegistryLookup>;

    async fn handle(
        &mut self,
        message: ResolveIdentity,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.lookup_stored(message.identity)
            .await
            .map(|identity| RegistryLookup { identity })
    }
}

impl Message<ConfigureClusterRoot> for IdentityRegistry {
    type Reply = ();

    async fn handle(
        &mut self,
        message: ConfigureClusterRoot,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.configure_cluster_root(message.cluster_root);
    }
}
