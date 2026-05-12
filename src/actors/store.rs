use kameo::actor::{Actor, ActorRef};
use kameo::message::{Context, Message};
use signal_criome::{
    Attestation, Identity, IdentityRegistration, IdentityRevocation, PrincipalStatus,
};

use crate::tables::{
    CriomeTables, StoreLocation, StoredAttestation, StoredIdentity, StoredRevocation,
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

pub fn active_status(identity: &StoredIdentity) -> bool {
    identity.status() == PrincipalStatus::Active
}
