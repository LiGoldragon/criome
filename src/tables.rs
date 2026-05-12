use std::path::{Path, PathBuf};

use sema::{Schema, SchemaVersion, Sema, Table};
use signal_criome::{
    Attestation, BlsPublicKey, Identity, IdentityReceipt, IdentityRegistration, IdentityRevocation,
    KeyPurpose, PrincipalName, PrincipalStatus, PublicKeyFingerprint,
};

use crate::Result;

const CRIOME_SCHEMA: Schema = Schema {
    version: SchemaVersion::new(1),
};

const IDENTITIES: Table<&'static str, StoredIdentity> = Table::new("identities");
const REVOCATIONS: Table<&'static str, StoredRevocation> = Table::new("revocations");
const ATTESTATIONS: Table<u64, StoredAttestation> = Table::new("attestations");
const ATTESTATION_NEXT_SLOT: Table<&'static str, u64> = Table::new("attestation_next_slot");
const ATTESTATION_NEXT_SLOT_KEY: &str = "next";

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
                None => Self::new("/tmp/criome.redb"),
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
            public_key: registration.public_key,
            fingerprint: registration.fingerprint,
            purpose: registration.purpose,
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
            status: self.status,
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
            fingerprint: revocation.fingerprint,
            reason: revocation.reason,
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

pub struct CriomeTables {
    database: Sema,
}

impl CriomeTables {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let database = Sema::open_with_schema(store.as_path(), &CRIOME_SCHEMA)?;
        database.write(|transaction| {
            IDENTITIES.ensure(transaction)?;
            REVOCATIONS.ensure(transaction)?;
            ATTESTATIONS.ensure(transaction)?;
            ATTESTATION_NEXT_SLOT.ensure(transaction)?;
            Ok(())
        })?;
        Ok(Self { database })
    }

    pub fn put_identity(&self, identity: &StoredIdentity) -> Result<()> {
        let key = IdentityKey::new(identity.identity()).into_string();
        self.database.write(|transaction| {
            IDENTITIES.insert(transaction, key.as_str(), identity)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn identity(&self, identity: &Identity) -> Result<Option<StoredIdentity>> {
        let key = IdentityKey::new(identity).into_string();
        Ok(self
            .database
            .read(|transaction| IDENTITIES.get(transaction, key.as_str()))?)
    }

    pub fn identities(&self) -> Result<Vec<StoredIdentity>> {
        Ok(self.database.read(|transaction| {
            Ok(IDENTITIES
                .iter(transaction)?
                .into_iter()
                .map(|(_key, identity)| identity)
                .collect())
        })?)
    }

    pub fn put_revocation(&self, revocation: &StoredRevocation) -> Result<()> {
        let key = IdentityKey::new(revocation.identity()).into_string();
        self.database.write(|transaction| {
            REVOCATIONS.insert(transaction, key.as_str(), revocation)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn put_attestation(&self, attestation: Attestation) -> Result<StoredAttestation> {
        let slot = self.next_attestation_slot()?;
        let stored = StoredAttestation::new(slot.value(), attestation);
        self.database.write(|transaction| {
            ATTESTATIONS.insert(transaction, stored.slot(), &stored)?;
            ATTESTATION_NEXT_SLOT.insert(
                transaction,
                ATTESTATION_NEXT_SLOT_KEY,
                &slot.next_value(),
            )?;
            Ok(())
        })?;
        Ok(stored)
    }

    pub fn attestations(&self) -> Result<Vec<StoredAttestation>> {
        Ok(self.database.read(|transaction| {
            Ok(ATTESTATIONS
                .iter(transaction)?
                .into_iter()
                .map(|(_slot, attestation)| attestation)
                .collect())
        })?)
    }

    fn next_attestation_slot(&self) -> Result<AttestationSlot> {
        let stored = self.database.read(|transaction| {
            ATTESTATION_NEXT_SLOT.get(transaction, ATTESTATION_NEXT_SLOT_KEY)
        })?;
        match stored {
            Some(next_slot) => Ok(AttestationSlot::new(next_slot)),
            None => Ok(AttestationSlot::after_records(&self.attestations()?)),
        }
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
