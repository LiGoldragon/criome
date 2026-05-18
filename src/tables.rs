use std::path::{Path, PathBuf};

use sema::{Schema, SchemaVersion, Sema, Table};
use signal_criome::{
    Attestation, AuthorizationDenial, AuthorizationGrant, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, BlsPublicKey, Identity, IdentityReceipt,
    IdentityRegistration, IdentityRevocation, KeyPurpose, ObjectDigest, PrincipalName,
    PrincipalStatus, PublicKeyFingerprint, ReplayNonce, SignatureSolicitationRoute,
    SignatureSubmission,
};

use crate::Result;

const CRIOME_SCHEMA: Schema = Schema {
    version: SchemaVersion::new(1),
};

const IDENTITIES: Table<&'static str, StoredIdentity> = Table::new("identities");
const REVOCATIONS: Table<&'static str, StoredRevocation> = Table::new("revocations");
const ATTESTATIONS: Table<u64, StoredAttestation> = Table::new("attestations");
const AUTHORIZATION_STATES: Table<&'static str, StoredAuthorizationState> =
    Table::new("authorization_requests");
const AUTHORIZATION_REPLAY_NONCES: Table<&'static str, AuthorizationRequestSlot> =
    Table::new("authorization_replay_nonces");
const SIGNATURE_SOLICITATIONS: Table<&'static str, StoredSignatureSolicitation> =
    Table::new("signature_solicitations");
const SUBMITTED_SIGNATURES: Table<&'static str, StoredSignatureSubmission> =
    Table::new("submitted_signatures");
const ATTESTATION_NEXT_SLOT: Table<&'static str, u64> = Table::new("attestation_next_slot");
const ATTESTATION_NEXT_SLOT_KEY: &str = "next";
const AUTHORIZATION_NEXT_SLOT: Table<&'static str, u64> = Table::new("authorization_next_slot");
const AUTHORIZATION_NEXT_SLOT_KEY: &str = "next";

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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredAuthorizationState {
    state: AuthorizationStateRecord,
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
            AUTHORIZATION_STATES.ensure(transaction)?;
            AUTHORIZATION_REPLAY_NONCES.ensure(transaction)?;
            SIGNATURE_SOLICITATIONS.ensure(transaction)?;
            SUBMITTED_SIGNATURES.ensure(transaction)?;
            ATTESTATION_NEXT_SLOT.ensure(transaction)?;
            AUTHORIZATION_NEXT_SLOT.ensure(transaction)?;
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

    pub fn put_authorization_state(&self, state: &StoredAuthorizationState) -> Result<()> {
        let key = AuthorizationSlotKey::new(&state.state().request_slot).into_string();
        self.database.write(|transaction| {
            AUTHORIZATION_STATES.insert(transaction, key.as_str(), state)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn put_new_authorization_state(
        &self,
        request_digest: ObjectDigest,
        status: AuthorizationStatus,
        missing_authorities: Vec<Identity>,
        grant: Option<AuthorizationGrant>,
        denial: Option<AuthorizationDenial>,
        replay_identity: AuthorizationReplayIdentity,
    ) -> Result<StoredAuthorizationState> {
        if self.authorization_replay_slot(&replay_identity)?.is_some() {
            return Err(crate::Error::AuthorizationReplayAttempted);
        }
        let slot = self.next_authorization_slot()?;
        let state = AuthorizationStateRecord {
            request_slot: slot.request_slot(),
            request_digest,
            status,
            missing_authorities,
            grant,
            denial,
        };
        let stored = StoredAuthorizationState::new(state);
        let key = AuthorizationSlotKey::new(&stored.state().request_slot).into_string();
        let replay_key = AuthorizationReplayKey::new(&replay_identity).into_string();
        self.database.write(|transaction| {
            AUTHORIZATION_STATES.insert(transaction, key.as_str(), &stored)?;
            AUTHORIZATION_REPLAY_NONCES.insert(
                transaction,
                replay_key.as_str(),
                &stored.state().request_slot,
            )?;
            AUTHORIZATION_NEXT_SLOT.insert(
                transaction,
                AUTHORIZATION_NEXT_SLOT_KEY,
                &slot.next_value(),
            )?;
            Ok(())
        })?;
        Ok(stored)
    }

    pub fn authorization_state(
        &self,
        slot: &AuthorizationRequestSlot,
    ) -> Result<Option<StoredAuthorizationState>> {
        let key = AuthorizationSlotKey::new(slot).into_string();
        Ok(self
            .database
            .read(|transaction| AUTHORIZATION_STATES.get(transaction, key.as_str()))?)
    }

    pub fn authorization_states(&self) -> Result<Vec<StoredAuthorizationState>> {
        Ok(self.database.read(|transaction| {
            Ok(AUTHORIZATION_STATES
                .iter(transaction)?
                .into_iter()
                .map(|(_key, state)| state)
                .collect())
        })?)
    }

    pub fn authorization_replay_slot(
        &self,
        replay_identity: &AuthorizationReplayIdentity,
    ) -> Result<Option<AuthorizationRequestSlot>> {
        let key = AuthorizationReplayKey::new(replay_identity).into_string();
        Ok(self
            .database
            .read(|transaction| AUTHORIZATION_REPLAY_NONCES.get(transaction, key.as_str()))?)
    }

    pub fn put_signature_solicitation(
        &self,
        solicitation: &StoredSignatureSolicitation,
    ) -> Result<()> {
        let key = SignatureSolicitationKey::new(solicitation.route()).into_string();
        self.database.write(|transaction| {
            SIGNATURE_SOLICITATIONS.insert(transaction, key.as_str(), solicitation)?;
            Ok(())
        })?;
        Ok(())
    }

    pub fn put_signature_submission(&self, submission: &StoredSignatureSubmission) -> Result<()> {
        let key = SignatureSubmissionKey::new(submission.submission()).into_string();
        self.database.write(|transaction| {
            SUBMITTED_SIGNATURES.insert(transaction, key.as_str(), submission)?;
            Ok(())
        })?;
        Ok(())
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

    fn next_authorization_slot(&self) -> Result<AuthorizationSlot> {
        let stored = self.database.read(|transaction| {
            AUTHORIZATION_NEXT_SLOT.get(transaction, AUTHORIZATION_NEXT_SLOT_KEY)
        })?;
        match stored {
            Some(next_slot) => Ok(AuthorizationSlot::new(next_slot)),
            None => Ok(AuthorizationSlot::after_records(
                &self.authorization_states()?,
            )),
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

struct AuthorizationSlot {
    value: u64,
}

impl AuthorizationSlot {
    fn new(value: u64) -> Self {
        Self { value }
    }

    fn after_records(records: &[StoredAuthorizationState]) -> Self {
        let value = records
            .iter()
            .filter_map(|record| record.state().request_slot.as_str().parse::<u64>().ok())
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

struct SignatureSolicitationKey {
    request_slot: String,
    routed_to: String,
}

impl SignatureSolicitationKey {
    fn new(route: &SignatureSolicitationRoute) -> Self {
        Self {
            request_slot: route.solicitation.request_slot.as_str().to_string(),
            routed_to: IdentityKey::new(&route.routed_to).into_string(),
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
            request_slot: submission.request_slot.as_str().to_string(),
            signer: IdentityKey::new(&submission.signer).into_string(),
        }
    }

    fn into_string(self) -> String {
        format!("{}:{}", self.request_slot, self.signer)
    }
}
