use std::path::{Path, PathBuf};

use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema_engine::{
    Engine, EngineOpen, EngineStoredValue, FamilyName, KeyedAssertion, KeyedMutation, QueryPlan,
    RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    VersionedStoreName, VersioningPolicy,
};
use signal_criome::{
    Attestation, AuthorizationDenial, AuthorizationGrant, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, BlsPublicKey, Contract, ContractDigest,
    Identity, IdentityReceipt, IdentityRegistration, IdentityRevocation, KeyPurpose, ObjectDigest,
    PrincipalName, PrincipalStatus, PublicKeyFingerprint, ReplayNonce, SignatureSolicitationRoute,
    SignatureSubmission,
};

use crate::Result;

const CRIOME_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(3);
const IDENTITIES: TableName = TableName::new("identities");
const REVOCATIONS: TableName = TableName::new("revocations");
const ATTESTATIONS: TableName = TableName::new("attestations");
const AUTHORIZATION_STATES: TableName = TableName::new("authorization_requests");
const AUTHORIZATION_REPLAY_NONCES: TableName = TableName::new("authorization_replay_nonces");
const CONTRACTS: TableName = TableName::new("contracts");
const SIGNATURE_SOLICITATIONS: TableName = TableName::new("signature_solicitations");
const SUBMITTED_SIGNATURES: TableName = TableName::new("submitted_signatures");
const ATTESTATION_NEXT_SLOT: TableName = TableName::new("attestation_next_slot");
const ATTESTATION_NEXT_SLOT_KEY: &str = "next";
const AUTHORIZATION_NEXT_SLOT: TableName = TableName::new("authorization_next_slot");
const AUTHORIZATION_NEXT_SLOT_KEY: &str = "next";
const IDENTITIES_FAMILY: &str = "criome-identity";
const REVOCATIONS_FAMILY: &str = "criome-revocation";
const ATTESTATIONS_FAMILY: &str = "criome-attestation";
const AUTHORIZATION_STATES_FAMILY: &str = "criome-authorization-state";
const AUTHORIZATION_REPLAY_NONCES_FAMILY: &str = "criome-authorization-replay-nonce";
const CONTRACTS_FAMILY: &str = "criome-contract";
const SIGNATURE_SOLICITATIONS_FAMILY: &str = "criome-signature-solicitation";
const SUBMITTED_SIGNATURES_FAMILY: &str = "criome-submitted-signature";
const ATTESTATION_NEXT_SLOT_FAMILY: &str = "criome-attestation-slot";
const AUTHORIZATION_NEXT_SLOT_FAMILY: &str = "criome-authorization-slot";

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
                None => Self::new("/tmp/criome.sema"),
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

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredContract {
    digest: ContractDigest,
    contract: Contract,
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

impl StoredContract {
    pub fn new(digest: ContractDigest, contract: Contract) -> Self {
        Self { digest, contract }
    }

    pub fn digest(&self) -> &ContractDigest {
        &self.digest
    }

    pub fn contract(&self) -> &Contract {
        &self.contract
    }

    pub fn into_parts(self) -> (ContractDigest, Contract) {
        (self.digest, self.contract)
    }
}

pub struct CriomeTables {
    engine: Engine,
    identities: TableReference<StoredIdentity>,
    revocations: TableReference<StoredRevocation>,
    attestations: TableReference<StoredAttestation>,
    authorization_states: TableReference<StoredAuthorizationState>,
    authorization_replay_nonces: TableReference<AuthorizationRequestSlot>,
    contracts: TableReference<StoredContract>,
    signature_solicitations: TableReference<StoredSignatureSolicitation>,
    submitted_signatures: TableReference<StoredSignatureSubmission>,
    attestation_next_slot: TableReference<u64>,
    authorization_next_slot: TableReference<u64>,
}

impl CriomeTables {
    pub fn open(store: &StoreLocation) -> Result<Self> {
        let mut engine = Engine::open(Self::engine_open(store))?;
        let identities =
            engine.register_table(Self::family_descriptor(IDENTITIES, IDENTITIES_FAMILY))?;
        let revocations =
            engine.register_table(Self::family_descriptor(REVOCATIONS, REVOCATIONS_FAMILY))?;
        let attestations =
            engine.register_table(Self::family_descriptor(ATTESTATIONS, ATTESTATIONS_FAMILY))?;
        let authorization_states = engine.register_table(Self::family_descriptor(
            AUTHORIZATION_STATES,
            AUTHORIZATION_STATES_FAMILY,
        ))?;
        let authorization_replay_nonces = engine.register_table(Self::family_descriptor(
            AUTHORIZATION_REPLAY_NONCES,
            AUTHORIZATION_REPLAY_NONCES_FAMILY,
        ))?;
        let contracts =
            engine.register_table(Self::family_descriptor(CONTRACTS, CONTRACTS_FAMILY))?;
        let signature_solicitations = engine.register_table(Self::family_descriptor(
            SIGNATURE_SOLICITATIONS,
            SIGNATURE_SOLICITATIONS_FAMILY,
        ))?;
        let submitted_signatures = engine.register_table(Self::family_descriptor(
            SUBMITTED_SIGNATURES,
            SUBMITTED_SIGNATURES_FAMILY,
        ))?;
        let attestation_next_slot = engine.register_table(Self::family_descriptor(
            ATTESTATION_NEXT_SLOT,
            ATTESTATION_NEXT_SLOT_FAMILY,
        ))?;
        let authorization_next_slot = engine.register_table(Self::family_descriptor(
            AUTHORIZATION_NEXT_SLOT,
            AUTHORIZATION_NEXT_SLOT_FAMILY,
        ))?;
        Ok(Self {
            engine,
            identities,
            revocations,
            attestations,
            authorization_states,
            authorization_replay_nonces,
            contracts,
            signature_solicitations,
            submitted_signatures,
            attestation_next_slot,
            authorization_next_slot,
        })
    }

    fn engine_open(store: &StoreLocation) -> EngineOpen {
        EngineOpen::new(store.as_path().to_path_buf(), CRIOME_SCHEMA_VERSION)
            .with_versioning(Self::versioning_policy())
    }

    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new("criome"))
    }

    fn family_descriptor<RecordValue>(
        table: TableName,
        family: &str,
    ) -> TableDescriptor<RecordValue> {
        TableDescriptor::new(
            table,
            FamilyName::new(family),
            SchemaHash::for_label(format!(
                "criome-{family}-v{}",
                CRIOME_SCHEMA_VERSION.value()
            )),
        )
    }

    pub fn put_identity(&self, identity: &StoredIdentity) -> Result<()> {
        let key = IdentityKey::new(identity.identity()).into_string();
        self.upsert(self.identities, key, identity.clone())?;
        Ok(())
    }

    pub fn identity(&self, identity: &Identity) -> Result<Option<StoredIdentity>> {
        let key = IdentityKey::new(identity).into_string();
        self.read_key(self.identities, key)
    }

    pub fn identities(&self) -> Result<Vec<StoredIdentity>> {
        self.read_all(self.identities)
    }

    pub fn put_revocation(&self, revocation: &StoredRevocation) -> Result<()> {
        let key = IdentityKey::new(revocation.identity()).into_string();
        self.upsert(self.revocations, key, revocation.clone())?;
        Ok(())
    }

    pub fn put_attestation(&self, attestation: Attestation) -> Result<StoredAttestation> {
        let slot = self.next_attestation_slot()?;
        let stored = StoredAttestation::new(slot.value(), attestation);
        self.upsert(self.attestations, stored.slot().to_string(), stored.clone())?;
        self.upsert(
            self.attestation_next_slot,
            ATTESTATION_NEXT_SLOT_KEY.to_owned(),
            slot.next_value(),
        )?;
        Ok(stored)
    }

    pub fn attestations(&self) -> Result<Vec<StoredAttestation>> {
        self.read_all(self.attestations)
    }

    pub fn put_authorization_state(&self, state: &StoredAuthorizationState) -> Result<()> {
        let key = AuthorizationSlotKey::new(&state.state().request_slot).into_string();
        self.upsert(self.authorization_states, key, state.clone())?;
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
        self.upsert(self.authorization_states, key, stored.clone())?;
        self.upsert(
            self.authorization_replay_nonces,
            replay_key,
            stored.state().request_slot.clone(),
        )?;
        self.upsert(
            self.authorization_next_slot,
            AUTHORIZATION_NEXT_SLOT_KEY.to_owned(),
            slot.next_value(),
        )?;
        Ok(stored)
    }

    pub fn authorization_state(
        &self,
        slot: &AuthorizationRequestSlot,
    ) -> Result<Option<StoredAuthorizationState>> {
        let key = AuthorizationSlotKey::new(slot).into_string();
        self.read_key(self.authorization_states, key)
    }

    pub fn authorization_states(&self) -> Result<Vec<StoredAuthorizationState>> {
        self.read_all(self.authorization_states)
    }

    pub fn authorization_replay_slot(
        &self,
        replay_identity: &AuthorizationReplayIdentity,
    ) -> Result<Option<AuthorizationRequestSlot>> {
        let key = AuthorizationReplayKey::new(replay_identity).into_string();
        self.read_key(self.authorization_replay_nonces, key)
    }

    pub fn put_contract(&self, contract: &StoredContract) -> Result<()> {
        let key = ContractDigestKey::new(contract.digest()).into_string();
        self.upsert(self.contracts, key, contract.clone())?;
        Ok(())
    }

    pub fn contract(&self, digest: &ContractDigest) -> Result<Option<StoredContract>> {
        let key = ContractDigestKey::new(digest).into_string();
        self.read_key(self.contracts, key)
    }

    pub fn contracts(&self) -> Result<Vec<StoredContract>> {
        self.read_all(self.contracts)
    }

    pub fn put_signature_solicitation(
        &self,
        solicitation: &StoredSignatureSolicitation,
    ) -> Result<()> {
        let key = SignatureSolicitationKey::new(solicitation.route()).into_string();
        self.upsert(self.signature_solicitations, key, solicitation.clone())?;
        Ok(())
    }

    pub fn put_signature_submission(&self, submission: &StoredSignatureSubmission) -> Result<()> {
        let key = SignatureSubmissionKey::new(submission.submission()).into_string();
        self.upsert(self.submitted_signatures, key, submission.clone())?;
        Ok(())
    }

    fn next_attestation_slot(&self) -> Result<AttestationSlot> {
        let stored = self.read_key(
            self.attestation_next_slot,
            ATTESTATION_NEXT_SLOT_KEY.to_owned(),
        )?;
        match stored {
            Some(next_slot) => Ok(AttestationSlot::new(next_slot)),
            None => Ok(AttestationSlot::after_records(&self.attestations()?)),
        }
    }

    fn next_authorization_slot(&self) -> Result<AuthorizationSlot> {
        let stored = self.read_key(
            self.authorization_next_slot,
            AUTHORIZATION_NEXT_SLOT_KEY.to_owned(),
        )?;
        match stored {
            Some(next_slot) => Ok(AuthorizationSlot::new(next_slot)),
            None => Ok(AuthorizationSlot::after_records(
                &self.authorization_states()?,
            )),
        }
    }

    fn upsert<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: String,
        record: RecordValue,
    ) -> Result<()>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let key = RecordKey::new(key);
        let exists = !self
            .engine
            .match_records(QueryPlan::key(table, key.clone()))?
            .records()
            .is_empty();
        if exists {
            self.engine
                .mutate_keyed(KeyedMutation::new(table, key, record))?;
        } else {
            self.engine
                .assert_keyed(KeyedAssertion::new(table, key, record))?;
        }
        Ok(())
    }

    fn read_key<RecordValue>(
        &self,
        table: TableReference<RecordValue>,
        key: String,
    ) -> Result<Option<RecordValue>>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .engine
            .match_records(QueryPlan::key(table, RecordKey::new(key)))?
            .records()
            .first()
            .cloned())
    }

    fn read_all<RecordValue>(&self, table: TableReference<RecordValue>) -> Result<Vec<RecordValue>>
    where
        RecordValue: EngineStoredValue + Send + Sync + 'static,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        Ok(self
            .engine
            .match_records(QueryPlan::all(table))?
            .records()
            .to_vec())
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

struct ContractDigestKey {
    digest: String,
}

impl ContractDigestKey {
    fn new(digest: &ContractDigest) -> Self {
        Self {
            digest: digest.object_digest().as_ref().to_string(),
        }
    }

    fn into_string(self) -> String {
        self.digest
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
