use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    ArchiveAttestationRequest, Attestation, AttestationReceipt, AttestedMoment,
    AttestedMomentProposition, AuditContext, AuthorizationGrant, AuthorizationPolicyClass,
    AuthorizationPolicySatisfaction, AuthorizationRequestSlot, AuthorizedObjectKind, BlsPublicKey,
    BlsSignature, ChannelGrantAttestationRequest, ComponentKind, ContentPurpose, ContentReference,
    CriomeReply, Identity, OperationDigest, RejectionReason, RequiredSignatureThreshold,
    RootFoundingStatement, SignReceipt, SignRequest, SignalCallAuthorization,
    SignatureAuthorizationResult, SignatureEnvelope, SignatureScheme, StampedSignatureEnvelope,
    TimeWindow, TimestampNanos,
};

use crate::actors::{CriomeActorReply, actor_reply, registry, rejection, store};
use crate::founding::FoundingStatementBytes;
use crate::language::{AttestedMomentStatement, OperationStatement};
use crate::master_key::{AttestationPreimage, MasterKey, SystemClock, WindowAdmission};
use crate::tables::StoredIdentity;

pub struct AttestationSigner {
    registry: ActorRef<registry::IdentityRegistry>,
    store: ActorRef<store::StoreKernel>,
    master_key: MasterKey,
    criome_identity: Identity,
    clock: SystemClock,
}

#[derive(Clone)]
pub struct Arguments {
    pub registry: ActorRef<registry::IdentityRegistry>,
    pub store: ActorRef<store::StoreKernel>,
    pub master_key: MasterKey,
    pub criome_identity: Identity,
    /// This node's clock. The witness-clock gate reads it before time-signing a
    /// quorum vote; a pinned clock makes that gate deterministic under test.
    pub clock: SystemClock,
}

pub struct SignContent {
    request: SignRequest,
}

pub struct AttestArchive {
    request: ArchiveAttestationRequest,
}

pub struct AttestChannelGrant {
    request: ChannelGrantAttestationRequest,
}

pub struct AttestAuthorization {
    content: ContentReference,
    source: Identity,
    audit_context: AuditContext,
}

pub struct SignAuthorizationGrant {
    request_slot: AuthorizationRequestSlot,
    authorization: SignalCallAuthorization,
    /// The policy satisfaction the grant attests. Absent, the grant is the
    /// single-signature self-signed shape (AutoApprove / owner approval);
    /// present, it carries the quorum satisfaction the cluster bridge
    /// assembled (ComplexQuorum with the contract threshold and the commit
    /// round's voters).
    policy_satisfaction: Option<AuthorizationPolicySatisfaction>,
}

/// Cast this node's quorum vote: sign the operation statement and time-sign the
/// attested moment with the master key, as the node's own identity. Unlike
/// `SignContent`, this mints no attestation record — a vote is a ballot, not an
/// attestation — and it is not identity-gated: criome always votes as itself.
pub struct SignQuorumVote {
    operation: OperationDigest,
    proposition: AttestedMomentProposition,
}

/// The two BLS signatures a single member contributes to a quorum round: one
/// over the `OperationStatement` (the vote), one over the moment proposition
/// (the time attestation). The originator wraps these into the assembled
/// `Evidence` when it judges the round.
#[derive(Clone, Debug, kameo::Reply)]
pub struct QuorumVoteSignatures {
    pub operation_signature: SignatureEnvelope,
    pub time_signature: SignatureEnvelope,
}

/// Read this node's Criome master public key. Exposes the key the public-socket
/// `ObserveNodePublicKey` read-op returns and that the founding path matches a
/// node against its seat in a cohort.
pub struct ReadNodePublicKey;

/// The node's master public key, reply-wrapped because the wire `BlsPublicKey` is
/// a foreign type and cannot itself derive `kameo::Reply`.
#[derive(Clone, Debug, kameo::Reply)]
pub struct NodeMasterPublicKey {
    pub public_key: BlsPublicKey,
}

/// Sign a root-founding statement with this node's master key. Unlike a quorum
/// vote, founding is NOT clock-gated (it is not a time-windowed operation) and
/// mints no attestation record — the reply is the bare, scheme-tagged envelope
/// the node contributes to the founding quorum.
pub struct SignFoundingStatement {
    statement: RootFoundingStatement,
}

/// A single node's founding signature envelope, reply-wrapped over the foreign
/// `SignatureEnvelope`.
#[derive(Clone, Debug, kameo::Reply)]
pub struct FoundingStatementSignature {
    pub envelope: SignatureEnvelope,
}

struct AuthorizationGrantStatement<'a> {
    grant: &'a AuthorizationGrant,
    expires_at: Option<TimestampNanos>,
}

impl SignContent {
    pub fn new(request: SignRequest) -> Self {
        Self { request }
    }
}

impl AttestArchive {
    pub fn new(request: ArchiveAttestationRequest) -> Self {
        Self { request }
    }
}

impl AttestChannelGrant {
    pub fn new(request: ChannelGrantAttestationRequest) -> Self {
        Self { request }
    }
}

impl AttestAuthorization {
    pub fn new(content: ContentReference, source: Identity, audit_context: AuditContext) -> Self {
        Self {
            content,
            source,
            audit_context,
        }
    }
}

impl SignAuthorizationGrant {
    pub fn new(
        request_slot: AuthorizationRequestSlot,
        authorization: SignalCallAuthorization,
    ) -> Self {
        Self {
            request_slot,
            authorization,
            policy_satisfaction: None,
        }
    }

    /// A grant attesting a satisfied quorum policy rather than the
    /// single-signature self-signed default.
    pub fn with_policy_satisfaction(
        request_slot: AuthorizationRequestSlot,
        authorization: SignalCallAuthorization,
        policy_satisfaction: AuthorizationPolicySatisfaction,
    ) -> Self {
        Self {
            request_slot,
            authorization,
            policy_satisfaction: Some(policy_satisfaction),
        }
    }
}

impl SignQuorumVote {
    pub fn new(operation: OperationDigest, proposition: AttestedMomentProposition) -> Self {
        Self {
            operation,
            proposition,
        }
    }
}

impl SignFoundingStatement {
    pub fn new(statement: RootFoundingStatement) -> Self {
        Self { statement }
    }
}

impl<'a> AuthorizationGrantStatement<'a> {
    fn new(grant: &'a AuthorizationGrant, expires_at: Option<TimestampNanos>) -> Self {
        Self { grant, expires_at }
    }

    // The V2 statement binds the typed authorized object — component and kind
    // as closed enum tags plus the object digest — in place of the V1 line's
    // contract/operation/scope string trio (retired with the typed ask).
    fn to_signing_bytes(&self) -> Vec<u8> {
        let mut bytes = b"CRIOME-AUTHORIZATION-GRANT-V2".to_vec();
        self.push_text(&mut bytes, self.grant.authorization_request_slot.as_str());
        self.push_authorized_object(&mut bytes);
        self.push_policy_satisfaction(&mut bytes);
        self.push_signature_result(&mut bytes);
        self.push_identity(&mut bytes, &self.grant.identity);
        self.push_timestamp(&mut bytes, self.grant.timestamp_nanos);
        match self.expires_at {
            Some(expires_at) => {
                bytes.extend_from_slice(b"some");
                self.push_timestamp(&mut bytes, expires_at);
            }
            None => bytes.extend_from_slice(b"none"),
        }
        bytes
    }

    fn push_authorized_object(&self, bytes: &mut Vec<u8>) {
        let component_tag: u8 = match self.grant.authorized_object_reference.component_kind {
            ComponentKind::Spirit => 0,
            ComponentKind::Criome => 1,
            ComponentKind::Router => 2,
            ComponentKind::Mirror => 3,
            ComponentKind::Lojix => 4,
            ComponentKind::Persona => 5,
            ComponentKind::Agent => 6,
        };
        bytes.push(component_tag);
        self.push_text(
            bytes,
            self.grant
                .authorized_object_reference
                .object_digest
                .as_str(),
        );
        let kind_tag: u8 = match self
            .grant
            .authorized_object_reference
            .authorized_object_kind
        {
            AuthorizedObjectKind::Operation => 0,
            AuthorizedObjectKind::Contract => 1,
            AuthorizedObjectKind::Agreement => 2,
            AuthorizedObjectKind::Time => 3,
            AuthorizedObjectKind::Head => 4,
        };
        bytes.push(kind_tag);
    }

    fn push_policy_satisfaction(&self, bytes: &mut Vec<u8>) {
        match self
            .grant
            .authorization_policy_satisfaction
            .authorization_policy_class
        {
            AuthorizationPolicyClass::SimpleSelfSigned => bytes.push(0),
            AuthorizationPolicyClass::ComplexQuorum => bytes.push(1),
        }
        bytes.extend_from_slice(
            &self
                .grant
                .authorization_policy_satisfaction
                .required_signature_threshold
                .into_u16()
                .to_le_bytes(),
        );
        bytes.extend_from_slice(
            &(self
                .grant
                .authorization_policy_satisfaction
                .vec_identity()
                .len() as u32)
                .to_le_bytes(),
        );
        for signer in self.grant.authorization_policy_satisfaction.vec_identity() {
            self.push_identity(bytes, signer);
        }
    }

    fn push_signature_result(&self, bytes: &mut Vec<u8>) {
        let tag = match self.grant.signature_authorization_result {
            SignatureAuthorizationResult::SingleSignature => 0,
            SignatureAuthorizationResult::RequiredSignaturesSatisfied => 1,
            SignatureAuthorizationResult::PendingSignatures => 2,
            SignatureAuthorizationResult::Rejected => 3,
            SignatureAuthorizationResult::Expired => 4,
        };
        bytes.push(tag);
    }

    fn push_identity(&self, bytes: &mut Vec<u8>, identity: &Identity) {
        let (tag, name) = match identity {
            Identity::Persona(name) => (0u8, name.as_str()),
            Identity::Agent(name) => (1u8, name.as_str()),
            Identity::Host(name) => (2u8, name.as_str()),
            Identity::Developer(name) => (3u8, name.as_str()),
            Identity::Cluster(name) => (4u8, name.as_str()),
        };
        bytes.push(tag);
        self.push_text(bytes, name);
    }

    fn push_timestamp(&self, bytes: &mut Vec<u8>, timestamp: TimestampNanos) {
        bytes.extend_from_slice(&timestamp.into_u64().to_le_bytes());
    }

    fn push_text(&self, bytes: &mut Vec<u8>, text: &str) {
        bytes.extend_from_slice(&(text.len() as u32).to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
    }
}

impl AttestationSigner {
    fn new(
        registry: ActorRef<registry::IdentityRegistry>,
        store: ActorRef<store::StoreKernel>,
        master_key: MasterKey,
        criome_identity: Identity,
        clock: SystemClock,
    ) -> Self {
        Self {
            registry,
            store,
            master_key,
            criome_identity,
            clock,
        }
    }

    /// This node's Criome host ID as a signing identity: `Host(<master public
    /// key>)`. It is the fabric identity by which a peer's criome verifies this
    /// node's session proofs and forward attestations (primary-79z1.18) — the
    /// master public key itself, distinct from the OS-name `node_identity` this
    /// criome uses for founding and quorum.
    fn own_criome_host_id(&self) -> Identity {
        Identity::host(self.master_key.public_key().as_str().to_string())
    }

    async fn sign(&self, request: SignRequest) -> CriomeReply {
        // The identity the attestation is minted under. A node may always attest
        // as its own Criome host ID (it holds the master key), so that request is
        // authorized without a registry lookup and signed as that host ID — this
        // is how the router stamps the pubkey fabric identity, verified by a
        // peer's criome by that same key. Every other requester must be a known,
        // active identity, and the attestation is signed as this criome's own
        // node identity (self-owned policy); the requester and the kernel-vouched
        // caller live in the audit context.
        let signer = if request.identity == self.own_criome_host_id() {
            request.identity.clone()
        } else {
            if self
                .active_public_key(request.identity.clone())
                .await
                .is_none()
            {
                return rejection(RejectionReason::UnknownIdentity);
            }
            self.criome_identity.clone()
        };
        let issued_at = self.clock.timestamp();
        let expires_at = request.expires_at();
        let mut attestation = Attestation::new(
            request.content_reference,
            signer,
            SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: self.master_key.public_key(),
                bls_signature: BlsSignature::new(String::new()),
            },
            issued_at,
            expires_at,
            request.audit_context,
        );
        // Sign the full attestation statement (everything but the signature),
        // then fill in the real signature.
        let signing_bytes = AttestationPreimage::from_attestation(&attestation).to_signing_bytes();
        attestation.signature_envelope.bls_signature = self.master_key.sign(&signing_bytes);
        if self.record_attestation(attestation.clone()).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::SignReceipt(SignReceipt {
            attestation,
            timestamp_nanos: issued_at,
        })
    }

    async fn attest_archive(&self, request: ArchiveAttestationRequest) -> CriomeReply {
        let sign = SignRequest::new(
            ContentReference {
                object_digest: request.component_release.object_digest,
                content_purpose: ContentPurpose::Archive,
                principal_name: request.component_release.principal_name,
            },
            request.component_release.identity,
            request.audit_context,
            None,
        );
        self.sign_as_receipt(sign).await
    }

    async fn attest_channel_grant(&self, request: ChannelGrantAttestationRequest) -> CriomeReply {
        let sign = SignRequest::new(
            request.content_reference,
            request.identity,
            request.audit_context,
            None,
        );
        self.sign_as_receipt(sign).await
    }

    async fn attest_authorization(&self, request: AttestAuthorization) -> CriomeReply {
        let sign = SignRequest::new(request.content, request.source, request.audit_context, None);
        self.sign_as_receipt(sign).await
    }

    async fn sign_authorization_grant(&self, request: SignAuthorizationGrant) -> CriomeReply {
        let issued_at = self.clock.timestamp();
        let expires_at = request.authorization.expires_at();
        let (policy_satisfaction, signature_result) = match request.policy_satisfaction {
            Some(satisfaction) => (
                satisfaction,
                SignatureAuthorizationResult::RequiredSignaturesSatisfied,
            ),
            None => (
                AuthorizationPolicySatisfaction::new(
                    AuthorizationPolicyClass::SimpleSelfSigned,
                    RequiredSignatureThreshold::new(1),
                    vec![self.criome_identity.clone()],
                ),
                SignatureAuthorizationResult::SingleSignature,
            ),
        };
        let mut grant = AuthorizationGrant::new(
            request.request_slot,
            request.authorization.authorized_object_reference.clone(),
            policy_satisfaction,
            signature_result,
            Vec::new(),
            self.criome_identity.clone(),
            issued_at,
            expires_at,
        );
        let signing_bytes = AuthorizationGrantStatement::new(&grant, expires_at).to_signing_bytes();
        let stamp = self.grant_stamp(issued_at);
        let signature = StampedSignatureEnvelope {
            attested_moment: stamp,
            signature_envelope: SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: self.master_key.public_key(),
                bls_signature: self.master_key.sign(&signing_bytes),
            },
        };
        grant.vec_stamped_signature_envelope = vec![signature];
        CriomeReply::AuthorizationGranted(grant)
    }

    /// Cast this node's vote and time attestation over `operation` under the
    /// shared moment `proposition`. Both preimages fold the proposition digest,
    /// so every member signs the same moment; the originator re-stamps these
    /// envelopes against the assembled moment when it judges.
    fn sign_quorum_vote(
        &self,
        operation: &OperationDigest,
        proposition: &AttestedMomentProposition,
    ) -> crate::Result<QuorumVoteSignatures> {
        // The witness-clock gate: this node emits its time-signature only when its
        // OWN clock places the present inside the request's window. A signature is
        // thus a genuine "now is inside this window" witness, not merely agreement
        // on a window value — so a proposer cannot manufacture "now" by choosing a
        // convenient window; an honest signer refuses a window its clock is not
        // inside, refusing the whole vote (a vote without a valid time-signature is
        // worthless to the round).
        match self.clock.admits_window(&proposition.time_window) {
            WindowAdmission::Inside => {}
            WindowAdmission::OutsideTimeWindow => return Err(crate::Error::OutsideTimeWindow),
        }
        let provisional_stamp = AttestedMoment::new(proposition.clone(), Vec::new());
        let operation_bytes =
            OperationStatement::new(&self.criome_identity, operation, &provisional_stamp)
                .to_signing_bytes()
                .map_err(|error| crate::Error::VoteSigning(error.to_string()))?;
        let moment_bytes = AttestedMomentStatement::new(proposition)
            .to_signing_bytes()
            .map_err(|error| crate::Error::VoteSigning(error.to_string()))?;
        Ok(QuorumVoteSignatures {
            operation_signature: SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: self.master_key.public_key(),
                bls_signature: self.master_key.sign(&operation_bytes),
            },
            time_signature: SignatureEnvelope {
                signature_scheme: SignatureScheme::Bls12_381MinPk,
                bls_public_key: self.master_key.public_key(),
                bls_signature: self.master_key.sign(&moment_bytes),
            },
        })
    }

    /// This node's Criome master public key.
    fn node_public_key(&self) -> BlsPublicKey {
        self.master_key.public_key()
    }

    /// Sign a root-founding statement as this node's willing establishment of the
    /// cohort. The master key signs the founding-statement preimage; the envelope
    /// is scheme-tagged (`Bls12_381MinPk`) with the master public key so a peer
    /// verifies it against the founding member's key. No clock gate — founding is
    /// not time-windowed — and no attestation record.
    fn sign_founding_statement(
        &self,
        statement: &RootFoundingStatement,
    ) -> crate::Result<SignatureEnvelope> {
        let signing_bytes = statement
            .signing_bytes()
            .map_err(|error| crate::Error::RootFounding(error.to_string()))?;
        Ok(SignatureEnvelope {
            signature_scheme: SignatureScheme::Bls12_381MinPk,
            bls_public_key: self.master_key.public_key(),
            bls_signature: self.master_key.sign(&signing_bytes),
        })
    }

    fn grant_stamp(&self, issued_at: TimestampNanos) -> AttestedMoment {
        AttestedMoment::new(
            AttestedMomentProposition::new(
                TimeWindow {
                    opens_at: issued_at,
                    closes_at: TimestampNanos::new(issued_at.into_u64().saturating_add(1)),
                },
                RequiredSignatureThreshold::new(0),
                Vec::new(),
            ),
            Vec::new(),
        )
    }

    async fn sign_as_receipt(&self, request: SignRequest) -> CriomeReply {
        match self.sign(request).await {
            CriomeReply::SignReceipt(receipt) => {
                CriomeReply::AttestationReceipt(AttestationReceipt::new(receipt.attestation))
            }
            other => other,
        }
    }

    async fn active_public_key(&self, identity: Identity) -> Option<signal_criome::BlsPublicKey> {
        self.registry
            .ask(registry::ResolveIdentity::new(identity))
            .await
            .ok()
            .and_then(|reply| reply.into_identity())
            .and_then(|identity: StoredIdentity| {
                if identity.status() == signal_criome::PrincipalStatus::Active {
                    Some(identity.public_key().clone())
                } else {
                    None
                }
            })
    }

    async fn record_attestation(&self, attestation: Attestation) -> crate::Result<()> {
        self.store
            .ask(store::StoreAttestation::new(attestation))
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(())
    }
}

impl Actor for AttestationSigner {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(
            arguments.registry,
            arguments.store,
            arguments.master_key,
            arguments.criome_identity,
            arguments.clock,
        ))
    }
}

impl Message<SignContent> for AttestationSigner {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: SignContent,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.sign(message.request).await)
    }
}

impl Message<AttestArchive> for AttestationSigner {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: AttestArchive,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.attest_archive(message.request).await)
    }
}

impl Message<AttestChannelGrant> for AttestationSigner {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: AttestChannelGrant,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.attest_channel_grant(message.request).await)
    }
}

impl Message<AttestAuthorization> for AttestationSigner {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: AttestAuthorization,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.attest_authorization(message).await)
    }
}

impl Message<SignAuthorizationGrant> for AttestationSigner {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: SignAuthorizationGrant,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.sign_authorization_grant(message).await)
    }
}

impl Message<SignQuorumVote> for AttestationSigner {
    type Reply = crate::Result<QuorumVoteSignatures>;

    async fn handle(
        &mut self,
        message: SignQuorumVote,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.sign_quorum_vote(&message.operation, &message.proposition)
    }
}

impl Message<ReadNodePublicKey> for AttestationSigner {
    type Reply = NodeMasterPublicKey;

    async fn handle(
        &mut self,
        _message: ReadNodePublicKey,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        NodeMasterPublicKey {
            public_key: self.node_public_key(),
        }
    }
}

impl Message<SignFoundingStatement> for AttestationSigner {
    type Reply = crate::Result<FoundingStatementSignature>;

    async fn handle(
        &mut self,
        message: SignFoundingStatement,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.sign_founding_statement(&message.statement)
            .map(|envelope| FoundingStatementSignature { envelope })
    }
}

// The wall clock now lives as `master_key::SystemClock`, shared by the signer
// (stamp issued_at) and the verifier (reject expired attestations).
