use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    ArchiveAttestationRequest, Attestation, AttestationReceipt, AuditContext, BlsSignature,
    ChannelGrantAttestationRequest, ContentPurpose, ContentReference, CriomeReply, Identity,
    RejectionReason, SignReceipt, SignRequest, SignatureEnvelope, SignatureScheme,
};

use crate::actors::{CriomeActorReply, actor_reply, registry, rejection, store};
use crate::master_key::{AttestationPreimage, MasterKey, SystemClock};
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

impl AttestationSigner {
    fn new(
        registry: ActorRef<registry::IdentityRegistry>,
        store: ActorRef<store::StoreKernel>,
        master_key: MasterKey,
        criome_identity: Identity,
    ) -> Self {
        Self {
            registry,
            store,
            master_key,
            criome_identity,
            clock: SystemClock::system(),
        }
    }

    async fn sign(&self, request: SignRequest) -> CriomeReply {
        // Gate: only a known, active identity may request an attestation. criome
        // then signs as itself with its master key (self-owned policy); the
        // requester and the kernel-vouched caller live in the audit context.
        if self
            .active_public_key(request.signer.clone())
            .await
            .is_none()
        {
            return rejection(RejectionReason::UnknownIdentity);
        }
        let issued_at = self.clock.timestamp();
        let mut attestation = Attestation {
            content: request.content,
            signer: self.criome_identity.clone(),
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key: self.master_key.public_key(),
                signature: BlsSignature::new(String::new()),
            },
            issued_at,
            expires_at: request.expires_at,
            audit_context: request.audit_context,
        };
        // Sign the full attestation statement (everything but the signature),
        // then fill in the real signature.
        let signing_bytes = AttestationPreimage::from_attestation(&attestation).to_signing_bytes();
        attestation.envelope.signature = self.master_key.sign(&signing_bytes);
        if self.record_attestation(attestation.clone()).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::SignReceipt(SignReceipt {
            attestation,
            issued_at,
        })
    }

    async fn attest_archive(&self, request: ArchiveAttestationRequest) -> CriomeReply {
        let sign = SignRequest {
            content: ContentReference {
                digest: request.release.artifact,
                purpose: ContentPurpose::Archive,
                schema_version: request.release.component,
            },
            signer: request.release.authorized_by,
            audit_context: request.audit_context,
            expires_at: None,
        };
        self.sign_as_receipt(sign).await
    }

    async fn attest_channel_grant(&self, request: ChannelGrantAttestationRequest) -> CriomeReply {
        let sign = SignRequest {
            content: request.grant_content,
            signer: request.source,
            audit_context: request.audit_context,
            expires_at: None,
        };
        self.sign_as_receipt(sign).await
    }

    async fn attest_authorization(&self, request: AttestAuthorization) -> CriomeReply {
        let sign = SignRequest {
            content: request.content,
            signer: request.source,
            audit_context: request.audit_context,
            expires_at: None,
        };
        self.sign_as_receipt(sign).await
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

// The wall clock now lives as `master_key::SystemClock`, shared by the signer
// (stamp issued_at) and the verifier (reject expired attestations).
