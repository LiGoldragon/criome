use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    ArchiveAttestationRequest, Attestation, AttestationReceipt, AuditContext, BlsSignature,
    ChannelGrantAttestationRequest, ContentPurpose, ContentReference, CriomeReply, Identity,
    RejectionReason, SignReceipt, SignRequest, SignatureEnvelope, SignatureScheme, TimestampNanos,
};

use crate::actors::{CriomeActorReply, actor_reply, registry, rejection, store};
use crate::tables::StoredIdentity;

pub struct AttestationSigner {
    registry: ActorRef<registry::IdentityRegistry>,
    store: ActorRef<store::StoreKernel>,
    clock: SignerClock,
}

#[derive(Clone)]
pub struct Arguments {
    pub registry: ActorRef<registry::IdentityRegistry>,
    pub store: ActorRef<store::StoreKernel>,
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
    ) -> Self {
        Self {
            registry,
            store,
            clock: SignerClock::system(),
        }
    }

    async fn sign(&self, request: SignRequest) -> CriomeReply {
        let Some(public_key) = self.active_public_key(request.signer.clone()).await else {
            return rejection(RejectionReason::UnknownIdentity);
        };
        let issued_at = self.clock.timestamp();
        let attestation = Attestation {
            content: request.content,
            signer: request.signer,
            envelope: SignatureEnvelope {
                scheme: SignatureScheme::Bls12_381MinPk,
                public_key,
                signature: BlsSignature::new("criome-skeleton-bls-signature".to_string()),
            },
            issued_at,
            expires_at: request.expires_at,
            audit_context: request.audit_context,
        };
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
        Ok(Self::new(arguments.registry, arguments.store))
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

struct SignerClock {
    epoch: std::time::SystemTime,
}

impl SignerClock {
    fn system() -> Self {
        Self {
            epoch: std::time::UNIX_EPOCH,
        }
    }

    fn timestamp(&self) -> TimestampNanos {
        let nanos = std::time::SystemTime::now()
            .duration_since(self.epoch)
            .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        TimestampNanos::new(nanos)
    }
}
