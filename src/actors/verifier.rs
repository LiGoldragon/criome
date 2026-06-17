use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    CriomeReply, PrincipalStatus, SignatureScheme, VerificationDecision, VerificationResult,
    VerifyRequest,
};

use crate::actors::{CriomeActorReply, actor_reply, registry};
use crate::master_key::{AttestationPreimage, SystemClock, VerifyBls};

pub struct AttestationVerifier {
    registry: ActorRef<registry::IdentityRegistry>,
    clock: SystemClock,
}

#[derive(Clone)]
pub struct Arguments {
    pub registry: ActorRef<registry::IdentityRegistry>,
}

pub struct VerifyAttestation {
    request: VerifyRequest,
}

impl VerifyAttestation {
    pub fn new(request: VerifyRequest) -> Self {
        Self { request }
    }
}

impl AttestationVerifier {
    fn new(registry: ActorRef<registry::IdentityRegistry>) -> Self {
        Self {
            registry,
            clock: SystemClock::system(),
        }
    }

    async fn verify(&self, request: VerifyRequest) -> CriomeReply {
        if request.attestation.content != request.content {
            return self.result(VerificationDecision::InvalidSignature, None, None);
        }

        let signer = request.attestation.signer.clone();
        let stored = self
            .registry
            .ask(registry::ResolveIdentity::new(signer.clone()))
            .await
            .ok()
            .and_then(|reply| reply.into_identity());

        let Some(identity) = stored else {
            return self.result(VerificationDecision::UnknownSigner, None, None);
        };
        if identity.status() == PrincipalStatus::Revoked {
            return self.result(VerificationDecision::Revoked, Some(signer), None);
        }
        if identity.public_key() != &request.attestation.envelope.public_key {
            return self.result(VerificationDecision::InvalidSignature, Some(signer), None);
        }

        // Only the implemented scheme is accepted; an envelope claiming another
        // scheme is rejected, never parsed as min-pk bytes (algorithm confusion).
        match request.attestation.envelope.scheme {
            SignatureScheme::Bls12_381MinPk => {}
            SignatureScheme::Bls12_381MinSig => {
                return self.result(
                    VerificationDecision::InvalidSignature,
                    Some(signer),
                    request.attestation.expires_at,
                );
            }
        }

        let signing_bytes =
            AttestationPreimage::from_attestation(&request.attestation).to_signing_bytes();
        if !request
            .attestation
            .envelope
            .public_key
            .verify_bls(&request.attestation.envelope.signature, &signing_bytes)
        {
            return self.result(
                VerificationDecision::InvalidSignature,
                Some(signer),
                request.attestation.expires_at,
            );
        }

        // A validly-signed but past-expiry attestation is Expired, not Valid.
        if request
            .attestation
            .expires_at
            .is_some_and(|deadline| self.clock.is_past(&deadline))
        {
            return self.result(
                VerificationDecision::Expired,
                Some(signer),
                request.attestation.expires_at,
            );
        }

        self.result(
            VerificationDecision::Valid,
            Some(signer),
            request.attestation.expires_at,
        )
    }

    fn result(
        &self,
        decision: VerificationDecision,
        identity: Option<signal_criome::Identity>,
        expires_at: Option<signal_criome::TimestampNanos>,
    ) -> CriomeReply {
        CriomeReply::VerificationResult(VerificationResult {
            decision,
            identity,
            expires_at,
        })
    }
}

impl Actor for AttestationVerifier {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(arguments.registry))
    }
}

impl Message<VerifyAttestation> for AttestationVerifier {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: VerifyAttestation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.verify(message.request).await)
    }
}
