use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    AuthorizationDenial, AuthorizationDenialReason, AuthorizationDenialSource, AuthorizationDenied,
    AuthorizationObservation, AuthorizationObservationRetracted, AuthorizationObservationSnapshot,
    AuthorizationObservationToken, AuthorizationRejection, AuthorizationRequestSlot,
    AuthorizationStateRecord, AuthorizationStatus, AuthorizationVerification, CriomeReply,
    RejectionReason, SignatureRouteReceipt, SignatureSolicitationRoute, SignatureSubmission,
    SignatureSubmissionReceipt,
};

use crate::actors::{CriomeActorReply, actor_reply, rejection, store};

pub struct AuthorizationCoordinator {
    store: ActorRef<store::StoreKernel>,
}

#[derive(Clone)]
pub struct Arguments {
    pub store: ActorRef<store::StoreKernel>,
}

pub struct ObserveAuthorization {
    observation: AuthorizationObservation,
}

pub struct VerifyAuthorization {
    verification: AuthorizationVerification,
}

pub struct RouteSignatureRequest {
    route: SignatureSolicitationRoute,
}

pub struct SubmitSignature {
    submission: SignatureSubmission,
}

pub struct RejectAuthorization {
    rejection: AuthorizationRejection,
}

pub struct CloseAuthorizationObservation {
    token: AuthorizationObservationToken,
}

impl ObserveAuthorization {
    pub fn new(observation: AuthorizationObservation) -> Self {
        Self { observation }
    }
}

impl VerifyAuthorization {
    pub fn new(verification: AuthorizationVerification) -> Self {
        Self { verification }
    }
}

impl RouteSignatureRequest {
    pub fn new(route: SignatureSolicitationRoute) -> Self {
        Self { route }
    }
}

impl SubmitSignature {
    pub fn new(submission: SignatureSubmission) -> Self {
        Self { submission }
    }
}

impl RejectAuthorization {
    pub fn new(rejection: AuthorizationRejection) -> Self {
        Self { rejection }
    }
}

impl CloseAuthorizationObservation {
    pub fn new(token: AuthorizationObservationToken) -> Self {
        Self { token }
    }
}

impl AuthorizationCoordinator {
    fn new(store: ActorRef<store::StoreKernel>) -> Self {
        Self { store }
    }

    async fn observe_authorization(&self, observation: AuthorizationObservation) -> CriomeReply {
        let state = self
            .lookup_authorization_state(observation.into_payload())
            .await
            .ok()
            .flatten()
            .map(|stored| stored.into_state())
            .into_iter()
            .collect();
        CriomeReply::AuthorizationObservationSnapshot(
            AuthorizationObservationSnapshot::from_states(state),
        )
    }

    async fn verify_authorization(&self, verification: AuthorizationVerification) -> CriomeReply {
        if &verification.object_digest
            == verification.authorization_grant.authorized_object_digest()
        {
            CriomeReply::AuthorizationGranted(verification.authorization_grant)
        } else {
            CriomeReply::AuthorizationDenied(AuthorizationDenied {
                authorization_request_slot: verification
                    .authorization_grant
                    .authorization_request_slot,
                authorization_denial: AuthorizationDenial {
                    authorization_denial_source: AuthorizationDenialSource::Policy,
                    authorization_denial_reason: AuthorizationDenialReason::RequestDigestMismatch,
                },
            })
        }
    }

    async fn route_signature_request(&self, route: SignatureSolicitationRoute) -> CriomeReply {
        let request_slot = route
            .signature_solicitation
            .authorization_request_slot
            .clone();
        let routed_to = route.identity.clone();
        if self.store_signature_solicitation(route).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::SignatureRouteReceipt(SignatureRouteReceipt {
            authorization_request_slot: request_slot,
            identity: routed_to,
        })
    }

    async fn submit_signature(&self, submission: SignatureSubmission) -> CriomeReply {
        let request_slot = submission.authorization_request_slot.clone();
        let signer = submission.identity.clone();
        if self.store_signature_submission(submission).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::SignatureSubmissionReceipt(SignatureSubmissionReceipt {
            authorization_request_slot: request_slot,
            identity: signer,
        })
    }

    async fn reject_authorization(&self, rejection: AuthorizationRejection) -> CriomeReply {
        let denial = AuthorizationDenial {
            authorization_denial_source: AuthorizationDenialSource::Signers,
            authorization_denial_reason: rejection.authorization_denial_reason,
        };
        if let Ok(Some(stored)) = self
            .lookup_authorization_state(rejection.authorization_request_slot.clone())
            .await
        {
            let state = stored.into_state();
            let missing_authorities = state.missing_authorities().to_vec();
            let grant = state.optional_authorization_grant().cloned();
            let parked_evaluation = state.parked_evaluation().cloned();
            let signal_authorization = state.optional_signal_call_authorization().cloned();
            let mut state = AuthorizationStateRecord::new(
                state.authorization_request_slot,
                state.object_digest,
                AuthorizationStatus::Denied,
                missing_authorities,
                grant,
                Some(denial.clone()),
            );
            if let Some(evaluation) = parked_evaluation {
                state = state.with_parked_evaluation(evaluation);
            }
            if let Some(authorization) = signal_authorization {
                state = state.with_signal_authorization(authorization);
            }
            let _ = self.store_authorization_state(state).await;
        }
        CriomeReply::AuthorizationDenied(AuthorizationDenied {
            authorization_request_slot: rejection.authorization_request_slot,
            authorization_denial: denial,
        })
    }

    async fn close_observation(&self, token: AuthorizationObservationToken) -> CriomeReply {
        CriomeReply::AuthorizationObservationRetracted(AuthorizationObservationRetracted::new(
            token,
        ))
    }

    async fn store_authorization_state(
        &self,
        state: AuthorizationStateRecord,
    ) -> crate::Result<crate::tables::StoredAuthorizationState> {
        let reply = self
            .store
            .ask(store::StoreAuthorizationState::new(state))
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(reply.into_state())
    }

    async fn lookup_authorization_state(
        &self,
        request_slot: AuthorizationRequestSlot,
    ) -> crate::Result<Option<crate::tables::StoredAuthorizationState>> {
        let reply = self
            .store
            .ask(store::LookupAuthorizationState::new(request_slot))
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(reply.into_state())
    }

    async fn store_signature_solicitation(
        &self,
        route: SignatureSolicitationRoute,
    ) -> crate::Result<crate::tables::StoredSignatureSolicitation> {
        let reply = self
            .store
            .ask(store::StoreSignatureSolicitation::new(route))
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(reply.into_solicitation())
    }

    async fn store_signature_submission(
        &self,
        submission: SignatureSubmission,
    ) -> crate::Result<crate::tables::StoredSignatureSubmission> {
        let reply = self
            .store
            .ask(store::StoreSignatureSubmission::new(submission))
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        Ok(reply.into_submission())
    }
}

impl Actor for AuthorizationCoordinator {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(arguments.store))
    }
}

impl Message<ObserveAuthorization> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: ObserveAuthorization,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.observe_authorization(message.observation).await)
    }
}

impl Message<VerifyAuthorization> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: VerifyAuthorization,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.verify_authorization(message.verification).await)
    }
}

impl Message<RouteSignatureRequest> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: RouteSignatureRequest,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.route_signature_request(message.route).await)
    }
}

impl Message<SubmitSignature> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: SubmitSignature,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.submit_signature(message.submission).await)
    }
}

impl Message<RejectAuthorization> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: RejectAuthorization,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.reject_authorization(message.rejection).await)
    }
}

impl Message<CloseAuthorizationObservation> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: CloseAuthorizationObservation,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.close_observation(message.token).await)
    }
}
