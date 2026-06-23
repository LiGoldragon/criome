use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    AuthorizationDenial, AuthorizationDenialReason, AuthorizationDenialSource, AuthorizationDenied,
    AuthorizationExpired, AuthorizationObservation, AuthorizationObservationRetracted,
    AuthorizationObservationSnapshot, AuthorizationObservationToken, AuthorizationPending,
    AuthorizationRejection, AuthorizationRequestSlot, AuthorizationStateRecord,
    AuthorizationStatus, AuthorizationVerification, CriomeReply, RejectionReason,
    SignalCallAuthorization, SignatureRouteReceipt, SignatureSolicitationRoute,
    SignatureSubmission, SignatureSubmissionReceipt, TimestampNanos,
};

use crate::actors::{CriomeActorReply, actor_reply, rejection, store};

pub struct AuthorizationCoordinator {
    store: ActorRef<store::StoreKernel>,
    clock: AuthorizationClock,
}

#[derive(Clone)]
pub struct Arguments {
    pub store: ActorRef<store::StoreKernel>,
}

pub struct AuthorizeSignalCall {
    authorization: SignalCallAuthorization,
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

impl AuthorizeSignalCall {
    pub fn new(authorization: SignalCallAuthorization) -> Self {
        Self { authorization }
    }
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
        Self {
            store,
            clock: AuthorizationClock::system(),
        }
    }

    async fn authorize_signal_call(&self, authorization: SignalCallAuthorization) -> CriomeReply {
        if let Some(expired_at) = authorization.expires_at()
            && self.clock.is_expired(expired_at)
        {
            return self.expire_authorization(authorization, expired_at).await;
        }

        let stored = match self
            .create_authorization_state(store::CreateAuthorizationState::signing(&authorization))
            .await
        {
            Ok(stored) => stored,
            Err(error) => return authorization_store_rejection(error),
        };
        let state = stored.into_state();
        let request_slot = state.request_slot.clone();
        CriomeReply::AuthorizationPending(AuthorizationPending::new(
            request_slot.clone(),
            authorization.request_digest,
            state.missing_authorities().to_vec(),
            AuthorizationObservationToken::new(request_slot),
        ))
    }

    async fn expire_authorization(
        &self,
        authorization: SignalCallAuthorization,
        expired_at: TimestampNanos,
    ) -> CriomeReply {
        let stored = match self
            .create_authorization_state(store::CreateAuthorizationState::expired(&authorization))
            .await
        {
            Ok(stored) => stored,
            Err(error) => return authorization_store_rejection(error),
        };
        CriomeReply::AuthorizationExpired(AuthorizationExpired {
            request_slot: stored.into_state().request_slot,
            expired_at,
        })
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
        if verification.request_digest == verification.authorization.authorized_object_digest {
            CriomeReply::AuthorizationGranted(verification.authorization)
        } else {
            CriomeReply::AuthorizationDenied(AuthorizationDenied {
                request_slot: verification.authorization.request_slot,
                denial: AuthorizationDenial {
                    source: AuthorizationDenialSource::Policy,
                    reason: AuthorizationDenialReason::RequestDigestMismatch,
                },
            })
        }
    }

    async fn route_signature_request(&self, route: SignatureSolicitationRoute) -> CriomeReply {
        let request_slot = route.solicitation.request_slot.clone();
        let routed_to = route.routed_to.clone();
        if self.store_signature_solicitation(route).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::SignatureRouteReceipt(SignatureRouteReceipt {
            request_slot,
            routed_to,
        })
    }

    async fn submit_signature(&self, submission: SignatureSubmission) -> CriomeReply {
        let request_slot = submission.request_slot.clone();
        let signer = submission.signer.clone();
        if self.store_signature_submission(submission).await.is_err() {
            return rejection(RejectionReason::MalformedRequest);
        }
        CriomeReply::SignatureSubmissionReceipt(SignatureSubmissionReceipt {
            request_slot,
            signer,
        })
    }

    async fn reject_authorization(&self, rejection: AuthorizationRejection) -> CriomeReply {
        let denial = AuthorizationDenial {
            source: AuthorizationDenialSource::Signers,
            reason: rejection.reason,
        };
        if let Ok(Some(stored)) = self
            .lookup_authorization_state(rejection.request_slot.clone())
            .await
        {
            let state = stored.into_state();
            let missing_authorities = state.missing_authorities().to_vec();
            let grant = state.grant().cloned();
            let parked_evaluation = state.parked_evaluation().cloned();
            let signal_authorization = state.signal_authorization().cloned();
            let mut state = AuthorizationStateRecord::new(
                state.request_slot,
                state.request_digest,
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
            request_slot: rejection.request_slot,
            denial,
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

    async fn create_authorization_state(
        &self,
        state: store::CreateAuthorizationState,
    ) -> crate::Result<crate::tables::StoredAuthorizationState> {
        let reply = self
            .store
            .ask(state)
            .await
            .map_err(|error| crate::Error::ActorCall(error.to_string()))?;
        reply.into_result()
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

fn authorization_store_rejection(error: crate::Error) -> CriomeReply {
    match error {
        crate::Error::AuthorizationReplayAttempted => rejection(RejectionReason::ReplayAttempted),
        _error => rejection(RejectionReason::MalformedRequest),
    }
}

#[derive(Debug, Clone, Copy)]
struct AuthorizationClock {
    epoch: std::time::SystemTime,
}

impl AuthorizationClock {
    fn system() -> Self {
        Self {
            epoch: std::time::UNIX_EPOCH,
        }
    }

    fn is_expired(&self, expires_at: TimestampNanos) -> bool {
        expires_at.into_u64() <= self.now().into_u64()
    }

    fn now(&self) -> TimestampNanos {
        let nanos = std::time::SystemTime::now()
            .duration_since(self.epoch)
            .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        TimestampNanos::new(nanos)
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

impl Message<AuthorizeSignalCall> for AuthorizationCoordinator {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: AuthorizeSignalCall,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.authorize_signal_call(message.authorization).await)
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
