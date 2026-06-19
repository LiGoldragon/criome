use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    AttestedMoment, AuthorizedObjectInterest, AuthorizedObjectUpdate,
    AuthorizedObjectUpdateRetracted, AuthorizedObjectUpdateSnapshot, AuthorizedObjectUpdateToken,
    ContractTimeCheck, ContractTimeCheckScheduled, CriomeReply, DueContractChecksEvaluated,
    IdentitySubscriptionToken, RejectionReason, SubscriptionRetracted,
};

use crate::actors::{CriomeActorReply, actor_reply, registry, rejection};

/// Tracks the set of currently-open `IdentityUpdateStream` subscriptions
/// by their `IdentitySubscriptionToken`. The retraction handler at the
/// daemon root closes the open subscription by removing the token here.
///
/// The push primitive that emits `IdentityUpdate` events to active
/// subscribers is not implemented in this daemon yet; this registry
/// tracks subscription presence so retraction is meaningful even before
/// the push side lands.
pub struct SubscriptionRegistry {
    registry: ActorRef<registry::IdentityRegistry>,
    identity_subscriptions: Vec<IdentitySubscriptionToken>,
    authorized_object_subscriptions: Vec<AuthorizedObjectUpdateToken>,
    authorized_object_updates: Vec<AuthorizedObjectUpdate>,
    contract_time_checks: Vec<ContractTimeCheck>,
}

#[derive(Clone)]
pub struct Arguments {
    pub registry: ActorRef<registry::IdentityRegistry>,
}

pub struct OpenIdentitySubscription {
    pub token: IdentitySubscriptionToken,
}

pub struct CloseIdentitySubscription {
    pub token: IdentitySubscriptionToken,
}

pub struct OpenAuthorizedObjectSubscription {
    pub token: AuthorizedObjectUpdateToken,
}

pub struct CloseAuthorizedObjectSubscription {
    pub token: AuthorizedObjectUpdateToken,
}

pub struct PublishAuthorizedObjectUpdate {
    update: AuthorizedObjectUpdate,
}

pub struct ScheduleContractTimeCheck {
    check: ContractTimeCheck,
}

pub struct RunDueContractChecks {
    stamp: AttestedMoment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct AuthorizedObjectPublication;

trait AuthorizedObjectFilter {
    fn matches_update(&self, update: &AuthorizedObjectUpdate) -> bool;
}

impl SubscriptionRegistry {
    fn new(registry: ActorRef<registry::IdentityRegistry>) -> Self {
        Self {
            registry,
            identity_subscriptions: Vec::new(),
            authorized_object_subscriptions: Vec::new(),
            authorized_object_updates: Vec::new(),
            contract_time_checks: Vec::new(),
        }
    }

    async fn open_subscription(&mut self, token: IdentitySubscriptionToken) -> CriomeReply {
        if !self.identity_subscriptions.contains(&token) {
            self.identity_subscriptions.push(token);
        }
        match self.registry.ask(registry::ReadIdentitySnapshot).await {
            Ok(reply) => reply.into_reply(),
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn close_subscription(&mut self, token: IdentitySubscriptionToken) -> CriomeReply {
        match self
            .identity_subscriptions
            .iter()
            .position(|existing| existing == &token)
        {
            Some(index) => {
                self.identity_subscriptions.remove(index);
                CriomeReply::SubscriptionRetracted(SubscriptionRetracted::new(token))
            }
            None => rejection(RejectionReason::UnknownIdentity),
        }
    }

    fn open_authorized_object_subscription(
        &mut self,
        token: AuthorizedObjectUpdateToken,
    ) -> CriomeReply {
        let interest = token.interest.clone();
        if !self.authorized_object_subscriptions.contains(&token) {
            self.authorized_object_subscriptions.push(token);
        }
        CriomeReply::AuthorizedObjectUpdateSnapshot(AuthorizedObjectUpdateSnapshot::from_updates(
            self.authorized_object_updates
                .iter()
                .filter(|update| interest.matches_update(update))
                .cloned()
                .collect(),
        ))
    }

    fn close_authorized_object_subscription(
        &mut self,
        token: AuthorizedObjectUpdateToken,
    ) -> CriomeReply {
        match self
            .authorized_object_subscriptions
            .iter()
            .position(|existing| existing == &token)
        {
            Some(index) => {
                self.authorized_object_subscriptions.remove(index);
                CriomeReply::AuthorizedObjectUpdateRetracted(AuthorizedObjectUpdateRetracted::new(
                    token,
                ))
            }
            None => rejection(RejectionReason::UnknownIdentity),
        }
    }

    fn publish_authorized_object_update(
        &mut self,
        update: AuthorizedObjectUpdate,
    ) -> AuthorizedObjectPublication {
        self.authorized_object_updates.push(update);
        AuthorizedObjectPublication
    }

    fn schedule_contract_time_check(&mut self, check: ContractTimeCheck) -> CriomeReply {
        self.contract_time_checks.push(check.clone());
        CriomeReply::ContractTimeCheckScheduled(ContractTimeCheckScheduled::new(check))
    }

    fn run_due_contract_checks(&mut self, stamp: AttestedMoment) -> CriomeReply {
        let closed_at = *stamp.proposition.window.closes_at.payload();
        let (due, pending): (Vec<_>, Vec<_>) = self
            .contract_time_checks
            .drain(..)
            .partition(|check| *check.due_at.payload() <= closed_at);
        self.contract_time_checks = pending;

        let mut triggered = Vec::new();
        for check in due {
            if self
                .authorized_object_updates
                .iter()
                .any(|update| check.absent.matches_update(update))
            {
                continue;
            }

            triggered.push(AuthorizedObjectUpdate {
                object: check.result,
                contract: check.contract,
                decision: signal_criome::EvaluationDecision::Authorized,
                stamp: stamp.clone(),
            });
        }

        self.authorized_object_updates.extend(triggered.clone());
        CriomeReply::DueContractChecksEvaluated(DueContractChecksEvaluated::from_triggered(
            triggered,
        ))
    }
}

impl PublishAuthorizedObjectUpdate {
    pub fn new(update: AuthorizedObjectUpdate) -> Self {
        Self { update }
    }
}

impl ScheduleContractTimeCheck {
    pub fn new(check: ContractTimeCheck) -> Self {
        Self { check }
    }
}

impl RunDueContractChecks {
    pub fn new(stamp: AttestedMoment) -> Self {
        Self { stamp }
    }
}

impl AuthorizedObjectFilter for AuthorizedObjectInterest {
    fn matches_update(&self, update: &AuthorizedObjectUpdate) -> bool {
        match self {
            Self::AnyAuthorizedObject => true,
            Self::Component(component) => update.object.component == *component,
            Self::ObjectKind(kind) => update.object.kind == *kind,
            Self::ComponentObject(component_object) => {
                update.object.component == component_object.component
                    && update.object.kind == component_object.kind
            }
        }
    }
}

impl Actor for SubscriptionRegistry {
    type Args = Arguments;
    type Error = Infallible;

    async fn on_start(
        arguments: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(arguments.registry))
    }
}

impl Message<OpenIdentitySubscription> for SubscriptionRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: OpenIdentitySubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.open_subscription(message.token).await)
    }
}

impl Message<CloseIdentitySubscription> for SubscriptionRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: CloseIdentitySubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.close_subscription(message.token).await)
    }
}

impl Message<OpenAuthorizedObjectSubscription> for SubscriptionRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: OpenAuthorizedObjectSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.open_authorized_object_subscription(message.token))
    }
}

impl Message<CloseAuthorizedObjectSubscription> for SubscriptionRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: CloseAuthorizedObjectSubscription,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.close_authorized_object_subscription(message.token))
    }
}

impl Message<PublishAuthorizedObjectUpdate> for SubscriptionRegistry {
    type Reply = AuthorizedObjectPublication;

    async fn handle(
        &mut self,
        message: PublishAuthorizedObjectUpdate,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.publish_authorized_object_update(message.update)
    }
}

impl Message<ScheduleContractTimeCheck> for SubscriptionRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: ScheduleContractTimeCheck,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.schedule_contract_time_check(message.check))
    }
}

impl Message<RunDueContractChecks> for SubscriptionRegistry {
    type Reply = CriomeActorReply;

    async fn handle(
        &mut self,
        message: RunDueContractChecks,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        actor_reply(self.run_due_contract_checks(message.stamp))
    }
}
