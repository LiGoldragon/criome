use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    CriomeReply, IdentitySubscriptionToken, RejectionReason, SubscriptionRetracted,
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
    open: Vec<IdentitySubscriptionToken>,
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

impl SubscriptionRegistry {
    fn new(registry: ActorRef<registry::IdentityRegistry>) -> Self {
        Self {
            registry,
            open: Vec::new(),
        }
    }

    async fn open_subscription(&mut self, token: IdentitySubscriptionToken) -> CriomeReply {
        if !self.open.contains(&token) {
            self.open.push(token);
        }
        match self.registry.ask(registry::ReadIdentitySnapshot).await {
            Ok(reply) => reply.into_reply(),
            Err(_error) => rejection(RejectionReason::MalformedRequest),
        }
    }

    async fn close_subscription(&mut self, token: IdentitySubscriptionToken) -> CriomeReply {
        match self.open.iter().position(|existing| existing == &token) {
            Some(index) => {
                self.open.remove(index);
                CriomeReply::SubscriptionRetracted(SubscriptionRetracted::new(token))
            }
            None => rejection(RejectionReason::UnknownIdentity),
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
