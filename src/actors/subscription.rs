use kameo::actor::{Actor, ActorRef};
use kameo::error::Infallible;
use kameo::message::{Context, Message};
use signal_criome::{
    AuthorizedObjectUpdate, AuthorizedObjectUpdateRetracted, AuthorizedObjectUpdateSnapshot,
    AuthorizedObjectUpdateToken, CriomeReply, IdentitySubscriptionToken, RejectionReason,
    SubscriptionRetracted,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, kameo::Reply)]
pub struct AuthorizedObjectPublication {
    subscriber_count: usize,
}

impl SubscriptionRegistry {
    fn new(registry: ActorRef<registry::IdentityRegistry>) -> Self {
        Self {
            registry,
            identity_subscriptions: Vec::new(),
            authorized_object_subscriptions: Vec::new(),
            authorized_object_updates: Vec::new(),
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
        if !self.authorized_object_subscriptions.contains(&token) {
            self.authorized_object_subscriptions.push(token);
        }
        CriomeReply::AuthorizedObjectUpdateSnapshot(AuthorizedObjectUpdateSnapshot::new(
            self.authorized_object_updates.clone(),
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
        AuthorizedObjectPublication {
            subscriber_count: self.authorized_object_subscriptions.len(),
        }
    }
}

impl PublishAuthorizedObjectUpdate {
    pub fn new(update: AuthorizedObjectUpdate) -> Self {
        Self { update }
    }
}

impl AuthorizedObjectPublication {
    pub const fn subscriber_count(self) -> usize {
        self.subscriber_count
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
