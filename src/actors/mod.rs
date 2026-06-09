pub mod authorization;
pub mod registry;
pub mod root;
pub mod signer;
pub mod store;
pub mod subscription;
pub mod verifier;

use signal_criome::{CriomeReply, Rejection, RejectionReason};

#[derive(Debug, Clone, PartialEq, Eq, kameo::Reply)]
pub struct CriomeActorReply {
    reply: CriomeReply,
}

impl CriomeActorReply {
    pub fn new(reply: CriomeReply) -> Self {
        Self { reply }
    }

    pub fn into_reply(self) -> CriomeReply {
        self.reply
    }
}

pub fn rejection(reason: RejectionReason) -> CriomeReply {
    CriomeReply::Rejection(Rejection::new(reason))
}

pub fn actor_reply(reply: CriomeReply) -> CriomeActorReply {
    CriomeActorReply::new(reply)
}
