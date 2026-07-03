use nota::{NotaEncode, NotaSource};
use signal_criome::{CriomeReply, CriomeRequest};

use crate::Result;

pub struct RequestDocument {
    request: CriomeRequest,
}

impl RequestDocument {
    pub fn parse(source: &str) -> Result<Self> {
        let request = NotaSource::new(source).parse::<CriomeRequest>()?;
        Ok(Self { request })
    }

    pub fn into_request(self) -> CriomeRequest {
        self.request
    }
}

pub struct ReplyDocument {
    reply: CriomeReply,
}

impl ReplyDocument {
    pub fn new(reply: CriomeReply) -> Self {
        Self { reply }
    }

    pub fn render(&self) -> Result<String> {
        Ok(self.reply.to_nota())
    }
}
