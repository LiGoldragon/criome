use nota_codec::{Decoder, Encoder, NotaDecode, NotaEncode};
use signal_criome::{CriomeReply, CriomeRequest};

use crate::{Error, Result};

pub struct RequestDocument {
    request: CriomeRequest,
}

impl RequestDocument {
    pub fn parse(source: &str) -> Result<Self> {
        let mut decoder = Decoder::new(source);
        let request = CriomeRequest::decode(&mut decoder)?;
        if decoder.peek_token()?.is_some() {
            return Err(Error::TooManyRequestRecords);
        }
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
        let mut encoder = Encoder::new();
        self.reply.encode(&mut encoder)?;
        Ok(encoder.into_string())
    }
}
