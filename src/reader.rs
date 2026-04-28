//! `Reader` actor — read side of criome's sema.
//!
//! Multiple `Reader` actors share the same `Arc<Sema>` and
//! answer `Query` messages concurrently via redb's MVCC. The
//! pool size comes from [`sema::Sema::reader_count`] at daemon
//! startup; each `Reader` actor is its own ractor mailbox so
//! a slow query on one reader doesn't block others.
//!
//! Read-only by construction — no message variant mutates
//! sema. Writes go through [`crate::engine::Engine`] instead.

use std::sync::Arc;

use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use sema::Sema;
use signal::{
    Edge, EdgeQuery, Graph, GraphQuery, Node, NodeQuery, PatternField, QueryOperation, Records,
};

use crate::kinds;

pub struct Reader;

pub struct State {
    sema: Arc<Sema>,
}

pub struct Arguments {
    pub sema: Arc<Sema>,
}

pub enum Message {
    Query {
        operation: QueryOperation,
        reply_port: RpcReplyPort<Records>,
    },
}

impl State {
    pub fn new(sema: Arc<Sema>) -> Self {
        Self { sema }
    }

    pub fn handle_query(&self, operation: QueryOperation) -> Records {
        match operation {
            QueryOperation::Node(query) => Records::Node(self.find_nodes(&query)),
            QueryOperation::Edge(query) => Records::Edge(self.find_edges(&query)),
            QueryOperation::Graph(query) => Records::Graph(self.find_graphs(&query)),
        }
    }

    fn find_nodes(&self, query: &NodeQuery) -> Vec<Node> {
        self.decode_kind::<Node>(kinds::NODE)
            .into_iter()
            .filter(|node| Self::matches_pattern_field(&node.name, &query.name))
            .collect()
    }

    fn find_edges(&self, query: &EdgeQuery) -> Vec<Edge> {
        self.decode_kind::<Edge>(kinds::EDGE)
            .into_iter()
            .filter(|edge| {
                Self::matches_pattern_field(&edge.from, &query.from)
                    && Self::matches_pattern_field(&edge.to, &query.to)
                    && Self::matches_pattern_field(&edge.kind, &query.kind)
            })
            .collect()
    }

    fn find_graphs(&self, query: &GraphQuery) -> Vec<Graph> {
        self.decode_kind::<Graph>(kinds::GRAPH)
            .into_iter()
            .filter(|graph| Self::matches_pattern_field(&graph.title, &query.title))
            .collect()
    }

    fn decode_kind<T>(&self, expected_tag: u8) -> Vec<T>
    where
        T: rkyv::Archive,
        T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
            + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
    {
        let mut decoded = Vec::new();
        for (_slot, bytes) in self.sema.iter().unwrap_or_default() {
            if bytes.first().copied() != Some(expected_tag) {
                continue;
            }
            if let Ok(value) = rkyv::from_bytes::<T, rkyv::rancor::Error>(&bytes[1..]) {
                decoded.push(value);
            }
        }
        decoded
    }

    fn matches_pattern_field<T: PartialEq>(value: &T, pattern: &PatternField<T>) -> bool {
        match pattern {
            PatternField::Wildcard | PatternField::Bind => true,
            PatternField::Match(literal) => value == literal,
        }
    }
}

#[ractor::async_trait]
impl Actor for Reader {
    type Msg = Message;
    type State = State;
    type Arguments = Arguments;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        arguments: Arguments,
    ) -> std::result::Result<Self::State, ActorProcessingErr> {
        Ok(State::new(arguments.sema))
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Message,
        state: &mut State,
    ) -> std::result::Result<(), ActorProcessingErr> {
        match message {
            Message::Query { operation, reply_port } => {
                let _ = reply_port.send(state.handle_query(operation));
            }
        }
        Ok(())
    }
}
