//! Query handler — `impl Daemon { handle_query + per-kind finders + matcher }`.
//!
//! Scans sema, filters records by their kind tag (one-byte
//! discriminator from [`crate::kinds`]), decodes each matching
//! record, filters by the query's `PatternField`s, returns a
//! typed `Records` reply.
//!
//! The kind-tag prefix is M0's solution for single-table
//! storage; per-kind tables in sema (M1+) replace the tag check
//! with table selection. The matcher logic stays the same.

use signal::{
    Edge, EdgeQuery, Graph, GraphQuery, KindDecl, KindDeclQuery, Node, NodeQuery, PatternField,
    QueryOperation, Records, Reply,
};

use crate::daemon::Daemon;
use crate::kinds;

impl Daemon {
    pub(crate) fn handle_query(&self, operation: QueryOperation) -> Reply {
        let records = match operation {
            QueryOperation::Node(query) => Records::Node(self.find_nodes(&query)),
            QueryOperation::Edge(query) => Records::Edge(self.find_edges(&query)),
            QueryOperation::Graph(query) => Records::Graph(self.find_graphs(&query)),
            QueryOperation::KindDecl(query) => Records::KindDecl(self.find_kind_decls(&query)),
        };
        Reply::Records(records)
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

    fn find_kind_decls(&self, query: &KindDeclQuery) -> Vec<KindDecl> {
        self.decode_kind::<KindDecl>(kinds::KIND_DECL)
            .into_iter()
            .filter(|kind_decl| Self::matches_pattern_field(&kind_decl.name, &query.name))
            .collect()
    }

    /// Iterate sema; for each record whose first byte matches
    /// `expected_tag`, decode the rest as `T` and yield it.
    /// Records of other kinds are skipped without decoding.
    fn decode_kind<T>(&self, expected_tag: u8) -> Vec<T>
    where
        T: rkyv::Archive,
        T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
            + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
    {
        let mut decoded = Vec::new();
        for (_slot, bytes) in self.sema().iter().unwrap_or_default() {
            if bytes.first().copied() != Some(expected_tag) {
                continue;
            }
            if let Ok(value) = rkyv::from_bytes::<T, rkyv::rancor::Error>(&bytes[1..]) {
                decoded.push(value);
            }
        }
        decoded
    }

    /// `Wildcard` and `Bind` both match anything. `Match(literal)`
    /// matches iff the value equals the literal. (M0 doesn't
    /// return bind captures — the reply is just the typed
    /// records; bind-capture values land at M1+ when the result
    /// projection surface grows.)
    fn matches_pattern_field<T: PartialEq>(value: &T, pattern: &PatternField<T>) -> bool {
        match pattern {
            PatternField::Wildcard | PatternField::Bind => true,
            PatternField::Match(literal) => value == literal,
        }
    }
}
