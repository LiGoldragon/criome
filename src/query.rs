//! Query handler — scans sema, filters records by their kind
//! tag (one-byte discriminator from [`crate::kinds`]), decodes
//! each matching record, filters by the query's `PatternField`s,
//! returns a typed `Records` reply.
//!
//! The kind-tag prefix is M0's solution for single-table
//! storage; per-kind tables in sema (M1+) replace the tag
//! check with table selection. The matcher logic stays the
//! same.

use std::sync::Arc;

use sema::Sema;
use signal::{
    Edge, EdgeQuery, Graph, GraphQuery, KindDecl, KindDeclQuery, Node, NodeQuery, PatternField,
    QueryOperation, Records, Reply,
};

use crate::kinds;

pub fn handle(operation: QueryOperation, sema: &Arc<Sema>) -> Reply {
    let records = match operation {
        QueryOperation::Node(query) => Records::Node(find_nodes(sema, &query)),
        QueryOperation::Edge(query) => Records::Edge(find_edges(sema, &query)),
        QueryOperation::Graph(query) => Records::Graph(find_graphs(sema, &query)),
        QueryOperation::KindDecl(query) => Records::KindDecl(find_kind_decls(sema, &query)),
    };
    Reply::Records(records)
}

// ─── Per-kind matchers ──────────────────────────────────────

fn find_nodes(sema: &Sema, query: &NodeQuery) -> Vec<Node> {
    decode_kind::<Node>(sema, kinds::NODE)
        .into_iter()
        .filter(|node| matches_pattern_field(&node.name, &query.name))
        .collect()
}

fn find_edges(sema: &Sema, query: &EdgeQuery) -> Vec<Edge> {
    decode_kind::<Edge>(sema, kinds::EDGE)
        .into_iter()
        .filter(|edge| {
            matches_pattern_field(&edge.from, &query.from)
                && matches_pattern_field(&edge.to, &query.to)
                && matches_pattern_field(&edge.kind, &query.kind)
        })
        .collect()
}

fn find_graphs(sema: &Sema, query: &GraphQuery) -> Vec<Graph> {
    decode_kind::<Graph>(sema, kinds::GRAPH)
        .into_iter()
        .filter(|graph| matches_pattern_field(&graph.title, &query.title))
        .collect()
}

fn find_kind_decls(sema: &Sema, query: &KindDeclQuery) -> Vec<KindDecl> {
    decode_kind::<KindDecl>(sema, kinds::KIND_DECL)
        .into_iter()
        .filter(|kind_decl| matches_pattern_field(&kind_decl.name, &query.name))
        .collect()
}

// ─── Kind-tag-filtered iteration ────────────────────────────

/// Iterate sema; for each record whose first byte matches
/// `expected_tag`, decode the rest as `T` and yield it.
/// Records of other kinds are skipped without decoding.
fn decode_kind<T>(sema: &Sema, expected_tag: u8) -> Vec<T>
where
    T: rkyv::Archive,
    T::Archived: for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    let mut decoded = Vec::new();
    for (_slot, bytes) in sema.iter().unwrap_or_default() {
        if bytes.first().copied() != Some(expected_tag) {
            continue;
        }
        if let Ok(value) = rkyv::from_bytes::<T, rkyv::rancor::Error>(&bytes[1..]) {
            decoded.push(value);
        }
    }
    decoded
}

// ─── PatternField matching ──────────────────────────────────

/// `Wildcard` and `Bind` both match anything. `Match(literal)`
/// matches iff the value equals the literal. (M0 doesn't
/// return bind captures — the reply is just the typed records;
/// bind-capture values land at M1+ when the result projection
/// surface grows.)
fn matches_pattern_field<T: PartialEq>(value: &T, pattern: &PatternField<T>) -> bool {
    match pattern {
        PatternField::Wildcard | PatternField::Bind => true,
        PatternField::Match(literal) => value == literal,
    }
}
