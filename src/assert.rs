//! Assert handler — rkyv-encode the inner record, prepend its
//! kind tag, and store in sema. Returns `Outcome(Ok)` on
//! success, a typed `Diagnostic` on encode or store failure.

use std::sync::Arc;

use sema::Sema;
use signal::{
    AssertOperation, Diagnostic, DiagnosticLevel, Ok as OkRecord, OutcomeMessage, Reply,
};

use crate::kinds;

pub fn handle(operation: AssertOperation, sema: &Arc<Sema>) -> Reply {
    let tagged_bytes_result: Result<Vec<u8>, String> = match &operation {
        AssertOperation::Node(value) => prepend_tag(kinds::NODE, value),
        AssertOperation::Edge(value) => prepend_tag(kinds::EDGE, value),
        AssertOperation::Graph(value) => prepend_tag(kinds::GRAPH, value),
        AssertOperation::KindDecl(value) => prepend_tag(kinds::KIND_DECL, value),
    };

    match tagged_bytes_result {
        Ok(bytes) => match sema.store(&bytes) {
            Ok(_slot) => Reply::Outcome(OutcomeMessage::Ok(OkRecord::default())),
            Err(error) => Reply::Outcome(OutcomeMessage::Diagnostic(diagnostic(
                "E0500",
                format!("sema write failed: {error}"),
            ))),
        },
        Err(error) => Reply::Outcome(OutcomeMessage::Diagnostic(diagnostic(
            "E0501",
            format!("rkyv encode failed: {error}"),
        ))),
    }
}

/// Encode `value` to rkyv bytes and prepend the one-byte kind
/// tag. Layout: `[tag, ..rkyv_archive..]`. Read-side mirror in
/// [`crate::query`].
fn prepend_tag<T>(tag: u8, value: &T) -> Result<Vec<u8>, String>
where
    T: for<'a> rkyv::Serialize<
        rkyv::api::high::HighSerializer<
            rkyv::util::AlignedVec,
            rkyv::ser::allocator::ArenaHandle<'a>,
            rkyv::rancor::Error,
        >,
    >,
{
    let archive = rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?;
    let mut tagged = Vec::with_capacity(archive.len() + 1);
    tagged.push(tag);
    tagged.extend_from_slice(&archive);
    Ok(tagged)
}

fn diagnostic(code: &str, message: String) -> Diagnostic {
    Diagnostic {
        level: DiagnosticLevel::Error,
        code: code.to_string(),
        message,
        primary_site: None,
        context: vec![],
        suggestions: vec![],
        durable_record: None,
    }
}
