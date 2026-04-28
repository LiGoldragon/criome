//! Step 1 — schema-check. Verify the request's record matches
//! the kind's field shapes. M0 validates against the closed
//! Rust enum in [`signal`](https://github.com/LiGoldragon/signal)
//! (`AssertOperation` / `MutateOperation` / etc. are the
//! authoritative type system). When `prism` lands and projects
//! Rust from records, the schema-check moves to record-driven
//! validation; until then, the closed Rust enum is the truth.

use crate::Result;

pub fn check() -> Result<()> {
    todo!("schema-check; emit Diagnostic on mismatch")
}
