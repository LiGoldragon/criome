//! Kind discriminators — a one-byte tag prepended to every
//! record's rkyv bytes before storing in sema, so scan-and-
//! try-decode in [`crate::query`] can short-circuit on the
//! tag rather than relying on rkyv's bytecheck (which does
//! not detect type-punning between same-size archives).
//!
//! Per-kind tables in sema are the M1+ replacement; this
//! single-byte discriminator is the M0 stop-gap that keeps
//! the wire / sema schema simple while we have one shared
//! `RECORDS` table.
//!
//! **Adding a kind**: pick the next free `u8` and add a const
//! here, then add a match arm in [`crate::assert`] and a
//! filter in [`crate::query`]. Discriminator values are *not*
//! load-bearing across rebuilds — they are an internal
//! storage detail and can renumber freely.

pub const NODE: u8 = 1;
pub const EDGE: u8 = 2;
pub const GRAPH: u8 = 3;
pub const KIND_DECL: u8 = 4;
