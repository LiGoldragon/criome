//! Step 6 — cascade. Re-derive any rule-derived records whose
//! premises were touched by the write. Fire subscriptions.
//! Cycle bound emits `E9999` diagnostic without rejecting the
//! originating mutation.

use crate::Result;

pub fn settle() -> Result<()> {
    todo!("cascade derived records; fire subscriptions; emit E9999 on cycle bound")
}
