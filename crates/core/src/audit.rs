//! On-disk audit event — the canonical NDJSON line shape used by the
//! runner's writer and post-hoc report consumers. Kept in `ts-core`
//! so both producers (live/paper runners) and consumers (reporter,
//! analytics) reference one authoritative schema.
//!
//! Gated behind the `serde` feature because audit logs exist only as
//! a wire format; the hot path moves [`ExecReport`] and [`Fill`]
//! directly without going through this wrapper.

use serde::{Deserialize, Serialize};

use crate::{ExecReport, Fill};

/// One line on an audit tape.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuditEvent {
    Report(ExecReport),
    Fill(Fill),
}
