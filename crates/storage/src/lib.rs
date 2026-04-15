//! Async storage layer: connection wrappers and a small migration runner
//! for Postgres (system of record) and ClickHouse (tick archive).
//!
//! The migration runner tracks applied versions in a `schema_migrations`
//! table on each backend, rejects on-disk checksum drift, and refuses to
//! apply out-of-order files. Postgres migrations run in a transaction that
//! includes the `schema_migrations` insert so a crashed `apply_all` is
//! safe to re-run.

#![forbid(unsafe_code)]

pub mod clickhouse;
pub mod config;
pub mod error;
pub mod migrate;
pub mod postgres;

pub use clickhouse::ClickHouse;
pub use config::{ClickHouseCfg, PostgresCfg};
pub use error::StorageError;
pub use postgres::Postgres;
