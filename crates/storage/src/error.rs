//! Unified error type for the storage crate.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("postgres: {0}")]
    Postgres(#[from] tokio_postgres::Error),

    #[error("postgres pool: {0}")]
    PostgresPool(String),

    #[error("postgres pool build: {0}")]
    PostgresBuild(String),

    #[error("clickhouse http {status}: {body}")]
    ClickHouse { status: u16, body: String },

    #[error("http transport: {0}")]
    Http(#[from] reqwest::Error),

    #[error("migration {version} checksum drift (on-disk {disk}, recorded {recorded})")]
    ChecksumDrift {
        version: i64,
        disk: String,
        recorded: String,
    },

    #[error("migration {version} would apply out of order (last applied {last})")]
    OutOfOrder { version: i64, last: i64 },

    #[error("migration filename invalid: {0}")]
    BadFilename(String),

    #[error("io {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
}
