//! Postgres migration runner.
//!
//! Each migration runs inside a transaction alongside its own insert into
//! `schema_migrations`. That makes `apply_all` crash-safe: a process that
//! dies mid-migration either commits everything (file + bookkeeping row)
//! or nothing, so a subsequent run is free to retry the same file.
//!
//! Migration files therefore must NOT contain their own `BEGIN` / `COMMIT`.

use tracing::info;

use super::Postgres;
use crate::error::StorageError;
use crate::migrate::Migration;

const SCHEMA_MIGRATIONS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    BIGINT PRIMARY KEY,
    name       TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
"#;

/// Apply every pending migration in order. Returns the versions that were
/// newly applied (existing ones are skipped idempotently).
pub async fn apply_all(pg: &Postgres, migrations: &[Migration]) -> Result<Vec<i64>, StorageError> {
    let mut client = pg
        .pool()
        .get()
        .await
        .map_err(|e| StorageError::PostgresPool(e.to_string()))?;
    client.batch_execute(SCHEMA_MIGRATIONS_DDL).await?;

    let rows = client
        .query(
            "SELECT version, sha256 FROM schema_migrations ORDER BY version",
            &[],
        )
        .await?;
    let applied: Vec<(i64, String)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1)))
        .collect();

    for (v, sha) in &applied {
        if let Some(m) = migrations.iter().find(|m| m.version == *v) {
            if &m.sha256 != sha {
                return Err(StorageError::ChecksumDrift {
                    version: *v,
                    disk: m.sha256.clone(),
                    recorded: sha.clone(),
                });
            }
        }
    }

    let last_applied = applied.iter().map(|(v, _)| *v).max().unwrap_or(0);
    let mut newly_applied: Vec<i64> = Vec::new();

    for m in migrations {
        if applied.iter().any(|(v, _)| *v == m.version) {
            continue;
        }
        if m.version <= last_applied {
            return Err(StorageError::OutOfOrder {
                version: m.version,
                last: last_applied,
            });
        }

        info!(version = m.version, name = %m.name, "applying postgres migration");
        let tx = client.transaction().await?;
        tx.batch_execute(&m.sql).await?;
        tx.execute(
            "INSERT INTO schema_migrations (version, name, sha256) VALUES ($1, $2, $3)",
            &[&m.version, &m.name, &m.sha256],
        )
        .await?;
        tx.commit().await?;
        newly_applied.push(m.version);
    }

    Ok(newly_applied)
}

/// List applied migrations as (version, name) pairs in apply order.
pub async fn status(pg: &Postgres) -> Result<Vec<(i64, String)>, StorageError> {
    let client = pg
        .pool()
        .get()
        .await
        .map_err(|e| StorageError::PostgresPool(e.to_string()))?;
    client.batch_execute(SCHEMA_MIGRATIONS_DDL).await?;
    let rows = client
        .query(
            "SELECT version, name FROM schema_migrations ORDER BY version",
            &[],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, String>(1)))
        .collect())
}
