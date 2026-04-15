//! ClickHouse migration runner.
//!
//! ClickHouse has no transactions, so we cannot atomically apply a file
//! and its `schema_migrations` insert. We mitigate two ways: migration
//! files use `CREATE TABLE IF NOT EXISTS` (idempotent re-runs), and the
//! runner inserts the bookkeeping row immediately after a successful
//! file application. A crash in between means the next run will re-apply
//! the file — and because the DDL is idempotent, that's safe.

use tracing::info;

use super::ClickHouse;
use crate::error::StorageError;
use crate::migrate::Migration;

const SCHEMA_MIGRATIONS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS schema_migrations
(
    version    Int64,
    name       String,
    sha256     String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = MergeTree
ORDER BY version
"#;

pub async fn apply_all(
    ch: &ClickHouse,
    migrations: &[Migration],
) -> Result<Vec<i64>, StorageError> {
    ch.execute(SCHEMA_MIGRATIONS_DDL).await?;

    let tsv = ch
        .query("SELECT version, sha256 FROM schema_migrations ORDER BY version FORMAT TSV")
        .await?;
    let applied: Vec<(i64, String)> = tsv
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let mut parts = line.splitn(2, '\t');
            let v: i64 = parts.next()?.parse().ok()?;
            let s = parts.next()?.to_string();
            Some((v, s))
        })
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

        info!(version = m.version, name = %m.name, "applying clickhouse migration");
        ch.execute(&m.sql).await?;

        let version_str = m.version.to_string();
        ch.execute_with_params(
            "INSERT INTO schema_migrations (version, name, sha256) \
             VALUES ({v:Int64}, {n:String}, {s:String})",
            &[("v", &version_str), ("n", &m.name), ("s", &m.sha256)],
        )
        .await?;

        newly_applied.push(m.version);
    }

    Ok(newly_applied)
}

pub async fn status(ch: &ClickHouse) -> Result<Vec<(i64, String)>, StorageError> {
    ch.execute(SCHEMA_MIGRATIONS_DDL).await?;
    let tsv = ch
        .query("SELECT version, name FROM schema_migrations ORDER BY version FORMAT TSV")
        .await?;
    Ok(tsv
        .lines()
        .filter(|l| !l.is_empty())
        .filter_map(|line| {
            let mut parts = line.splitn(2, '\t');
            let v: i64 = parts.next()?.parse().ok()?;
            let n = parts.next()?.to_string();
            Some((v, n))
        })
        .collect())
}
