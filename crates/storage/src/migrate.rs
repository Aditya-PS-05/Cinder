//! Backend-agnostic migration loading.
//!
//! Migration files live in a directory and follow the filename grammar
//! `V<digits>__<name>.sql` (e.g. `V001__create_orders.sql`). The loader
//! reads each file, hashes its contents with SHA-256, and returns the
//! migrations sorted by version. Duplicate versions are rejected here so
//! backend runners can assume well-formed input.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::error::StorageError;

#[derive(Debug, Clone)]
pub struct Migration {
    pub version: i64,
    pub name: String,
    pub sql: String,
    pub sha256: String,
}

/// Read every `*.sql` in `dir`, parse `V<version>__<name>.sql` filenames,
/// and return them sorted by version. Non-`.sql` files are ignored.
pub async fn load_from_dir(dir: &Path) -> Result<Vec<Migration>, StorageError> {
    let mut rd = tokio::fs::read_dir(dir)
        .await
        .map_err(|source| StorageError::Io {
            path: dir.display().to_string(),
            source,
        })?;

    let mut out: Vec<Migration> = Vec::new();
    while let Some(entry) = rd.next_entry().await.map_err(|source| StorageError::Io {
        path: dir.display().to_string(),
        source,
    })? {
        let path: PathBuf = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("sql") {
            continue;
        }
        let fname = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_string();
        let (version, name) =
            parse_filename(&fname).ok_or_else(|| StorageError::BadFilename(fname.clone()))?;
        let sql = tokio::fs::read_to_string(&path)
            .await
            .map_err(|source| StorageError::Io {
                path: path.display().to_string(),
                source,
            })?;
        let sha256 = hash(&sql);
        out.push(Migration {
            version,
            name,
            sql,
            sha256,
        });
    }
    out.sort_by_key(|m| m.version);

    for w in out.windows(2) {
        if w[0].version == w[1].version {
            return Err(StorageError::BadFilename(format!(
                "duplicate version {} ({} and {})",
                w[0].version, w[0].name, w[1].name
            )));
        }
    }
    Ok(out)
}

fn hash(sql: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(sql.as_bytes());
    hex::encode(hasher.finalize())
}

fn parse_filename(name: &str) -> Option<(i64, String)> {
    let stem = name.strip_suffix(".sql")?;
    let rest = stem.strip_prefix('V')?;
    let sep = rest.find("__")?;
    let (vs, tail) = rest.split_at(sep);
    let version: i64 = vs.parse().ok()?;
    let title = tail.trim_start_matches("__").to_string();
    if title.is_empty() {
        return None;
    }
    Some((version, title))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_filename_happy() {
        assert_eq!(
            parse_filename("V001__create_orders.sql"),
            Some((1, "create_orders".to_string()))
        );
        assert_eq!(parse_filename("V42__ok.sql"), Some((42, "ok".to_string())));
    }

    #[test]
    fn parse_filename_rejects_bad() {
        assert_eq!(parse_filename("create.sql"), None);
        assert_eq!(parse_filename("V__x.sql"), None);
        assert_eq!(parse_filename("V1_nope.sql"), None);
        assert_eq!(parse_filename("Vabc__x.sql"), None);
        assert_eq!(parse_filename("V1__.sql"), None);
    }

    #[test]
    fn hash_is_deterministic() {
        assert_eq!(hash("SELECT 1"), hash("SELECT 1"));
        assert_ne!(hash("SELECT 1"), hash("SELECT 2"));
    }
}
