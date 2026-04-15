//! Layered YAML configuration loader.
//!
//! Every service loads `base.yaml`, merges one environment overlay
//! (`dev.yaml` / `staging.yaml` / `prod.yaml`), then lets process env vars
//! of the form `<PREFIX><UPPER_DOTTED_PATH>` override any scalar. Later
//! layers win on conflict.
//!
//! ```no_run
//! use ts_config::{Env, Loader};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct Cfg { service: Service }
//! #[derive(Deserialize)]
//! struct Service { name: String, port: u16 }
//!
//! let cfg: Cfg = Loader::new("./config", Env::Dev).load().unwrap();
//! println!("{} on :{}", cfg.service.name, cfg.service.port);
//! ```

#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde_yaml_ng::Value;
use thiserror::Error;

/// Deployment environment discriminator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Env {
    Dev,
    Staging,
    Prod,
}

impl Env {
    pub fn as_str(self) -> &'static str {
        match self {
            Env::Dev => "dev",
            Env::Staging => "staging",
            Env::Prod => "prod",
        }
    }

    pub fn parse(s: &str) -> Result<Self, ConfigError> {
        match s {
            "dev" => Ok(Env::Dev),
            "staging" => Ok(Env::Staging),
            "prod" => Ok(Env::Prod),
            other => Err(ConfigError::InvalidEnv(other.to_string())),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid env: {0}")]
    InvalidEnv(String),

    #[error("read {path}: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("parse {path}: {source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_yaml_ng::Error,
    },

    #[error("decode into target type: {0}")]
    Decode(#[source] serde_yaml_ng::Error),
}

/// Loader assembles a merged configuration tree from disk + environment.
#[derive(Debug, Clone)]
pub struct Loader {
    dir: PathBuf,
    env: Env,
    prefix: String,
}

impl Loader {
    /// Construct a loader pointing at a config directory. Prefix defaults
    /// to `TS_`.
    pub fn new(dir: impl Into<PathBuf>, env: Env) -> Self {
        Self {
            dir: dir.into(),
            env,
            prefix: "TS_".into(),
        }
    }

    /// Override the environment variable prefix. Useful for tests.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Read `base.yaml` + `<env>.yaml`, apply env overrides, and decode
    /// into the target type.
    pub fn load<T: DeserializeOwned>(&self) -> Result<T, ConfigError> {
        let base = read_yaml(&self.dir.join("base.yaml"))?;
        let overlay_name = format!("{}.yaml", self.env.as_str());
        let overlay = read_yaml(&self.dir.join(&overlay_name))?;

        let mut merged = deep_merge(base, overlay);
        apply_env_overrides(&mut merged, &self.prefix);

        serde_yaml_ng::from_value(merged).map_err(ConfigError::Decode)
    }
}

fn read_yaml(path: &Path) -> Result<Value, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return Ok(Value::Mapping(Default::default()));
    }
    serde_yaml_ng::from_str(&raw).map_err(|source| ConfigError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

/// Returns `dst` with `src` layered on top. Maps recurse; every other
/// YAML node is replaced wholesale by `src`.
fn deep_merge(dst: Value, src: Value) -> Value {
    match (dst, src) {
        (Value::Mapping(mut dm), Value::Mapping(sm)) => {
            for (k, sv) in sm {
                let merged = match dm.remove(&k) {
                    Some(dv) => deep_merge(dv, sv),
                    None => sv,
                };
                dm.insert(k, merged);
            }
            Value::Mapping(dm)
        }
        (_, src) => src,
    }
}

/// Walks the tree and replaces scalar leaves where a matching env var
/// exists. Path `a.b.c` becomes `<prefix>A_B_C`. Values are routed through
/// the YAML scalar grammar so "7777" becomes an int, "true" a bool, etc.
fn apply_env_overrides(node: &mut Value, prefix: &str) {
    walk("", node, prefix);
}

fn walk(path: &str, node: &mut Value, prefix: &str) {
    let Value::Mapping(map) = node else {
        return;
    };
    // Snapshot keys so the mutable iteration that follows can touch values
    // without aliasing the map.
    let keys: Vec<String> = map
        .iter()
        .filter_map(|(k, _)| k.as_str().map(|s| s.to_string()))
        .collect();

    for k in keys {
        let child_path = if path.is_empty() {
            k.clone()
        } else {
            format!("{path}.{k}")
        };
        let key_value = Value::String(k.clone());
        let Some(child) = map.get_mut(&key_value) else {
            continue;
        };

        if matches!(child, Value::Mapping(_)) {
            walk(&child_path, child, prefix);
            continue;
        }

        let env_key = format!("{prefix}{}", child_path.replace('.', "_").to_uppercase());
        if let Ok(raw) = std::env::var(&env_key) {
            *child = parse_scalar(&raw);
        }
    }
}

/// parse_scalar routes an env-var string through the YAML scalar grammar so
/// callers transparently get int/float/bool/string coercion.
fn parse_scalar(s: &str) -> Value {
    serde_yaml_ng::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::fs;

    #[derive(Debug, Deserialize)]
    struct TestCfg {
        service: Service,
        postgres: Postgres,
    }

    #[derive(Debug, Deserialize)]
    struct Service {
        name: String,
        port: u16,
    }

    #[derive(Debug, Deserialize)]
    struct Postgres {
        host: String,
        db: String,
    }

    fn tmp_write(dir: &Path, name: &str, body: &str) {
        fs::write(dir.join(name), body).unwrap();
    }

    #[test]
    fn base_only() {
        let dir = tempdir();
        tmp_write(
            dir.path(),
            "base.yaml",
            r#"
service:
  name: market-data
  port: 8080
postgres:
  host: localhost
  db: trading
"#,
        );
        tmp_write(dir.path(), "dev.yaml", "{}\n");

        let cfg: TestCfg = Loader::new(dir.path(), Env::Dev).load().unwrap();
        assert_eq!(cfg.service.name, "market-data");
        assert_eq!(cfg.service.port, 8080);
        assert_eq!(cfg.postgres.host, "localhost");
    }

    #[test]
    fn overlay_wins() {
        let dir = tempdir();
        tmp_write(
            dir.path(),
            "base.yaml",
            r#"
service:
  name: market-data
  port: 8080
postgres:
  host: localhost
  db: trading
"#,
        );
        tmp_write(
            dir.path(),
            "prod.yaml",
            r#"
service:
  port: 9090
postgres:
  host: pg.prod.internal
"#,
        );

        let cfg: TestCfg = Loader::new(dir.path(), Env::Prod).load().unwrap();
        assert_eq!(cfg.service.port, 9090, "overlay port");
        assert_eq!(cfg.service.name, "market-data", "base name preserved");
        assert_eq!(cfg.postgres.host, "pg.prod.internal", "overlay host");
        assert_eq!(cfg.postgres.db, "trading", "base db preserved");
    }

    #[test]
    fn env_override_coerces_scalar() {
        let dir = tempdir();
        tmp_write(
            dir.path(),
            "base.yaml",
            r#"
service:
  name: market-data
  port: 8080
postgres:
  host: localhost
  db: trading
"#,
        );
        tmp_write(dir.path(), "dev.yaml", "{}\n");

        // Unique prefix per test so concurrent tests don't race on env vars.
        let prefix = "TEST_OVERRIDE_";
        // Scoped env setter that clears on drop would be safer; std doesn't
        // ship one, so we set + unset inline.
        std::env::set_var(format!("{prefix}SERVICE_PORT"), "7777");
        let cfg: TestCfg = Loader::new(dir.path(), Env::Dev)
            .with_prefix(prefix)
            .load()
            .unwrap();
        std::env::remove_var(format!("{prefix}SERVICE_PORT"));

        assert_eq!(cfg.service.port, 7777);
    }

    #[test]
    fn env_parse_rejects_unknown() {
        assert!(Env::parse("nope").is_err());
        assert_eq!(Env::parse("dev").unwrap(), Env::Dev);
    }

    // --- tiny tempdir helper so we don't pull in the tempfile crate for
    // a single use case ---

    struct Tmp(PathBuf);
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    impl Tmp {
        fn path(&self) -> &Path {
            &self.0
        }
    }
    fn tempdir() -> Tmp {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!("ts-config-test-{}-{}", std::process::id(), n));
        fs::create_dir_all(&p).unwrap();
        Tmp(p)
    }
}
