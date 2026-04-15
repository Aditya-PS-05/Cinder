//! Config structs the storage crate deserializes from the app config tree.
//!
//! Field names mirror `config/base.yaml` so a service can simply embed
//! `PostgresCfg` / `ClickHouseCfg` inside its own config struct and let
//! `ts-config` populate them.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresCfg {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    /// Upper bound on open connections in the pool.
    #[serde(default = "default_pg_pool")]
    pub max_open_conns: usize,
}

fn default_pg_pool() -> usize {
    16
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClickHouseCfg {
    /// Base HTTP URL without trailing slash, e.g. `http://localhost:8123`.
    pub http_url: String,
    pub database: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub password: String,
}
