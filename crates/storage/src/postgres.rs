//! Postgres connection pool built on deadpool + tokio-postgres.
//!
//! `Postgres::connect` dials the configured host and validates the pool
//! by fetching one client and issuing `SELECT 1` so startup fails fast
//! rather than handing out a broken pool to request handlers.

pub mod migrate;

use deadpool_postgres::{Config as DpCfg, Pool, PoolConfig, Runtime};
use tokio_postgres::NoTls;

use crate::config::PostgresCfg;
use crate::error::StorageError;

pub struct Postgres {
    pool: Pool,
}

impl Postgres {
    pub async fn connect(cfg: &PostgresCfg) -> Result<Self, StorageError> {
        let mut dp = DpCfg::new();
        dp.host = Some(cfg.host.clone());
        dp.port = Some(cfg.port);
        dp.user = Some(cfg.user.clone());
        dp.password = Some(cfg.password.clone());
        dp.dbname = Some(cfg.database.clone());
        dp.pool = Some(PoolConfig::new(cfg.max_open_conns));

        let pool = dp
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .map_err(|e| StorageError::PostgresBuild(e.to_string()))?;

        let client = pool
            .get()
            .await
            .map_err(|e| StorageError::PostgresPool(e.to_string()))?;
        client.simple_query("SELECT 1").await?;
        drop(client);

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &Pool {
        &self.pool
    }
}
