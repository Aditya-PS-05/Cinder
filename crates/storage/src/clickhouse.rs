//! Minimal ClickHouse HTTP client.
//!
//! ClickHouse has no native transactions, so this wrapper focuses on
//! three primitives the rest of the crate needs: fire a DDL/DML string,
//! run a `SELECT` and get the body back as text, and parameter-bind a
//! small INSERT via ClickHouse's `param_<name>` HTTP query-string syntax.

pub mod migrate;

use reqwest::Client;

use crate::config::ClickHouseCfg;
use crate::error::StorageError;

pub struct ClickHouse {
    http: Client,
    url: String,
    database: String,
    username: String,
    password: String,
}

impl ClickHouse {
    pub async fn connect(cfg: &ClickHouseCfg) -> Result<Self, StorageError> {
        let http = Client::builder()
            .pool_idle_timeout(std::time::Duration::from_secs(60))
            .build()?;
        let ch = Self {
            http,
            url: cfg.http_url.trim_end_matches('/').to_string(),
            database: cfg.database.clone(),
            username: cfg.username.clone(),
            password: cfg.password.clone(),
        };
        ch.ping().await?;
        Ok(ch)
    }

    pub async fn ping(&self) -> Result<(), StorageError> {
        let resp = self.http.get(format!("{}/ping", self.url)).send().await?;
        let status = resp.status().as_u16();
        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::ClickHouse { status, body });
        }
        Ok(())
    }

    /// Execute DDL or DML. `multiquery=1` lets a single file hold several
    /// `;`-separated statements; we keep it on so the runner can apply
    /// whatever the migration file contains without surprises.
    pub async fn execute(&self, sql: &str) -> Result<(), StorageError> {
        let query: Vec<(&str, &str)> =
            vec![("database", self.database.as_str()), ("multiquery", "1")];
        let mut req = self
            .http
            .post(&self.url)
            .query(&query)
            .body(sql.to_string());
        if !self.username.is_empty() {
            req = req.basic_auth(&self.username, Some(&self.password));
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::ClickHouse { status, body });
        }
        Ok(())
    }

    /// Execute a statement that uses `{name:Type}` parameter placeholders.
    /// Values are sent via `?param_<name>=...` which ClickHouse escapes
    /// server-side; avoids string interpolation.
    pub async fn execute_with_params(
        &self,
        sql: &str,
        params: &[(&str, &str)],
    ) -> Result<(), StorageError> {
        let mut query: Vec<(String, String)> =
            vec![("database".to_string(), self.database.clone())];
        for (k, v) in params {
            query.push((format!("param_{k}"), (*v).to_string()));
        }
        let mut req = self
            .http
            .post(&self.url)
            .query(&query)
            .body(sql.to_string());
        if !self.username.is_empty() {
            req = req.basic_auth(&self.username, Some(&self.password));
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(StorageError::ClickHouse { status, body });
        }
        Ok(())
    }

    /// Run a `SELECT` and return the raw response body. Callers are
    /// expected to append a `FORMAT ...` clause to the SQL.
    pub async fn query(&self, sql: &str) -> Result<String, StorageError> {
        let query: Vec<(&str, &str)> = vec![("database", self.database.as_str())];
        let mut req = self
            .http
            .post(&self.url)
            .query(&query)
            .body(sql.to_string());
        if !self.username.is_empty() {
            req = req.basic_auth(&self.username, Some(&self.password));
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        if status >= 400 {
            return Err(StorageError::ClickHouse { status, body });
        }
        Ok(body)
    }
}
