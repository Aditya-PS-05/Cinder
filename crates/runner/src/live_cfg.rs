//! Static configuration for the `ts-live-run` binary.
//!
//! Layered loading via [`ts_config::Loader`] — same YAML, env-var, and
//! CLI flow as [`crate::paper_cfg`]. The shape differs in one meaningful
//! way: live trading needs API credentials. Credentials are never
//! stored in YAML; the config carries environment-variable names
//! pointing at the key/secret, and the binary resolves them at startup
//! via [`std::env::var`].

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use ts_config::{ConfigError, Env, Loader};

use crate::paper_cfg::{AuditCfg, MakerCfg, MarketCfg, MetricsCfg, RunnerCfg};
pub use crate::paper_cfg::{KillSwitchCfg, RiskCfg};

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct LiveCfg {
    pub market: MarketCfg,
    pub maker: MakerCfg,
    pub runner: RunnerCfg,
    pub binance: BinanceCfg,
    #[serde(default)]
    pub audit: Option<AuditCfg>,
    #[serde(default)]
    pub metrics: Option<MetricsCfg>,
    #[serde(default)]
    pub kill_switch: Option<KillSwitchCfg>,
    #[serde(default)]
    pub risk: Option<RiskCfg>,
    /// Crash-safety intent WAL. When set, the live runner records every
    /// pre-trade-passed submit and every terminal completion to this
    /// file with an fsync, so a post-crash replay can surface dangling
    /// intents. Omit to disable the WAL (tests, paper-like dry runs).
    #[serde(default)]
    pub intent_log: Option<IntentLogCfg>,
}

/// Filesystem path of the intent WAL. One-field struct so future knobs
/// (fsync cadence, rotation) slot in without breaking YAML.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct IntentLogCfg {
    pub path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct BinanceCfg {
    /// REST base URL (e.g. mainnet or testnet). No trailing slash.
    pub rest_base: String,
    /// User-data-stream WebSocket base. Append `/<listenKey>`.
    pub user_ws_base: String,
    /// Environment variable that holds the Binance API key.
    pub api_key_env: String,
    /// Environment variable that holds the Binance API secret.
    pub api_secret_env: String,
    /// Reconcile cadence in milliseconds. The runner polls the engine
    /// at this interval so venue-side fills arrive bounded-latency
    /// even during quiet market periods.
    pub reconcile_ms: u64,
}

impl Default for BinanceCfg {
    fn default() -> Self {
        Self {
            rest_base: "https://testnet.binance.vision".into(),
            user_ws_base: "wss://testnet.binance.vision/ws".into(),
            api_key_env: "BINANCE_API_KEY".into(),
            api_secret_env: "BINANCE_API_SECRET".into(),
            reconcile_ms: 100,
        }
    }
}

impl LiveCfg {
    pub fn load(dir: impl Into<PathBuf>, env: Env) -> Result<Self, ConfigError> {
        Loader::new(dir, env).load::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmpdir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p =
            std::env::temp_dir().join(format!("ts-live-cfg-{}-{}-{}", std::process::id(), tag, n));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn write(path: &std::path::Path, body: &str) {
        fs::write(path, body).unwrap();
    }

    #[test]
    fn default_targets_testnet() {
        let cfg = LiveCfg::default();
        assert!(cfg.binance.rest_base.contains("testnet"));
        assert!(cfg.binance.user_ws_base.contains("testnet"));
        assert_eq!(cfg.binance.api_key_env, "BINANCE_API_KEY");
        assert_eq!(cfg.binance.reconcile_ms, 100);
    }

    #[test]
    fn loads_full_base_and_env_overlay() {
        let dir = tmpdir("load");
        write(
            &dir.join("base.yaml"),
            r#"
market:
  symbol: BTCUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://stream.binance.com:9443/ws
maker:
  quote_qty: 2
  half_spread_ticks: 5
  inventory_skew_ticks: 1
  max_inventory: 20
  cid_prefix: live
runner:
  summary_secs: 5
  channel: 4096
binance:
  rest_base: https://testnet.binance.vision
  user_ws_base: wss://testnet.binance.vision/ws
  api_key_env: BINANCE_API_KEY
  api_secret_env: BINANCE_API_SECRET
  reconcile_ms: 100
"#,
        );
        write(
            &dir.join("prod.yaml"),
            r#"
binance:
  rest_base: https://api.binance.com
  user_ws_base: wss://stream.binance.com:9443/ws
  reconcile_ms: 50
"#,
        );

        let cfg = LiveCfg::load(&dir, Env::Prod).unwrap();
        assert_eq!(cfg.binance.rest_base, "https://api.binance.com");
        assert_eq!(cfg.binance.reconcile_ms, 50);
        assert_eq!(cfg.binance.api_key_env, "BINANCE_API_KEY", "base preserved");

        fs::remove_dir_all(&dir).ok();
    }
}
