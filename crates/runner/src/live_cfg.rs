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
}

/// Optional PnL-guard wiring. A missing section leaves the runner
/// without a guard; a present section with both limits `None` is a
/// no-op (guard constructed but nothing to trip on). Mantissas match
/// [`ts_pnl::Accountant`]: `price_scale * qty_scale`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct RiskCfg {
    /// Drop from peak equity (realized-net + unrealized) that trips
    /// [`ts_risk::TripReason::MaxDrawdown`].
    #[serde(default)]
    pub max_drawdown: Option<i64>,
    /// Realized-net loss since day-start that trips
    /// [`ts_risk::TripReason::DailyLoss`].
    #[serde(default)]
    pub max_daily_loss: Option<i64>,
    /// Sliding-window length for the daily baseline, in seconds.
    /// Defaults to 24 h.
    #[serde(default = "default_day_length_secs")]
    pub day_length_secs: u64,
}

fn default_day_length_secs() -> u64 {
    24 * 60 * 60
}

/// Optional kill-switch wiring for the live runner. A missing section
/// leaves the runner with no switch attached; an empty section enables
/// defaults (reject-rate trigger only, no halt-file watcher).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct KillSwitchCfg {
    /// Filesystem path polled by the halt-file watcher. Creating the
    /// file (e.g. `touch /tmp/ts-halt`) trips the switch. Omit to leave
    /// manual halt unmonitored.
    #[serde(default)]
    pub halt_file: Option<PathBuf>,
    /// Rejects inside `window_ms` that force the switch to trip.
    #[serde(default = "default_reject_threshold")]
    pub reject_threshold: u32,
    /// Sliding reject-window horizon, in milliseconds.
    #[serde(default = "default_window_ms")]
    pub window_ms: u64,
    /// Halt-file polling cadence, in milliseconds.
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

impl Default for KillSwitchCfg {
    fn default() -> Self {
        Self {
            halt_file: None,
            reject_threshold: default_reject_threshold(),
            window_ms: default_window_ms(),
            poll_ms: default_poll_ms(),
        }
    }
}

fn default_reject_threshold() -> u32 {
    10
}
fn default_window_ms() -> u64 {
    5_000
}
fn default_poll_ms() -> u64 {
    250
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
