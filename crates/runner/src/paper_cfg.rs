//! Static configuration for the `ts-paper-run` binary.
//!
//! The struct layout mirrors the runtime knobs the binary exposes on the
//! command line: market identity, maker parameters, runner plumbing, and
//! an optional audit sink. Loading is delegated to [`ts_config::Loader`]
//! so the binary gets layered `base.yaml` + env overlay + `TS_*`
//! environment-variable overrides for free.
//!
//! CLI flags remain the outermost layer — any flag explicitly provided
//! on the command line wins over a value loaded from YAML.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use ts_config::{ConfigError, Env, Loader};

/// Root config tree.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PaperCfg {
    pub market: MarketCfg,
    pub maker: MakerCfg,
    pub runner: RunnerCfg,
    #[serde(default)]
    pub audit: Option<AuditCfg>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MarketCfg {
    pub symbol: String,
    pub price_scale: u8,
    pub qty_scale: u8,
    pub ws_url: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct MakerCfg {
    pub quote_qty: i64,
    pub half_spread_ticks: i64,
    pub inventory_skew_ticks: i64,
    pub max_inventory: i64,
    pub cid_prefix: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct RunnerCfg {
    pub summary_secs: u64,
    pub channel: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AuditCfg {
    pub path: PathBuf,
}

impl Default for MarketCfg {
    fn default() -> Self {
        Self {
            symbol: "BTCUSDT".into(),
            price_scale: 2,
            qty_scale: 8,
            ws_url: "wss://stream.binance.com:9443/ws".into(),
        }
    }
}

impl Default for MakerCfg {
    fn default() -> Self {
        Self {
            quote_qty: 2,
            half_spread_ticks: 5,
            inventory_skew_ticks: 1,
            max_inventory: 20,
            cid_prefix: "pr".into(),
        }
    }
}

impl Default for RunnerCfg {
    fn default() -> Self {
        Self {
            summary_secs: 5,
            channel: 4096,
        }
    }
}

impl PaperCfg {
    /// Load `<dir>/base.yaml` + `<dir>/<env>.yaml` through [`ts_config`].
    pub fn load(dir: impl Into<PathBuf>, env: Env) -> Result<Self, ConfigError> {
        Loader::new(dir, env).load::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmpdir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let p =
            std::env::temp_dir().join(format!("ts-paper-cfg-{}-{}-{}", std::process::id(), tag, n));
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn write(path: &Path, body: &str) {
        fs::write(path, body).unwrap();
    }

    #[test]
    fn default_is_self_consistent() {
        let cfg = PaperCfg::default();
        assert_eq!(cfg.market.symbol, "BTCUSDT");
        assert_eq!(cfg.runner.channel, 4096);
        assert!(cfg.audit.is_none());
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
  cid_prefix: pr
runner:
  summary_secs: 5
  channel: 4096
"#,
        );
        write(
            &dir.join("prod.yaml"),
            r#"
market:
  ws_url: wss://stream.binance.com:443/ws
maker:
  quote_qty: 3
  cid_prefix: live
"#,
        );

        let cfg = PaperCfg::load(&dir, Env::Prod).unwrap();
        assert_eq!(cfg.market.symbol, "BTCUSDT");
        assert_eq!(cfg.market.ws_url, "wss://stream.binance.com:443/ws");
        assert_eq!(cfg.maker.quote_qty, 3);
        assert_eq!(cfg.maker.cid_prefix, "live");
        assert_eq!(cfg.maker.half_spread_ticks, 5, "base preserved");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn audit_sink_round_trips() {
        let dir = tmpdir("audit");
        write(
            &dir.join("base.yaml"),
            r#"
market:
  symbol: ETHUSDT
  price_scale: 2
  qty_scale: 8
  ws_url: wss://x
maker:
  quote_qty: 1
  half_spread_ticks: 4
  inventory_skew_ticks: 0
  max_inventory: 10
  cid_prefix: t
runner:
  summary_secs: 0
  channel: 128
audit:
  path: /tmp/out.ndjson
"#,
        );
        write(&dir.join("dev.yaml"), "{}\n");

        let cfg = PaperCfg::load(&dir, Env::Dev).unwrap();
        assert_eq!(
            cfg.audit.as_ref().map(|a| a.path.as_path()),
            Some(std::path::Path::new("/tmp/out.ndjson"))
        );
        fs::remove_dir_all(&dir).ok();
    }
}
