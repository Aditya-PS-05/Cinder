//! `ts-report` — summarize an NDJSON audit tape.
//!
//! Feed it the path written by `ts-paper-run --audit ...` or
//! `ts-live-run --audit ...` and it emits either a human-readable
//! table (default) or the full structured [`Report`] as JSON
//! (`--json`). No runtime — this is a read-once batch tool.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use ts_report::{format_text, read_audit_file};

#[derive(Parser, Debug)]
#[command(name = "ts-report", about = "Summarize an NDJSON audit tape")]
struct Cli {
    /// Path to an NDJSON audit log.
    path: PathBuf,

    /// Emit the report as JSON instead of the plain-text summary.
    #[arg(long)]
    json: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let (report, stats) = read_audit_file(&cli.path)
        .with_context(|| format!("read audit tape {}", cli.path.display()))?;

    if cli.json {
        let j = serde_json::to_string_pretty(&report).context("render report as JSON")?;
        println!("{j}");
    } else {
        print!("{}", format_text(&report, &stats));
    }
    Ok(())
}
