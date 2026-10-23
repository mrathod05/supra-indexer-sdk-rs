//! Reproducible performance harness for `supra_indexer_sdk`.
//!
//! ```bash
//! # release build is mandatory; run from the repository root
//! cargo run --release -p supra_indexer_sdk --example perf_bench -- all
//! cargo run --release -p supra_indexer_sdk --example perf_bench -- s2 s3 --quick
//! ```
//!
//! Scenarios: s1 s2 s3 s4 s5 s6 s7 (or `all`), plus `live` (real Supra RPC,
//! needs network; env `SUPRA_RPC_URL`, `LIVE_SPAN_BLOCKS`). Results land in
//! `docs/perf/results/<scenario>.{json,md}`.
//!
//! Env: `PERF_DATABASE_URL` (default a dedicated `supra_indexer_sdk_perf`
//! DB). The harness refuses any database whose name lacks `perf` or `test`,
//! because every scenario TRUNCATEs its tables.

mod fixtures;
mod live;
mod scenarios;
mod stats;
mod stub_rpc;

use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result, bail};
use diesel::{Connection, PgConnection};

use stats::{
    HostInfo, ResourceDelta, ResourceProbe, ResourceSnapshot, free_disk_bytes, peak_rss_kb,
};

const DEFAULT_DB_URL: &str = "postgres://postgres:postgres@localhost/supra_indexer_sdk_perf";
const RESULTS_DIR: &str = "docs/perf/results";
/// Abort a run before it can push the host disk to full.
const MIN_FREE_DISK: u64 = 1_500_000_000;

pub struct Ctx {
    pub db_url: String,
    pub quick: bool,
    probe: ResourceProbe,
    /// Out-of-band connection for resets and resource snapshots, so scenario
    /// pools measure only the workload.
    admin: Mutex<PgConnection>,
}

impl Ctx {
    pub fn reset(&self) -> Result<()> {
        ensure_disk()?;
        fixtures::reset(&mut self.admin.lock().unwrap())
    }

    pub fn rows(&self) -> Result<i64> {
        fixtures::row_count(&mut self.admin.lock().unwrap())
    }

    pub fn snapshot(&self) -> Result<ResourceSnapshot> {
        self.probe.snapshot(&mut self.admin.lock().unwrap())
    }

    pub fn delta(&self, start: ResourceSnapshot) -> Result<ResourceDelta> {
        self.probe.delta(&mut self.admin.lock().unwrap(), start)
    }
}

fn ensure_disk() -> Result<()> {
    let free = free_disk_bytes();
    if free < MIN_FREE_DISK {
        bail!("only {free} bytes free on / — aborting to avoid filling the disk");
    }
    Ok(())
}

const ALL: [&str; 7] = ["s1", "s2", "s3", "s4", "s5", "s6", "s7"];

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let quick = args.iter().any(|a| a == "--quick");
    let mut chosen: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|a| !a.starts_with("--"))
        .collect();
    if chosen.is_empty() || chosen.contains(&"all") {
        chosen = ALL.to_vec();
    }
    if let Some(bad) = chosen.iter().find(|s| !ALL.contains(s) && **s != "live") {
        bail!("unknown scenario '{bad}', expected one of {ALL:?}, 'live' or 'all'");
    }
    if cfg!(debug_assertions) {
        eprintln!("WARNING: debug build — numbers are meaningless; use --release");
    }

    let db_url = std::env::var("PERF_DATABASE_URL").unwrap_or_else(|_| DEFAULT_DB_URL.to_string());
    let db_name = db_url.rsplit('/').next().unwrap_or_default();
    if !(db_name.contains("perf") || db_name.contains("test")) {
        bail!("refusing to run against '{db_name}': benchmarks TRUNCATE their tables");
    }
    let mut admin =
        PgConnection::establish(&db_url).with_context(|| format!("connecting to {db_name}"))?;
    fixtures::create_tables(&mut admin)?;
    let host = HostInfo::collect(&mut admin)?;
    let ctx = Ctx {
        db_url,
        quick,
        probe: ResourceProbe::new(),
        admin: Mutex::new(admin),
    };
    let dir = Path::new(RESULTS_DIR);

    for scenario in chosen {
        println!("== {scenario}{} ==", if quick { " (quick)" } else { "" });
        let mut report = match scenario {
            "s1" => scenarios::s1_decode(&ctx).await?,
            "s2" => scenarios::s2_page_store(&ctx).await?,
            "s3" => scenarios::s3_latency(&ctx).await?,
            "s4" => scenarios::s4_multi_event(&ctx).await?,
            "s5" => scenarios::s5_sparse(&ctx).await?,
            "s6" => scenarios::s6_rpc_client(&ctx).await?,
            "s7" => scenarios::s7_recovery(&ctx).await?,
            "live" => live::live(&ctx).await?,
            other => bail!("unhandled scenario {other}"),
        };
        report.note(format!(
            "Bench process peak RSS so far: {} MiB.",
            peak_rss_kb() / 1024
        ));
        if quick {
            report.note("QUICK MODE — reduced sizes; not for the report.");
        }
        report.write(dir, &host)?;
    }
    ctx.reset()?;
    Ok(())
}
