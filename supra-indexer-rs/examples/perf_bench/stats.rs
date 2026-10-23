//! Latency summaries, result tables, and host/Postgres resource probes.

use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use diesel::prelude::*;
use diesel::sql_types::{BigInt, Text};
use serde::Serialize;
use serde_json::Value;

/// Nearest-rank percentile summary, in milliseconds.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct Summary {
    pub n: usize,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl Summary {
    pub fn from_samples(samples: &mut [Duration]) -> Self {
        let n = samples.len();
        if n == 0 {
            return Self::default();
        }
        samples.sort_unstable();
        let ms = |d: Duration| d.as_secs_f64() * 1e3;
        let pct = |p: f64| {
            let rank = ((p / 100.0) * n as f64).ceil() as usize;
            ms(samples[rank.clamp(1, n) - 1])
        };
        let total: f64 = samples.iter().copied().map(ms).sum();
        Self {
            n,
            avg_ms: total / n as f64,
            p50_ms: pct(50.0),
            p90_ms: pct(90.0),
            p95_ms: pct(95.0),
            p99_ms: pct(99.0),
            max_ms: ms(samples[n - 1]),
        }
    }
}

pub fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

pub fn per_sec(count: u64, wall: Duration) -> f64 {
    let secs = wall.as_secs_f64();
    if secs == 0.0 {
        0.0
    } else {
        (count as f64 / secs).round()
    }
}

#[derive(Debug, Serialize)]
pub struct Table {
    pub title: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

impl Table {
    pub fn new(title: impl Into<String>, headers: &[&str]) -> Self {
        Self {
            title: title.into(),
            headers: headers.iter().map(|h| (*h).to_string()).collect(),
            rows: Vec::new(),
        }
    }

    pub fn push(&mut self, row: Vec<Value>) {
        assert_eq!(row.len(), self.headers.len(), "row width != headers");
        self.rows.push(row);
    }

    fn to_markdown(&self) -> String {
        let mut out = format!("### {}\n\n| {} |\n|", self.title, self.headers.join(" | "));
        out.push_str(&"---|".repeat(self.headers.len()));
        out.push('\n');
        for row in &self.rows {
            let cells: Vec<String> = row
                .iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();
            out.push_str(&format!("| {} |\n", cells.join(" | ")));
        }
        out
    }
}

/// Builds a table row from heterogeneous values.
#[macro_export]
macro_rules! row {
    ($($cell:expr),* $(,)?) => {
        vec![$(serde_json::Value::from($cell)),*]
    };
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub scenario: String,
    pub title: String,
    pub notes: Vec<String>,
    pub tables: Vec<Table>,
}

impl Report {
    pub fn new(scenario: &str, title: &str) -> Self {
        Self {
            scenario: scenario.to_string(),
            title: title.to_string(),
            notes: Vec::new(),
            tables: Vec::new(),
        }
    }

    pub fn note(&mut self, note: impl Into<String>) {
        self.notes.push(note.into());
    }

    pub fn write(&self, dir: &Path, host: &HostInfo) -> Result<()> {
        std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
        let json = serde_json::json!({ "host": host, "report": self });
        let json_path = dir.join(format!("{}.json", self.scenario));
        std::fs::write(&json_path, serde_json::to_string_pretty(&json)?)
            .with_context(|| format!("writing {}", json_path.display()))?;

        let mut md = format!(
            "## {} — {}\n\n_Host: {}_\n\n",
            self.scenario,
            self.title,
            host.one_line()
        );
        for note in &self.notes {
            md.push_str(&format!("- {note}\n"));
        }
        md.push('\n');
        for table in &self.tables {
            md.push_str(&table.to_markdown());
            md.push('\n');
        }
        let md_path = dir.join(format!("{}.md", self.scenario));
        std::fs::write(&md_path, md).with_context(|| format!("writing {}", md_path.display()))?;
        println!("wrote {} and {}", json_path.display(), md_path.display());
        Ok(())
    }
}

#[derive(QueryableByName)]
struct Setting {
    #[diesel(sql_type = Text)]
    setting: String,
}

#[derive(QueryableByName)]
struct Wal {
    #[diesel(sql_type = BigInt)]
    wal: i64,
}

fn show(conn: &mut PgConnection, name: &str) -> Result<String> {
    let s: Setting = diesel::sql_query(format!("SELECT current_setting('{name}') AS setting"))
        .get_result(conn)
        .with_context(|| format!("reading {name}"))?;
    Ok(s.setting)
}

#[derive(Debug, Serialize)]
pub struct HostInfo {
    pub cpu_model: String,
    pub logical_cpus: usize,
    pub mem_total_kb: u64,
    pub pg_version: String,
    pub pg_settings: Vec<(String, String)>,
    pub disk_rotational: String,
    pub timestamp_utc: String,
}

impl HostInfo {
    pub fn collect(conn: &mut PgConnection) -> Result<Self> {
        let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").unwrap_or_default();
        let cpu_model = cpuinfo
            .lines()
            .find_map(|l| l.strip_prefix("model name"))
            .map(|l| l.trim_start_matches([' ', '\t', ':']).to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let mem_total_kb = meminfo
            .lines()
            .find_map(|l| l.strip_prefix("MemTotal:"))
            .and_then(|l| l.trim().trim_end_matches(" kB").parse().ok())
            .unwrap_or(0);
        let pg_version = show(conn, "server_version")?;
        let mut pg_settings = Vec::new();
        for name in [
            "max_connections",
            "shared_buffers",
            "synchronous_commit",
            "fsync",
            "wal_level",
            "max_wal_size",
            "commit_delay",
        ] {
            pg_settings.push((name.to_string(), show(conn, name)?));
        }
        let timestamp_utc = Command::new("date")
            .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        Ok(Self {
            cpu_model,
            logical_cpus: std::thread::available_parallelism().map_or(0, |n| n.get()),
            mem_total_kb,
            pg_version,
            pg_settings,
            disk_rotational: std::fs::read_to_string("/sys/block/sda/queue/rotational")
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
            timestamp_utc,
        })
    }

    fn one_line(&self) -> String {
        let settings: Vec<String> = self
            .pg_settings
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        format!(
            "{} ×{} · {} GiB RAM · PostgreSQL {} ({}) · disk rotational={} · {}",
            self.cpu_model,
            self.logical_cpus,
            self.mem_total_kb / 1024 / 1024,
            self.pg_version,
            settings.join(", "),
            self.disk_rotational,
            self.timestamp_utc
        )
    }
}

/// CPU (bench process + every `postgres` process) and WAL position at one
/// instant. Bench-process CPU includes the in-process RPC stub.
#[derive(Debug, Clone, Copy)]
pub struct ResourceSnapshot {
    at: Instant,
    self_ticks: u64,
    pg_ticks: u64,
    wal_bytes: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ResourceDelta {
    pub wall_s: f64,
    /// Average cores busy in the bench process (SDK + stub RPC).
    pub self_cpu_cores: f64,
    /// Average cores busy across all postgres processes.
    pub pg_cpu_cores: f64,
    pub wal_bytes: i64,
}

pub struct ResourceProbe {
    clk_tck: f64,
}

impl ResourceProbe {
    pub fn new() -> Self {
        let clk_tck = Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(100.0);
        Self { clk_tck }
    }

    pub fn snapshot(&self, conn: &mut PgConnection) -> Result<ResourceSnapshot> {
        let wal: Wal =
            diesel::sql_query("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint AS wal")
                .get_result(conn)
                .context("reading WAL position")?;
        Ok(ResourceSnapshot {
            at: Instant::now(),
            self_ticks: proc_ticks("/proc/self/stat").unwrap_or(0),
            pg_ticks: postgres_ticks(),
            wal_bytes: wal.wal,
        })
    }

    pub fn delta(&self, conn: &mut PgConnection, start: ResourceSnapshot) -> Result<ResourceDelta> {
        let end = self.snapshot(conn)?;
        let wall_s = end.at.duration_since(start.at).as_secs_f64();
        let cores = |ticks: u64| {
            if wall_s == 0.0 {
                0.0
            } else {
                round3(ticks as f64 / self.clk_tck / wall_s)
            }
        };
        Ok(ResourceDelta {
            wall_s: round3(wall_s),
            self_cpu_cores: cores(end.self_ticks.saturating_sub(start.self_ticks)),
            pg_cpu_cores: cores(end.pg_ticks.saturating_sub(start.pg_ticks)),
            wal_bytes: end.wal_bytes - start.wal_bytes,
        })
    }
}

/// utime + stime from a `/proc/<pid>/stat` file.
pub fn proc_ticks(path: &str) -> Option<u64> {
    let stat = std::fs::read_to_string(path).ok()?;
    let after_comm = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // Field 3 (state) is fields[0], so utime (14) / stime (15) are [11] / [12].
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

fn postgres_ticks() -> u64 {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .bytes()
                .all(|b| b.is_ascii_digit())
        })
        .filter_map(|e| {
            let path = e.path().join("stat");
            let stat = std::fs::read_to_string(&path).ok()?;
            let comm = stat.get(stat.find('(')? + 1..stat.rfind(')')?)?;
            (comm == "postgres")
                .then(|| proc_ticks(path.to_str()?))
                .flatten()
        })
        .sum()
}

/// Peak resident set size of the bench process, in KiB.
pub fn peak_rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("VmHWM:"))
                .and_then(|l| l.trim().trim_end_matches(" kB").parse().ok())
        })
        .unwrap_or(0)
}

/// Free bytes on the filesystem holding `/`.
pub fn free_disk_bytes() -> u64 {
    Command::new("df")
        .args(["--output=avail", "-B1", "/"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.lines().nth(1).and_then(|l| l.trim().parse().ok()))
        .unwrap_or(u64::MAX)
}
