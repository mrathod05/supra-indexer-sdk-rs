//! Benchmark scenarios. Every SDK number comes from the real `SupraIndexer`
//! (or the public `SupraClient` / response types) driven against the stub RPC.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use supra_indexer_sdk::rpc::{EventQuery, PageStart, SupraClient};
use supra_indexer_sdk::{
    EventsResponse, IndexerConfig, IndexerStatus, Mode, SupraEvent, SupraIndexer, create_pool,
};

use crate::Ctx;
use crate::fixtures::{
    self, BenchStore, CONTRACT, StoreKind, TokenPayload, TradePayload, event_names, qualified,
};
use crate::row;
use crate::stats::{Report, ResourceDelta, Summary, Table, per_sec, round3};
use crate::stub_rpc::{SERVER_PAGE_CAP, StubRpc};

const START: u64 = 1_000_000;
const FINALITY_LAG: u64 = 5;

pub struct RunOutcome {
    pub wall: Duration,
    pub status: IndexerStatus,
    pub res: ResourceDelta,
}

fn base_config(stub: &StubRpc, page_limit: u16) -> IndexerConfig {
    IndexerConfig::new(&stub.url, CONTRACT)
        .start_block(START)
        .page_limit(page_limit)
        .finality_lag(FINALITY_LAG)
        .poll_interval(Duration::from_millis(50))
}

/// Run a real `SupraIndexer` until `done`, then shut it down cleanly.
async fn drive(
    ctx: &Ctx,
    names: &[String],
    config: IndexerConfig,
    store: &BenchStore,
    done: impl Fn(&IndexerStatus) -> bool,
) -> Result<RunOutcome> {
    let pool = create_pool(&ctx.db_url, 4)?;
    let mut indexer = SupraIndexer::new(pool, config)?.handle_signals(false);
    for name in names {
        indexer.register_event::<TradePayload, _>(name, store.clone());
    }
    let handle = indexer.handle();
    let mut rx = handle.subscribe();

    let snap = ctx.snapshot()?;
    let started = Instant::now();
    let task = tokio::spawn(indexer.run());
    let waited = tokio::time::timeout(Duration::from_secs(900), rx.wait_for(|s| done(s))).await;
    let wall = started.elapsed();
    let res = ctx.delta(snap)?;
    let status = handle.status();

    handle.shutdown();
    task.await?.context("indexer run failed")?;
    waited
        .context("timed out waiting for the indexer")?
        .map_err(|_| anyhow::anyhow!("indexer stopped before finishing"))?;
    Ok(RunOutcome { wall, status, res })
}

fn caught_up(n: u64) -> impl Fn(&IndexerStatus) -> bool {
    move |s| s.mode == Mode::Live && s.events_indexed >= n
}

// ---------------------------------------------------------------- S1 decode

fn decode_samples<T: serde::de::DeserializeOwned>(body: &[u8], pages: usize) -> Vec<Duration> {
    (0..pages)
        .map(|_| {
            let t = Instant::now();
            let resp: EventsResponse<SupraEvent<T>> = serde_json::from_slice(body).unwrap();
            std::hint::black_box(&resp);
            t.elapsed()
        })
        .collect()
}

pub async fn s1_decode(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new("s1_decode", "CPU cost of decoding an RPC page");
    report.note(
        "Decodes a 100-event page into `EventsResponse<SupraEvent<T>>` with serde_json, the exact call the SDK makes per page.",
    );
    let pages = if ctx.quick { 2_000 } else { 20_000 };
    let ty = qualified("bench::E0");

    let mut t = Table::new(
        "Per-event decode cost (single core)",
        &[
            "payload",
            "bytes/event",
            "pages",
            "p50 µs/event",
            "p99 µs/event",
            "events/core·s",
        ],
    );
    let token: Vec<String> = (0..100)
        .map(|i| fixtures::token_event_json(&ty, i, START + i))
        .collect();
    let trade: Vec<String> = (0..100)
        .map(|i| fixtures::trade_event_json(&ty, i, START + i))
        .collect();
    for (label, body, mut samples) in [
        {
            let b = fixtures::page_body(&token);
            let s = decode_samples::<TokenPayload>(&b, pages);
            ("token (4 fields)", b, s)
        },
        {
            let b = fixtures::page_body(&trade);
            let s = decode_samples::<TradePayload>(&b, pages);
            ("trade (9 fields)", b, s)
        },
    ] {
        let sum = Summary::from_samples(&mut samples);
        let per_event_us = |ms: f64| round3(ms * 1e3 / 100.0);
        t.push(row![
            label,
            body.len() / 100,
            pages,
            per_event_us(sum.p50_ms),
            per_event_us(sum.p99_ms),
            (100.0 / (sum.avg_ms / 1e3)).round()
        ]);
    }
    report.tables.push(t);

    // Thread scaling: independent decoders, trade payload.
    let body = Arc::new(fixtures::page_body(&trade));
    let run_for = Duration::from_millis(if ctx.quick { 500 } else { 2_000 });
    let mut t = Table::new(
        "Decode throughput vs OS threads (trade payload)",
        &["threads", "events/s", "speed-up"],
    );
    let mut base = 0.0;
    for threads in [1usize, 2, 4, 8, 16] {
        let started = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let body = Arc::clone(&body);
                std::thread::spawn(move || {
                    let mut n = 0u64;
                    while started.elapsed() < run_for {
                        let r: EventsResponse<SupraEvent<TradePayload>> =
                            serde_json::from_slice(&body).unwrap();
                        std::hint::black_box(&r);
                        n += 100;
                    }
                    n
                })
            })
            .collect();
        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let eps = per_sec(total, started.elapsed());
        if threads == 1 {
            base = eps;
        }
        t.push(row![threads, eps, round3(eps / base)]);
    }
    report.tables.push(t);
    Ok(report)
}

// --------------------------------------------------- S2 page size × store

pub async fn s2_page_store(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new(
        "s2_page_store",
        "Throughput vs page size and store implementation (zero-latency RPC)",
    );
    let n: u64 = if ctx.quick { 2_000 } else { 20_000 };
    report.note(format!(
        "One event type, {n} trade events, 2 events/block, stub RPC latency 0 ms. Each run: real `SupraIndexer` from an empty checkpoint to `Live`."
    ));
    report.note(
        "page cycle = gap between consecutive events requests = RPC + decode + store + checkpoint UPDATE + COMMIT. SDK overhead = page cycle p50 − store p50.",
    );
    let names = event_names(1);
    let mut t = Table::new(
        "Results",
        &[
            "store",
            "events/page",
            "events/s",
            "pages/s",
            "page cycle p50 ms",
            "page cycle p99 ms",
            "store p50 ms",
            "SDK overhead p50 ms",
            "bench CPU cores",
            "PG CPU cores",
            "WAL B/event",
        ],
    );
    for kind in [StoreKind::Noop, StoreKind::Batch, StoreKind::RowByRow] {
        for page in [1u16, 10, 25, 50, 100] {
            ctx.reset()?;
            let (data, last) = fixtures::dense(&names, n, START, 2);
            let stub = StubRpc::start(data, last + 10, Duration::ZERO).await?;
            let store = BenchStore::new(kind);
            let out = drive(ctx, &names, base_config(&stub, page), &store, caught_up(n)).await?;
            let rows = ctx.rows()?;
            if kind != StoreKind::Noop && rows != n as i64 {
                bail!(
                    "{} page={page}: stored {rows} rows, expected {n}",
                    kind.label()
                );
            }
            let cycle = Summary::from_samples(&mut stub.page_cycle_samples());
            let st = Summary::from_samples(&mut store.samples());
            t.push(row![
                kind.label(),
                page,
                per_sec(n, out.wall),
                per_sec(out.status.pages_committed, out.wall),
                round3(cycle.p50_ms),
                round3(cycle.p99_ms),
                round3(st.p50_ms),
                round3(cycle.p50_ms - st.p50_ms),
                out.res.self_cpu_cores,
                out.res.pg_cpu_cores,
                out.res.wal_bytes / n as i64
            ]);
            println!(
                "  {} page={page}: {} ev/s",
                kind.label(),
                per_sec(n, out.wall)
            );
        }
    }
    report.tables.push(t);
    Ok(report)
}

// ------------------------------------------------------- S3 RPC latency

pub async fn s3_latency(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new("s3_rpc_latency", "Throughput vs RPC round-trip latency");
    report.note(
        "One event type, batch-insert store, 100 events/page (the Supra RPC cap). Stub delays every response by L ms.",
    );
    report.note(
        "calculated = 100 / (L + t0) where t0 is the measured L=0 page cycle; the SDK keeps one request in flight per indexer.",
    );
    let names = event_names(1);
    let mut t = Table::new(
        "Results",
        &[
            "L ms",
            "events",
            "events/s measured",
            "events/s calculated",
            "page cycle p50 ms",
            "RPC requests",
            "wall s",
        ],
    );
    let mut t0_ms = 0.0;
    for latency in [0u64, 5, 20, 50, 135, 300] {
        let n: u64 = match (ctx.quick, latency) {
            (true, _) => 1_000,
            (false, 0) => 20_000,
            (false, l) if l <= 20 => 10_000,
            _ => 5_000,
        };
        ctx.reset()?;
        let (data, last) = fixtures::dense(&names, n, START, 2);
        let stub = StubRpc::start(data, last + 10, Duration::from_millis(latency)).await?;
        let store = BenchStore::new(StoreKind::Batch);
        let out = drive(ctx, &names, base_config(&stub, 100), &store, caught_up(n)).await?;
        let cycle = Summary::from_samples(&mut stub.page_cycle_samples());
        if latency == 0 {
            t0_ms = cycle.p50_ms;
        }
        let calculated = (100.0 / ((latency as f64 + t0_ms) / 1e3)).round();
        t.push(row![
            latency,
            n,
            per_sec(n, out.wall),
            calculated,
            round3(cycle.p50_ms),
            stub.event_requests() + stub.block_requests(),
            round3(out.wall.as_secs_f64())
        ]);
        println!("  L={latency}ms: {} ev/s", per_sec(n, out.wall));
    }
    report.tables.push(t);
    Ok(report)
}

// ------------------------------------------------- S4 multiple event types

pub async fn s4_multi_event(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new("s4_multi_event", "Throughput vs number of event types");
    let n: u64 = if ctx.quick { 2_600 } else { 26_000 };
    report.note(format!(
        "{n} trade events in total, round-robin over k event types, 2 events/block, batch-insert store, 100 events/page."
    ));
    report.note(
        "Event types are drained one after another each cycle (ordering guarantee), so total throughput does not grow with k.",
    );
    let mut t = Table::new(
        "Results",
        &[
            "event types",
            "L ms",
            "events/s",
            "RPC requests",
            "cycles",
            "wall s",
        ],
    );
    for latency in [0u64, 20] {
        for k in [1usize, 4, 13] {
            ctx.reset()?;
            let names = event_names(k);
            let (data, last) = fixtures::dense(&names, n, START, 2);
            let stub = StubRpc::start(data, last + 10, Duration::from_millis(latency)).await?;
            let store = BenchStore::new(StoreKind::Batch);
            let out = drive(ctx, &names, base_config(&stub, 100), &store, caught_up(n)).await?;
            t.push(row![
                k,
                latency,
                per_sec(n, out.wall),
                stub.event_requests() + stub.block_requests(),
                out.status.cycles_completed,
                round3(out.wall.as_secs_f64())
            ]);
            println!("  k={k} L={latency}ms: {} ev/s", per_sec(n, out.wall));
        }
    }
    report.tables.push(t);
    Ok(report)
}

// ---------------------------------------------------- S5 sparse catch-up

pub async fn s5_sparse(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new(
        "s5_sparse_catchup",
        "Catching up over a long, mostly empty block range",
    );
    let span: u64 = 14_000_000;
    let count: u64 = 20;
    let latency_ms = 5u64;
    let real_rtt_ms = 270.0;
    report.note(format!(
        "{count} events spread over {span} blocks (the shape of the real testnet `TokenCreated` history), one event type, stub latency {latency_ms} ms."
    ));
    report.note(format!(
        "Each window costs 1 events request (+1 for a non-empty range); the chain tip is fetched once per catch-up, not per window. calculated wall = requests × {real_rtt_ms} ms (median RTT measured to rpc-testnet.supra.com from the bench host)."
    ));
    let mut t = Table::new(
        "Results",
        &[
            "max_blocks_per_cycle",
            "cycles",
            "RPC requests",
            "wall s measured",
            "blocks/s measured",
            "wall s calculated @270 ms",
        ],
    );
    let windows: &[u64] = if ctx.quick {
        &[1_000_000, 14_000_000]
    } else {
        &[10_000, 100_000, 1_000_000, 14_000_000]
    };
    for &window in windows {
        ctx.reset()?;
        let data = fixtures::sparse("bench::E0", count, START, span);
        let stub =
            StubRpc::start(data, START + span + 10, Duration::from_millis(latency_ms)).await?;
        let store = BenchStore::new(StoreKind::Batch);
        let config = base_config(&stub, 100).max_blocks_per_cycle(window);
        let out = drive(ctx, &event_names(1), config, &store, caught_up(count)).await?;
        let requests = stub.event_requests() + stub.block_requests();
        t.push(row![
            window,
            out.status.cycles_completed,
            requests,
            round3(out.wall.as_secs_f64()),
            per_sec(span, out.wall),
            round3(requests as f64 * real_rtt_ms / 1e3)
        ]);
        println!(
            "  window={window}: {requests} requests, {:.2}s",
            out.wall.as_secs_f64()
        );
    }
    report.tables.push(t);
    Ok(report)
}

// ------------------------------------------------- S6 RPC client capacity

pub async fn s6_rpc_client(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new(
        "s6_rpc_client",
        "SupraClient request capacity vs concurrency",
    );
    report.note(
        "Concurrent `SupraClient::fetch_events` calls (100-event trade pages, ~68 KB) against the stub. The indexer itself keeps one request in flight; this measures headroom for parallel use.",
    );
    let names = event_names(1);
    let (data, last) = fixtures::dense(&names, 1_000, START, 2);
    let run_for = Duration::from_millis(if ctx.quick { 500 } else { 3_000 });
    let mut t = Table::new(
        "Results",
        &[
            "L ms",
            "concurrency",
            "req/s",
            "events/s",
            "p50 ms",
            "p99 ms",
            "max ms",
        ],
    );
    for (latency, levels) in [
        (20u64, vec![1usize, 2, 4, 8, 16, 32, 64, 128, 256]),
        (0, vec![1, 4, 16, 64, 256]),
    ] {
        let stub = StubRpc::start(
            fixtures::dense(&names, 1_000, START, 2).0,
            last + 10,
            Duration::from_millis(latency),
        )
        .await?;
        let client = SupraClient::new(&IndexerConfig::new(&stub.url, CONTRACT))?;
        for c in levels {
            let started = Instant::now();
            let tasks: Vec<_> = (0..c)
                .map(|_| {
                    let client = client.clone();
                    tokio::spawn(async move {
                        let ty = qualified("bench::E0");
                        let query = EventQuery {
                            event_type: &ty,
                            start: PageStart::Height(START),
                            end_height_exclusive: START + 10_000,
                            limit: 100,
                        };
                        let mut samples = Vec::new();
                        while started.elapsed() < run_for {
                            let t = Instant::now();
                            let page = client.fetch_events(&query).await.expect("fetch");
                            std::hint::black_box(page.body.len());
                            samples.push(t.elapsed());
                        }
                        samples
                    })
                })
                .collect();
            let mut samples = Vec::new();
            for task in tasks {
                samples.extend(task.await?);
            }
            let wall = started.elapsed();
            let reqs = samples.len() as u64;
            let sum = Summary::from_samples(&mut samples);
            t.push(row![
                latency,
                c,
                per_sec(reqs, wall),
                per_sec(reqs * SERVER_PAGE_CAP as u64, wall),
                round3(sum.p50_ms),
                round3(sum.p99_ms),
                round3(sum.max_ms)
            ]);
            println!("  L={latency} C={c}: {} req/s", per_sec(reqs, wall));
        }
    }
    drop(data);
    report.tables.push(t);
    Ok(report)
}

// ------------------------------------------ S7 startup, crash recovery, replay

pub async fn s7_recovery(ctx: &Ctx) -> Result<Report> {
    let mut report = Report::new(
        "s7_recovery",
        "Startup latency, crash recovery and idempotent replay",
    );
    let names = event_names(1);

    // (a) Cold start to first completed cycle.
    let starts = if ctx.quick { 5 } else { 30 };
    let mut samples = Vec::new();
    for _ in 0..starts {
        ctx.reset()?;
        let (data, last) = fixtures::dense(&names, 10, START, 2);
        let stub = StubRpc::start(data, last + 10, Duration::ZERO).await?;
        let out = drive(
            ctx,
            &names,
            base_config(&stub, 100),
            &BenchStore::new(StoreKind::Batch),
            |s| s.cycles_completed >= 1,
        )
        .await?;
        samples.push(out.wall);
    }
    let s = Summary::from_samples(&mut samples);
    let mut t = Table::new(
        "Startup: `run()` to first completed cycle (schema check, seed, advisory lock, 1 block + 2 events requests, 1 page commit)",
        &["runs", "p50 ms", "p90 ms", "p99 ms", "max ms"],
    );
    t.push(row![
        starts,
        round3(s.p50_ms),
        round3(s.p90_ms),
        round3(s.p99_ms),
        round3(s.max_ms)
    ]);
    report.tables.push(t);

    // (b) Crash mid-range, then restart.
    let n: u64 = if ctx.quick { 2_000 } else { 20_000 };
    let fail_page = (n / 100 / 2) as usize;
    ctx.reset()?;
    let (data, last) = fixtures::dense(&names, n, START, 2);
    let stub = StubRpc::start(data, last + 10, Duration::ZERO).await?;
    let failing = BenchStore::new(StoreKind::Batch).failing_on(fail_page);
    let crashed = drive(ctx, &names, base_config(&stub, 100), &failing, |s| {
        s.consecutive_failures >= 1
    })
    .await?;
    let committed_before = ctx.rows()?;
    let resumed = drive(
        ctx,
        &names,
        base_config(&stub, 100),
        &BenchStore::new(StoreKind::Batch),
        caught_up(n - committed_before as u64),
    )
    .await?;
    let final_rows = ctx.rows()?;
    let mut t = Table::new(
        "Crash on a page mid-range (store error after its INSERT, transaction rolled back), then restart",
        &[
            "events",
            "failed page",
            "rows committed before crash",
            "rows after restart",
            "missing",
            "resume wall s",
            "resume events/s",
        ],
    );
    t.push(row![
        n,
        fail_page,
        committed_before,
        final_rows,
        n as i64 - final_rows,
        round3(resumed.wall.as_secs_f64()),
        per_sec(n - committed_before as u64, resumed.wall)
    ]);
    report.tables.push(t);
    report.note(format!(
        "Crash run stopped after {:.3}s with error: {}",
        crashed.wall.as_secs_f64(),
        crashed.status.last_error.unwrap_or_default()
    ));
    report.note(
        "`bench_trades.tx_hash` is the primary key, so `rows after restart == events` means nothing was lost or duplicated.",
    );

    // (c) Full reindex over already-stored rows (all conflicts) vs fresh load.
    let mut t = Table::new(
        "Idempotent replay: `force_start_block` reindex over existing rows vs a fresh load",
        &["run", "events", "events/s", "rows after"],
    );
    for (label, truncate_rows) in [("fresh load", true), ("replay (all ON CONFLICT)", false)] {
        if truncate_rows {
            ctx.reset()?;
        }
        let store = BenchStore::new(StoreKind::Batch);
        let config = base_config(&stub, 100).force_start_block(true);
        let out = drive(ctx, &names, config, &store, caught_up(n)).await?;
        t.push(row![label, n, per_sec(n, out.wall), ctx.rows()?]);
    }
    report.tables.push(t);
    Ok(report)
}
