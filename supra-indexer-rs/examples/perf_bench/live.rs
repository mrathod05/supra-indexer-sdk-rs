//! `live`: the real `SupraIndexer` against the real Supra RPC (not part of
//! `all`, needs network). Measures RTT, chain production rate and a bounded
//! catch-up over real framework events.

use std::time::{Duration, Instant};

use anyhow::Result;
use diesel::prelude::*;
use supra_indexer_sdk::rpc::{EventQuery, PageStart, SupraClient};
use supra_indexer_sdk::{
    EventStore, IndexerConfig, IndexerStatus, Mode, SupraEvent, SupraIndexer, create_pool,
};

use crate::Ctx;
use crate::row;
use crate::stats::{Report, Summary, Table, per_sec, proc_ticks, round3};

const LIVE_EVENTS: [&str; 2] = ["0x1::coin::CoinDeposit", "0x1::coin::CoinWithdraw"];

/// Inserts the raw payload as text: real write cost without a schema per type.
#[derive(Clone)]
struct RawStore;

impl EventStore<SupraEvent<serde_json::Value>> for RawStore {
    fn store_events(
        &self,
        conn: &mut PgConnection,
        events: &[SupraEvent<serde_json::Value>],
    ) -> Result<()> {
        for e in events {
            diesel::sql_query(
                "INSERT INTO bench_live (event_type, block_height, tx_hash, data) VALUES ($1, $2, $3, $4)",
            )
            .bind::<diesel::sql_types::Text, _>(&e.event.event_type)
            .bind::<diesel::sql_types::BigInt, _>(i64::try_from(e.block_height)?)
            .bind::<diesel::sql_types::Nullable<diesel::sql_types::Text>, _>(&e.transaction_hash)
            .bind::<diesel::sql_types::Text, _>(e.data().to_string())
            .execute(conn)?;
        }
        Ok(())
    }
}

pub async fn live(ctx: &Ctx) -> Result<Report> {
    let url = std::env::var("SUPRA_RPC_URL")
        .unwrap_or_else(|_| "https://rpc-testnet.supra.com".to_string());
    let span: u64 = std::env::var("LIVE_SPAN_BLOCKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let mut report = Report::new("live", "Real SupraIndexer against the real Supra RPC");
    report.note(format!(
        "RPC: {url}. Measured from the bench host over the public internet."
    ));

    let client = SupraClient::new(&IndexerConfig::new(&url, "0x1"))?;

    // RTT samples.
    let samples_n = if ctx.quick { 5 } else { 30 };
    let mut block_rtt = Vec::new();
    for _ in 0..samples_n {
        let t = Instant::now();
        client.latest_block_height().await?;
        block_rtt.push(t.elapsed());
    }
    let tip = client.latest_block_height().await?;
    let mut page_rtt = Vec::new();
    let mut page_bytes = 0;
    for _ in 0..samples_n {
        let q = EventQuery {
            event_type: LIVE_EVENTS[0],
            start: PageStart::Height(tip - span),
            end_height_exclusive: tip - 10,
            limit: 100,
        };
        let t = Instant::now();
        let page = client.fetch_events(&q).await?;
        page_rtt.push(t.elapsed());
        page_bytes = page.body.len();
    }
    let mut t = Table::new(
        "RPC round-trip time",
        &["request", "samples", "p50 ms", "p90 ms", "p99 ms", "max ms"],
    );
    for (label, samples) in [
        ("GET /rpc/v3/block".to_string(), &mut block_rtt),
        (
            format!("GET /rpc/v3/events (page of {page_bytes} B)"),
            &mut page_rtt,
        ),
    ] {
        let s = Summary::from_samples(samples);
        t.push(row![
            label,
            s.n,
            round3(s.p50_ms),
            round3(s.p90_ms),
            round3(s.p99_ms),
            round3(s.max_ms)
        ]);
    }
    report.tables.push(t);

    // Chain production rate.
    let watch = Duration::from_secs(if ctx.quick { 5 } else { 30 });
    let h0 = client.latest_block_height().await?;
    let t0 = Instant::now();
    tokio::time::sleep(watch).await;
    let h1 = client.latest_block_height().await?;
    let blocks_per_s = (h1 - h0) as f64 / t0.elapsed().as_secs_f64();
    let mut t = Table::new("Chain production", &["window s", "blocks", "blocks/s"]);
    t.push(row![watch.as_secs(), h1 - h0, round3(blocks_per_s)]);
    report.tables.push(t);

    // Bounded catch-up with the real indexer.
    {
        let mut conn = PgConnection::establish(&ctx.db_url)?;
        diesel::sql_query(
            "CREATE TABLE IF NOT EXISTS bench_live (event_type TEXT NOT NULL, \
             block_height BIGINT NOT NULL, tx_hash TEXT, data TEXT NOT NULL)",
        )
        .execute(&mut conn)?;
        diesel::sql_query("TRUNCATE bench_live").execute(&mut conn)?;
        diesel::sql_query("DELETE FROM supra_indexer_checkpoints WHERE event_key LIKE '0x1::%'")
            .execute(&mut conn)
            .ok();
    }
    let start = h1 - span;
    let config = IndexerConfig::new(&url, "0x1")
        .start_block(start)
        .force_start_block(true)
        .poll_interval(Duration::from_millis(500));
    let mut indexer =
        SupraIndexer::new(create_pool(&ctx.db_url, 4)?, config)?.handle_signals(false);
    for ev in LIVE_EVENTS {
        indexer.register_event::<serde_json::Value, _>(ev, RawStore);
    }
    let handle = indexer.handle();
    let mut rx = handle.subscribe();
    let ticks0 = proc_ticks("/proc/self/stat").unwrap_or(0);
    let started = Instant::now();
    let task = tokio::spawn(indexer.run());
    rx.wait_for(|s: &IndexerStatus| s.mode == Mode::Live)
        .await?;
    let wall = started.elapsed();
    let ticks = proc_ticks("/proc/self/stat").unwrap_or(0) - ticks0;
    let status = handle.status();
    handle.shutdown();
    task.await??;

    let clk_tck = 100.0;
    let mut t = Table::new(
        format!(
            "Catch-up over the last {span} blocks: {} (default config: 100 events/page, 100 000 blocks/cycle, finality lag 5)",
            LIVE_EVENTS.join(" + ")
        ),
        &[
            "blocks",
            "events",
            "pages",
            "cycles",
            "RPC requests",
            "wall s",
            "blocks/s",
            "events/s",
            "avg ms/request",
            "bench CPU cores",
        ],
    );
    t.push(row![
        status.indexed_height.unwrap_or(start) - start + 1,
        status.events_indexed,
        status.pages_committed,
        status.cycles_completed,
        status.rpc_requests,
        round3(wall.as_secs_f64()),
        per_sec(status.indexed_height.unwrap_or(start) - start + 1, wall),
        per_sec(status.events_indexed, wall),
        round3(wall.as_secs_f64() * 1e3 / status.rpc_requests.max(1) as f64),
        round3(ticks as f64 / clk_tck / wall.as_secs_f64())
    ]);
    report.tables.push(t);
    report.note(format!(
        "Final status: chain_height={:?} indexed_height={:?} lag={:?}",
        status.chain_height,
        status.indexed_height,
        status.lag()
    ));
    Ok(report)
}
