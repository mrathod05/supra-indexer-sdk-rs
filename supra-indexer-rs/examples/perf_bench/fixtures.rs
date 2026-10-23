//! Synthetic events, the benchmark table, and the stores under test.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use diesel::prelude::*;
use serde::Deserialize;
use supra_indexer_sdk::{EventStore, SupraEvent};

use crate::stub_rpc::StubEvent;

/// Contract address used by every synthetic event type.
pub const CONTRACT: &str = "0xbe";

/// `bench::E{i}` — event type names relative to [`CONTRACT`].
pub fn event_names(k: usize) -> Vec<String> {
    (0..k).map(|i| format!("bench::E{i}")).collect()
}

pub fn qualified(name: &str) -> String {
    format!("{CONTRACT}::{name}")
}

/// Small payload, shaped like `token_factory::TokenCreated` (4 fields).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TokenPayload {
    pub creator: String,
    pub token_address: String,
    pub initial_supply: String,
    pub timestamp: String,
}

/// Larger payload, shaped like a prediction-market `TradeExecutedEvent`
/// (9 fields, Move integers as strings).
#[derive(Debug, Deserialize)]
pub struct TradePayload {
    pub market_id: String,
    pub trader: String,
    pub outcome_index: String,
    pub is_buy: bool,
    pub shares: String,
    pub cost: String,
    pub fee: String,
    pub price_after: String,
    pub timestamp: String,
}

/// Kind of synthetic 32-byte id; keeps market, trader, tx, ... ids distinct.
#[derive(Clone, Copy)]
enum Fake {
    Market = 1,
    Trader,
    Tx,
    Creator,
    Token,
}

/// Deterministic fake address/hash for synthetic events (not cryptographic).
fn fake_id(n: u64, kind: Fake) -> String {
    let k = kind as u64;
    format!(
        "0x{:016x}{:016x}{:016x}{:016x}",
        k,
        n,
        n ^ k,
        n.wrapping_mul(31)
    )
}

/// Full RPC event object with a trade payload, matching the real response
/// shape (guid, sequence_number, type, data, block_height, transaction_hash).
pub fn trade_event_json(event_type: &str, seq: u64, height: u64) -> String {
    format!(
        concat!(
            r#"{{"event":{{"guid":{{"creation_number":"0","account_address":"0x0"}},"#,
            r#""sequence_number":"0","type":"{ty}","data":{{"market_id":"{market}","#,
            r#""trader":"{trader}","outcome_index":"{outcome}","is_buy":{buy},"#,
            r#""shares":"{shares}","cost":"{cost}","fee":"{fee}","price_after":"{price}","#,
            r#""timestamp":"{ts}"}}}},"block_height":{h},"transaction_hash":"{tx}"}}"#
        ),
        ty = event_type,
        market = fake_id(seq % 64, Fake::Market),
        trader = fake_id(seq % 5000, Fake::Trader),
        outcome = seq % 2,
        buy = seq % 3 != 0,
        shares = 1_000_000 + seq * 7,
        cost = 990_000 + seq * 5,
        fee = 1_000 + seq % 97,
        price = 50_000_000 + seq % 1000,
        ts = 1_790_000_000 + seq,
        h = height,
        tx = fake_id(seq, Fake::Tx),
    )
}

pub fn token_event_json(event_type: &str, seq: u64, height: u64) -> String {
    format!(
        concat!(
            r#"{{"event":{{"guid":{{"creation_number":"0","account_address":"0x0"}},"#,
            r#""sequence_number":"0","type":"{ty}","data":{{"creator":"{creator}","#,
            r#""initial_supply":"100000000000000","timestamp":"{ts}","#,
            r#""token_address":"{token}"}}}},"block_height":{h},"transaction_hash":"{tx}"}}"#
        ),
        ty = event_type,
        creator = fake_id(seq % 100, Fake::Creator),
        token = fake_id(seq, Fake::Token),
        ts = 1_790_000_000 + seq,
        h = height,
        tx = fake_id(seq, Fake::Tx),
    )
}

pub fn page_body(events: &[String]) -> Vec<u8> {
    format!(r#"{{"data":[{}]}}"#, events.join(",")).into_bytes()
}

/// `total` trade events spread round-robin over `types`, `per_block` events
/// per block starting at `start`. Returns the data and the last height.
pub fn dense(
    types: &[String],
    total: u64,
    start: u64,
    per_block: u64,
) -> (HashMap<String, Vec<StubEvent>>, u64) {
    let mut data: HashMap<String, Vec<StubEvent>> =
        types.iter().map(|t| (qualified(t), Vec::new())).collect();
    let mut last = start;
    for seq in 0..total {
        let ty = qualified(&types[(seq as usize) % types.len()]);
        let height = start + seq / per_block;
        last = height;
        let json = trade_event_json(&ty, seq, height);
        data.get_mut(&ty).unwrap().push(StubEvent { height, json });
    }
    (data, last)
}

/// `count` events evenly spread over `span` blocks (sparse, like most real
/// contracts).
pub fn sparse(name: &str, count: u64, start: u64, span: u64) -> HashMap<String, Vec<StubEvent>> {
    let ty = qualified(name);
    let step = span / count.max(1);
    let events = (0..count)
        .map(|seq| {
            let height = start + seq * step + step / 2;
            StubEvent {
                height,
                json: trade_event_json(&ty, seq, height),
            }
        })
        .collect();
    HashMap::from([(ty, events)])
}

diesel::table! {
    bench_trades (tx_hash) {
        tx_hash -> Text,
        event_type -> Text,
        block_height -> Int8,
        market_id -> Text,
        trader -> Text,
        outcome_index -> Int4,
        is_buy -> Bool,
        shares -> Text,
        cost -> Text,
        fee -> Text,
        price_after -> Text,
        ts -> Int8,
    }
}

pub fn create_tables(conn: &mut PgConnection) -> Result<()> {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS bench_trades (
            tx_hash       TEXT PRIMARY KEY,
            event_type    TEXT NOT NULL,
            block_height  BIGINT NOT NULL,
            market_id     TEXT NOT NULL,
            trader        TEXT NOT NULL,
            outcome_index INT NOT NULL,
            is_buy        BOOLEAN NOT NULL,
            shares        TEXT NOT NULL,
            cost          TEXT NOT NULL,
            fee           TEXT NOT NULL,
            price_after   TEXT NOT NULL,
            ts            BIGINT NOT NULL
        )",
    )
    .execute(conn)?;
    diesel::sql_query(
        "CREATE INDEX IF NOT EXISTS bench_trades_market ON bench_trades (market_id, block_height)",
    )
    .execute(conn)?;
    Ok(())
}

/// Empty the bench table and this harness's checkpoints, then CHECKPOINT so
/// no checkpoint lands mid-measurement.
pub fn reset(conn: &mut PgConnection) -> Result<()> {
    diesel::sql_query("TRUNCATE bench_trades").execute(conn)?;
    diesel::sql_query(format!(
        "DO $$ BEGIN IF to_regclass('supra_indexer_checkpoints') IS NOT NULL THEN \
         DELETE FROM supra_indexer_checkpoints WHERE event_key LIKE '{CONTRACT}::%'; \
         END IF; END $$"
    ))
    .execute(conn)?;
    // Needs superuser; harmless to skip otherwise.
    let _ = diesel::sql_query("CHECKPOINT").execute(conn);
    Ok(())
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

pub fn row_count(conn: &mut PgConnection) -> Result<i64> {
    let c: Count = diesel::sql_query("SELECT count(*) AS n FROM bench_trades").get_result(conn)?;
    Ok(c.n)
}

#[derive(Insertable)]
#[diesel(table_name = bench_trades)]
struct NewTrade<'a> {
    tx_hash: &'a str,
    event_type: &'a str,
    block_height: i64,
    market_id: &'a str,
    trader: &'a str,
    outcome_index: i32,
    is_buy: bool,
    shares: &'a str,
    cost: &'a str,
    fee: &'a str,
    price_after: &'a str,
    ts: i64,
}

impl<'a> NewTrade<'a> {
    fn from_event(e: &'a SupraEvent<TradePayload>) -> Result<Self> {
        let d = e.data();
        Ok(Self {
            tx_hash: e.transaction_hash.as_deref().unwrap_or_default(),
            event_type: &e.event.event_type,
            block_height: i64::try_from(e.block_height)?,
            market_id: &d.market_id,
            trader: &d.trader,
            outcome_index: d.outcome_index.parse()?,
            is_buy: d.is_buy,
            shares: &d.shares,
            cost: &d.cost,
            fee: &d.fee,
            price_after: &d.price_after,
            ts: d.timestamp.parse()?,
        })
    }
}

/// How a store writes a page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    /// Writes nothing: isolates SDK cost (decode + checkpoint + commit).
    Noop,
    /// One multi-row `INSERT .. ON CONFLICT DO NOTHING` per page.
    Batch,
    /// One `INSERT .. ON CONFLICT DO NOTHING` per event, same transaction.
    RowByRow,
}

impl StoreKind {
    pub fn label(self) -> &'static str {
        match self {
            StoreKind::Noop => "noop",
            StoreKind::Batch => "batch insert",
            StoreKind::RowByRow => "row-by-row insert",
        }
    }
}

/// Store under test. Records the time spent inside `store_events` and can
/// fail once on a given call to simulate a crash.
#[derive(Clone)]
pub struct BenchStore {
    kind: StoreKind,
    samples: Arc<Mutex<Vec<Duration>>>,
    calls: Arc<AtomicUsize>,
    fail_on_call: Option<usize>,
}

impl BenchStore {
    pub fn new(kind: StoreKind) -> Self {
        Self {
            kind,
            samples: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::new(AtomicUsize::new(0)),
            fail_on_call: None,
        }
    }

    pub fn failing_on(mut self, call: usize) -> Self {
        self.fail_on_call = Some(call);
        self
    }

    pub fn samples(&self) -> Vec<Duration> {
        self.samples.lock().unwrap().clone()
    }
}

impl EventStore<SupraEvent<TradePayload>> for BenchStore {
    fn store_events(
        &self,
        conn: &mut PgConnection,
        events: &[SupraEvent<TradePayload>],
    ) -> Result<()> {
        let started = Instant::now();
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        match self.kind {
            StoreKind::Noop => {}
            StoreKind::Batch => {
                let rows = events
                    .iter()
                    .map(NewTrade::from_event)
                    .collect::<Result<Vec<_>>>()?;
                diesel::insert_into(bench_trades::table)
                    .values(&rows)
                    .on_conflict_do_nothing()
                    .execute(conn)?;
            }
            StoreKind::RowByRow => {
                for e in events {
                    diesel::insert_into(bench_trades::table)
                        .values(&NewTrade::from_event(e)?)
                        .on_conflict_do_nothing()
                        .execute(conn)?;
                }
            }
        }
        if self.fail_on_call == Some(call) {
            anyhow::bail!("injected failure on store call {call}");
        }
        self.samples.lock().unwrap().push(started.elapsed());
        Ok(())
    }
}
