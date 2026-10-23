//! End-to-end tests against a mock Supra RPC and a real PostgreSQL.
//!
//! Set `TEST_DATABASE_URL` to run them; they are skipped otherwise.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Text};
use serde::Deserialize;
use supra_indexer_sdk::RetryPolicy;
use supra_indexer_sdk::prelude::*;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const HEIGHTS: [u64; 7] = [10, 10, 20, 30, 30, 40, 50];
/// Like Supra RPC, the mock caps pages below the requested `limit`.
const SERVER_PAGE_CAP: usize = 2;

fn database_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

/// Serves `HEIGHTS` as events, paginated like Supra RPC: `end_height` is
/// exclusive and the cursor is set on every non-empty page.
struct EventsResponder;

impl Respond for EventsResponder {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let q: std::collections::HashMap<_, _> = req.url.query_pairs().into_owned().collect();
        let limit: usize = q["limit"].parse().unwrap();
        let end: u64 = q["end_height"].parse().unwrap();
        let from = match (q.get("start"), q.get("start_height")) {
            (Some(c), _) => c.strip_prefix("idx:").unwrap().parse().unwrap(),
            (None, Some(h)) => {
                let h: u64 = h.parse().unwrap();
                if h >= end {
                    return ResponseTemplate::new(400);
                }
                HEIGHTS
                    .iter()
                    .position(|&x| x >= h)
                    .unwrap_or(HEIGHTS.len())
            }
            _ => panic!("no start"),
        };
        let idx: Vec<usize> = (from..HEIGHTS.len())
            .filter(|&i| HEIGHTS[i] < end)
            .take(limit.min(SERVER_PAGE_CAP))
            .collect();
        let data: Vec<_> = idx
            .iter()
            .map(|&i| {
                serde_json::json!({
                    "event": {"type": "x", "sequence_number": "0", "data": {"seq": i}},
                    "block_height": HEIGHTS[i],
                    "transaction_hash": format!("0x{i}"),
                })
            })
            .collect();
        let cursor = idx
            .last()
            .map(|i| format!("idx:{}", i + 1))
            .unwrap_or_default();
        ResponseTemplate::new(200)
            .insert_header("x-supra-cursor", cursor)
            .set_body_json(serde_json::json!({ "data": data }))
    }
}

async fn mock_rpc(tip: u64) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rpc/v3/block"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"header": {"height": tip}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("^/rpc/v3/events/"))
        .respond_with(EventsResponder)
        .mount(&server)
        .await;
    server
}

#[derive(Debug, Deserialize)]
struct Payload {
    seq: i32,
}

/// Plain INSERT on a unique column: a duplicate delivery fails the test.
struct Store {
    table: String,
    calls: Arc<AtomicUsize>,
    fail_on_call: Option<usize>,
}

impl EventStore<SupraEvent<Payload>> for Store {
    fn store_events(
        &self,
        conn: &mut PgConnection,
        events: &[SupraEvent<Payload>],
    ) -> anyhow::Result<()> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        for e in events {
            diesel::sql_query(format!(
                "INSERT INTO {} (seq, block) VALUES ($1, $2)",
                self.table
            ))
            .bind::<Integer, _>(e.data().seq)
            .bind::<BigInt, _>(e.block_height as i64)
            .execute(conn)?;
        }
        if self.fail_on_call == Some(call) {
            anyhow::bail!("injected failure on call {call}");
        }
        Ok(())
    }
}

fn setup(url: &str, table: &str, contract: &str) -> DbPool {
    let pool = create_pool(url, 4).unwrap();
    let mut conn = pool.get().unwrap();
    diesel::sql_query(format!("DROP TABLE IF EXISTS {table}"))
        .execute(&mut conn)
        .unwrap();
    diesel::sql_query(format!(
        "CREATE TABLE {table} (seq INT PRIMARY KEY, block BIGINT NOT NULL)"
    ))
    .execute(&mut conn)
    .unwrap();
    // Clear this test's checkpoints if a previous run created the table.
    diesel::sql_query(format!(
        "DO $$ BEGIN IF to_regclass('supra_indexer_checkpoints') IS NOT NULL THEN \
         DELETE FROM supra_indexer_checkpoints WHERE event_key LIKE '{contract}::%'; \
         END IF; END $$"
    ))
    .execute(&mut conn)
    .unwrap();
    pool
}

fn config(rpc: &MockServer, contract: &str, max_blocks: u64) -> IndexerConfig {
    IndexerConfig::new(rpc.uri(), contract)
        .start_block(1)
        .page_limit(5)
        .finality_lag(5)
        .max_blocks_per_cycle(max_blocks)
        .poll_interval(Duration::from_millis(20))
        .retry(RetryPolicy {
            max_attempts: 2,
            base_delay: Duration::from_millis(5),
            max_delay: Duration::from_millis(20),
        })
}

async fn run_until<F>(indexer: SupraIndexer, done: F) -> IndexerStatus
where
    F: Fn(&IndexerStatus) -> bool,
{
    let handle = indexer.handle();
    let mut rx = handle.subscribe();
    let task = tokio::spawn(indexer.run());
    tokio::time::timeout(Duration::from_secs(10), rx.wait_for(|s| done(s)))
        .await
        .expect("timed out waiting for indexer")
        .unwrap();
    let status = handle.status();
    handle.shutdown();
    task.await.unwrap().unwrap();
    status
}

#[derive(QueryableByName)]
struct Row {
    #[diesel(sql_type = Integer)]
    seq: i32,
}

#[derive(QueryableByName, Debug)]
struct Cp {
    #[diesel(sql_type = BigInt)]
    next_block: i64,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    cursor: Option<String>,
}

fn stored(pool: &DbPool, table: &str) -> Vec<i32> {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(format!("SELECT seq FROM {table} ORDER BY seq"))
        .load::<Row>(&mut conn)
        .unwrap()
        .into_iter()
        .map(|r| r.seq)
        .collect()
}

fn checkpoint(pool: &DbPool, key: &str) -> Cp {
    let mut conn = pool.get().unwrap();
    diesel::sql_query(
        "SELECT next_block, cursor FROM supra_indexer_checkpoints WHERE event_key = $1",
    )
    .bind::<Text, _>(key)
    .get_result(&mut conn)
    .unwrap()
}

#[tokio::test]
async fn resumes_after_failure_without_gaps_or_duplicates() {
    let Some(url) = database_url() else {
        eprintln!("TEST_DATABASE_URL not set; skipping");
        return;
    };
    let contract = "0xa1";
    let table = "sdk_test_resume";
    let key = format!("{contract}::m::E");
    let pool = setup(&url, table, contract);
    let rpc = mock_rpc(1005).await;

    // Run 1: second page fails (its rows roll back), then we "crash".
    let calls = Arc::new(AtomicUsize::new(0));
    let mut indexer = SupraIndexer::new(pool.clone(), config(&rpc, contract, 1_000))
        .unwrap()
        .handle_signals(false);
    indexer.register_event::<Payload, _>(
        "m::E",
        Store {
            table: table.into(),
            calls: calls.clone(),
            fail_on_call: Some(2),
        },
    );
    let status = run_until(indexer, |s| s.consecutive_failures >= 1).await;
    assert!(status.last_error.unwrap().contains("injected failure"));
    assert_eq!(
        stored(&pool, table),
        vec![0, 1],
        "only the first page committed"
    );
    let cp = checkpoint(&pool, &key);
    assert_eq!(cp.next_block, 1);
    assert_eq!(
        cp.cursor.as_deref(),
        Some("idx:2"),
        "cursor saved with page 1"
    );

    // Run 2: resume from the saved cursor, in small windows.
    let mut indexer = SupraIndexer::new(pool.clone(), config(&rpc, contract, 15))
        .unwrap()
        .handle_signals(false);
    indexer.register_event::<Payload, _>(
        "m::E",
        Store {
            table: table.into(),
            calls: Arc::new(AtomicUsize::new(0)),
            fail_on_call: None,
        },
    );
    let status = run_until(indexer, |s| s.mode == Mode::Live).await;

    assert_eq!(stored(&pool, table), vec![0, 1, 2, 3, 4, 5, 6]);
    let cp = checkpoint(&pool, &key);
    assert_eq!(cp.next_block, 1001, "indexed through tip - lag");
    assert_eq!(cp.cursor, None);
    assert_eq!(status.indexed_height, Some(1000));
    assert_eq!(status.lag(), Some(5));
}

#[tokio::test]
async fn second_instance_is_rejected() {
    let Some(url) = database_url() else {
        eprintln!("TEST_DATABASE_URL not set; skipping");
        return;
    };
    let contract = "0xa2";
    let table = "sdk_test_lock";
    let pool = setup(&url, table, contract);
    let rpc = mock_rpc(1005).await;

    let new_indexer = || {
        let mut indexer = SupraIndexer::new(pool.clone(), config(&rpc, contract, 1_000))
            .unwrap()
            .handle_signals(false);
        indexer.register_event::<Payload, _>(
            "m::E",
            Store {
                table: table.into(),
                calls: Arc::new(AtomicUsize::new(0)),
                fail_on_call: None,
            },
        );
        indexer
    };

    let first = new_indexer();
    let handle = first.handle();
    let mut rx = handle.subscribe();
    let task = tokio::spawn(first.run());
    rx.wait_for(|s| s.mode == Mode::Live).await.unwrap();

    let err = new_indexer().run().await.unwrap_err();
    assert!(matches!(err, IndexerError::AlreadyRunning(_)), "{err}");

    handle.shutdown();
    task.await.unwrap().unwrap();

    // Lock released on shutdown: a new instance can start.
    let status = run_until(new_indexer(), |s| s.mode == Mode::Live).await;
    assert_eq!(status.indexed_height, Some(1000));
}

#[tokio::test]
async fn rejects_duplicate_registration() {
    let Some(url) = database_url() else {
        eprintln!("TEST_DATABASE_URL not set; skipping");
        return;
    };
    let pool = create_pool(&url, 2).unwrap();
    let rpc = mock_rpc(10).await;
    let mut indexer = SupraIndexer::new(pool, config(&rpc, "0xa3", 10)).unwrap();
    let store = || Store {
        table: "unused".into(),
        calls: Arc::new(AtomicUsize::new(0)),
        fail_on_call: None,
    };
    indexer
        .register_event::<Payload, _>("m::E", store())
        .register_event::<Payload, _>("0xa3::m::E", store());
    assert!(matches!(indexer.run().await, Err(IndexerError::Config(_))));
}
