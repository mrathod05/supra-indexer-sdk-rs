# Supra Indexer SDK

Build crash-safe indexers for [Supra](https://supra.com) Move events, backed by PostgreSQL (diesel).

You describe an event's payload and how to store it; the SDK handles RPC pagination, retries, checkpoints, finality lag, graceful shutdown and (optionally) health/admin endpoints.

```toml
[dependencies]
supra_indexer_sdk = { version = "0.2", features = ["server"] }
diesel = { version = "2.3", features = ["postgres", "r2d2"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
```

## Quick start

```rust
use diesel::prelude::*;
use serde::Deserialize;
use supra_indexer_sdk::prelude::*;

/// The event's `data` object. Move u64/u128 values arrive as strings.
#[derive(Debug, Deserialize)]
struct TokenCreated {
    creator: String,
    token_address: String,
    initial_supply: String,
}

struct TokenCreatedStore;

impl EventStore<SupraEvent<TokenCreated>> for TokenCreatedStore {
    fn store_events(
        &self,
        conn: &mut PgConnection,                  // inside the SDK's transaction
        events: &[SupraEvent<TokenCreated>],
    ) -> anyhow::Result<()> {
        for e in events {
            // e.data(), e.block_height, e.transaction_hash
            // INSERT ... ON CONFLICT DO NOTHING
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let pool = create_pool(&std::env::var("DATABASE_URL")?, 5)?;
    let config = IndexerConfig::new("https://rpc-testnet.supra.com", "0xa802...0ddf")
        .start_block(135_000_000);

    let mut indexer = SupraIndexer::new(pool, config)?;
    indexer.register_event::<TokenCreated, _>("token_factory::TokenCreated", TokenCreatedStore);
    indexer.run().await?; // until SIGINT/SIGTERM
    Ok(())
}
```

A full, runnable app (migrations, NUMERIC amounts, Docker, HTTP server) lives in [`sdk-example`](https://github.com/mrathod05/supra-indexer-sdk-rs/tree/main/sdk-example).

## Performance

Measured on an i5-13400 with PostgreSQL 14 on the same host, using the real `SupraIndexer`. See the [full report](docs/perf/PERFORMANCE_REPORT.md) for method and raw data.

| | |
|---|---|
| Full Supra testnet history (14.88 M blocks), real binary | **37 s**, 0.011 CPU cores, 17.8 MiB RSS |
| Max ingest (100 events/page, batch insert) | **≈ 27 000 events/s** per indexer |
| SDK-only ceiling (decode + checkpoint + commit) | **54 752 events/s** |
| At real RPC latency (135 / 300 ms) | **643 / 304 events/s** (RTT-bound) |
| Decode | **0.83 µs/event** |
| Crash mid-range, then restart | **0 lost, 0 duplicated** |
| Cold start to first page | **5.6 ms p50** |

Reproduce: `cargo run --release -p supra_indexer_sdk --example perf_bench -- all`.

## How it works

Each **cycle**:

1. Read the chain tip from `GET /rpc/v3/block`; target = `tip - finality_lag`, capped at `max_blocks_per_cycle` blocks past the slowest event.
2. For each registered event, in registration order, page through `GET /rpc/v3/events/{address}::{module}::{Event}` up to the target with the `x-supra-cursor` header.
3. Every page is stored **in one database transaction together with its checkpoint**.

Progress lives in the `supra_indexer_checkpoints` table (created automatically):

| column       | meaning                                                  |
|--------------|----------------------------------------------------------|
| `event_key`  | `0x<address>::<module>::<Event>`                          |
| `next_block` | first block not yet fully indexed                        |
| `cursor`     | RPC cursor for the next page of an unfinished range       |
| `cursor_end` | exclusive end of that range                               |

After a crash the indexer resumes from the saved cursor, so no events are skipped.

### Guarantees and contract

- **At-least-once per page.** A page whose transaction did not commit is fetched again, so `store_events` must be idempotent (`ON CONFLICT DO NOTHING` on a natural key).
- **Ordering.** Events of one type arrive in block order (validated). All event types advance to the same target each cycle, in registration order. Register events that others depend on first (e.g. `MarketCreated` before `Trade`).
- **A failing page halts the cycle.** The error is logged and shown in `/status`, and the cycle is retried with exponential backoff. Later event types do not run ahead of a stuck one.
- **One instance per contract/database**, enforced by a PostgreSQL advisory lock. A second process exits with `IndexerError::AlreadyRunning`.

## Configuration

`IndexerConfig::new(node_url, contract_address)` plus chained setters:

| setter                 | default | purpose                                                 |
|------------------------|---------|---------------------------------------------------------|
| `start_block`          | `0`     | first block for events without a checkpoint             |
| `force_start_block`    | `false` | rewind every registered event to `start_block` on start |
| `poll_interval`        | `2s`    | sleep between cycles once caught up                     |
| `page_limit`           | `100`   | events per RPC page (Supra RPC serves at most 100)      |
| `finality_lag`         | `5`     | blocks to stay behind the tip                           |
| `max_blocks_per_cycle` | `100000`| window size during catch-up                             |
| `http_timeout`         | `30s`   | per request                                             |
| `retry`                | 3 tries, 500ms base, 30s cap | RPC retries (5xx/408/429/network only) and cycle backoff |
| `instance_lock`        | `true`  | advisory lock (uses one pooled connection)              |

Event names are `module::Event`, resolved against `contract_address`. Pass a fully qualified `0x1::coin::CoinDeposit` to index another address.

Custom response shapes: implement `HasBlockHeight` for your event type and `ExtractEventData<E>` for your response type, then call `register::<E, R, S>(..)`.

## Control and HTTP server

`indexer.handle()` returns a cloneable `IndexerHandle` with `pause()`, `resume()`, `shutdown()`, `status()` and `subscribe()`.

With the `server` feature:

```rust
let indexer = SupraIndexer::new(pool, config)?
    .serve(ServerConfig::new("0.0.0.0:8080".parse()?)
        .admin_token(std::env::var("ADMIN_TOKEN").ok())
        .max_ready_lag(100));
```

| route                   | auth   |                                                                  |
|-------------------------|--------|------------------------------------------------------------------|
| `GET /health`           | none   | liveness                                                         |
| `GET /ready`            | none   | 200 when DB reachable, caught up, lag ≤ `max_ready_lag`; else 503 |
| `GET /status`           | none   | progress JSON                                                    |
| `POST /admin/pause`     | bearer | stop starting cycles                                             |
| `POST /admin/resume`    | bearer | resume                                                           |
| `POST /admin/shutdown`  | bearer | graceful stop                                                    |

The admin routes are only mounted when a token is configured.

## Testing

```bash
cargo test -p supra_indexer_sdk --all-features
# include PostgreSQL end-to-end tests:
TEST_DATABASE_URL=postgres://postgres:postgres@localhost/supra_indexer_sdk_test \
  cargo test -p supra_indexer_sdk --all-features
```

## Upgrading from 0.1

See [CHANGELOG.md](supra-indexer-rs/CHANGELOG.md).

## License

MIT OR Apache-2.0
