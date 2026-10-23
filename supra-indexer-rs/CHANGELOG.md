# Changelog

## 0.2.0

Rewrite based on the production `indexer-tagible-events` indexer, with its checkpoint bugs fixed.

### Added
- The indexer targets `chain tip - finality_lag`, and every event type is driven to the same target each cycle in registration order.
- Catch-up works in bounded windows (`max_blocks_per_cycle`).
- RPC client with:
  - a timeout and a user agent
  - retries only on 5xx, 408, 429 and network errors, using exponential backoff with jitter
  - support for `Retry-After`
  - URL-encoded query parameters
- `x-supra-oldest-block` warning when `start_block` is older than the node's history.
- Graceful shutdown on SIGINT/SIGTERM, plus `IndexerHandle` (pause, resume, shutdown, status, subscribe) and `with_cancellation_token`.
- `IndexerStatus` with a `Reindexing`/`Live` mode, lag, the last error and the failure count.
- `server` feature: `/health`, `/ready`, `/status` and the token-protected `/admin/{pause,resume,shutdown}` routes.
- Single-instance guard via a PostgreSQL advisory lock.
- `SupraEvent<T>` and `EventsResponse<E>` for the standard RPC shape, with `register_event::<Payload, _>()`.
- `force_start_block` for a full reindex.
- `utils::normalize_hex_to_fixed`.
- Unit tests, plus PostgreSQL end-to-end tests (`TEST_DATABASE_URL`).
- `IndexerStatus` counters: `events_indexed`, `pages_committed`, `cycles_completed`, `rpc_requests`.
- Performance harness (`examples/perf_bench`, scenarios s1–s7 plus `live`) and `docs/perf/PERFORMANCE_REPORT.md`.

### Fixed
- **Event loss when `page_limit` > 100.** Supra RPC caps pages at 100 events, and a short page was taken as the end of the range. A range now ends only on an empty page or a missing cursor.
- A 404 `not yet available on this node` from a lagging RPC replica is now retried instead of failing the cycle.
- During catch-up the chain tip is fetched once instead of once per window. This halves RPC requests: a full-history catch-up went from 90 s to 37 s.
- Events are no longer skipped at page boundaries or after a restart mid-range. The checkpoint now stores the cursor of the *next* page and is read back on resume.
- A page and its checkpoint commit in one transaction.
- Fetch errors are no longer swallowed as success.
- Unchecked `u64`/`i64` casts are gone.
- Blocking diesel calls now run on `spawn_blocking`.

### Breaking changes (migrating from 0.1)
- `EventStore::store_events` is now synchronous: `fn store_events(&self, conn: &mut PgConnection, events: &[E]) -> anyhow::Result<()>`. Write through `conn`; you no longer take a connection from a pool. `async-trait` is no longer needed.
- `IndexerConfig` is built with `IndexerConfig::new(node_url, contract)` plus setters:
  - `last_block_height` → `start_block`
  - `fetch_interval_ms` → `poll_interval`
  - the `events` list is gone; registration replaces it
- `SupraIndexer::new(pool, config)` is now sync and returns `Result`.
- `register_collector` + `start_all` → `register` / `register_event` + `run`.
- The checkpoint table `block_heights` is replaced by `supra_indexer_checkpoints`. To carry progress over, insert rows with `event_key = '<contract>::<module>::<Event>'` and `next_block = last_block_height + 1`.
- Modules `processor`, `events`, `database` and `types` were removed. Use the crate-root re-exports or `prelude`.
