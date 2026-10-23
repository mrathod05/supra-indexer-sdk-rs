# Supra Indexer SDK: Performance Report

| | |
|---|---|
| Scope | `supra_indexer_sdk` 0.2.0: the real `SupraIndexer`, `SupraClient` and response types |
| Harness | `supra-indexer-rs/examples/perf_bench/` (`cargo run --release -p supra_indexer_sdk --example perf_bench -- all`) |
| Live script | [`scripts/live_example.sh`](scripts/live_example.sh): the real `sdk-example` binary against Supra testnet |
| Raw data | [`results/`](results/). Every number here is copied from one of those files |
| Host | Intel i5-13400 (10C/16T), 23 GiB RAM, SATA SSD, PostgreSQL 14.24 on the same host (`synchronous_commit=on`, `fsync=on`, `shared_buffers=128MB`) |
| Date | 2026-10-23 |

**Labels.**
- `[MEASURED]`: produced by the harness or script.
- `[CALCULATED]`: arithmetic on measured numbers, with the formula given.

All stub-RPC runs use an in-process server that reproduces the real Supra RPC v3 behaviour: exclusive `end_height`, 100-event page cap, `x-supra-cursor`.

---

## Headline numbers

| What | Number | Label |
|---|---|---|
| **Full testnet history, real binary**: 14.88 M blocks of `TokenCreated` | **37 s** (402 k blocks/s), 152 RPC requests | `[MEASURED]` live_example |
| Resource use during that run | **0.011 CPU cores, 17.8 MiB peak RSS**, clean SIGTERM exit (0) | `[MEASURED]` live_example |
| 1 M real blocks of `0x1::coin::CoinDeposit` + `CoinWithdraw` (1 221 events) | **12.2 s**, 43 RPC requests, 0.009 cores | `[MEASURED]` live |
| Max ingest throughput: 100 events/page, batch-insert store, zero-latency RPC | **≈ 27 000 events/s** (one indexer, one core-equivalent) | `[MEASURED]` s2/s3/s4 |
| SDK ceiling (no-op store: decode + checkpoint + commit only) | **54 752 events/s** | `[MEASURED]` s2 |
| Throughput at real-world RPC latency (135 ms / 300 ms) | **643 / 304 events/s** | `[MEASURED]` s3 |
| Decode cost per event | **0.83 µs** (1.16 M events/core·s), scales to 9 M/s on 16 threads | `[MEASURED]` s1 |
| SDK overhead per page (checkpoint UPDATE + COMMIT + hand-off) | **≈ 1.1–1.4 ms p50** | `[MEASURED]` s2 |
| Crash mid-range, then restart | **0 events lost, 0 duplicated**, resumed at 26.9 k events/s | `[MEASURED]` s7 |
| Cold start to first indexed page | **5.6 ms p50**, 12 ms p99 | `[MEASURED]` s7 |
| Full reindex over existing rows (idempotent replay) | **39 656 events/s** | `[MEASURED]` s7 |
| RPC client headroom | 11 292 req/s at 256 concurrent requests (20 ms RTT); 218 k req/s ceiling | `[MEASURED]` s6 |

**Context.**
- Supra testnet produces **4.4–4.7 blocks/s** `[MEASURED]`, and the indexed contracts emit a handful of events per day.
- The SDK runs at about **0.01 cores** in that setting and has 10³–10⁴× headroom over the live event rate.
- The binding limit is **RPC round-trip time**, not CPU or the database.

---

## Found and fixed by benchmarking

| # | Finding | Before | After |
|---|---|---|---|
| F1 | **Silent event loss with `page_limit > 100`.** Supra caps pages at 100 events. The SDK treated a short page as "range finished" and skipped the rest of the range. | Events lost when `page_limit` was set above 100 | The range ends only on an empty page or a missing cursor. Regression test: the mock caps pages at 2 while the SDK asks for 5 |
| F2 | **Lagging replica 404.** `Requested data is not yet available on this node` failed the whole cycle. | Cycle error, then backoff | Retried like a 5xx |
| F3 | **The chain tip was re-fetched for every catch-up window.** | 300 requests, **90.1 s** for the full history | 152 requests, **37.0 s**: **2.4× faster** `[MEASURED]` (`live_example.before_fix1` vs `live_example`) |

---

## Results by scenario

### S1: Decode CPU `[MEASURED]`

| payload | bytes/event | p50 µs/event | events/core·s |
|---|---|---|---|
| token (4 fields) | 458 | 0.54 | 1.77 M |
| trade (9 fields) | 525 | 0.83 | 1.16 M |

Thread scaling for the trade payload is 2.0× / 3.9× / 6.9× / 8.5× at 2 / 4 / 8 / 16 threads. That is near-linear up to the 10 physical cores. CPU is never the bottleneck.

### S2: Page size × store implementation, zero-latency RPC, 20 000 events `[MEASURED]`

| store | 1 event/page | 10 | 25 | 50 | **100** |
|---|---|---|---|---|---|
| no-op (SDK only) | 714 | 8 317 | 17 212 | 26 987 | **54 752** |
| batch insert | 636 | 7 551 | 10 567 | 16 283 | **26 557** |
| row-by-row insert | 756 | 6 898 | 10 978 | 14 003 | **12 911** |

All values are events/s.
- Throughput tracks page size, because each page costs one transaction commit (fsync).
- At 100 events/page a single multi-row INSERT is **2× faster** than row-by-row. Use it in your `EventStore`.
- WAL is about 755 B per stored event.

### S3: RPC latency `[MEASURED]` / `[CALCULATED]`

| RTT | 0 ms | 5 ms | 20 ms | 50 ms | 135 ms | 300 ms |
|---|---|---|---|---|---|---|
| events/s measured | 27 213 | 6 038 | 2 761 | 1 501 | 643 | 304 |
| events/s calculated, `100 / (RTT + t₀)` | 30 953 | 12 150 | 4 305 | 1 879 | 723 | 330 |

- One indexer keeps one request in flight, so throughput ≈ 100 events / RTT.
- The measured-to-calculated gap at small RTT is a host effect, not SDK code. The CPU governor is `powersave` (idle cores drop to 800 MHz). After every idle wait the r2d2 checkout `SELECT 1` went from 30 µs to 400 µs and the page commit from 3 ms to 13 ms.
- Expect production hosts with the `performance` governor to sit closer to the calculated line.

### S4: Number of event types, 26 000 events `[MEASURED]`

| event types | 1 | 4 | 13 |
|---|---|---|---|
| events/s @ 0 ms RTT | 28 660 | 27 113 | 27 239 |
| events/s @ 20 ms RTT | 2 783 | 2 703 | 2 714 |

Throughput is flat as event types are added. Types are drained in order each cycle, which preserves cross-event ordering. Each type adds only one request per window.

### S5: Sparse catch-up, 20 events over 14 M blocks `[MEASURED]` / `[CALCULATED]`

| max_blocks_per_cycle | RPC requests | wall @ 5 ms stub | wall @ 270 ms RTT, calculated |
|---|---|---|---|
| 10 000 | 1 422 | 11.5 s | 384 s |
| 100 000 (default) | 162 | 1.7 s | 44 s |
| 1 000 000 | 30 | 0.29 s | 8.1 s |

For contracts with sparse history, raise `max_blocks_per_cycle` to cut catch-up time roughly 5×.

### S6: RPC client capacity, 100-event pages of about 68 KB `[MEASURED]`

At 20 ms RTT, throughput scales linearly from 46 req/s (C=1) to **11 292 req/s (C=256)** with flat p99 (22–25 ms). With 0 ms RTT the ceiling is **218 k req/s** at C=64. The HTTP client leaves plenty of room for future parallel fetching.

### S7: Startup, crash recovery and replay `[MEASURED]`

- Startup (schema check, seed, advisory lock, first page): **p50 5.6 ms / p99 12 ms** over 30 runs.
- Crash test:
  - Setup: a store error on page 100 of 200 rolled back that page. Before the crash, 9 900 rows had committed with their checkpoint.
  - After restart there were **20 000 / 20 000 rows**: 0 missing and 0 duplicated (enforced by the primary key).
  - The restart resumed from the saved cursor in 0.38 s.
- Replay with `force_start_block` over existing rows ran at **39 656 events/s**, against 28 752 events/s for a fresh load.

### Live: real Supra testnet `[MEASURED]`

| | |
|---|---|
| RTT `GET /rpc/v3/block` | p50 296 ms, p99 334 ms |
| RTT `GET /rpc/v3/events` (40 KB page) | p50 313 ms, p99 888 ms |
| Chain production | 4.72 blocks/s |
| `sdk-example`, 135 000 000 → tip (14 883 413 blocks) | 37.0 s, 149 cycles, 152 requests, 0.011 cores, 17.8 MiB RSS |

---

## Reproduce

```bash
createdb supra_indexer_sdk_perf          # name must contain "perf" or "test"
cargo run --release -p supra_indexer_sdk --example perf_bench -- all     # ~4 min
cargo run --release -p supra_indexer_sdk --example perf_bench -- live    # needs network
DATABASE_URL=postgres://.../supra_indexer_example docs/perf/scripts/live_example.sh
```

**Caveats.**
- Each cell is a single run, so treat ±15 % as noise.
- The database, stub RPC and indexer share one host, which makes database-bound numbers an upper bound for a co-located deployment.
- Live RTT depends on the network path from the host to `rpc-testnet.supra.com`.
