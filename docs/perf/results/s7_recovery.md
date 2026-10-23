## s7_recovery — Startup latency, crash recovery and idempotent replay

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:42:57Z_

- Crash run stopped after 0.366s with error: event store for `0xbe::bench::E0` failed: injected failure on store call 100
- `bench_trades.tx_hash` is the primary key, so `rows after restart == events` means nothing was lost or duplicated.
- Bench process peak RSS so far: 150 MiB.

### Startup: `run()` to first completed cycle (schema check, seed, advisory lock, 1 block + 2 events requests, 1 page commit)

| runs | p50 ms | p90 ms | p99 ms | max ms |
|---|---|---|---|---|
| 30 | 5.642 | 10.132 | 11.996 | 11.996 |

### Crash on a page mid-range (store error after its INSERT, transaction rolled back), then restart

| events | failed page | rows committed before crash | rows after restart | missing | resume wall s | resume events/s |
|---|---|---|---|---|---|---|
| 20000 | 100 | 9900 | 20000 | 0 | 0.376 | 26868.0 |

### Idempotent replay: `force_start_block` reindex over existing rows vs a fresh load

| run | events | events/s | rows after |
|---|---|---|---|
| fresh load | 20000 | 28752.0 | 20000 |
| replay (all ON CONFLICT) | 20000 | 39656.0 | 20000 |

