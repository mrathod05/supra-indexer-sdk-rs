## s1_decode — CPU cost of decoding an RPC page

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:42:57Z_

- Decodes a 100-event page into `EventsResponse<SupraEvent<T>>` with serde_json, the exact call the SDK makes per page.
- Bench process peak RSS so far: 16 MiB.

### Per-event decode cost (single core)

| payload | bytes/event | pages | p50 µs/event | p99 µs/event | events/core·s |
|---|---|---|---|---|---|
| token (4 fields) | 458 | 20000 | 0.54 | 0.926 | 1772588.0 |
| trade (9 fields) | 525 | 20000 | 0.833 | 0.975 | 1164403.0 |

### Decode throughput vs OS threads (trade payload)

| threads | events/s | speed-up |
|---|---|---|
| 1 | 1063236.0 | 1.0 |
| 2 | 2127744.0 | 2.001 |
| 4 | 4121737.0 | 3.877 |
| 8 | 7335531.0 | 6.899 |
| 16 | 9064926.0 | 8.526 |

