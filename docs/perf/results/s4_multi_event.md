## s4_multi_event — Throughput vs number of event types

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:42:57Z_

- 26000 trade events in total, round-robin over k event types, 2 events/block, batch-insert store, 100 events/page.
- Event types are drained one after another each cycle (ordering guarantee), so total throughput does not grow with k.
- Bench process peak RSS so far: 40 MiB.

### Results

| event types | L ms | events/s | RPC requests | cycles | wall s |
|---|---|---|---|---|---|
| 1 | 0 | 28660.0 | 262 | 1 | 0.907 |
| 4 | 0 | 27113.0 | 265 | 1 | 0.959 |
| 13 | 0 | 27239.0 | 274 | 1 | 0.955 |
| 1 | 20 | 2783.0 | 262 | 1 | 9.343 |
| 4 | 20 | 2703.0 | 265 | 1 | 9.618 |
| 13 | 20 | 2714.0 | 274 | 1 | 9.579 |

