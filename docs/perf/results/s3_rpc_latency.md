## s3_rpc_latency — Throughput vs RPC round-trip latency

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:49:18Z_

- One event type, batch-insert store, 100 events/page (the Supra RPC cap). Stub delays every response by L ms.
- calculated = 100 / (L + t0) where t0 is the measured L=0 page cycle; the SDK keeps one request in flight per indexer.
- Bench process peak RSS so far: 32 MiB.

### Results

| L ms | events | events/s measured | events/s calculated | page cycle p50 ms | RPC requests | wall s |
|---|---|---|---|---|---|---|
| 0 | 20000 | 27281.0 | 31206.0 | 3.205 | 202 | 0.733 |
| 5 | 10000 | 6038.0 | 12188.0 | 16.596 | 102 | 1.656 |
| 20 | 10000 | 2761.0 | 4310.0 | 35.758 | 102 | 3.621 |
| 50 | 5000 | 1501.0 | 1880.0 | 65.554 | 52 | 3.332 |
| 135 | 5000 | 647.0 | 724.0 | 148.536 | 52 | 7.731 |
| 300 | 5000 | 304.0 | 330.0 | 316.218 | 52 | 16.424 |

