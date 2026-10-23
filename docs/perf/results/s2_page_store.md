## s2_page_store — Throughput vs page size and store implementation (zero-latency RPC)

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:42:57Z_

- One event type, 20000 trade events, 2 events/block, stub RPC latency 0 ms. Each run: real `SupraIndexer` from an empty checkpoint to `Live`.
- page cycle = gap between consecutive events requests = RPC + decode + store + checkpoint UPDATE + COMMIT. SDK overhead = page cycle p50 − store p50.
- Bench process peak RSS so far: 34 MiB.

### Results

| store | events/page | events/s | pages/s | page cycle p50 ms | page cycle p99 ms | store p50 ms | SDK overhead p50 ms | bench CPU cores | PG CPU cores | WAL B/event |
|---|---|---|---|---|---|---|---|---|---|---|
| noop | 1 | 714.0 | 714.0 | 1.199 | 4.455 | 0.0 | 1.198 | 0.291 | 0.301 | 131 |
| noop | 10 | 8317.0 | 832.0 | 1.144 | 2.176 | 0.0 | 1.144 | 0.353 | 0.34 | 13 |
| noop | 25 | 17212.0 | 689.0 | 1.394 | 2.685 | 0.0 | 1.394 | 0.429 | 0.343 | 5 |
| noop | 50 | 26987.0 | 541.0 | 1.668 | 4.518 | 0.0 | 1.668 | 0.43 | 0.268 | 2 |
| noop | 100 | 54752.0 | 550.0 | 1.362 | 4.722 | 0.0 | 1.362 | 0.407 | 0.19 | 1 |
| batch insert | 1 | 636.0 | 636.0 | 1.325 | 4.82 | 0.219 | 1.106 | 0.252 | 0.366 | 887 |
| batch insert | 10 | 7551.0 | 756.0 | 1.157 | 3.048 | 0.32 | 0.836 | 0.204 | 0.43 | 765 |
| batch insert | 25 | 10567.0 | 423.0 | 1.811 | 5.926 | 0.749 | 1.061 | 0.148 | 0.443 | 757 |
| batch insert | 50 | 16283.0 | 326.0 | 2.496 | 6.802 | 1.15 | 1.346 | 0.13 | 0.503 | 754 |
| batch insert | 100 | 26557.0 | 267.0 | 3.313 | 7.854 | 1.894 | 1.419 | 0.132 | 0.541 | 753 |
| row-by-row insert | 1 | 756.0 | 756.0 | 1.108 | 4.297 | 0.167 | 0.941 | 0.268 | 0.365 | 883 |
| row-by-row insert | 10 | 6898.0 | 690.0 | 1.281 | 4.072 | 0.522 | 0.759 | 0.227 | 0.468 | 765 |
| row-by-row insert | 25 | 10978.0 | 440.0 | 2.091 | 5.008 | 1.193 | 0.898 | 0.236 | 0.592 | 757 |
| row-by-row insert | 50 | 14003.0 | 281.0 | 3.277 | 6.572 | 2.24 | 1.036 | 0.23 | 0.58 | 754 |
| row-by-row insert | 100 | 12911.0 | 130.0 | 6.031 | 26.583 | 4.46 | 1.572 | 0.213 | 0.625 | 753 |

