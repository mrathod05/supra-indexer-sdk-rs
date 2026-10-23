## s5_sparse_catchup — Catching up over a long, mostly empty block range

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:42:57Z_

- 20 events spread over 14000000 blocks (the shape of the real testnet `TokenCreated` history), one event type, stub latency 5 ms.
- Each window costs 1 block request + 1 events request (+1 for a non-empty range). calculated wall = requests × 270 ms (median RTT measured to rpc-testnet.supra.com from the bench host).
- Bench process peak RSS so far: 40 MiB.

### Results

| max_blocks_per_cycle | cycles | RPC requests | wall s measured | blocks/s measured | wall s calculated @270 ms |
|---|---|---|---|---|---|
| 10000 | 1401 | 2822 | 21.069 | 664487.0 | 761.94 |
| 100000 | 141 | 302 | 2.165 | 6465986.0 | 81.54 |
| 1000000 | 15 | 44 | 0.345 | 40629180.0 | 11.88 |
| 14000000 | 2 | 5 | 0.042 | 330204894.0 | 1.35 |

