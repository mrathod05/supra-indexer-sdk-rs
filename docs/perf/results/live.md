## live — Real SupraIndexer against the real Supra RPC

_Host: 13th Gen Intel(R) Core(TM) i5-13400 ×16 · 23 GiB RAM · PostgreSQL 14.24 (Ubuntu 14.24-1.pgdg22.04+2) (max_connections=100, shared_buffers=128MB, synchronous_commit=on, fsync=on, wal_level=replica, max_wal_size=1GB, commit_delay=0) · disk rotational=0 · 2026-10-23T06:55:25Z_

- RPC: https://rpc-testnet.supra.com. Measured from the bench host over the public internet.
- Final status: chain_height=Some(149883328) indexed_height=Some(149883323) lag=Some(5)
- Bench process peak RSS so far: 20 MiB.

### RPC round-trip time

| request | samples | p50 ms | p90 ms | p99 ms | max ms |
|---|---|---|---|---|---|
| GET /rpc/v3/block | 30 | 295.761 | 303.072 | 334.079 | 334.079 |
| GET /rpc/v3/events (page of 39672 B) | 30 | 312.767 | 319.424 | 887.701 | 887.701 |

### Chain production

| window s | blocks | blocks/s |
|---|---|---|
| 30 | 143 | 4.72 |

### Catch-up over the last 1000000 blocks: 0x1::coin::CoinDeposit + 0x1::coin::CoinWithdraw (default config: 100 events/page, 100 000 blocks/cycle, finality lag 5)

| blocks | events | pages | cycles | RPC requests | wall s | blocks/s | events/s | avg ms/request | bench CPU cores |
|---|---|---|---|---|---|---|---|---|---|
| 999999 | 1221 | 42 | 10 | 43 | 12.155 | 82268.0 | 100.0 | 282.684 | 0.009 |

