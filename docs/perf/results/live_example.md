## live_example — real `sdk-example` binary (release) against rpc-testnet.supra.com

- Indexes `token_factory::TokenCreated` from START_BLOCK until `/ready` returns 200, then SIGTERM.

| start_block | indexed_height | blocks | events | pages | cycles | rpc_requests | wall_s | blocks_per_s | avg_ms_per_request | cpu_s | avg_cpu_cores | peak_rss_mib | sigterm_exit_code |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 135000000 | 149883412 | 14883413 | 2 | 151 | 149 | 152 | 37.0 | 402254 | 243.4 | 0.4 | 0.011 | 17.8 | 0 |
