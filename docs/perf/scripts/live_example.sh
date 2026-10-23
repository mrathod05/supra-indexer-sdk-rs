#!/usr/bin/env bash
# Run the real `sdk-example` binary (release) against Supra testnet from
# START_BLOCK to the chain tip and record wall time, CPU, RSS and /status.
#
#   DATABASE_URL=postgres://postgres:postgres@localhost/supra_indexer_example \
#     docs/perf/scripts/live_example.sh
#
# Writes docs/perf/results/live_example.{json,md}. Rewinds the example's
# checkpoint (FORCE_START_BLOCK=true); stored rows are kept (idempotent store).
set -euo pipefail

: "${DATABASE_URL:?set DATABASE_URL}"
START_BLOCK="${START_BLOCK:-135000000}"
PORT="${PORT:-18090}"
OUT_DIR="docs/perf/results"
CLK_TCK="$(getconf CLK_TCK)"

cargo build --release -q -p sdk-example
mkdir -p "$OUT_DIR"
LOG="$(mktemp)"

DATABASE_URL="$DATABASE_URL" START_BLOCK="$START_BLOCK" FORCE_START_BLOCK=true \
  HTTP_ADDR="127.0.0.1:$PORT" RUST_LOG=warn \
  ./target/release/sdk-example >"$LOG" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true' EXIT

T0=$(date +%s.%N)
until [ "$(curl -s -o /dev/null -w '%{http_code}' "localhost:$PORT/ready")" = 200 ]; do
  kill -0 "$PID" 2>/dev/null || { cat "$LOG"; exit 1; }
  sleep 0.2
done
T1=$(date +%s.%N)

STATUS="$(curl -s "localhost:$PORT/status")"
read -r UTIME STIME < <(awk '{print $14, $15}' "/proc/$PID/stat")
RSS_KB="$(awk '/VmHWM/{print $2}' "/proc/$PID/status")"
kill -TERM "$PID"
wait "$PID" && EXIT=0 || EXIT=$?
trap - EXIT

python3 - "$STATUS" "$T0" "$T1" "$UTIME" "$STIME" "$CLK_TCK" "$RSS_KB" "$START_BLOCK" "$EXIT" "$OUT_DIR" <<'PY'
import json, sys
status, t0, t1, ut, st, tck, rss, start, exit_code, out = sys.argv[1:]
s = json.loads(status)
wall = float(t1) - float(t0)
cpu_s = (int(ut) + int(st)) / float(tck)
blocks = s["indexed_height"] - int(start) + 1
row = {
    "start_block": int(start),
    "indexed_height": s["indexed_height"],
    "blocks": blocks,
    "events": s["events_indexed"],
    "pages": s["pages_committed"],
    "cycles": s["cycles_completed"],
    "rpc_requests": s["rpc_requests"],
    "wall_s": round(wall, 2),
    "blocks_per_s": round(blocks / wall),
    "avg_ms_per_request": round(wall * 1e3 / max(s["rpc_requests"], 1), 1),
    "cpu_s": round(cpu_s, 2),
    "avg_cpu_cores": round(cpu_s / wall, 3),
    "peak_rss_mib": round(int(rss) / 1024, 1),
    "sigterm_exit_code": int(exit_code),
}
json.dump(row, open(f"{out}/live_example.json", "w"), indent=2)
md = "## live_example — real `sdk-example` binary (release) against rpc-testnet.supra.com\n\n"
md += "- Indexes `token_factory::TokenCreated` from START_BLOCK until `/ready` returns 200, then SIGTERM.\n\n"
md += "| " + " | ".join(row) + " |\n|" + "---|" * len(row) + "\n"
md += "| " + " | ".join(str(v) for v in row.values()) + " |\n"
open(f"{out}/live_example.md", "w").write(md)
print(md)
PY
