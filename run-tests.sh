#!/usr/bin/env bash
set -u

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
server="${ROUCHDB_SERVER_BIN:-$root/target/release/rouchdb-server}"
out="${ROUCHDB_TEST_OUTPUT_DIR:-$root/test-results}"
mkdir -p "$out"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/rouchdb-pouchdb.XXXXXX")
active_pid=""
cleanup() {
  if [ -n "$active_pid" ]; then
    kill "$active_pid" >/dev/null 2>&1 || true
    wait "$active_pid" 2>/dev/null || true
  fi
  rm -rf -- "$scratch"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
failures=0

run_one() {
  local name="$1"
  local script="$2"
  local port=""
  local db="$scratch/${name}.redb"
  local log="$out/${name}-server.log"
  local result="$out/${name}.json"
  local stderr="$out/${name}.stderr"
  local pid
  local ready=0
  local attempt
  nohup "$server" "$db" --host 127.0.0.1 --port 0 --db-name appserver </dev/null > "$log" 2>&1 &
  pid=$!
  active_pid="$pid"
  printf '%s\n' "$pid" > "$out/${name}.pid"
  for attempt in $(seq 1 50); do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    port=$(sed -n 's|^RouchDB server listening on http://127\.0\.0\.1:\([0-9][0-9]*\)$|\1|p' "$log")
    if [ -n "$port" ] && curl --max-time 1 -fsS "http://127.0.0.1:${port}/appserver" -o /dev/null 2> "$out/${name}-probe.stderr"; then
      ready=1
      break
    fi
    sleep 0.2
  done
  if [ "$ready" -ne 1 ]; then
    printf '%s\n' '{"status":"KALDI","error":"Sunucu 10 saniye içinde hazır olmadı"}' > "$result"
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
    active_pid=""
    return 1
  fi
  REMOTE_URL="http://127.0.0.1:${port}/appserver" node "$root/pouchdb-tests/$script" > "$result" 2> "$stderr"
  local rc=$?
  printf '%s\n' "$rc" > "$out/${name}.exit"
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" 2>/dev/null || true
  active_pid=""
  rm -f "$db"
  if [ "$rc" -ne 0 ]; then
    return 1
  fi
  return 0
}

run_one t1 t1-push.js || failures=$((failures + 1))
run_one t2 t2-pull.js || failures=$((failures + 1))
run_one t3 t3-bidirectional.js || failures=$((failures + 1))
run_one t4 t4-live.js || failures=$((failures + 1))
run_one t5 t5-tombstone.js || failures=$((failures + 1))
run_one t6 t6-conflict.js || failures=$((failures + 1))
run_one t7 t7-checkpoint.js || failures=$((failures + 1))
run_one t8 t8-scale-push.js || failures=$((failures + 1))
run_one t9 t9-scale-tombstones.js || failures=$((failures + 1))
printf 'TEST_RUNNER_FAILURES=%s\n' "$failures"
exit "$((failures > 0))"
