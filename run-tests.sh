#!/usr/bin/env bash
set -u

root='/Users/guvensoft/Desktop/QuicklyDEV/rouchdb'
server="$root/target/release/rouchdb-server"
out="$root/test-results"
mkdir -p "$out"
run_id=$(python3 -c 'import time; print(int(time.time()))')
failures=0

run_one() {
  local name="$1"
  local script="$2"
  local port="$3"
  local db="$root/${name}-${run_id}.redb"
  local log="$out/${name}-server.log"
  local result="$out/${name}.json"
  local stderr="$out/${name}.stderr"
  local pid
  local ready=0
  local attempt
  nohup "$server" "$db" --host 127.0.0.1 --port "$port" --db-name appserver </dev/null > "$log" 2>&1 &
  pid=$!
  printf '%s\n' "$pid" > "$out/${name}.pid"
  for attempt in $(seq 1 50); do
    if curl -fsS "http://127.0.0.1:${port}/appserver" -o /dev/null 2> "$out/${name}-probe.stderr"; then
      ready=1
      break
    fi
    sleep 0.2
  done
  if [ "$ready" -ne 1 ]; then
    printf '%s\n' '{"status":"KALDI","error":"Sunucu 10 saniye içinde hazır olmadı"}' > "$result"
    kill "$pid" >/dev/null 2>&1 || true
    return 1
  fi
  REMOTE_URL="http://127.0.0.1:${port}/appserver" node "$root/pouchdb-tests/$script" > "$result" 2> "$stderr"
  local rc=$?
  printf '%s\n' "$rc" > "$out/${name}.exit"
  kill "$pid" >/dev/null 2>&1 || true
  rm -f "$db"
  if [ "$rc" -ne 0 ]; then
    return 1
  fi
  return 0
}

run_one t1 t1-push.js 16001 || failures=$((failures + 1))
run_one t2 t2-pull.js 16002 || failures=$((failures + 1))
run_one t3 t3-bidirectional.js 16003 || failures=$((failures + 1))
run_one t4 t4-live.js 16004 || failures=$((failures + 1))
run_one t5 t5-tombstone.js 16005 || failures=$((failures + 1))
run_one t6 t6-conflict.js 16006 || failures=$((failures + 1))
run_one t7 t7-checkpoint.js 16007 || failures=$((failures + 1))
run_one t8 t8-scale-push.js 16008 || failures=$((failures + 1))
run_one t9 t9-scale-tombstones.js 16009 || failures=$((failures + 1))
printf 'TEST_RUNNER_FAILURES=%s\n' "$failures"
