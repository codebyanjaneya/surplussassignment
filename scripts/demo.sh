#!/usr/bin/env bash
# Demo scenarios. Usage: ./scripts/demo.sh <scenario>   (API defaults to http://localhost:3001)
#   duplicate | temp-failure | perm-failure | parallel | crash | burst | all
set -euo pipefail
API="${API:-http://localhost:3001}"
RUN="${RUN_ID:-$(date +%s)}"   # makes eventIds unique per run so scenarios can be repeated

post() { # post <eventId> <simulate|""> [orderId]
  local sim=""; [ -n "${2:-}" ] && sim=", \"simulate\": \"$2\""
  curl -s -o /dev/null -w "%{http_code} " -X POST "$API/webhooks" -H 'content-type: application/json' \
    -d "{\"eventId\":\"$1\",\"type\":\"order.created\",\"data\":{\"orderId\":\"ORD-${3:-$1}\",\"customerId\":\"CUS-42\"$sim}}"
}
show() { curl -s "$API/events/$1" | python3 "$(dirname "$0")/show.py"; }
orders() { curl -s "$API/processed-orders" | python3 -c "
import json,sys; rows=[r for r in json.load(sys.stdin) if r['eventId'].startswith('$1')]
print(f'  processed_orders rows for $1*: {len(rows)}')"; }
wait_status() { # wait_status <eventId> <status> [timeout]
  for _ in $(seq 1 "${3:-90}"); do
    [ "$(curl -s "$API/events/$1" | python3 -c 'import json,sys;print(json.load(sys.stdin)["status"])')" = "$2" ] && return 0; sleep 1
  done; echo "  timed out waiting for $1 -> $2"; return 1
}

duplicate() {
  echo "== Duplicate: 20 simultaneous deliveries of one eventId =="
  for _ in $(seq 20); do post "dup-$RUN" "" & done; wait; echo
  wait_status "dup-$RUN" succeeded 30; sleep 1; show "dup-$RUN"; orders "dup-$RUN"
}
temp_failure() {
  echo "== Temporary failure: fail_then_succeed:2 (fails attempts 1,2, succeeds on 3) =="
  post "temp-$RUN" "fail_then_succeed:2"; echo; wait_status "temp-$RUN" succeeded 60; show "temp-$RUN"; orders "temp-$RUN"
}
perm_failure() {
  echo "== Permanent failure: always_fail (stops after MAX_ATTEMPTS=5), then manual retry =="
  post "perm-$RUN" "always_fail"; echo; wait_status "perm-$RUN" failed 120; show "perm-$RUN"; orders "perm-$RUN"
  echo "  -> manual retry (fresh budget, attempt numbers continue 6,7,...):"
  curl -s -X POST "$API/events/perm-$RUN/retry"; echo
  wait_status "perm-$RUN" failed 120; show "perm-$RUN"
}
parallel() {
  echo "== Parallel: 8 x slow:5 across 2 workers (4 slots each) -> ~5s total, not 40s =="
  local t0=$SECONDS
  for i in $(seq 8); do post "par-$RUN-$i" "slow:5"; done; echo
  for i in $(seq 8); do wait_status "par-$RUN-$i" succeeded 60; done
  echo "  all 8 finished in $((SECONDS - t0))s"; for i in 1 2 3 4 5 6 7 8; do curl -s "$API/events/par-$RUN-$i" | python3 -c 'import json,sys;e=json.load(sys.stdin);print("   ",e["eventId"],e["history"][0]["workerId"])'; done
  orders "par-$RUN"
}
crash() {
  echo "== Worker crash: slow:30 event, kill the worker that picked it up =="
  post "crash-$RUN" "slow:30"; echo; sleep 3
  local w; w=$(curl -s "$API/events/crash-$RUN" | python3 -c 'import json,sys;print(json.load(sys.stdin)["history"][0]["workerId"])')
  echo "  claimed by $w -> docker kill $w"
  docker kill "$(docker compose ps -q "$w")" >/dev/null
  wait_status "crash-$RUN" succeeded 120; show "crash-$RUN"; orders "crash-$RUN"
  echo "  restarting $w"; docker compose start "$w" >/dev/null
}
burst() {
  echo "== Burst: 500 events at once =="
  local t0=$SECONDS
  seq 1 500 | xargs -P 50 -I{} curl -s -o /dev/null -X POST "$API/webhooks" -H 'content-type: application/json' \
    -d "{\"eventId\":\"burst-$RUN-{}\",\"type\":\"order.created\",\"data\":{\"orderId\":\"ORD-b{}\"}}"
  echo "  submitted in $((SECONDS - t0))s; waiting for terminal states..."
  for _ in $(seq 120); do
    s=$(curl -s "$API/stats"); echo "  $s"
    python3 -c "import json,sys;d=json.loads('$s');sys.exit(0 if d['pending']+d['processing']==0 else 1)" && break; sleep 2
  done
  orders "burst-$RUN"
}

case "${1:-all}" in
  duplicate) duplicate;; temp-failure) temp_failure;; perm-failure) perm_failure;;
  parallel) parallel;; crash) crash;; burst) burst;;
  all) duplicate; temp_failure; perm_failure; parallel; crash; burst;;
  *) echo "unknown scenario"; exit 1;;
esac
