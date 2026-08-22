#!/bin/sh
# Fault-injection script for a LIVE cluster (Docker Compose or EC2), using
# the /admin/* HTTP endpoints exposed by internal/transport/admin.go. This
# exercises the same 5 failure modes as the in-process Go test suite
# (internal/raft/raft_test.go), but over real sockets/containers instead
# of the in-memory FakeNetwork.
#
# Usage: ./scripts/fault_injection.sh [base_client_port] [base_raft_port]
#   defaults assume docker-compose.yml's host port mappings: 8001-8005 / 7001-7005
#
# Requires: curl
set -eu

CPORT_BASE="${1:-800}"   # node N's client port is ${CPORT_BASE}N -> 8001..8005
RPORT_BASE="${2:-700}"   # node N's raft/admin port is ${RPORT_BASE}N -> 7001..7005

cport() { echo "${CPORT_BASE}$1"; }
rport() { echo "${RPORT_BASE}$1"; }

status() { curl -s --max-time 2 "localhost:$(cport "$1")/status"; }
put()    { curl -s --max-time 2 -X POST "localhost:$(cport "$1")/kv/put" -d "$2"; }
get()    { curl -s --max-time 2 "localhost:$(cport "$1")/kv/get?key=$2"; }

find_leader() {
  for i in 1 2 3 4 5; do
    s=$(status "$i" || true)
    case "$s" in *'"state":"Leader"'*) echo "$i"; return 0 ;; esac
  done
  echo ""
}

echo "== baseline =="
for i in 1 2 3 4 5; do echo "node$i: $(status "$i")"; done
LEADER=$(find_leader)
echo "leader: node$LEADER"

echo ""
echo "== fault mode 1/5: RPC delay (leader's outbound RPCs +200ms) =="
curl -s -X POST "localhost:$(rport "$LEADER")/admin/delay" -d '{"ms":200}'; echo
put "$LEADER" '{"key":"chaos1","value":"delayed-but-fine"}'; echo
curl -s -X POST "localhost:$(rport "$LEADER")/admin/delay" -d '{"ms":0}'; echo

echo ""
echo "== fault mode 2/5: RPC loss (leader drops 30% of outbound RPCs) =="
curl -s -X POST "localhost:$(rport "$LEADER")/admin/droprate" -d '{"rate":0.3}'; echo
put "$LEADER" '{"key":"chaos2","value":"lossy-but-fine"}'; echo
curl -s -X POST "localhost:$(rport "$LEADER")/admin/droprate" -d '{"rate":0}'; echo

echo ""
echo "== fault mode 3/5: network partition (isolate current leader from everyone) =="
OTHERS=""
for i in 1 2 3 4 5; do [ "$i" != "$LEADER" ] && OTHERS="$OTHERS\"node$i\","; done
OTHERS="[${OTHERS%,}]"
curl -s -X POST "localhost:$(rport "$LEADER")/admin/partition" -d "{\"peers\":$OTHERS}"; echo
sleep 2
echo "cluster view without the isolated node:"
for i in 1 2 3 4 5; do [ "$i" != "$LEADER" ] && echo "node$i: $(status "$i")"; done
NEWLEADER=""
for i in 1 2 3 4 5; do
  [ "$i" = "$LEADER" ] && continue
  s=$(status "$i")
  case "$s" in *'"state":"Leader"'*) NEWLEADER=$i ;; esac
done
echo "new leader while old leader partitioned: node$NEWLEADER"
put "$NEWLEADER" '{"key":"chaos3","value":"partition-survived"}'; echo
echo "healing partition..."
curl -s -X POST "localhost:$(rport "$LEADER")/admin/heal" -d '{}'; echo

echo ""
echo "== fault mode 4/5: crash (via /admin/crash -- the process stays up but drops all traffic, easiest to chaos-test without losing container logs; see scripts/docker_crash_restart.sh for a REAL kill -9 + WAL replay demo) =="
curl -s -X POST "localhost:$(rport "$NEWLEADER")/admin/crash"; echo
sleep 2
echo "cluster view with node$NEWLEADER crashed:"
for i in 1 2 3 4 5; do [ "$i" != "$NEWLEADER" ] && echo "node$i: $(status "$i")"; done

echo ""
echo "== fault mode 5/5: restart (recover from simulated crash) =="
curl -s -X POST "localhost:$(rport "$NEWLEADER")/admin/recover"; echo
sleep 2
echo "node$NEWLEADER after recovery: $(status "$NEWLEADER")"
echo "did it catch up? $(get "$NEWLEADER" chaos3)"

echo ""
echo "All 5 failure modes exercised against the live cluster."
