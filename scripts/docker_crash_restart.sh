#!/bin/sh
# Unlike /admin/crash (which just makes a live process ignore RPCs, useful
# for chaos-testing while keeping logs/inspection easy), this does a REAL
# `docker compose kill -9` + restart, forcing an actual process exit and
# WAL replay on restart -- the strongest form of the "crashes" +
# "restarts" failure modes.
#
# Usage: ./scripts/docker_crash_restart.sh [node_name] [client_port]
set -eu

NODE="${1:-node1}"
PORT="${2:-8001}"

echo "== before crash =="
curl -s "localhost:$PORT/status"; echo

echo "== docker compose kill -s SIGKILL $NODE =="
docker compose kill -s SIGKILL "$NODE"
sleep 2
echo "(port should now be unreachable)"
curl -s --max-time 2 "localhost:$PORT/status" || echo "confirmed: unreachable"

echo "== docker compose start $NODE (restart from the SAME volume -> WAL replay) =="
docker compose start "$NODE"
sleep 3

echo "== after restart, check logs for WAL recovery line =="
docker compose logs --tail 5 "$NODE" | grep -i recovered || true

echo "== status after restart =="
curl -s "localhost:$PORT/status"; echo
