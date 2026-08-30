#!/bin/bash
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$DIR"

echo "== Building raftkv-server =="
go build -o raftkv-server ./cmd/server

PID_DIR="$DIR/.run"
LOG_DIR="$DIR/logs"
DATA_BASE_DIR="/tmp/raftkv"

mkdir -p "$PID_DIR" "$LOG_DIR"

# Stop any previously running nodes
if [ -f "$PID_DIR/pids" ]; then
    echo "Stopping existing cluster..."
    "$DIR/scripts/stop_local.sh" 2>/dev/null || true
fi

PEERS="node1=127.0.0.1:7001,node2=127.0.0.1:7002,node3=127.0.0.1:7003,node4=127.0.0.1:7004,node5=127.0.0.1:7005"
CLIENTS="node1=127.0.0.1:8001,node2=127.0.0.1:8002,node3=127.0.0.1:8003,node4=127.0.0.1:8004,node5=127.0.0.1:8005"

echo "== Starting 5-node cluster =="
> "$PID_DIR/pids"

for i in {1..5}; do
    NODE="node$i"
    RPORT=$((7000 + i))
    CPORT=$((8000 + i))
    NDATA="$DATA_BASE_DIR/$NODE"
    mkdir -p "$NDATA"

    NODE_ID="$NODE" \
    PEERS="$PEERS" \
    CLIENT_ADDRS="$CLIENTS" \
    DATA_DIR="$NDATA" \
    RAFT_LISTEN_ADDR=":$RPORT" \
    CLIENT_LISTEN_ADDR=":$CPORT" \
    ./raftkv-server > "$LOG_DIR/$NODE.log" 2>&1 &
    
    PID=$!
    echo "$PID" >> "$PID_DIR/pids"
    echo "Started $NODE (PID: $PID) - Raft: :$RPORT, Client: :$CPORT, Data: $NDATA"
done

echo "Waiting for leader election..."
LEADER=""
for attempt in {1..30}; do
    sleep 0.5
    for i in {1..5}; do
        res=$(curl -s --max-time 1 "http://127.0.0.1:$((8000 + i))/status" 2>/dev/null || true)
        if [[ "$res" == *'"state":"Leader"'* ]]; then
            LEADER="node$i"
            break 2
        fi
    done
done

echo ""
if [ -n "$LEADER" ]; then
    echo "Cluster is healthy! Current leader: $LEADER"
else
    echo "Cluster started, leader election in progress..."
fi

echo ""
echo "Cluster Status:"
for i in {1..5}; do
    status=$(curl -s --max-time 1 "http://127.0.0.1:$((8000 + i))/status" 2>/dev/null || echo '{"error":"offline"}')
    echo "  node$i (http://127.0.0.1:$((8000 + i))): $status"
done

echo ""
echo "You can interact with the cluster via:"
echo "  curl http://127.0.0.1:8001/status"
echo "  curl -X POST http://127.0.0.1:8001/kv/put -d '{\"key\":\"hello\",\"value\":\"world\"}'"
echo "  curl http://127.0.0.1:8003/kv/get?key=hello"
echo "  ./scripts/fault_injection.sh 800 700"
echo "  ./scripts/stop_local.sh"
