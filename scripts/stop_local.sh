#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="$DIR/.run/pids"

echo "== Stopping raftkv-server nodes =="

if [ -f "$PID_FILE" ]; then
    while IFS= read -r pid; do
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "Stopping PID $pid..."
            kill "$pid" 2>/dev/null || true
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

# Also clean up any lingering raftkv-server processes started from this directory
pkill -f "$DIR/raftkv-server" 2>/dev/null || true

echo "All nodes stopped."
