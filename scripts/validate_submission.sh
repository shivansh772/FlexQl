#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_BIN="$ROOT_DIR/build/flexql-server"
BENCH_BIN="$ROOT_DIR/build/benchmark"
SERVER_PID=""
SERVER_LOG="/tmp/flexql-validate-server.log"

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

start_server() {
    : >"$SERVER_LOG"
    "$SERVER_BIN" 9000 >"$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    sleep 1

    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "Validation server failed to start." >&2
        if [[ -s "$SERVER_LOG" ]]; then
            echo "--- server log ---" >&2
            sed -n '1,120p' "$SERVER_LOG" >&2 || true
            echo "------------------" >&2
        fi
        if command -v ss >/dev/null 2>&1 && ss -ltn "( sport = :9000 )" 2>/dev/null | grep -q ":9000"; then
            echo "Port 9000 is already in use. Stop the existing server and retry." >&2
        fi
        exit 1
    fi
}

stop_server() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}

trap cleanup EXIT

cd "$ROOT_DIR"

make all

echo "== Fresh unit-test validation =="
make clean-data
start_server
"$BENCH_BIN" --unit-test
stop_server

echo "== Fresh benchmark validation =="
make clean-data
start_server
"$BENCH_BIN" 200000
