#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/build"
SERVER_BIN="$BUILD_DIR/flexql-server"
API_BENCH_BIN="$BUILD_DIR/flexql-benchmark"
COMPAT_BENCH_BIN="$BUILD_DIR/benchmark"

HOST="127.0.0.1"
PORT="9000"
ROWS="10000"
MODE="api"
CLEAN_DATA="0"
SKIP_BUILD="0"
UNIT_TEST_ONLY="0"

usage() {
    cat <<'EOF'
Usage: scripts/run_benchmark.sh [options]

Options:
  --api               Run build/flexql-benchmark (default)
  --compat            Run build/benchmark
  --unit-test         Run build/benchmark --unit-test
  --host HOST         Host for benchmark client (default: 127.0.0.1)
  --port PORT         Server port (default: 9000)
  --rows N            Row count for benchmark run (default: 10000)
  --clean-data        Clear persisted table data before running
  --skip-build        Do not invoke make before running
  --help              Show this help text
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --api)
            MODE="api"
            ;;
        --compat)
            MODE="compat"
            ;;
        --unit-test)
            MODE="compat"
            UNIT_TEST_ONLY="1"
            ;;
        --host)
            HOST="${2:?missing host value}"
            shift
            ;;
        --port)
            PORT="${2:?missing port value}"
            shift
            ;;
        --rows)
            ROWS="${2:?missing rows value}"
            shift
            ;;
        --clean-data)
            CLEAN_DATA="1"
            ;;
        --skip-build)
            SKIP_BUILD="1"
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
    shift
done

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

if [[ "$SKIP_BUILD" != "1" ]]; then
    make -C "$ROOT_DIR"
fi

if [[ "$CLEAN_DATA" == "1" ]]; then
    make -C "$ROOT_DIR" clean-data
fi

if [[ ! -x "$SERVER_BIN" ]]; then
    echo "Missing server binary: $SERVER_BIN" >&2
    exit 1
fi

if [[ "$MODE" == "api" && ! -x "$API_BENCH_BIN" ]]; then
    echo "Missing benchmark binary: $API_BENCH_BIN" >&2
    exit 1
fi

if [[ "$MODE" == "compat" && ! -x "$COMPAT_BENCH_BIN" ]]; then
    echo "Missing benchmark binary: $COMPAT_BENCH_BIN" >&2
    exit 1
fi

if [[ "$MODE" == "compat" && "$PORT" != "9000" ]]; then
    echo "The provided build/benchmark binary always connects to 127.0.0.1:9000." >&2
    echo "Use --port 9000 for --compat or --unit-test mode." >&2
    exit 1
fi

echo "Starting FlexQL server on port $PORT..."
"$SERVER_BIN" "$PORT" >/tmp/flexql-benchmark-server.log 2>&1 &
SERVER_PID=$!

sleep 1
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Server failed to start. See /tmp/flexql-benchmark-server.log" >&2
    if [[ -f /tmp/flexql-benchmark-server.log ]]; then
        echo "--- server log ---" >&2
        sed -n '1,120p' /tmp/flexql-benchmark-server.log >&2 || true
        echo "------------------" >&2
    fi
    if command -v ss >/dev/null 2>&1; then
        if ss -ltn "( sport = :$PORT )" 2>/dev/null | grep -q ":$PORT"; then
            echo "Port $PORT is already in use. Stop the existing server or run with a different port." >&2
        fi
    fi
    exit 1
fi

echo "Running benchmark..."
if [[ "$MODE" == "api" ]]; then
    "$API_BENCH_BIN" "$HOST" "$PORT" "$ROWS"
elif [[ "$UNIT_TEST_ONLY" == "1" ]]; then
    "$COMPAT_BENCH_BIN" --unit-test
else
    "$COMPAT_BENCH_BIN" "$ROWS"
fi
