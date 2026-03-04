#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export TLS_CONFIG="${TLS_CONFIG:-/etc/jtl-comparator/mtls.ini}"
export HOST="${HOST:-0.0.0.0}"
export PORT="${PORT:-8443}"

exec python "${PROJECT_DIR}/app.py"
