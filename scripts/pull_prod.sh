#!/usr/bin/env bash
# Pull production DB and logs to local machine for analysis.
# Usage: bash scripts/pull_prod.sh
set -euo pipefail

HOST="almalinux@slonkn.mathslug.com"
LOG_DIR="/var/log/kalshi-arb"
DATA_DIR="/var/lib/kalshi-arb"

echo "==> Pulling DB..."
scp "$HOST:$DATA_DIR/kalshi_arb.db" kalshi_arb_prod.db

echo "==> Pulling logs..."
scp "$HOST:$LOG_DIR/cron.log" cron.log
scp "$HOST:$LOG_DIR/scan.log" scan.log
scp "$HOST:$LOG_DIR/evaluate.log" evaluate.log
scp "$HOST:$LOG_DIR/evaluate-high.log" evaluate-high.log

echo "==> Done. Files:"
ls -lh kalshi_arb_prod.db cron.log scan.log evaluate.log evaluate-high.log
