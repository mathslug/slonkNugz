#!/usr/bin/env bash
# Pull production DB and logs to local machine for analysis.
# Backs up existing local files first, then pulls fresh copies
# named so `uv run app.py` uses the pulled DB directly.
# Usage: bash scripts/pull_prod.sh
set -euo pipefail

HOST="almalinux@karb.mathslug.com"
LOG_DIR="/var/log/slonk-arb"
DATA_DIR="/var/lib/slonk-arb"

LOCAL_DB="slonk_arb.db"
LOCAL_LOGS=(cron.log scan.log evaluate.log evaluate-high.log evaluate-afternoon.log)

# Back up existing local files if any exist
BACKUP_DIR="db_backups/$(date +%Y%m%d_%H%M%S)"
has_existing=false
for f in "$LOCAL_DB" "${LOCAL_LOGS[@]}"; do
    if [ -f "$f" ]; then
        has_existing=true
        break
    fi
done

if $has_existing; then
    echo "==> Backing up existing files to $BACKUP_DIR/"
    mkdir -p "$BACKUP_DIR"
    for f in "$LOCAL_DB" "${LOCAL_LOGS[@]}"; do
        if [ -f "$f" ]; then
            mv "$f" "$BACKUP_DIR/"
            echo "  moved $f"
        fi
    done
fi

echo "==> Pulling DB..."
scp "$HOST:$DATA_DIR/slonk_arb.db" "$LOCAL_DB"

echo "==> Pulling logs..."
for log in "${LOCAL_LOGS[@]}"; do
    scp "$HOST:$LOG_DIR/$log" "$log" 2>/dev/null || echo "  $log not found on server, skipping"
done

echo "==> Done. Files:"
ls -lh "$LOCAL_DB" "${LOCAL_LOGS[@]}" 2>/dev/null
