#!/usr/bin/env bash
# Wrapper for cron jobs — loads env vars and runs a command via uv.
# Usage: deploy/run.sh scan.py --from-db --db /var/lib/kalshi-arb/kalshi_arb.db
set -euo pipefail
cd /opt/kalshi-arb
set -a; source /var/lib/kalshi-arb/.env; set +a
export KALSHI_DB=/var/lib/kalshi-arb/kalshi_arb.db
export UV_PYTHON_INSTALL_DIR=/opt/uv-python
exec /usr/local/bin/uv run "$@"
