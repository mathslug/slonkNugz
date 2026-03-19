#!/usr/bin/env bash
# Quick server health check: deployed commit, service status, webapp response.
# Usage: bash scripts/check_server.sh
set -euo pipefail

HOST="almalinux@karb.mathslug.com"

echo "==> Deployed commit:"
ssh "$HOST" "sudo git -C /opt/slonk-arb -c safe.directory=/opt/slonk-arb log --oneline -3"

echo ""
echo "==> Service status:"
ssh "$HOST" "sudo systemctl status slonk-arb --no-pager -l 2>&1 | head -12"

echo ""
echo "==> Webapp HTTP response:"
ssh "$HOST" "curl -s -o /dev/null -w 'HTTP %{http_code}\n' http://127.0.0.1:8000/"

echo ""
echo "==> Cron jobs:"
ssh "$HOST" "sudo cat /etc/cron.d/slonk-arb"

echo ""
echo "==> Disk usage:"
ssh "$HOST" "du -sh /var/lib/slonk-arb/slonk_arb.db /var/log/slonk-arb/ 2>/dev/null; echo ''; df -h /"
