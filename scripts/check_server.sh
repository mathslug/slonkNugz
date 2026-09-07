#!/usr/bin/env bash
# karb health check: deployed commit, container state, HTTP, job receipts, disk.
# Usage: bash scripts/check_server.sh
set -euo pipefail

SVC=podsvc
SVC_HOME="/home/${SVC}"

# LAN first, tunnel second. `timeout` because cloudflared blocks on a browser
# login when the Access token has expired.
PI="${PI_HOST:-}"
if [ -z "$PI" ]; then
    for c in mypi mypi-remote; do
        if timeout 30 ssh -o ConnectTimeout=10 -o BatchMode=yes "$c" true 2>/dev/null; then
            PI="$c"; break
        fi
    done
fi
[ -n "$PI" ] || { echo "cannot reach the Pi (mypi, mypi-remote)" >&2; exit 1; }
echo "==> Host: $PI"

SVC_UID=$(ssh "$PI" "id -u ${SVC}")
# podman and systemctl --user need the service account's own session bus.
asvc() { ssh "$PI" "sudo -u ${SVC} env HOME=${SVC_HOME} XDG_RUNTIME_DIR=/run/user/${SVC_UID} $*"; }

echo ""
echo "==> Deployed commit:"
ssh "$PI" "sudo git -C ${SVC_HOME}/apps/karb -c safe.directory='*' log --oneline -3"

echo ""
echo "==> Container + timers:"
asvc "systemctl --user --no-pager --plain list-units 'karb*' 2>&1 | head -20"

echo ""
echo "==> Webapp HTTP response:"
ssh "$PI" "curl -s -o /dev/null -w 'HTTP %{http_code}\n' http://127.0.0.1:8000/healthz"

echo ""
echo "==> Job receipts (age of last clean run):"
# JOB_RECEIPTS in ~/src/rpi/apps/karb.conf sets the tolerated staleness.
ssh "$PI" 'for f in /var/lib/rpi-health/jobs/karb.*; do
    [ -e "$f" ] || { echo "  none"; break; }
    printf "  %-24s %sh ago\n" "$(basename "$f")" "$(( ( $(date +%s) - $(cat "$f") ) / 3600 ))"
done'

echo ""
echo "==> Job durations (last 3 days):"
# The user systemd manager tags its own "Starting"/"Finished" lines for a job
# with USER_UNIT=; the job's stdout is tagged _SYSTEMD_USER_UNIT=. Pairing the
# manager's two lines gives wall time, which is what sets how tightly the
# timers can be spaced.
for job in sports daily sweep hot; do
    ssh "$PI" "sudo journalctl _UID=${SVC_UID} USER_UNIT=karb-job@${job}.service \
        --since '3 days ago' --no-pager -o short-unix 2>/dev/null" \
    | awk -v job="$job" '
        /Starting/ { t = $1 + 0; next }
        /Finished|Deactivated successfully/ {
            if (t > 0) { d[n++] = $1 - t; t = 0 }
        }
        END {
            if (n == 0) { printf "  %-8s no completed runs\n", job; exit }
            s = 0; mx = 0
            for (i = 0; i < n; i++) { s += d[i]; if (d[i] > mx) mx = d[i] }
            printf "  %-8s %2d runs   avg %4ds   max %4ds\n", job, n, s / n, mx
        }'
done

echo ""
echo "==> Recent job failures:"
# sudo + _UID match: the login user is not in systemd-journal, so reading
# podsvc's user journal as podsvc returns nothing.
ssh "$PI" "sudo journalctl _UID=${SVC_UID} --since '2 days ago' --no-pager -p warning 2>&1 | tail -15"

echo ""
echo "==> Disk usage:"
ssh "$PI" "sudo du -sh ${SVC_HOME}/data/karb 2>/dev/null; df -h / | tail -1"
