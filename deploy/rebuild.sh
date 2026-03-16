#!/usr/bin/env bash
# Provision a new slonk-arb droplet after cloud-init has finished.
#
# Prerequisites:
#   1. Create an AlmaLinux 10 droplet in the DigitalOcean console with:
#      - Image: AlmaLinux 10
#      - Size: s-1vcpu-512mb-10gb ($4/mo) or similar
#      - Region: nyc1
#      - User data: paste contents of deploy/cloud-init.yml
#      - SSH key: your personal key (johnbentley@oldblue.lan)
#   2. Update DNS: point slonkn.mathslug.com A record to the new IP
#
# .env is created by GitHub Actions deploy (ANTHROPIC_KEY secret).
# Push a commit to main after this script finishes to trigger it.
#
# Usage:
#   bash deploy/rebuild.sh <NEW_IP> [--db path/to/backup.db]
#
set -euo pipefail

DOMAIN=slonkn.mathslug.com
SSH_USER=almalinux
SENTINEL=/var/lib/slonk-arb/.cloud-init-done

# ── Parse args ────────────────────────────────────────────────────────────
IP=""
DB_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --db) DB_FILE="$2"; shift 2 ;;
        *)    IP="$1"; shift ;;
    esac
done

if [[ -z "$IP" ]]; then
    echo "Usage: bash deploy/rebuild.sh <NEW_IP> [--db path/to/backup.db]"
    exit 1
fi

if [[ -n "$DB_FILE" && ! -f "$DB_FILE" ]]; then
    echo "ERROR: DB file not found: $DB_FILE"
    exit 1
fi

echo "==> Target: $SSH_USER@$IP ($DOMAIN)"

# ── Phase 1: Wait for DNS ────────────────────────────────────────────────
echo ""
echo "==> Checking DNS for $DOMAIN -> $IP"
RESOLVED=$(dig +short "$DOMAIN" 2>/dev/null | tail -1)
if [[ "$RESOLVED" != "$IP" ]]; then
    echo "    DNS currently resolves to: ${RESOLVED:-<nothing>}"
    echo "    Update the A record for $DOMAIN to $IP in Namecheap, then press Enter."
    read -r
    echo "    Waiting for DNS propagation..."
    DNS_WAIT=15
    while true; do
        RESOLVED=$(dig +short "$DOMAIN" 2>/dev/null | tail -1)
        if [[ "$RESOLVED" == "$IP" ]]; then
            echo "    DNS resolved!"
            break
        fi
        echo "    Still resolving to: ${RESOLVED:-<nothing>} (retrying in ${DNS_WAIT}s)"
        sleep "$DNS_WAIT"
        DNS_WAIT=$(( DNS_WAIT * 2 > 120 ? 120 : DNS_WAIT * 2 ))
    done
else
    echo "    DNS already correct."
fi

# ── Phase 2: Wait for cloud-init ─────────────────────────────────────────
echo ""
echo "==> Waiting for cloud-init to finish on $IP..."
while true; do
    if ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
        "$SSH_USER@$IP" "test -f $SENTINEL" 2>/dev/null; then
        echo "    Cloud-init complete."
        break
    fi
    echo "    Not ready yet (retrying in 15s)"
    sleep 15
done

# ── Phase 3: Push DB (optional) ──────────────────────────────────────────
if [[ -n "$DB_FILE" ]]; then
    echo ""
    echo "==> Pushing DB: $DB_FILE"
    scp "$DB_FILE" "$SSH_USER@$IP:/tmp/slonk_arb.db"
    ssh "$SSH_USER@$IP" "
        sudo cp /tmp/slonk_arb.db /var/lib/slonk-arb/slonk_arb.db
        sudo chown slonk:slonk /var/lib/slonk-arb/slonk_arb.db
        rm /tmp/slonk_arb.db
    "
    echo "    Done."
fi

# ── Phase 4: SSL ─────────────────────────────────────────────────────────
echo ""
echo "==> Setting up SSL with certbot"
ssh "$SSH_USER@$IP" "
    sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos --register-unsafely-without-email --redirect
"
echo "    Done."

# ── Phase 5: Verify HTTP ─────────────────────────────────────────────────
echo ""
echo "==> Verifying HTTPS is up (nginx only, webapp not started yet)..."
sleep 2
HTTP_CODE=$(curl -s -o /dev/null -w '%{http_code}' "https://$DOMAIN/" --max-time 10 2>/dev/null || echo "000")
echo "    https://$DOMAIN/ returned HTTP $HTTP_CODE"

echo ""
echo "==> Server is ready!"
echo "    IP:     $IP"
echo "    Domain: https://$DOMAIN/"
echo ""
echo "Next steps:"
echo "  1. Push a commit to main -- GitHub Actions will deploy code, create .env, and start the webapp"
echo "  2. Verify: curl https://$DOMAIN/"
echo "  3. Check cron: ssh $SSH_USER@$IP 'tail -50 /var/log/slonk-arb/cron.log'"
echo "  (If the domain changed, update DROPLET_URL GitHub secret)"
