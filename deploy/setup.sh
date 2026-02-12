#!/usr/bin/env bash
# Idempotent server provisioning for kalshi-arb on AlmaLinux / RHEL.
# Run as root: sudo bash deploy/setup.sh
set -euo pipefail

APP_USER=kalshi
APP_DIR=/opt/kalshi-arb
DATA_DIR=/var/lib/kalshi-arb
LOG_DIR=/var/log/kalshi-arb
DOMAIN=slonkn.mathslug.com

echo "==> Creating system user and directories"
id -u "$APP_USER" &>/dev/null || useradd --system --shell /sbin/nologin "$APP_USER"
mkdir -p "$DATA_DIR" "$DATA_DIR/backups" "$LOG_DIR"
chown "$APP_USER:$APP_USER" "$DATA_DIR" "$DATA_DIR/backups" "$LOG_DIR"

echo "==> Installing system packages"
dnf install -y -q nginx certbot python3-certbot-nginx httpd-tools cronie

# Install uv if not present
if ! command -v uv &>/dev/null; then
    echo "==> Installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
fi

echo "==> Setting up app directory"
if [ ! -d "$APP_DIR/.git" ]; then
    echo "    Clone the repo to $APP_DIR first, then re-run this script."
    echo "    git clone <repo-url> $APP_DIR"
    exit 1
fi

# Ensure venv + deps
cd "$APP_DIR"
uv sync

echo "==> Writing nginx config"
# RHEL uses conf.d instead of sites-available/sites-enabled
cat > /etc/nginx/conf.d/kalshi-arb.conf <<NGINX
server {
    listen 80;
    server_name $DOMAIN;

    auth_basic "Restricted";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        proxy_pass http://unix:/run/kalshi-arb/kalshi-arb.sock;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
NGINX

# Remove default server block if it exists in the main config
# (RHEL nginx.conf has a default server{} block — comment it out)
if grep -q 'listen.*80 default_server' /etc/nginx/nginx.conf; then
    sed -i '/^    server {/,/^    }/s/^/#/' /etc/nginx/nginx.conf
fi

nginx -t && systemctl enable --now nginx && systemctl reload nginx

# Open firewall for HTTP/HTTPS
if command -v firewall-cmd &>/dev/null; then
    firewall-cmd --permanent --add-service=http --add-service=https 2>/dev/null || true
    firewall-cmd --reload 2>/dev/null || true
fi

echo "==> Writing systemd service"
cat > /etc/systemd/system/kalshi-arb.service <<EOF
[Unit]
Description=Kalshi Arb Webapp
After=network.target

[Service]
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$DATA_DIR/.env
Environment=KALSHI_DB=$DATA_DIR/kalshi_arb.db
ExecStart=$APP_DIR/.venv/bin/gunicorn --bind unix:/run/kalshi-arb/kalshi-arb.sock --workers 2 "app:create_app()"
RuntimeDirectory=kalshi-arb

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable kalshi-arb

echo "==> Enabling crond"
systemctl enable --now crond

echo "==> Writing cron jobs"
cat > /etc/cron.d/kalshi-arb <<CRON
# Kalshi Arb scheduled jobs (times in UTC)
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/.local/bin

# Fetch Treasury yields daily at 6:00 AM ET (10:00 UTC)
0 10 * * * $APP_USER $APP_DIR/deploy/run.sh fetch_yields.py --db $DATA_DIR/kalshi_arb.db >> $LOG_DIR/cron.log 2>&1

# Scan for new pairs daily at 6:30 AM ET (10:30 UTC)
30 10 * * * $APP_USER $APP_DIR/deploy/run.sh scan.py --from-db --db $DATA_DIR/kalshi_arb.db --log-file $LOG_DIR/scan.log >> $LOG_DIR/cron.log 2>&1

# Evaluate confirmed pairs at 7:00 AM ET (11:00 UTC)
0 11 * * * $APP_USER $APP_DIR/deploy/run.sh evaluate.py --db $DATA_DIR/kalshi_arb.db --log-file $LOG_DIR/evaluate.log >> $LOG_DIR/cron.log 2>&1

# Evaluate high-confidence pairs at 7:30 AM ET (11:30 UTC)
30 11 * * * $APP_USER $APP_DIR/deploy/run.sh evaluate.py --mode high --db $DATA_DIR/kalshi_arb.db --log-file $LOG_DIR/evaluate.log >> $LOG_DIR/cron.log 2>&1

# Backup DB weekly (Sunday 3:00 AM ET / 7:00 UTC)
0 7 * * 0 $APP_USER cp $DATA_DIR/kalshi_arb.db $DATA_DIR/backups/kalshi_arb_\$(date +\%Y\%m\%d).db 2>&1
CRON

chmod 644 /etc/cron.d/kalshi-arb

echo "==> Setup complete!"
echo ""
echo "Remaining manual steps:"
echo "  1. Create htpasswd:  htpasswd -c /etc/nginx/.htpasswd <username>"
echo "  2. Create env file:  nano $DATA_DIR/.env"
echo "     (ANTHROPIC_API_KEY, MAILGUN_API_KEY, MAILGUN_DOMAIN, NOTIFY_EMAIL)"
echo "  3. Copy DB:          cp kalshi_arb.db $DATA_DIR/ && chown $APP_USER:$APP_USER $DATA_DIR/kalshi_arb.db"
echo "  4. Start webapp:     systemctl start kalshi-arb"
echo "  5. SSL:              certbot --nginx -d $DOMAIN"
