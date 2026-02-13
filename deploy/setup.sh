#!/usr/bin/env bash
# Idempotent server provisioning for slonk-arb on AlmaLinux / RHEL.
# Run as root: sudo bash deploy/setup.sh
set -euo pipefail

APP_USER=slonk
APP_DIR=/opt/slonk-arb
DATA_DIR=/var/lib/slonk-arb
LOG_DIR=/var/log/slonk-arb
DOMAIN=slonkn.mathslug.com

echo "==> Creating system user and directories"
id -u "$APP_USER" &>/dev/null || useradd --system --shell /sbin/nologin -d /home/$APP_USER "$APP_USER"
mkdir -p "$DATA_DIR" "$DATA_DIR/backups" "$LOG_DIR" /home/$APP_USER/.cache
chown "$APP_USER:$APP_USER" "$DATA_DIR" "$DATA_DIR/backups" "$LOG_DIR" /home/$APP_USER /home/$APP_USER/.cache
touch "$LOG_DIR"/{cron,scan,evaluate,evaluate-high}.log
chown "$APP_USER:$APP_USER" "$LOG_DIR"/*.log

echo "==> Installing system packages"
dnf install -y -q epel-release 2>/dev/null || true
dnf install -y -q nginx certbot python3-certbot-nginx httpd-tools cronie git policycoreutils-python-utils

echo "==> Writing /etc/environment"
echo "UV_PYTHON_INSTALL_DIR=/opt/uv-python" > /etc/environment

# Install uv if not present
if ! command -v /usr/local/bin/uv &>/dev/null; then
    echo "==> Installing uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    cp /root/.local/bin/uv /usr/local/bin/uv
    cp /root/.local/bin/uvx /usr/local/bin/uvx
    chmod 755 /usr/local/bin/uv /usr/local/bin/uvx
fi

echo "==> Setting up app directory"
if [ ! -d "$APP_DIR/.git" ]; then
    echo "    Clone the repo to $APP_DIR first, then re-run this script."
    echo "    git clone <repo-url> $APP_DIR"
    exit 1
fi

# Install Python and deps via uv into a shared location
export UV_PYTHON_INSTALL_DIR=/opt/uv-python
cd "$APP_DIR"
uv sync

# Fix SELinux contexts for venv binaries
echo "==> Fixing SELinux contexts"
semanage fcontext -a -t bin_t '/opt/slonk-arb/.venv/bin(/.*)?' 2>/dev/null || true
semanage fcontext -a -t bin_t '/opt/uv-python/.*/bin(/.*)?' 2>/dev/null || true
restorecon -Rv /opt/slonk-arb/.venv/bin/ /opt/uv-python/ 2>/dev/null || true

# Allow nginx to connect to gunicorn (TCP)
setsebool -P httpd_can_network_connect 1

if [ ! -f /etc/nginx/conf.d/slonk-arb.conf ]; then
    echo "==> Writing nginx config"
    cat > /etc/nginx/conf.d/slonk-arb.conf <<NGINX
server {
    listen 80;
    server_name $DOMAIN;

    auth_basic "Restricted";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
NGINX
    nginx -t && systemctl enable --now nginx && systemctl reload nginx
    echo "==> Setting up SSL"
    certbot --nginx -d $DOMAIN --non-interactive --redirect
else
    echo "==> Nginx config already exists, skipping (certbot manages SSL)"
    systemctl enable --now nginx && systemctl reload nginx
fi

echo "==> Writing systemd service"
cat > /etc/systemd/system/slonk-arb.service <<EOF
[Unit]
Description=Slonk Arb Webapp
After=network.target

[Service]
User=$APP_USER
Group=$APP_USER
WorkingDirectory=$APP_DIR
EnvironmentFile=$DATA_DIR/.env
Environment=SLONK_DB=$DATA_DIR/slonk_arb.db
ExecStart=$APP_DIR/.venv/bin/gunicorn --bind 127.0.0.1:8000 --workers 2 "app:create_app()"

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable slonk-arb

echo "==> Setting up deploy user"
id -u deploy &>/dev/null || useradd -m deploy
usermod -aG slonk deploy
cat > /etc/sudoers.d/deploy <<'SUDOERS'
deploy ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart slonk-arb, /usr/bin/systemctl reload nginx, /usr/bin/tee /var/lib/slonk-arb/.env, /usr/bin/chown slonk\:slonk /var/lib/slonk-arb/.env, /usr/bin/chmod 600 /var/lib/slonk-arb/.env
SUDOERS
chmod 440 /etc/sudoers.d/deploy
chown -R deploy:slonk "$APP_DIR"
chmod -R g+rw "$APP_DIR"
chown -R $APP_USER:$APP_USER "$APP_DIR/.venv"

echo "==> Enabling crond"
systemctl enable --now crond

echo "==> Writing cron jobs"
cat > /etc/cron.d/slonk-arb <<CRON
# Kalshi Arb scheduled jobs (times in UTC)
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin

# Fetch Treasury yields daily at 6:00 AM ET (10:00 UTC)
0 10 * * * $APP_USER $APP_DIR/deploy/run.sh fetch_yields.py --db $DATA_DIR/slonk_arb.db >> $LOG_DIR/cron.log 2>&1

# Fetch all sports tickers into DB (no LLM calls)
# 5:30 AM ET (09:30 UTC)
30 9 * * * $APP_USER $APP_DIR/deploy/run.sh scan.py --category Sports --max-pairs 0 --db $DATA_DIR/slonk_arb.db --log-file $LOG_DIR/scan.log >> $LOG_DIR/cron.log 2>&1

# Scan then evaluate (chained so they don't overlap)
# 6:30 AM ET (10:30 UTC): scan -> evaluate confirmed -> evaluate high
30 10 * * * $APP_USER $APP_DIR/deploy/run.sh scan.py --from-db --min-volume 200 --db $DATA_DIR/slonk_arb.db --log-file $LOG_DIR/scan.log >> $LOG_DIR/cron.log 2>&1 && $APP_DIR/deploy/run.sh evaluate.py --db $DATA_DIR/slonk_arb.db --log-file $LOG_DIR/evaluate.log >> $LOG_DIR/cron.log 2>&1 && $APP_DIR/deploy/run.sh evaluate.py --mode high --db $DATA_DIR/slonk_arb.db --log-file $LOG_DIR/evaluate-high.log >> $LOG_DIR/cron.log 2>&1

# Backup DB weekly (Sunday 3:00 AM ET / 7:00 UTC)
0 7 * * 0 $APP_USER cp $DATA_DIR/slonk_arb.db $DATA_DIR/backups/slonk_arb_\$(date +\%Y\%m\%d).db 2>&1
CRON

chmod 644 /etc/cron.d/slonk-arb

echo "==> Setup complete!"
echo ""
echo "Remaining manual steps:"
echo "  1. Create htpasswd:  htpasswd -c /etc/nginx/.htpasswd <username>"
echo "  2. Copy DB:          cp slonk_arb.db $DATA_DIR/ && chown $APP_USER:$APP_USER $DATA_DIR/slonk_arb.db"
echo "  3. Start webapp:     systemctl start slonk-arb"
echo "  4. SSL:              certbot --nginx -d $DOMAIN"
