"""Send email notifications for BUY recommendations via Gmail SMTP."""

import logging
import os
import smtplib
from email.message import EmailMessage

log = logging.getLogger("notify")


def send_buy_alert(results: list[dict]) -> bool:
    """Send email summarizing BUY recommendations. Returns True on success."""
    smtp_user = os.environ.get("SMTP_USER")
    smtp_password = os.environ.get("SMTP_PASSWORD")
    to_email = os.environ.get("NOTIFY_EMAIL")
    if not all([smtp_user, smtp_password, to_email]):
        log.warning("SMTP not configured, skipping notification")
        return False

    buys = [r for r in results if r.get("recommendation") == "buy"]
    if not buys:
        return False

    total_capital = sum(r.get("total_cost", 0) for r in buys)
    lines = [f"Found {len(buys)} BUY recommendation(s). Total capital: ${total_capital:.2f}\n"]
    for r in sorted(buys, key=lambda x: -(x.get("excess_yield") or 0)):
        ann_pct = r['annualized_yield'] * 100 if r.get('annualized_yield') is not None else 0
        exc_pct = r['excess_yield'] * 100 if r.get('excess_yield') is not None else 0
        lines.append(
            f"  Pair #{r['pair_id']:>3}  n={r['n_contracts']:>4}  "
            f"yield={ann_pct:>6.2f}%  "
            f"excess={exc_pct:>+6.2f}%  "
            f"cost=${r['total_cost']:>8.2f}"
        )

    body = "\n".join(lines)
    msg = EmailMessage()
    msg["Subject"] = f"[Karb Scanner] {len(buys)} BUY signal(s)"
    msg["From"] = smtp_user
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=10) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    log.info("Sent BUY alert email (%d recommendations)", len(buys))
    return True
