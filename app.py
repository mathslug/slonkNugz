#!/usr/bin/env python3
"""Flask webapp for reviewing Kalshi arbitrage candidate pairs."""

import os

from flask import Flask, redirect, render_template, request, url_for

import db as db_mod

DB_PATH = os.environ.get("KALSHI_DB", "kalshi_arb.db")


def create_app(db_path: str = DB_PATH) -> Flask:
    app = Flask(__name__)
    app.config["DB_PATH"] = db_path

    def get_conn():
        return db_mod.get_connection(app.config["DB_PATH"])

    @app.route("/")
    def index():
        conn = get_conn()
        stats = db_mod.get_pair_stats(conn)
        conn.close()
        return render_template("base.html", page="dashboard", stats=stats)

    def _filter_by_confidence(pairs, confidence):
        if confidence and confidence in ("high", "medium", "low"):
            return [p for p in pairs if p.get("confidence") == confidence]
        return pairs

    @app.route("/review")
    def review():
        conn = get_conn()
        pairs = db_mod.get_pairs_for_review(conn, "unreviewed")
        conn.close()
        conf = request.args.get("confidence")
        pairs = _filter_by_confidence(pairs, conf)
        return render_template("review.html", pairs=pairs, status="unreviewed", title="Unreviewed Pairs", confidence=conf)

    @app.route("/reviewed")
    def reviewed():
        conn = get_conn()
        confirmed = db_mod.get_pairs_for_review(conn, "confirmed")
        rejected = db_mod.get_pairs_for_review(conn, "rejected")
        conn.close()
        conf = request.args.get("confidence")
        pairs = _filter_by_confidence(confirmed + rejected, conf)
        return render_template("review.html", pairs=pairs, status="reviewed", title="Reviewed Pairs", confidence=conf)

    @app.route("/pair/<int:pair_id>")
    def pair_detail(pair_id):
        conn = get_conn()
        pair = db_mod.get_pair_detail(conn, pair_id)
        conn.close()
        if not pair:
            return "Pair not found", 404
        return render_template("detail.html", pair=pair)

    @app.route("/trades")
    def trades():
        conn = get_conn()
        evals = db_mod.get_latest_evaluations(conn)
        conn.close()
        return render_template("trades.html", evals=evals)

    @app.route("/settings")
    def settings():
        conn = get_conn()
        all_settings = db_mod.get_all_settings(conn)
        latest_yields = db_mod.get_latest_yields(conn)
        conn.close()
        return render_template("settings.html", settings=all_settings, latest_yields=latest_yields)

    @app.route("/settings", methods=["POST"])
    def update_settings():
        conn = get_conn()
        buffer_bps = request.form.get("buffer_bps", "100")
        borrow_rate_bps = request.form.get("borrow_rate_bps", "600")
        db_mod.set_setting(conn, "buffer_bps", buffer_bps)
        db_mod.set_setting(conn, "borrow_rate_bps", borrow_rate_bps)
        conn.close()
        return redirect(url_for("settings"))

    @app.route("/pair/<int:pair_id>/review", methods=["POST"])
    def submit_review(pair_id):
        decision = request.form.get("decision")
        if decision not in ("confirmed", "rejected"):
            return "Invalid decision", 400
        conn = get_conn()
        db_mod.set_review(conn, pair_id, decision)
        conn.close()
        next_url = request.form.get("next") or url_for("review")
        return redirect(next_url)

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5001)
