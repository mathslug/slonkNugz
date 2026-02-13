# kalshi-arb

Kalshi cross-market arbitrage checker and scanner for binary prediction markets.

## Architecture

Seven code files + templates + deploy scripts:

- **`kalshi.py`** -- shared Kalshi API helpers, fee model, and orderbook utilities. Contains `KALSHI_BASE`, `TAKER_FEE_COEFF`, `fetch_market()`, `fetch_orderbook()`, `taker_fee()`, `walk_book()`, and the `Fill`, `LegResult`, `Side` types.

- **`main.py`** -- evaluates a known arb pair. Has `evaluate_arb(ticker_a, side_a, ticker_b, side_b, n, settlement_date, discount_rate)` which walks both orderbooks, computes all-in cost with fees, and returns an `ArbResult` (key field: `npv`). CLI wrapper hardcodes the Musetti FO/GS tennis tickers.

- **`scan.py`** -- discovers arb pairs automatically. Fetches sports markets from Kalshi, groups by entity (`yes_sub_title`), generates cross-series candidate pairs, screens via Claude Haiku for logical implication, persists to SQLite DB, prints terminal summary.

- **`db.py`** -- pure SQLite persistence functions. Every function takes `conn` as first arg — no global state. Tables: `tickers`, `prices`, and `candidate_pairs`. Designed for REPL use: `import db; conn = db.get_connection("kalshi_arb.db")`.

- **`evaluate.py`** -- evaluates confirmed arb pairs against live orderbooks. Fetches orderbooks, finds optimal contract count via binary search, stores results in DB.

- **`app.py`** -- Flask webapp for human review of candidate pairs. Dashboard, review queue, reviewed pairs list, pair detail with confirm/reject buttons.

- **`fetch_yields.py`** -- fetches Treasury CMT daily yield curve data from treasury.gov and stores in the DB.

- **`notify.py`** -- sends email notifications for BUY recommendations via Mailgun HTTP API. Called by `evaluate.py`.

- **`templates/`** -- Jinja2 templates (`base.html`, `review.html`, `detail.html`) using Pico CSS.

- **`deploy/`** -- server provisioning (`setup.sh`), cron wrapper (`run.sh`), and GitHub Actions workflow.

## Running

**Always prefix commands with `uv run`** to use the project's managed dependencies and virtual environment.

Requires `ANTHROPIC_API_KEY` in `.env`.

### Evaluate a known pair
```
uv run main.py -n 100
uv run main.py -n 500 --rfr 0.04 --buffer 0.005
```

### Scan for new pairs
```
uv run scan.py --filter "tennis,atp,wta,french open,grand slam,wimbledon"
uv run scan.py --filter tennis --min-volume 100
uv run scan.py --model claude-haiku-4-5-20251001 --batch-size 12
```

### Incremental scanning with DB
```
uv run scan.py --filter tennis --db kalshi_arb.db                  # fetch + screen new pairs
uv run scan.py --from-db                                            # re-use tickers in DB, screen unscreened pairs
uv run scan.py --from-db --rescan                                   # re-screen all pairs
```

### Evaluate confirmed pairs
```
uv run evaluate.py                                  # evaluate human-confirmed pairs (default)
uv run evaluate.py --mode high                      # evaluate high-confidence unreviewed pairs
uv run evaluate.py --max-n 500 --log-file eval.log  # custom max contracts + log path
```

### Review webapp
```
uv run app.py                                       # http://localhost:5001
KALSHI_DB=my.db uv run app.py                       # custom DB path
```

### Import from legacy JSON cache
```
uv run python -c "import db; conn = db.get_connection(); db.import_from_cache(conn, 'sports_cache.json', 'scan_results.json')"
```

### CLI args -- main.py
- `-n` / `--contracts` -- number of contract pairs (default 100)
- `--rfr` -- risk-free rate (default 0.035)
- `--buffer` -- buffer above RFR (default 0.01)

### CLI args -- scan.py
- `--filter` / `-f` -- comma-separated keywords to filter series (e.g. "tennis,atp,grand slam")
- `--model` -- Anthropic model name (default: `claude-haiku-4-5-20251001`)
- `--min-volume` -- exclude markets below this volume (default: 0)
- `--batch-size` -- pairs per LLM call (default: 12)
- `--category` -- Kalshi category (default: Sports)
- `--db` -- SQLite database path (default: kalshi_arb.db)
- `--from-db` -- skip fetching, use tickers already in DB
- `--rescan` -- re-screen all pairs even if already evaluated in DB
- `--max-pairs` -- cap number of new pairs to screen per run (limits LLM calls)
- `--log-file` -- log file path (default: scan.log)

### CLI args -- evaluate.py
- `--db` -- SQLite database path (default: kalshi_arb.db)
- `--max-n` -- max contracts to search for optimal fill (default: 500)
- `--mode` -- `confirmed` (human-approved, default) or `high` (high-confidence unreviewed)
- `--log-file` -- log file path (default: evaluate.log)

## Scanner data flow

```
Fetch filtered series -> Fetch events + nested markets per series
  -> Extract minimal market representations
  -> Upsert tickers into SQLite DB
  -> Group markets by entity (yes_sub_title) from DB
  -> Generate cross-series candidate pairs per entity
  -> Filter out already-screened pairs (unless --rescan)
  -> LLM screens each pair for logical implication (A YES -> B YES?)
  -> Store ALL results in DB (including "none" confidence)
  -> Print terminal summary
```

### Pre-filtering strategy

Implication relationships almost always involve the same entity: "Alcaraz wins FO" -> "Alcaraz wins a GS". Grouping by `yes_sub_title` then only pairing across different series is a near-perfect pre-filter that reduces O(n^2) to ~50-200 candidates.

### LLM screening

Uses Claude Haiku via the Anthropic API (~$0.03/scan). The prompt requests `ticker_a`/`ticker_b` echo-back fields so results are matched to input pairs by ticker rather than array index — prevents silent data corruption if the LLM skips, reorders, or merges results.

## Database

SQLite database (`kalshi_arb.db` by default) with three tables:

- **`tickers`** -- all market info fetched from Kalshi (ticker, series, event, title, prices, volume, timestamps). Primary key: `ticker`. Price columns are the "latest" cache, overwritten each scan.
- **`prices`** -- append-only price history. One row per ticker per scan with `last_price`, `yes_ask`, `no_ask`, and `recorded_at` timestamp. Populated by `record_prices()` during each scan.
- **`candidate_pairs`** -- LLM screening results with `ticker_a`/`ticker_b` (always stored in sorted order), `antecedent_ticker`/`consequent_ticker`, confidence, reasoning, and `human_review` (confirmed/rejected/NULL).

### `db.py` key functions

All take `conn: sqlite3.Connection` as first arg:

- `init_db(db_path)` -- create tables (idempotent)
- `get_connection(db_path)` -- REPL helper (sets WAL, foreign keys, Row factory)
- `upsert_tickers(conn, markets)` -- insert/update from fetched dicts
- `record_prices(conn, markets)` -- append price snapshots to history table
- `get_tickers_by_entity(conn)` -- group active tickers by entity (2+ series)
- `get_screened_pair_keys(conn)` -- set of already-evaluated pair keys
- `bulk_upsert_pair_results(conn, results, model)` -- store LLM results
- `get_pairs_for_review(conn, status)` -- fetch pairs for review UI
- `get_pair_detail(conn, pair_id)` -- full info for a single pair
- `set_review(conn, pair_id, decision)` -- set human review
- `import_from_cache(conn, cache_path, results_path)` -- bootstrap from JSON

## Review webapp

Flask app (`app.py`) on port 5001 with routes:

| Route | Purpose |
|-------|---------|
| `/` | Dashboard with pair counts |
| `/review` | Unreviewed pairs table |
| `/reviewed` | Confirmed + rejected pairs |
| `/pair/<id>` | Pair detail with confirm/reject buttons |
| `POST /pair/<id>/review` | Submit review decision |

Uses Pico CSS (CDN, classless). Kalshi links: `https://kalshi.com/markets/<event_ticker>`.

## Logging

`kalshi.py`, `main.py`, `scan.py`, and `evaluate.py` use Python `logging`. `print()` is for user-facing CLI output; `logging` is for diagnostics written to log files.

- **`kalshi.py`** -- DEBUG traces on `fetch_market` and `fetch_orderbook` (ticker, status code, latency)
- **`main.py`** -- DEBUG for orderbook fetches, yield calculations, binary search; WARNING for empty orderbooks
- **`scan.py`** -- writes to `scan.log` (configurable via `--log-file`). Batch matching summaries, raw LLM responses, unmatched result warnings.
- **`evaluate.py`** -- writes to `evaluate.log` (configurable via `--log-file`). Per-pair INFO for BUY/PASS, WARNING for API errors.

When `main.py` is imported by `evaluate.py`, its log calls flow through evaluate's `basicConfig`. When run standalone as CLI, log calls are no-ops.

Both `scan.py` and `evaluate.py` open log files with `filemode="w"` (overwrite). The 10:30 scan overwrites the 09:30 fetch's `scan.log`, but the fetch run does minimal logging (no LLM calls). All `print()` output is preserved in `cron.log` via `>>` append redirect.

## Key types

- `ArbResult` (main.py) -- full evaluation output (legs, costs, fees, npv, market data)
- `LegResult` (kalshi.py) -- per-leg fills, cost, fees, filled vs requested
- `Fill` (kalshi.py) -- single price-level fill (price, qty, fee)

## Fee model

Kalshi taker fee: `ceil(0.07 * C * P * (1 - P) * 100) / 100` where P is contract price in dollars [0,1] and C is contract count. Computed per fill level when walking the book. Source: https://kalshi.com/fee-schedule

## API

Uses the Kalshi public REST API at `https://api.elections.kalshi.com/trade-api/v2`. Docs: https://docs.kalshi.com

- Market status: code accepts both `open` and `active` for live markets.
- Event status from the API is `None` -- do not filter events by status.
- Orderbook endpoint returns bids only: YES bid at $P = NO ask at $(1-P). Arrays arrive sorted ascending from API; code reverses to walk best-first.
- No API key needed for market data endpoints.

## Deployment

Deployed to a single Digital Ocean droplet (Debian 12) at `mathslug.me`.

### Server layout

```
/opt/kalshi-arb/              # Code (git clone, deployed via GitHub Actions)
/var/lib/kalshi-arb/          # Persistent data (DB, .env, backups/)
/var/log/kalshi-arb/          # Log files (scan.log, evaluate.log, cron.log)
```

### Stack

- **nginx** -- reverse proxy with basic auth + Let's Encrypt SSL
- **gunicorn** -- WSGI server via systemd (`kalshi-arb.service`)
- **cron** -- scheduled jobs via `/etc/cron.d/kalshi-arb`
- **Mailgun** -- email notifications for BUY signals

### Deploy scripts

- **`deploy/setup.sh`** -- idempotent server provisioning (run as root). Creates user, dirs, nginx config, systemd unit, cron jobs.
- **`deploy/run.sh`** -- cron wrapper that loads `.env` and runs commands via `uv run`.
- **`.github/workflows/deploy.yml`** -- GitHub Actions: `git pull` + `uv sync` + create `.env` from `ANTHROPIC_KEY` secret if missing + restart webapp on push to `main`.

### Cron schedule (ET / UTC)

| Time ET | UTC | Job |
|---------|-----|-----|
| 5:30 AM | 09:30 | `scan.py --category Sports --max-pairs 0` -- fetch all sports tickers into DB (no LLM) |
| 6:00 AM | 10:00 | `fetch_yields.py` -- Treasury yield curve |
| 6:30 AM | 10:30 | `scan.py --from-db --min-volume 200` && `evaluate.py` && `evaluate.py --mode high` (chained) |
| Sun 3 AM | Sun 7:00 | DB backup to `/var/lib/kalshi-arb/backups/` |

### Email notifications

**`notify.py`** -- `send_buy_alert(results)` sends a summary email via Mailgun HTTP API when BUY signals are found. Called automatically by `evaluate.py`. Requires env vars: `MAILGUN_API_KEY`, `MAILGUN_DOMAIN`, `NOTIFY_EMAIL`.

### Environment variables (`.env`)

```
ANTHROPIC_API_KEY=...
MAILGUN_API_KEY=...
MAILGUN_DOMAIN=...
NOTIFY_EMAIL=...
```

### GitHub Actions secrets

- `DROPLET_URL` -- server hostname (e.g. slonkn.mathslug.com)
- `SSH_PRIVATE_KEY` -- deploy user's private key
- `ANTHROPIC_KEY` -- Anthropic API key (used to create `.env` on fresh deploys)
