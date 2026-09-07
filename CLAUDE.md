# slonk-arb

Kalshi cross-market arbitrage checker and scanner for binary prediction markets.

## Architecture

Twelve code files + templates + container/deploy files + tests:

- **`kalshi.py`** -- shared Kalshi API helpers, fee model, and orderbook utilities. Contains `KALSHI_BASE`, `TAKER_FEE_COEFF`, `fetch_market()`, `fetch_orderbook()`, `taker_fee()`, `est_fee_per_contract()` (amortized fee rate without the penny ceiling, for display estimates), `walk_book()`, and the `Fill`, `LegResult`, `Side` types.

- **`main.py`** -- evaluates a known arb pair. Has `evaluate_arb(ticker_a, side_a, ticker_b, side_b, n, settlement_date, discount_rate)` which walks both orderbooks, computes all-in cost with fees, and returns an `ArbResult` (key field: `npv`). CLI wrapper hardcodes the Musetti FO/GS tennis tickers.

- **`scan.py`** -- discovers arb pairs automatically. Fetches sports markets from Kalshi, groups by entity (`yes_sub_title`), generates candidate pairs within each entity, screens via an LLM for logical implication, persists to SQLite DB, prints terminal summary. LLM backend: if `LLM_BASE_URL` is set, uses that OpenAI-compatible endpoint; otherwise calls the Anthropic API directly, which is what production does — both GPU backends are quota-blocked.

- **`gpu_droplet.py`** -- default LLM backend orchestrator: ephemeral MI300X GPU droplet ($1.99/hr, atl1) running Ollama with `gpt-oss:120b` (`create`/`destroy`/`status`/`snapshot`). `create` prefers the `slonk-llm-snapshot` snapshot (model weights baked in, ~5 min boot; without it, first boot installs Ollama + pulls the model via cloud-init, ~20-40 min), maintains a tag-scoped cloud firewall admitting port 11434 only from the scanner host + the creating machine (Ollama has no auth), waits for a warm-up inference, and prints `export LLM_BASE_URL/LLM_API_KEY/LLM_MODEL` lines for the shell wrapper to eval. `destroy` deletes by tag. Requires `DIGITALOCEAN_TOKEN`.

- **`dedicated.py`** -- alternative LLM backend orchestrator using DigitalOcean dedicated inference (managed; `openai/gpt-oss-120b` on MI300X, $2.59/hr, atl1). Same `create`/`destroy`/`status` contract and export lines as `gpu_droplet.py`. Blocked until the account's dedicated-inference quota is granted. Requires `DIGITALOCEAN_TOKEN`.

- **`db.py`** -- SQLite persistence functions plus review-row economics. Every function takes `conn` as first arg — no global state. Tables: `tickers`, `prices`, `candidate_pairs`, `trade_evaluations`, `treasury_yields`, `settings`. Pair dicts for the review UI carry an estimated post-fee yield (`_add_pair_economics`, using `est_fee_per_contract` from kalshi.py — its only project import). Designed for REPL use: `import db; conn = db.get_connection("slonk_arb.db")`.

- **`evaluate.py`** -- evaluates confirmed arb pairs against live orderbooks. Fetches orderbooks, sizes positions by maximizing NPV at the hurdle discount rate (takes every marginal fill that beats the hurdle; scans every size exhaustively because the fee ceiling makes NPV non-concave in size), stores results in DB.

- **`app.py`** -- Flask webapp for human review of candidate pairs. Dashboard, review queue, reviewed pairs list, pair detail with confirm/reject buttons.

- **`fetch_yields.py`** -- fetches Treasury CMT daily yield curve data from treasury.gov and stores in the DB.

- **`notify.py`** -- sends email notifications for BUY recommendations via Gmail SMTP. Called by `evaluate.py`.

- **`snapshot.py`** -- writes a consistent copy of the live DB to `/data/.backup.db` via `VACUUM INTO`. Run inside the container by the off-box backup so it sees the same file the app has open; a plain file copy of a WAL database silently loses everything since the last checkpoint.

- **`healthcheck.py`** -- container health probe; exits 0 if `/healthz` answers.

- **`templates/`** -- Jinja2 templates (`base.html`, `review.html`, `detail.html`, `trades.html`, `evaluations.html`, `settings.html`) using Pico CSS.

- **`Containerfile`** -- the one image both roles run from (web UI via `CMD`, scheduled jobs via a different command). Runs the test suite at build time, so a failing suite fails the build and no container starts.

- **`deploy/`** -- `karb.container` (Quadlet unit for the web UI), `units/` (one templated job service + four timers), `run-jobs.sh` (runs one scheduled job as short-lived containers). Retired droplet-era files kept for reference only: `cloud-init.yml`, `rebuild.sh`, `run.sh`, `slonk-arb.cron`. `run_scan_gpu.sh` provisions the LLM backend via the orchestrator named in `SLONK_LLM_ORCH` (default `gpu_droplet.py`), runs the scan against it with the orchestrator-provided `LLM_MODEL`, and always destroys it (trap on EXIT); unused while the GPU backends are quota-blocked.

- **`tests/`** -- 178 pytest tests, no network. Run with `uv run pytest`.

- **`scripts/`** -- helper scripts for operations (`pull_prod.sh`, `db_summary.py`, `check_server.sh`, `log_errors.sh`, `pair_details.py`). `pull_prod.sh` and `check_server.sh` target the Pi.

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
uv run scan.py --filter tennis --min-volume 100
uv run scan.py --filter "Tennis,Soccer"                              # multiple Kalshi tags
uv run scan.py --model claude-haiku-4-5-20251001 --batch-size 12
```

### Incremental scanning with DB
```
uv run scan.py --filter tennis --db slonk_arb.db                  # fetch + screen new pairs
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
SLONK_DB=my.db uv run app.py                       # custom DB path
```

### CLI args -- main.py
- `-n` / `--contracts` -- number of contract pairs (default 100)
- `--rfr` -- risk-free rate (default 0.035)
- `--buffer` -- buffer above RFR (default 0.01)

### CLI args -- scan.py
- `--filter` / `-f` -- comma-separated sport/competition names to filter (e.g. "tennis", "tennis,hockey", "tennis,pro football"). Values map to `sub_sport` for local entity filtering; special values like "pro football" and "college football" are translated to the correct Kalshi API tag ("Football") for fetching.
- `--model` -- LLM model name (default: `claude-sonnet-5`; the GPU path passes the orchestrator-provided `$LLM_MODEL`, e.g. `gpt-oss:120b` for Ollama)
- `--min-volume` -- exclude markets below this volume (default: 200)
- `--batch-size` -- pairs per LLM call (default: 12)
- `--category` -- Kalshi category (default: Sports)
- `--db` -- SQLite database path (default: slonk_arb.db)
- `--from-db` -- skip fetching, use tickers already in DB
- `--rescan` -- re-screen all pairs even if already evaluated in DB
- `--max-pairs` -- cap number of new pairs to screen per run (limits LLM calls)
- `--log-file` -- log file path (default: scan.log)

### CLI args -- evaluate.py
- `--db` -- SQLite database path (default: slonk_arb.db)
- `--max-n` -- max contracts to search for optimal fill (default: 500)
- `--mode` -- `confirmed` (human-approved, default) or `high` (high-confidence unreviewed)
- `--hot` -- only evaluate pairs whose latest `tob_cost` is below `--hot-threshold` (default 1.03); used by the hourly `karb-job@hot` timer to re-check near-parity pairs cheaply
- `--log-file` -- log file path (default: evaluate.log)

## Scanner data flow

```
Fetch series for category (filtered by API tags if --filter) -> Fetch events + nested markets per series
  -> Extract minimal market representations + sport_tag from series tags + sub_sport from event product_metadata
  -> Upsert tickers into SQLite DB + deactivate missing tickers
  -> Group markets by entity (yes_sub_title) from DB (entities spanning 2+ events)
  -> Apply sub_sport filter + min-volume at entity/pair level
  -> Generate candidate pairs per entity (reject cross-sub_sport pairs + blocklisted/numeric entities)
  -> Filter out already-screened pairs (unless --rescan)
  -> Rule screener decides deterministic pairs for free (finish-position lattices)
  -> Structural reuse: pairs whose (series, event) signature matches an already-decided
     structure inherit that verdict (skipped with --rescan)
  -> LLM screens one representative per remaining structure; verdict fans out to
     structurally identical siblings
  -> Store ALL results in DB (including "none" and "need_more_info" confidence)
  -> Print terminal summary
```

### Pre-filtering strategy

Implication relationships almost always involve the same entity: "Alcaraz wins FO" -> "Alcaraz wins a GS". Grouping by `yes_sub_title` (keeping only entities that appear in 2+ events) is a near-perfect pre-filter that reduces O(n^2) to ~50-200 candidates. All markets within an entity are paired, including same-series pairs. Cross-sport pairs (different `sub_sport`) and blocklisted entities (e.g. "Tie", "Yes") are rejected. `sub_sport` is derived from event `product_metadata.competition` for Football (giving "Pro Football" vs "College Football"), otherwise falls back to `sport_tag` from the series `tags` field.

### Rule-based screening

Before any LLM call, `rule_screen_pairs()` decides pairs whose answer is deterministic from series tickers (~60% of historical volume, ~85% of golf): finish-position lattices (`KXPGATOUR ⊂ KXPGATOP5 ⊂ KXPGATOP10 ⊂ KXPGATOP20 ⊂ KXPGAMAKECUT`, the `KXPGAR1*` round-1 lattice, and LIV equivalents) → `high` with the narrower market as antecedent when same tournament; `none` for cross-tournament or cross-lattice (round vs final) combinations. Results are stored with `llm_model = "rule-screener-v1"`; `high` results are auto-confirmed (`human_review = 'confirmed'`) since the verdicts are deterministic — they skip the review queue and go straight to the confirmed evaluation sweeps. An existing human review is never overwritten. Everything else — `KXPGAR2LEAD` (cut-timing domain knowledge), tennis/hockey structures — defers to the LLM. Validated against 5,451 LLM-screened pairs: 98.5% agreement, zero direction mismatches; all 82 disagreements were LLM errors on one tournament (Valspar), not rule errors.

`aggregate_screen_pair()` then handles season aggregates — "X wins the US Open" → "X wins a Grand Slam", `KXPGAMAJORWIN` → `KXPGATOUR`. **Every pair that has ever produced a BUY is this one shape.** No hardcoded list of slams or majors: an aggregate market enumerates its constituents in its own `rules_primary` ("wins a tennis major (the Australian Open, the U.S. Open, the French Open, or Wimbledon)"), so the rule reads Kalshi's wording and survives their inconsistent naming — Wimbledon is `KXATP-26WIM`, but the French Open is its own series `KXFOMEN`.

The governing verb carries the rule: the other leg must resolve on *winning* a named constituent. A placement or participation market at the same tournament ("finishes top 5 in the Masters", "competes in The Masters") names a major but implies nothing about winning one — ignoring the verb drops agreement from 99.6% to 73%.

It fails closed. When the other leg names no constituent at all it returns `None` and defers, rather than answering `none` — that keeps conjunctive markets like `KXGRANDSLAM` ("wins *all 4*"), whose implication runs the other way, out of the rule's hands. Validated against every stored LLM label: 885 pairs decided (144 high, 741 none), 507 comparable to a real LLM verdict, **100% agreement, zero direction mismatches**, and all 13 historical BUY-producing pairs recovered as `high`.

### LLM screening

Two backends, selected by env var: without `LLM_BASE_URL` (production default), scan.py calls the Anthropic API directly (requires `ANTHROPIC_API_KEY`). With `LLM_BASE_URL` set, it POSTs to `{LLM_BASE_URL}/v1/chat/completions` (OpenAI-compatible; Bearer auth via `LLM_API_KEY`) — the on-standby GPU path: an ephemeral MI300X droplet running Ollama `gpt-oss:120b` (see `gpu_droplet.py`; `dedicated.py` is the managed alternative), billed per GPU-hour to the DO account. Both GPU orchestrators are currently blocked on DO account quotas.

The prompt requests `ticker_a`/`ticker_b` echo-back fields so results are matched to input pairs by ticker rather than array index — prevents silent data corruption if the LLM skips, reorders, or merges results. A failed batch (API error, malformed JSON) is logged and skipped; its pairs stay unscreened and are retried on the next run.

The response schema separates the **verdict** (`implication`: `a_implies_b`/`b_implies_a`/`none`/`unclear`) from **certainty** (`confidence`: `high`/`medium`/`low`); `_normalize_result()` maps this onto the stored schema (confidence + antecedent/consequent). The previous schema overloaded `confidence` as both verdict and certainty, which let the model answer "high" meaning "highly confident there is NO implication" — observed in production as batches of false-positive highs whose own reasoning denied any implication (benchmark: old prompt 36/48, new prompt 144/144 on structurally-labeled pairs).

### Structural reuse

Pairs sharing a **signature** — the sorted ((series, event), (series, event)) of their two legs — ask the same logical question about different entities ("Sinner wins USO -> Sinner wins a Slam" vs the Zverev equivalent), so verdicts transfer. Two layers, both in scan.py: (1) *cross-run*: `get_signature_verdicts()` maps every already-screened structure to its verdict where rows are unanimous (human rejection counts as `none`, `need_more_info` or conflicting rows disqualify the structure); `reuse_screen_pairs()` applies these before any LLM call, and high verdicts whose source structure a human confirmed are auto-confirmed. (2) *within-run*: `screen_pairs_with_llm()` sends one representative per signature to the LLM and fans the verdict out to siblings. Reused rows carry a `[structural reuse of <src pair>]` reasoning prefix and the source's `llm_model`. Replayed on July 11-12 production data: 745 pair-screenings -> 160 (79% saved).

## Database

SQLite database (`slonk_arb.db` by default) with six tables:

- **`tickers`** -- all market info fetched from Kalshi (ticker, series, event, title, prices, volume, sport_tag, sub_sport, timestamps). Primary key: `ticker`. Price columns are the "latest" cache, overwritten each scan. `sport_tag` stores the first tag from the series' `tags` array (e.g., "Tennis"). `sub_sport` is a derived field: for Football series, uses `event.product_metadata.competition` (e.g., "Pro Football", "College Football"); for all other sports, equals `sport_tag`.
- **`prices`** -- append-only price history. One row per ticker per scan with `last_price`, `yes_ask`, `no_ask`, and `recorded_at` timestamp. Populated by `record_prices()` during each scan and when `evaluate.py` fetches pair orderbooks.
- **`candidate_pairs`** -- screening results with `ticker_a`/`ticker_b` (always stored in sorted order), `antecedent_ticker`/`consequent_ticker`, confidence (`high`/`medium`/`low`/`need_more_info`/`none`), reasoning, and `human_review` (confirmed/rejected/NULL). `llm_model` records the screener: an Anthropic/OpenAI model name, or `rule-screener-v1` for deterministic lattice pairs. `code_version` records the git commit (`git describe --always --dirty`) of the screener code that produced the decision; NULL on rows screened before tracking existed or when git is unavailable.
- **`trade_evaluations`** -- append-only evaluation results per pair (orderbook snapshots, yields, costs, recommendation).
- **`treasury_yields`** -- daily Treasury CMT yield curve data for discount rate calculations.
- **`settings`** -- key/value app settings (`buffer_bps`, `borrow_rate_bps`), seeded by `get_connection()` and editable via the webapp settings page.

### `db.py` key functions

All take `conn: sqlite3.Connection` as first arg:

- `init_db(db_path)` -- create tables (idempotent)
- `get_connection(db_path)` -- REPL helper (sets WAL, foreign keys, Row factory)
- `upsert_tickers(conn, markets)` -- insert/update from fetched dicts
- `record_prices(conn, markets)` -- append price snapshots to history table
- `get_tickers_by_entity(conn, min_volume)` -- group active tickers by entity (2+ events)
- `get_screened_pair_keys(conn)` -- set of already-evaluated pair keys
- `get_signature_verdicts(conn)` -- map of pair structure ((series, event) of both legs) to unanimous screening verdict, for structural reuse
- `bulk_upsert_pair_results(conn, results, model, auto_confirm_high=False, code_version=None)` -- store screening results; `auto_confirm_high` marks `high` results confirmed (rule screener); `code_version` records the screener's git commit
- `deactivate_missing_tickers(conn, active_tickers)` -- mark disappeared tickers inactive
- `get_pairs_for_review(conn, status, exclude_expired=False)` -- fetch pairs for review UI (`unreviewed`/`confirmed`/`rejected`/`need_more_info`/`high_unreviewed`); rows carry `arb_cost` (raw ask sum), `est_fees`, and an estimated post-fee `annualized_yield`/`excess_yield` (top-of-book, at-size estimate — the evaluator's stored numbers are authoritative); `exclude_expired=True` drops pairs where either leg's `expected_expiration_time` has passed — the arb needs both markets open (used by the review queue and evaluate.py so resolved pairs retire instead of being shown/evaluated forever; legs with unknown expiration are treated as open)
- `get_pair_detail(conn, pair_id)` -- full info for a single pair
- `set_review(conn, pair_id, decision)` -- set human review

## Review webapp

Flask app (`app.py`) on port 5001 with routes:

| Route | Purpose |
|-------|---------|
| `/` | Dashboard with pair counts |
| `/review` | Unreviewed pairs table (filterable by confidence; expired pairs hidden) |
| `/reviewed` | Confirmed + rejected pairs (history; includes expired) |
| `/pair/<id>` | Pair detail with confirm/reject buttons |
| `/trades` | Latest BUY recommendations per pair |
| `/evaluations` | Recent evaluations stream (`?days=N`) |
| `/settings` | Yield benchmark settings + Treasury curve |
| `/login` | Authentication |
| `/healthz` | Liveness probe for the container health check |
| `POST /pair/<id>/review` | Submit review decision (confirm/reject/reverse) |
| `POST /settings` | Update hurdle rate settings |

Uses Pico CSS (CDN, classless). Kalshi links: `https://kalshi.com/markets/<series_ticker_lower>/<event_ticker_lower>` (Kalshi redirects to include the slug).

## Logging

`kalshi.py`, `main.py`, `scan.py`, and `evaluate.py` use Python `logging`. `print()` is for user-facing CLI output; `logging` is for diagnostics.

In production every job passes `--log-file -`, which sends logs to stderr and so, under systemd, to the journal — the file defaults are unrotated and reached 162MB. Locally the defaults still write files.

- **`kalshi.py`** -- DEBUG traces on `fetch_market` and `fetch_orderbook` (ticker, status code, latency)
- **`main.py`** -- DEBUG for orderbook fetches, yield calculations, contract-size scan; WARNING for empty orderbooks
- **`scan.py`** -- `--log-file` (default `scan.log`, `-` for stderr). Batch matching summaries, raw LLM responses, unmatched result warnings.
- **`evaluate.py`** -- `--log-file` (default `evaluate.log`, `-` for stderr). Per-pair INFO for BUY/PASS, WARNING for API errors.

When `main.py` is imported by `evaluate.py`, its log calls flow through evaluate's `basicConfig`. When run standalone as CLI, log calls are no-ops.

Both `scan.py` and `evaluate.py` open log files with `filemode="a"` (append).

Read production logs with:
```
ssh mypi-remote 'sudo journalctl _UID=$(id -u podsvc) --since "2 days ago"'
```

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
- Series have a `tags` field (e.g., `["Tennis"]`). The `/series` endpoint accepts a `tags` query parameter for server-side filtering (e.g., `GET /series?category=Sports&tags=Tennis`).

## Deployment

Runs on a Raspberry Pi as a rootless podman container, served publicly at
`karb.mathslug.com` through a Cloudflare tunnel. The Pi is behind NAT with no
inbound path, so `karb.mathslug.com` resolves to Cloudflare and does **not**
accept SSH. Reach the host itself with the `mypi` (LAN) or `mypi-remote`
(tunnel) ssh aliases.

The `~/src/rpi` repo owns the machine; `~/src/rpi/apps/karb.conf` is the single
source of truth for this app's hostname, port, data directory, units, backup
command and job thresholds. Deploys, auto-deploy, backups, the health dashboard
and the tunnel ingress all read it, so changing where karb lives means editing
that file, not five scripts.

### Server layout

```
/home/podsvc/apps/karb/         # Code (git checkout, updated by auto-deploy)
/home/podsvc/data/karb/         # Persistent data (slonk_arb.db) — bind-mounted to /data
/home/podsvc/.config/karb.env   # Secrets (see below)
/var/lib/rpi-health/jobs/       # Job receipts read by the Pi's dashboard
```

Logs go to the systemd journal, not to files.

### Stack

- **cloudflared** -- public ingress + TLS; the only thing in front of the app
- **podman (rootless)** -- one image, `localhost/karb:latest`, built on the Pi
- **gunicorn** -- WSGI server inside the container, 2 workers, port 8000 on loopback
- **systemd (user units)** -- `karb.service` for the web UI (from the Quadlet
  `deploy/karb.container`), plus four timers driving `karb-job@.service`.
  Requires `loginctl enable-linger podsvc` so these run without a login.
- **Gmail SMTP** -- email notifications for BUY signals

The web UI and the scheduled jobs run the *same image* with different commands,
so the scanner and the review UI can never disagree about which code version
they are running. They are separate units so a redeploy cannot kill a scan
halfway through, and a crashed web app does not silently stop the scanner.

### Deploying

Pull-based: nothing can reach the Pi from outside. A timer runs `git ls-remote`
every 15 minutes and rebuilds only when the branch has moved
(`~/src/rpi/auto-deploy.sh`). The test suite runs at image build time, so a
failing suite fails the build and the previous container keeps serving.

`.github/workflows/deploy.yml` is retired and manual-trigger only.

### Scheduled jobs

Four timers, each running `deploy/run-jobs.sh <job>`. Every step in a job runs
even if an earlier one fails, and the script exits non-zero so systemd marks
the unit failed rather than burying it in a log.

| UTC | Unit | Job |
|-----|------|-----|
| 07:35 | `karb-job@sports` | `scan.py --category Sports --max-pairs 0` -- fetch all sports tickers into DB (no LLM) |
| 08:00 | `karb-job@daily` | `fetch_yields.py` + `scan.py --from-db --filter "tennis,hockey,golf" --min-volume 200 --max-pairs 0` (rule screener + structural reuse; **no LLM**) + `evaluate.py` + `evaluate.py --mode high` |
| every 2h except 08:00 | `karb-job@sweep` | `evaluate.py` + `evaluate.py --mode high` -- full re-evaluation sweeps against fresh orderbooks |
| every 15 min (:07/:22/:37/:52) | `karb-job@hot` | `evaluate.py --hot` (+ `--mode high`) -- re-check near-parity pairs only (latest tob_cost < 1.03) |

Measured wall times on the Pi (`scripts/check_server.sh` reports these): sports
~28 min, daily ~5 min, sweep ~5 min, hot ~30s. The sweep is what promotes a
pair into the hot tier -- `--hot` filters on the *latest stored* `tob_cost`, so
a pair drifting toward parity is invisible to the hot job until a sweep
evaluates it. Sweep cadence, not hot cadence, bounds how long a new opportunity
can sit unnoticed.

Sweeps avoid 07:35-08:10, where the 28-minute sports fetch bulk-writes the same
SQLite file (`busy_timeout` is 5s; a long write transaction can outlast it).

A clean run touches `/var/lib/rpi-health/jobs/karb.<job>`. The Pi's dashboard
watches the *age* of those files, which catches both a job that fails and a job
that has stopped being scheduled. Thresholds live in `JOB_RECEIPTS` in
`karb.conf`.

### LLM screening is off

The daily scan passes `--max-pairs 0`, which skips the LLM. The rule screener
and structural reuse still run and persist first — screening happens, it just
costs nothing. `--max-pairs 0` returns after those steps, and
`anthropic.Anthropic()` is constructed lazily inside `_call_anthropic`, so a
run makes no API call and needs no key.

This is safe because the rules cover the aggregate/constituent family, which is
every pair that has ever produced a BUY. What is given up is discovery of
market structures the rules do not know; those pairs stay unscreened (~50-60 a
day) rather than being marked `none`, and are picked up whenever the LLM runs
again.

**To turn it back on:** drop `--max-pairs 0` from the `daily` case in
`deploy/run-jobs.sh`, and make sure `ANTHROPIC_API_KEY` is set in
`~/.config/karb.env` on the Pi. Nothing else was removed — `screen_pairs_with_llm`,
both backends and the GPU orchestrators are all still there. The accumulated
backlog is screened on the first run, so expect a larger bill than a normal day.

For a one-off without changing the schedule:

```
uv run scan.py --from-db --filter "tennis,hockey,golf" --min-volume 200
```

### Backups

`~/src/rpi/backup/pull-backups.sh` runs daily on the Mac and pulls, rather than
the Pi pushing. It runs `snapshot.py` *inside* the container (`VACUUM INTO`,
never a file copy — WAL means copying the main file yields a valid but
incomplete database), retrieves it gzipped, verifies the copy it kept, and
writes a dated snapshot under `~/src/rpi/backups/`.

`scripts/pull_prod.sh` decompresses the newest of those into the working tree.
It does not touch production.

### Operations

```
bash scripts/check_server.sh                    # commit, units, HTTP, receipts, durations, disk
bash scripts/pull_prod.sh                       # production DB into the working tree
ssh mypi-remote 'sudo journalctl _UID=$(id -u podsvc) -f'
```

### Email notifications

**`notify.py`** -- `send_buy_alert(results)` sends a summary email via Gmail SMTP when BUY signals are found. Called automatically by `evaluate.py`. Requires env vars: `SMTP_USER`, `SMTP_PASSWORD`, `NOTIFY_EMAIL`.

### Environment variables

On the Pi these live in `/home/podsvc/.config/karb.env`, created from
`ENV_TEMPLATE` in `karb.conf` on first deploy and then left alone. Locally, in
`.env`.

```
ANTHROPIC_API_KEY=...       # LLM screening in scan.py
SLONK_ADMIN_PASSWORD=...    # HTTP auth on write routes; empty = no admin
FLASK_SECRET_KEY=...        # session signing; regenerating it logs everyone out
SMTP_USER=...
SMTP_PASSWORD=...
NOTIFY_EMAIL=...
```

`DIGITALOCEAN_TOKEN` is deliberately absent on the Pi: without it
`gpu_droplet.py` cannot spin up a $1.99/hr GPU droplet.

`LLM_BASE_URL` / `LLM_API_KEY` are never stored; `run_scan_gpu.sh` gets them
per-run from the orchestrator.
