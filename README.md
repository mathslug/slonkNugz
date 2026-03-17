# Karb Scanner

Cross-market arbitrage scanner for [Kalshi](https://kalshi.com), a regulated prediction market exchange.

## What this does

Prediction markets sometimes misprice logically related contracts. If "Djokovic wins the French Open" is priced at 8c but "Djokovic wins a Grand Slam in 2026" is priced at 5c, there is a potential arbitrage because winning the French Open *guarantees* winning a Grand Slam. You can buy NO on the French Open and YES on the Grand Slam, lock in a risk-free profit, and wait for settlement.

This project finds those opportunities automatically, using an LLM for the logical inference.

## How it works

The system runs a daily pipeline:

1. **Fetch** all sports markets from the Kalshi API
2. **Group** markets by entity (e.g., all contracts mentioning "Djokovic")
3. **Screen** cross-series pairs with an LLM (Claude Sonnet) to identify logical implications — does A resolving YES *necessarily* mean B resolves YES?
4. **Human review** — a webapp presents candidate pairs for confirmation or rejection
5. **Evaluate** confirmed pairs against live orderbooks, computing all-in cost with Kalshi's fee model
6. **Alert** via email when a profitable trade is found

The LLM screening step is the key step. There are thousands of markets but only a handful of logical implication relationships, and they require understanding sport-specific rules ("you must win the conference championship to reach the Super Bowl"). Programmatic pre-filtering narrows candidates to ~50-200 pairs by grouping on entity names, then the LLM does the actual reasoning.

## The t-bill analogy

When you find a valid arb pair, you're locking in a guaranteed payout at some future settlement date, so it's structurally identical to a Treasury bill. A 10% return sounds great, but if settlement is 8 months away and you could earn 4.5% risk-free in Treasuries, the excess yield shrinks. The system fetches the daily Treasury yield curve and interpolates to the settlement date to compute whether the arb actually beats your opportunity cost (plus a configurable buffer for your borrowing rate).

## Has it found anything?

Yes. At one point, Lorenzo Musetti's odds of winning the French Open were priced substantially higher than his odds of winning *any* Grand Slam in 2025, a logical impossibility. The spread implied roughly a 10% annualized yield.

## Live instance

The review webapp is running at [slonkn.mathslug.com](https://slonkn.mathslug.com). You can browse pairs and evaluation results, but confirming/rejecting pairs requires authentication.

Currently scoped to tennis and hockey markets. The architecture supports any Kalshi sport category, with a `sub_sport` system that distinguishes pro from college leagues (e.g., NFL vs college football).

## Built with Claude

This project was built with [Claude Code](https://claude.com/claude-code). The LLM screening step uses the Claude API directly: Claude Sonnet evaluates candidate pairs for logical implication, returning structured JSON with confidence levels and reasoning. The [`CLAUDE.md`](CLAUDE.md) file that guides Claude Code's understanding of the codebase is committed and kept up to date.

## Architecture

```
scan.py          Discover and screen pairs (fetch -> group -> LLM -> DB)
evaluate.py      Evaluate confirmed pairs against live orderbooks
main.py          Evaluate a single known pair (CLI)
app.py           Flask webapp for human review
db.py            SQLite persistence (all functions take conn, no global state)
kalshi.py        API helpers, fee model, orderbook walking
fetch_yields.py  Treasury yield curve data
notify.py        Email alerts via Mailgun
```

## Quick start

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
# Scan for tennis arb pairs
echo "ANTHROPIC_API_KEY=sk-..." > .env
uv run scan.py --filter tennis --min-volume 200

# Evaluate from DB
uv run evaluate.py

# Review webapp
uv run app.py  # http://localhost:5001
```

## Workflow

```
Morning fetch (all sports tickers) -> LLM screening (tennis + hockey, filtered) -> evaluate confirmed pairs
                                                                       -> evaluate high-confidence unreviewed
Afternoon -> re-evaluate confirmed pairs with fresh orderbooks
```

The human-in-the-loop step matters. The LLM is good but not perfect, as it occasionally misjudges implication direction or misunderstands tournament structures. High-confidence pairs get auto-evaluated as a safety net, but real money should only follow human-confirmed pairs.

## License

MIT
