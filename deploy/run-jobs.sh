#!/usr/bin/env bash
#
# run-jobs.sh — run one scheduled karb job as short-lived containers.
#
#     deploy/run-jobs.sh daily
#
# This replaces /etc/cron.d/slonk-arb. Each job is a sequence of steps run from
# the same image the web UI runs, so the scanner and the review UI can never
# disagree about which code version they are running.
#
# Two deliberate departures from the cron file it replaces:
#
#   * Every step runs even if an earlier one fails — that was cron's `;`
#     semantics, and losing a whole evaluation sweep because a Treasury yield
#     fetch 500'd would be a regression. Unlike cron, a failure is remembered
#     and this script exits non-zero, so systemd marks the unit failed and the
#     failure is visible rather than buried in a log nobody opens.
#
#   * Logs go to stderr, which under systemd means the journal, rather than to
#     the four unrotated files that reached 162MB on the droplet. See the
#     logging comment in scan.py for why that also means INFO rather than
#     DEBUG.

set -uo pipefail   # deliberately not -e: see above

JOB="${1:-}"
IMAGE="localhost/karb:latest"
DATA="${HOME}/data/karb"
ENV_FILE="${HOME}/.config/karb.env"
DB="/data/slonk_arb.db"

failed=0

step() {
  local name="$1"; shift
  printf '== %s\n' "$name"
  # --rm so exited containers do not accumulate. --memory is sized to hold the
  # database rather than to constrain the process — these jobs sweep the whole
  # 702MB of it, and a cap below that makes the kernel reclaim page cache from
  # this cgroup and re-read from the SD card. See deploy/karb.container.
  if podman run --rm \
      --memory=1536m \
      --env-file "$ENV_FILE" \
      --env "SLONK_DB=${DB}" \
      --volume "${DATA}:/data" \
      "$IMAGE" python "$@"
  then
    printf '== %s: ok\n' "$name"
  else
    printf '== %s: FAILED (exit %d)\n' "$name" "$?" >&2
    failed=1
  fi
}

# Receipt for the Pi's dashboard, written only if every step succeeded.
#
# systemd already knows a unit's Result, and querying that would need no new
# machinery — but it is the wrong source for two reasons. A unit that has never
# run reports Result=success with no timestamp, so "success" alone is not
# evidence anything happened; and user unit state resets on reboot, so every
# power cut would blank the record on a machine built to survive power cuts.
#
# A file on disk has neither problem, and its AGE catches both failure modes at
# once: a job that fails and a job that stops being scheduled both stop
# refreshing it.
receipt() {
  local dir=/var/lib/rpi-health/jobs
  if [ ! -d "$dir" ]; then
    # Say so rather than skipping quietly. This account cannot create the
    # directory (its parent is root-owned), so without a word here the
    # dashboard would show "no clean run" forever for jobs that ran perfectly,
    # and the obvious place to look would be the wrong one.
    printf '== NOTE: %s missing; dashboard cannot see this run (bootstrap.sh creates it)\n' \
      "$dir" >&2
    return 0
  fi
  date +%s > "${dir}/karb.${JOB}" 2>/dev/null \
    || printf '== NOTE: could not write the job receipt to %s\n' "$dir" >&2
}

case "$JOB" in
  # 07:35 UTC — fetch every sports ticker into the DB. No LLM calls.
  sports)
    step "scan sports" \
      scan.py --category Sports --max-pairs 0 --db "$DB" --log-file -
    ;;

  # 08:00 UTC — the main pass: yields, then screening, then both evaluation
  # modes over what it found.
  #
  # --max-pairs 0 skips the LLM: the rule screener and structural reuse still
  # run and persist first, so this screens for free. The rules cover the
  # aggregate/constituent family (US Open -> a Grand Slam), which is every pair
  # that has ever produced a BUY. Everything the LLM would add on top has never
  # closed below parity. Drop the flag to re-enable paid screening.
  daily)
    step "fetch yields" \
      fetch_yields.py --db "$DB"
    step "scan" \
      scan.py --from-db --filter "tennis,hockey,golf" --min-volume 200 \
              --max-pairs 0 --db "$DB" --log-file -
    step "evaluate" \
      evaluate.py --db "$DB" --log-file -
    step "evaluate high" \
      evaluate.py --mode high --db "$DB" --log-file -
    ;;

  # Every 2 hours except 08:00 — full re-evaluation of confirmed and
  # high-confidence pairs. Also what refreshes hot-tier membership: the hot
  # job only re-checks pairs a sweep has already stored near parity.
  sweep)
    step "evaluate" \
      evaluate.py --db "$DB" --log-file -
    step "evaluate high" \
      evaluate.py --mode high --db "$DB" --log-file -
    ;;

  # Every 15 minutes — re-check only pairs whose latest top-of-book cost is
  # near parity. Small set, ~30s; catches intraday dislocations between
  # sweeps.
  hot)
    step "evaluate hot" \
      evaluate.py --hot --db "$DB" --log-file -
    step "evaluate hot high" \
      evaluate.py --hot --mode high --db "$DB" --log-file -
    ;;

  *)
    printf 'usage: %s {sports|daily|sweep|hot}\n' "$0" >&2
    exit 2
    ;;
esac

[ "$failed" = "0" ] && receipt

exit "$failed"
