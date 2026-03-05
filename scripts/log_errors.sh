#!/usr/bin/env bash
# Scan local log files for errors, warnings, and tracebacks.
# Usage: bash scripts/log_errors.sh [cron.log scan.log evaluate.log evaluate-high.log]

if [ $# -gt 0 ]; then
    FILES=("$@")
else
    FILES=(cron.log scan.log evaluate.log evaluate-high.log)
fi

for f in "${FILES[@]}"; do
    if [ ! -f "$f" ]; then
        echo "--- $f: not found, skipping ---"
        continue
    fi
    lines=$(wc -l < "$f" | tr -d ' ')
    errors=$(grep -cE '(ERROR|Traceback|PermissionError|Exception)' "$f" || true)
    warnings=$(grep -cE 'WARNING' "$f" || true)
    echo "--- $f: $lines lines, $errors errors, $warnings warnings ---"
    if [ "$errors" -gt 0 ]; then
        echo "  Errors:"
        grep -nE '(ERROR|PermissionError|Exception:)' "$f" | head -10 | sed 's/^/    /'
    fi
    echo ""
done
