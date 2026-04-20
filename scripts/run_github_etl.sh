#!/bin/bash
# GitHub PR ETL cron wrapper
# Runs the ETL, logs output, and tracks errors separately.
#
# Cron entry (daily at 8am, adjust to your timezone):
#   0 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_github_etl.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/data/logs"
LOG_FILE="$LOG_DIR/pr_etl.log"
ERROR_FILE="$LOG_DIR/pr_etl_errors.log"

mkdir -p "$LOG_DIR"

echo "===== $(date -u '+%Y-%m-%d %H:%M:%S UTC') =====" >> "$LOG_FILE"

python3 "$PROJECT_DIR/github_etl.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] ETL FAILED (exit $EXIT_CODE) — see $LOG_FILE" >> "$ERROR_FILE"
fi

# Keep logs from growing forever — trim to last 2000 lines
tail -2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
