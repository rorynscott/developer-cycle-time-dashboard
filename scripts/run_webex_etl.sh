#!/bin/bash
# Webex ETL cron wrapper — reads messages from the configured room.
# Requires ~/.webex_refresh_token (created by scripts/webex_auth.py).
#
# Cron entry (daily at 8:10am, 5 minutes after Jira):
#   10 8 * * * /path/to/developer-cycle-time-dashboard/scripts/run_webex_etl.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/data/logs"
LOG_FILE="$LOG_DIR/webex_etl.log"
ERROR_FILE="$LOG_DIR/webex_etl_errors.log"

mkdir -p "$LOG_DIR"

echo "===== $(date -u '+%Y-%m-%d %H:%M:%S UTC') =====" >> "$LOG_FILE"

PYTHON="${PYTHON:-$(command -v python3)}"
"$PYTHON" "$PROJECT_DIR/webex_etl.py" >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] ETL FAILED (exit $EXIT_CODE) — see $LOG_FILE" >> "$ERROR_FILE"
fi

tail -2000 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
