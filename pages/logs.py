"""
ETL Logs — view GitHub and Jira ETL run history, errors, and recent output.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.config import get_log_dir

LOG_DIR = get_log_dir()
ET = timezone(timedelta(hours=-4))

ETL_CONFIGS = [
    {
        "name": "GitHub PR ETL",
        "log_file": os.path.join(LOG_DIR, "pr_etl.log"),
        "error_file": os.path.join(LOG_DIR, "pr_etl_errors.log"),
    },
    {
        "name": "Jira ETL",
        "log_file": os.path.join(LOG_DIR, "jira_etl.log"),
        "error_file": os.path.join(LOG_DIR, "jira_etl_errors.log"),
    },
]


def read_tail(path, max_lines=500):
    """Read the last max_lines from a file."""
    if not os.path.exists(path):
        return None
    with open(path) as f:
        lines = f.readlines()
    return lines[-max_lines:]


def parse_runs(lines):
    """Split log lines into per-run chunks keyed by timestamp."""
    runs = []
    current_ts = None
    current_lines = []
    for line in lines:
        if line.startswith("=====") and "UTC" in line:
            if current_ts:
                runs.append((current_ts, current_lines))
            ts_str = line.strip().strip("= ")
            try:
                current_ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S %Z").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                current_ts = ts_str
            current_lines = []
        else:
            current_lines.append(line)
    if current_ts:
        runs.append((current_ts, current_lines))
    return runs


def render_etl_logs(etl_config):
    """Render the log viewer for a single ETL pipeline."""
    name = etl_config["name"]

    # Errors banner
    error_lines = read_tail(etl_config["error_file"], max_lines=50)
    if error_lines:
        recent_errors = [l for l in error_lines if l.strip()]
        if recent_errors:
            st.error(f"**{len(recent_errors)} error(s) recorded** in error log")
            with st.expander("Error log"):
                st.code("".join(recent_errors), language="text")

    # Run history
    log_lines = read_tail(etl_config["log_file"])
    if not log_lines:
        st.warning(f"No log file found for {name}. ETL may not have run yet.")
        return

    runs = parse_runs(log_lines)
    if not runs:
        st.warning("Could not parse any runs from the log file.")
        return

    st.subheader(f"Run History ({len(runs)} runs in log)")

    run_summaries = []
    for ts, lines in reversed(runs):
        text = "".join(lines)
        has_error = "Traceback" in text or "FAILED" in text or "Error" in text
        local_ts = ts.astimezone(ET) if isinstance(ts, datetime) else ts
        run_summaries.append({
            "time_et": local_ts.strftime("%Y-%m-%d %I:%M %p ET") if isinstance(local_ts, datetime) else str(local_ts),
            "lines": len(lines),
            "status": "FAILED" if has_error else "OK",
        })

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Runs", len(runs))
    failed = sum(1 for r in run_summaries if r["status"] == "FAILED")
    col2.metric("Failed", failed)
    col3.metric("Last Run", run_summaries[0]["time_et"] if run_summaries else "—")

    st.dataframe(
        run_summaries,
        column_config={
            "time_et": st.column_config.TextColumn("Run Time (ET)"),
            "lines": st.column_config.NumberColumn("Log Lines"),
            "status": st.column_config.TextColumn("Status"),
        },
        hide_index=True,
        use_container_width=True,
    )

    # Expandable per-run logs
    st.subheader("Run Details")
    for ts, lines in reversed(runs):
        local_ts = ts.astimezone(ET) if isinstance(ts, datetime) else ts
        label = local_ts.strftime("%Y-%m-%d %I:%M %p ET") if isinstance(local_ts, datetime) else str(local_ts)
        text = "".join(lines)
        has_error = "Traceback" in text or "FAILED" in text or "Error" in text
        prefix = "FAILED" if has_error else "OK"
        with st.expander(f"{prefix} — {label}", expanded=has_error):
            st.code(text if text.strip() else "(no output)", language="text")


def main():
    st.set_page_config(page_title="ETL Logs", layout="wide")
    st.title("ETL Logs")

    tabs = st.tabs([c["name"] for c in ETL_CONFIGS])
    for tab, etl_config in zip(tabs, ETL_CONFIGS):
        with tab:
            render_etl_logs(etl_config)


if __name__ == "__main__":
    main()
