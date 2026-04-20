"""
Shared database utilities for the ETL pipelines.

Provides SQLite connection management, retry logic for concurrent access,
and generic watermark helpers for incremental loading.
"""

import os
import sys
import time
import sqlite3
from datetime import datetime, timezone


def log(msg):
    """Log a message to stderr."""
    print(msg, file=sys.stderr)


def is_error(result):
    """Check if an API result dict indicates an error."""
    return isinstance(result, dict) and "error" in result


def load_token(path):
    """Read a token from a file, stripping whitespace."""
    with open(os.path.expanduser(path)) as f:
        return f.read().strip()


def get_db_connection(db_path):
    """Open a SQLite connection with WAL mode and a generous busy timeout.

    The 60-second timeout lets concurrent ETL processes wait for each
    other's writes rather than failing immediately.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def with_db_retry(fn, max_retries=5, base_delay=1.0):
    """Call fn(), retrying on sqlite3 'database is locked' with exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                log(f"  DB locked, retry {attempt + 1}/{max_retries} "
                    f"in {delay:.1f}s...")
                time.sleep(delay)
                continue
            raise


def read_watermark(conn, table, pipeline):
    """Read the last_updated_since value from a watermark table.

    Args:
        conn: SQLite connection
        table: Watermark table name (e.g. 'etl_watermark', 'etl_watermark_jira')
        pipeline: Pipeline name key (e.g. 'github_pr_etl', 'jira_etl')
    """
    cur = conn.execute(
        f"SELECT last_updated_since FROM {table} "
        f"WHERE pipeline_name = ?",
        (pipeline,),
    )
    row = cur.fetchone()
    if row and row[0]:
        return row[0]
    return None


def update_watermark(conn, table, pipeline, since_used, count, count_col):
    """Write a watermark record after a successful ETL run.

    Args:
        conn: SQLite connection
        table: Watermark table name
        pipeline: Pipeline name key
        since_used: The 'since' timestamp that was used for this run
        count: Number of records processed
        count_col: Column name for the count (e.g. 'prs_processed')
    """
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        f"INSERT OR REPLACE INTO {table} "
        f"(pipeline_name, last_run_at, last_updated_since, {count_col}) "
        f"VALUES (?, ?, ?, ?)",
        (pipeline, now, since_used, count),
    )
    conn.commit()
