#!/usr/bin/env python3
"""
Jira ETL to SQLite

Extracts Jira issue data from the Jira Cloud REST API and loads it into the
same SQLite database used by the GitHub PR ETL. This enables joining PRs to
Jira issues via the bridge_pr_jira table.

Usage:
    python3 jira_etl.py                              # auto (since last run, or last 30 days)
    python3 jira_etl.py --since 2026-01-01           # from a specific date
    python3 jira_etl.py --since 2026-01-01 --until 2026-03-31  # date range
    python3 jira_etl.py --backfill                   # fetch all issues referenced in bridge_pr_jira
    python3 jira_etl.py --dry-run --verbose          # preview without writing
"""

import json
import sys
import os
import time
import sqlite3
import argparse
import base64
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    load_config, get_jira_token, get_jira_email, get_jira_base_url,
    get_jira_projects, get_jira_custom_fields, get_jira_status_categories,
    get_db_path,
)
from lib.db import (
    log, is_error, get_db_connection, with_db_retry,
    read_watermark, update_watermark,
)

# ── Configuration ────────────────────────────────────────────────────────────

cfg = load_config()
custom_fields = get_jira_custom_fields()
status_cats = get_jira_status_categories()

# Build the list of Jira fields to request
JIRA_FIELDS = [
    "summary", "issuetype", "status", "priority", "assignee", "reporter",
    "labels", "fixVersions", "created", "updated", "resolutiondate",
    "parent",
]
# Add configured custom fields
for field_id in custom_fields.values():
    if field_id not in JIRA_FIELDS:
        JIRA_FIELDS.append(field_id)

# ── Jira API ─────────────────────────────────────────────────────────────────


def jira_api(email, token, url, retries=3, method="GET", body=None):
    """Make a request to the Jira REST API with Basic Auth."""
    credentials = base64.b64encode(f"{email}:{token}".encode()).decode()
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Basic {credentials}")
    req.add_header("Accept", "application/json")
    if body is not None:
        req.add_header("Content-Type", "application/json")

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 10))
                log(f"  Rate limited (429), sleeping {retry_after}s... "
                    f"(attempt {attempt + 1}/{retries})")
                time.sleep(retry_after)
                continue
            if e.code == 401:
                log("ERROR: Jira authentication failed (401). "
                    "Check your email and API token.")
                return {"error": str(e), "status": e.code}
            body_text = e.read().decode() if attempt == 0 else ""
            if attempt == retries - 1:
                return {"error": str(e), "status": e.code, "body": body_text}
            time.sleep(2)
    return {"error": "Max retries exceeded"}


def fetch_changelog(email, token, base_url, issue_key):
    """Fetch all changelog entries for an issue, return status transitions."""
    transitions = []
    start_at = 0
    while True:
        url = (f"{base_url}/rest/api/3/issue/{issue_key}"
               f"/changelog?maxResults=100&startAt={start_at}")
        result = jira_api(email, token, url)
        if is_error(result):
            log(f"    Changelog error for {issue_key}: {result}")
            break
        for entry in result.get("values", []):
            for item in entry.get("items", []):
                if item.get("field") == "status":
                    transitions.append({
                        "issue_key": issue_key,
                        "changed_at": normalize_jira_timestamp(entry["created"]),
                        "from_status": item.get("fromString"),
                        "to_status": item.get("toString"),
                    })
        if result.get("isLast", True):
            break
        start_at += 100
    return transitions


def search_issues(email, token, base_url, jql, fields=None, max_results=100,
                   next_page_token=None):
    """Execute a JQL search via the POST /search/jql endpoint."""
    url = f"{base_url}/rest/api/3/search/jql"
    payload = {
        "jql": jql,
        "maxResults": max_results,
        "fields": fields or JIRA_FIELDS,
    }
    if next_page_token:
        payload["nextPageToken"] = next_page_token
    body = json.dumps(payload).encode()
    return jira_api(email, token, url, method="POST", body=body)


def search_all_issues(email, token, base_url, jql, fields=None):
    """Paginate through all results for a JQL query."""
    all_issues = []
    next_page_token = None
    page_size = 100

    while True:
        result = search_issues(email, token, base_url, jql, fields, page_size,
                               next_page_token=next_page_token)
        if is_error(result):
            log(f"  Error searching issues: {result}")
            break

        issues = result.get("issues", [])
        all_issues.extend(issues)

        log(f"  Fetched {len(all_issues)} issues so far")

        if result.get("isLast", True) or not issues:
            break

        next_page_token = result.get("nextPageToken")
        if not next_page_token:
            break

        time.sleep(0.2)

    return all_issues


# ── Transform ────────────────────────────────────────────────────────────────


def normalize_jira_timestamp(ts):
    """Normalize Jira timestamps to ISO 8601 with +00:00 offset."""
    if not ts:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            dt = datetime.strptime(ts, fmt)
            if dt.tzinfo:
                dt = dt.astimezone(timezone.utc)
            return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        except ValueError:
            continue
    return ts


def extract_sprint_info(sprint_field_value):
    """Extract sprint details from the sprint custom field."""
    if not sprint_field_value:
        return None, None, None, None, None, None

    sprints = sprint_field_value if isinstance(sprint_field_value, list) else [sprint_field_value]
    active = [s for s in sprints if s.get("state") == "active"]
    closed = [s for s in sprints if s.get("state") == "closed"]

    sprint = active[0] if active else (closed[-1] if closed else sprints[-1])

    return (
        sprint.get("id"),
        sprint.get("name"),
        sprint.get("state"),
        sprint.get("boardId"),
        sprint.get("startDate"),
        sprint.get("endDate"),
    )


def transform_issue(issue):
    """Transform a Jira API issue into a flat record for SQLite."""
    fields = issue.get("fields", {})
    key = issue["key"]
    project_key = key.split("-")[0]

    issue_type = (fields.get("issuetype") or {}).get("name")
    status = (fields.get("status") or {}).get("name")
    status_category = (fields.get("status") or {}).get("statusCategory", {}).get("key")
    priority = (fields.get("priority") or {}).get("name")

    assignee = fields.get("assignee") or {}
    assignee_id = assignee.get("accountId")
    assignee_name = assignee.get("displayName")

    reporter = fields.get("reporter") or {}
    reporter_id = reporter.get("accountId")
    reporter_name = reporter.get("displayName")

    parent = fields.get("parent") or {}
    parent_key = parent.get("key")
    parent_type = (parent.get("fields", {}).get("issuetype") or {}).get("name")

    labels = [l for l in (fields.get("labels") or [])]
    fix_versions = [v.get("name") for v in (fields.get("fixVersions") or [])]

    # Custom fields from config
    story_points_field = custom_fields.get("story_points")
    sprint_field = custom_fields.get("sprint")

    story_points = fields.get(story_points_field) if story_points_field else None

    sprint_id, sprint_name, sprint_state, board_id, sprint_start, sprint_end = \
        extract_sprint_info(fields.get(sprint_field) if sprint_field else None)

    # Collect any extra custom fields beyond story_points and sprint
    extra_custom = {}
    for logical_name, field_id in custom_fields.items():
        if logical_name not in ("story_points", "sprint"):
            val = fields.get(field_id)
            if val is not None:
                extra_custom[logical_name] = val

    return {
        "issue_key": key,
        "issue_id": issue.get("id"),
        "project_key": project_key,
        "issue_type": issue_type,
        "summary": (fields.get("summary") or "")[:2000],
        "status": status,
        "status_category": status_category,
        "priority": priority,
        "assignee_account_id": assignee_id,
        "assignee_display_name": assignee_name,
        "reporter_account_id": reporter_id,
        "reporter_display_name": reporter_name,
        "parent_key": parent_key,
        "parent_type": parent_type,
        "labels": json.dumps(labels) if labels else None,
        "fix_versions": json.dumps(fix_versions) if fix_versions else None,
        "story_points": story_points,
        "sprint_id": sprint_id,
        "sprint_name": sprint_name,
        "sprint_state": sprint_state,
        "board_id": board_id,
        "sprint_start_date": sprint_start,
        "sprint_end_date": sprint_end,
        "custom_fields_json": json.dumps(extra_custom) if extra_custom else None,
        "created_at": normalize_jira_timestamp(fields.get("created")),
        "updated_at": normalize_jira_timestamp(fields.get("updated")),
        "resolved_at": normalize_jira_timestamp(fields.get("resolutiondate")),
        "in_progress_at": None,
        "done_at": None,
    }


# ── SQLite ───────────────────────────────────────────────────────────────────

JIRA_DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_jira_issue (
        issue_key               TEXT PRIMARY KEY,
        issue_id                TEXT,
        project_key             TEXT NOT NULL,
        issue_type              TEXT,
        summary                 TEXT,
        status                  TEXT,
        status_category         TEXT,
        priority                TEXT,
        assignee_account_id     TEXT,
        assignee_display_name   TEXT,
        reporter_account_id     TEXT,
        reporter_display_name   TEXT,
        parent_key              TEXT,
        parent_type             TEXT,
        labels                  TEXT,
        fix_versions            TEXT,
        story_points            REAL,
        sprint_id               INTEGER,
        sprint_name             TEXT,
        sprint_state            TEXT,
        board_id                INTEGER,
        sprint_start_date       TEXT,
        sprint_end_date         TEXT,
        custom_fields_json      TEXT,
        created_at              TEXT,
        updated_at              TEXT,
        resolved_at             TEXT,
        in_progress_at          TEXT,
        done_at                 TEXT,
        etl_loaded_at           TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_jira_issue_project ON dim_jira_issue(project_key)",
    "CREATE INDEX IF NOT EXISTS idx_jira_issue_status ON dim_jira_issue(status)",
    "CREATE INDEX IF NOT EXISTS idx_jira_issue_assignee ON dim_jira_issue(assignee_account_id)",
    "CREATE INDEX IF NOT EXISTS idx_jira_issue_sprint ON dim_jira_issue(sprint_name)",
    "CREATE INDEX IF NOT EXISTS idx_jira_issue_parent ON dim_jira_issue(parent_key)",
    "CREATE INDEX IF NOT EXISTS idx_jira_issue_type ON dim_jira_issue(issue_type)",
    """
    CREATE TABLE IF NOT EXISTS dim_jira_status_change (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_key           TEXT NOT NULL,
        changed_at          TEXT NOT NULL,
        from_status         TEXT,
        to_status           TEXT,
        etl_loaded_at       TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_jira_sc_issue ON dim_jira_status_change(issue_key)",
    "CREATE INDEX IF NOT EXISTS idx_jira_sc_to ON dim_jira_status_change(to_status)",
    """
    CREATE TABLE IF NOT EXISTS etl_watermark_jira (
        pipeline_name       TEXT PRIMARY KEY,
        last_run_at         TEXT,
        last_updated_since  TEXT,
        issues_processed    INTEGER
    )
    """,
]


def ensure_jira_tables(conn):
    for ddl in JIRA_DDL:
        conn.execute(ddl)

    cols = {row[1] for row in conn.execute("PRAGMA table_info(dim_jira_issue)").fetchall()}
    for col in ("in_progress_at", "done_at"):
        if col not in cols:
            conn.execute(f"ALTER TABLE dim_jira_issue ADD COLUMN {col} TEXT")
            log(f"  Migration: added {col} column to dim_jira_issue")
    if "custom_fields_json" not in cols:
        conn.execute("ALTER TABLE dim_jira_issue ADD COLUMN custom_fields_json TEXT")
        log("  Migration: added custom_fields_json column to dim_jira_issue")

    conn.commit()
    log("  Jira tables ensured.")


def upsert_jira_issue(conn, record):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO dim_jira_issue (
            issue_key, issue_id, project_key, issue_type, summary,
            status, status_category, priority,
            assignee_account_id, assignee_display_name,
            reporter_account_id, reporter_display_name,
            parent_key, parent_type, labels, fix_versions,
            story_points, sprint_id, sprint_name, sprint_state,
            board_id, sprint_start_date, sprint_end_date, custom_fields_json,
            created_at, updated_at, resolved_at,
            in_progress_at, done_at, etl_loaded_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            record["issue_key"], record["issue_id"], record["project_key"],
            record["issue_type"], record["summary"],
            record["status"], record["status_category"], record["priority"],
            record["assignee_account_id"], record["assignee_display_name"],
            record["reporter_account_id"], record["reporter_display_name"],
            record["parent_key"], record["parent_type"],
            record["labels"], record["fix_versions"],
            record["story_points"], record["sprint_id"],
            record["sprint_name"], record["sprint_state"],
            record["board_id"], record["sprint_start_date"],
            record["sprint_end_date"], record["custom_fields_json"],
            record["created_at"], record["updated_at"],
            record["resolved_at"], record["in_progress_at"],
            record["done_at"], now,
        ),
    )


def upsert_status_changes(conn, issue_key, transitions):
    """Save status transitions and return (in_progress_at, done_at)."""
    conn.execute(
        "DELETE FROM dim_jira_status_change WHERE issue_key = ?", (issue_key,)
    )
    now = datetime.now(timezone.utc).isoformat()
    in_progress_at = None
    done_at = None
    in_progress_statuses = set(status_cats["in_progress"])
    done_statuses = set(status_cats["done"])
    for t in transitions:
        conn.execute(
            "INSERT INTO dim_jira_status_change "
            "(issue_key, changed_at, from_status, to_status, etl_loaded_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (t["issue_key"], t["changed_at"], t["from_status"], t["to_status"], now),
        )
        if t["to_status"] in in_progress_statuses and not in_progress_at:
            in_progress_at = t["changed_at"]
        if t["to_status"] in done_statuses:
            done_at = t["changed_at"]
    return in_progress_at, done_at


def get_bridge_jira_keys(conn):
    """Get all unique Jira keys from the PR bridge table that aren't yet in dim_jira_issue."""
    cur = conn.execute(
        """
        SELECT DISTINCT b.jira_key
        FROM bridge_pr_jira b
        LEFT JOIN dim_jira_issue j ON b.jira_key = j.issue_key
        WHERE j.issue_key IS NULL
        """
    )
    return [row[0] for row in cur.fetchall()]


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Jira ETL to SQLite")
    parser.add_argument("--since", type=str,
                        help="Start date (YYYY-MM-DD). Overrides watermark.")
    parser.add_argument("--until", type=str,
                        help="End date (YYYY-MM-DD). Defaults to now.")
    parser.add_argument("--backfill", action="store_true",
                        help="Fetch all issues referenced in bridge_pr_jira that aren't loaded yet")
    parser.add_argument("--backfill-changelog", action="store_true",
                        help="Fetch changelogs for all issues already in dim_jira_issue")
    parser.add_argument("--projects", type=str,
                        help="Comma-separated project keys (overrides config)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract and transform only, no DB writes")
    parser.add_argument("--verbose", action="store_true",
                        help="Extra logging")
    args = parser.parse_args()

    token = get_jira_token()
    email = get_jira_email()
    base_url = get_jira_base_url()
    db_path = get_db_path()

    # DB setup
    db_conn = None
    if not args.dry_run:
        db_conn = get_db_connection(db_path)
        ensure_jira_tables(db_conn)

    total_processed = 0

    if args.backfill:
        # ── Backfill mode: fetch issues referenced from PRs ──────────────
        if not db_conn:
            db_conn = get_db_connection(db_path)
            ensure_jira_tables(db_conn)

        missing_keys = get_bridge_jira_keys(db_conn)
        if not missing_keys:
            log("No missing Jira issues to backfill.")
        else:
            log(f"Backfilling {len(missing_keys)} Jira issues referenced from PRs...")

            for i in range(0, len(missing_keys), 50):
                batch = missing_keys[i:i + 50]
                keys_str = ", ".join(batch)
                jql = f"key in ({keys_str})"

                issues = search_all_issues(email, token, base_url, jql)
                for issue in issues:
                    record = transform_issue(issue)

                    transitions = fetch_changelog(email, token, base_url, record["issue_key"])
                    if transitions and not args.dry_run:
                        def _write_bf_changelog(r=record, t=transitions):
                            ip, done = upsert_status_changes(
                                db_conn, r["issue_key"], t
                            )
                            r["in_progress_at"] = ip
                            r["done_at"] = done
                        with_db_retry(_write_bf_changelog)

                    if args.dry_run:
                        if args.verbose or total_processed < 3:
                            print(json.dumps(record, indent=2, default=str))
                    else:
                        with_db_retry(
                            lambda r=record: upsert_jira_issue(db_conn, r)
                        )
                    total_processed += 1

                if not args.dry_run and total_processed % 200 == 0:
                    with_db_retry(db_conn.commit)

                time.sleep(0.2)

            found_keys = set()
            if not args.dry_run:
                cur = db_conn.execute(
                    "SELECT issue_key FROM dim_jira_issue WHERE issue_key IN "
                    f"({','.join('?' * len(missing_keys))})",
                    missing_keys,
                )
                found_keys = {row[0] for row in cur.fetchall()}

            not_found = set(missing_keys) - found_keys
            if not_found:
                log(f"  {len(not_found)} keys not found in Jira (may be deleted or "
                    f"in inaccessible projects): {sorted(not_found)[:10]}...")

    elif args.backfill_changelog:
        # ── Backfill changelogs for existing issues ─────────────────────
        if not db_conn:
            db_conn = get_db_connection(db_path)
            ensure_jira_tables(db_conn)

        cur = db_conn.execute(
            "SELECT issue_key FROM dim_jira_issue WHERE in_progress_at IS NULL"
        )
        keys = [row[0] for row in cur.fetchall()]
        log(f"Backfilling changelogs for {len(keys)} issues without in_progress_at...")

        for i, key in enumerate(keys):
            transitions = fetch_changelog(email, token, base_url, key)
            if transitions:
                def _write_cl(k=key, t=transitions):
                    ip, done = upsert_status_changes(db_conn, k, t)
                    if ip or done:
                        db_conn.execute(
                            "UPDATE dim_jira_issue SET in_progress_at = ?, "
                            "done_at = ? WHERE issue_key = ?",
                            (ip, done, k),
                        )
                with_db_retry(_write_cl)
            total_processed += 1
            if total_processed % 50 == 0:
                with_db_retry(db_conn.commit)
                log(f"  Processed {total_processed}/{len(keys)} changelogs...")
            time.sleep(0.1)

    else:
        # ── Incremental mode: fetch recently updated issues ──────────────
        if args.since:
            since_date = args.since
        elif db_conn:
            wm = read_watermark(db_conn, "etl_watermark_jira", "jira_etl")
            if wm:
                since_date = wm[:10]
            else:
                since_date = None
        else:
            since_date = None

        if not since_date:
            since_dt = datetime.now(timezone.utc) - timedelta(days=30)
            since_date = since_dt.strftime("%Y-%m-%d")
            log("No watermark found, defaulting to last 30 days")

        until_date = args.until or datetime.now(timezone.utc).strftime("%Y-%m-%d")

        projects = (
            args.projects.split(",") if args.projects
            else get_jira_projects()
        )
        projects_str = ", ".join(projects)

        jql = (
            f"project in ({projects_str}) "
            f"AND updated >= '{since_date}' "
            f"AND updated <= '{until_date} 23:59' "
            f"ORDER BY updated ASC"
        )

        log(f"JQL: {jql}")
        issues = search_all_issues(email, token, base_url, jql)
        log(f"Fetched {len(issues)} issues")

        for issue in issues:
            record = transform_issue(issue)

            transitions = fetch_changelog(email, token, base_url, record["issue_key"])
            if transitions and not args.dry_run:
                def _write_changelog(r=record, t=transitions):
                    ip, done = upsert_status_changes(
                        db_conn, r["issue_key"], t
                    )
                    r["in_progress_at"] = ip
                    r["done_at"] = done
                with_db_retry(_write_changelog)

            if args.dry_run:
                if args.verbose or total_processed < 3:
                    print(json.dumps(record, indent=2, default=str))
            else:
                with_db_retry(lambda r=record: upsert_jira_issue(db_conn, r))

            total_processed += 1

            if not args.dry_run and total_processed % 200 == 0:
                with_db_retry(db_conn.commit)

    # Finalize
    if db_conn and not args.dry_run:
        with_db_retry(db_conn.commit)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with_db_retry(lambda: update_watermark(
            db_conn, "etl_watermark_jira", "jira_etl",
            now_iso, total_processed, "issues_processed",
        ))
        db_conn.close()

    log(f"\nDone. Processed {total_processed} issues.")
    log(f"Database: {db_path}")
    print(json.dumps({
        "status": "success",
        "issues_processed": total_processed,
        "backfill": args.backfill,
        "dry_run": args.dry_run,
        "db_path": db_path,
    }))


if __name__ == "__main__":
    main()
