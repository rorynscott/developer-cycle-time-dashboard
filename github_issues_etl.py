#!/usr/bin/env python3
"""
GitHub Issues ETL to SQLite

Extracts GitHub issues assigned to team members (the team tracks tasks as
GitHub issues instead of Jira) and loads them into the same SQLite database
as the PR ETL. Issues are found by assignee — there is no other reliable
marker of team ownership — and lifecycle timestamps come from Projects v2
status-change events on the issue timeline.

Every run re-syncs all team-assigned issues (it's a handful of GraphQL
queries), so reassignments and status changes are always picked up.

Usage:
    python3 github_issues_etl.py                      # full sync
    python3 github_issues_etl.py --since 2026-09-01   # only issues updated since
    python3 github_issues_etl.py --dry-run --verbose  # preview without writing
"""

import json
import sys
import os
import time
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timezone

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    get_team_members, get_team_resolver, get_github_token,
    get_github_issues_config, get_db_path,
)
from lib.db import log, get_db_connection, with_db_retry, update_watermark

# ── Configuration ────────────────────────────────────────────────────────────

ALL_TEAM_MEMBERS = get_team_members()
TEAM_AT = get_team_resolver()

GRAPHQL_URL = "https://api.github.com/graphql"

SEARCH_QUERY = """
query($q: String!, $cursor: String) {
  search(query: $q, type: ISSUE, first: 50, after: $cursor) {
    pageInfo { hasNextPage endCursor }
    nodes {
      ... on Issue {
        number
        title
        url
        createdAt
        updatedAt
        closedAt
        state
        stateReason
        repository { nameWithOwner }
        issueType { name }
        parent { number repository { nameWithOwner } }
        subIssues { totalCount }
        assignees(first: 10) { nodes { login } }
        closedByPullRequestsReferences(first: 20, includeClosedPrs: true) {
          nodes { number repository { nameWithOwner } }
        }
        timelineItems(first: 100, itemTypes: [
          PROJECT_V2_ITEM_STATUS_CHANGED_EVENT, CROSS_REFERENCED_EVENT
        ]) {
          pageInfo { hasNextPage }
          nodes {
            __typename
            ... on ProjectV2ItemStatusChangedEvent {
              createdAt previousStatus status
            }
            ... on CrossReferencedEvent {
              willCloseTarget
              source {
                __typename
                ... on PullRequest { number repository { nameWithOwner } }
              }
            }
          }
        }
      }
    }
  }
}
"""

# ── GitHub API ───────────────────────────────────────────────────────────────


def graphql(token, query, variables, retries=3):
    body = json.dumps({"query": query, "variables": variables}).encode()
    for attempt in range(retries):
        req = urllib.request.Request(GRAPHQL_URL, data=body, method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (403, 429, 502):
                wait = 30 * (attempt + 1)
                log(f"  GraphQL HTTP {e.code}, sleeping {wait}s... "
                    f"(attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            raise
        if result.get("errors"):
            raise RuntimeError(f"GraphQL errors: {result['errors']}")
        return result["data"]
    raise RuntimeError("GraphQL: max retries exceeded")


def search_issues(token, query_str):
    """Yield every issue node matching a search query (paginated)."""
    cursor = None
    while True:
        data = graphql(token, SEARCH_QUERY, {"q": query_str, "cursor": cursor})
        search = data["search"]
        for node in search["nodes"]:
            if node:
                yield node
        if not search["pageInfo"]["hasNextPage"]:
            return
        cursor = search["pageInfo"]["endCursor"]
        time.sleep(0.5)


# ── Transform ────────────────────────────────────────────────────────────────


def attribute_issue(assignees, closed_at):
    """Return (primary_assignee, team_name) for an issue.

    Attributed to the first assignee (in GitHub's order) who was on a team
    when the issue closed; open issues use current membership.
    """
    for a in assignees:
        team = TEAM_AT(a, closed_at)
        if team:
            return a, team
    return None, None


def transform_issue(node, in_progress_statuses):
    repo = node["repository"]["nameWithOwner"]
    issue_key = f"{repo}#{node['number']}"
    assignees = [a["login"] for a in filter(None, node["assignees"]["nodes"])]

    primary_assignee, team_name = attribute_issue(assignees, node.get("closedAt"))

    timeline = node["timelineItems"]
    if timeline["pageInfo"]["hasNextPage"]:
        log(f"  WARNING: {issue_key} has >100 timeline events; "
            f"status history truncated")

    status_changes = []
    pr_links = {}  # pr_key -> link_type ('closes' wins over 'mentions')
    # GitHub returns null nodes for items the token can't read (e.g. PRs in
    # private repos outside its scope), so skip them throughout.
    for ev in filter(None, timeline["nodes"]):
        if ev["__typename"] == "ProjectV2ItemStatusChangedEvent":
            status_changes.append({
                "from_status": ev.get("previousStatus") or None,
                "to_status": ev.get("status") or None,
                "changed_at": ev["createdAt"],
            })
        elif ev["__typename"] == "CrossReferencedEvent":
            src = ev.get("source") or {}
            if src.get("__typename") != "PullRequest":
                continue
            pr_key = f"{src['repository']['nameWithOwner']}#{src['number']}"
            if ev.get("willCloseTarget"):
                pr_links[pr_key] = "closes"
            else:
                pr_links.setdefault(pr_key, "mentions")
    for pr in filter(None, node["closedByPullRequestsReferences"]["nodes"]):
        pr_links[f"{pr['repository']['nameWithOwner']}#{pr['number']}"] = "closes"

    # Status names differ per project board, so compare case-insensitively
    in_progress_times = sorted(
        sc["changed_at"] for sc in status_changes
        if (sc["to_status"] or "").lower() in in_progress_statuses
    )

    parent = node.get("parent")
    parent_key = (
        f"{parent['repository']['nameWithOwner']}#{parent['number']}"
        if parent else None
    )

    return {
        "issue_key": issue_key,
        "repo_key": repo,
        "issue_number": node["number"],
        "title": node["title"],
        "url": node["url"],
        "issue_type": (node.get("issueType") or {}).get("name"),
        "state": node["state"],
        "state_reason": node.get("stateReason"),
        "created_at": node["createdAt"],
        "updated_at": node["updatedAt"],
        "closed_at": node.get("closedAt"),
        "first_in_progress_at": in_progress_times[0] if in_progress_times else None,
        "primary_assignee": primary_assignee,
        "team_name": team_name,
        "assignees_json": json.dumps(assignees),
        "parent_key": parent_key,
        "sub_issue_count": node["subIssues"]["totalCount"],
        "status_changes": status_changes,
        "pr_links": pr_links,
    }


# ── Database ─────────────────────────────────────────────────────────────────

DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_gh_issue (
        issue_key             TEXT PRIMARY KEY,
        repo_key              TEXT NOT NULL,
        issue_number          INTEGER NOT NULL,
        title                 TEXT,
        url                   TEXT,
        issue_type            TEXT,
        state                 TEXT,
        state_reason          TEXT,
        created_at            TEXT,
        updated_at            TEXT,
        closed_at             TEXT,
        first_in_progress_at  TEXT,
        primary_assignee      TEXT,
        team_name             TEXT,
        assignees_json        TEXT,
        parent_key            TEXT,
        sub_issue_count       INTEGER,
        etl_loaded_at         TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_gh_issue_status_change (
        issue_key      TEXT NOT NULL,
        from_status    TEXT,
        to_status      TEXT,
        changed_at     TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bridge_pr_gh_issue (
        pr_key     TEXT NOT NULL,
        issue_key  TEXT NOT NULL,
        link_type  TEXT,
        PRIMARY KEY (pr_key, issue_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS etl_watermark_gh_issues (
        pipeline_name       TEXT PRIMARY KEY,
        last_run_at         TEXT,
        last_updated_since  TEXT,
        issues_processed    INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_gh_issue_team ON dim_gh_issue(team_name)",
    "CREATE INDEX IF NOT EXISTS idx_gh_issue_sc_key "
    "ON dim_gh_issue_status_change(issue_key)",
    "CREATE INDEX IF NOT EXISTS idx_bridge_gh_issue ON bridge_pr_gh_issue(issue_key)",
]


def ensure_tables(conn):
    for ddl in DDL:
        conn.execute(ddl)
    conn.commit()


def reattribute_issues(conn):
    """Recompute issue attribution for every stored issue from the current
    roster, so membership changes apply to history without a re-fetch."""
    rows = conn.execute(
        "SELECT issue_key, assignees_json, closed_at, primary_assignee, "
        "team_name FROM dim_gh_issue"
    ).fetchall()
    changes = []
    for key, assignees_json, closed_at, old_assignee, old_team in rows:
        new = attribute_issue(json.loads(assignees_json or "[]"), closed_at)
        if new != (old_assignee, old_team):
            changes.append((*new, key))
    conn.executemany(
        "UPDATE dim_gh_issue SET primary_assignee = ?, team_name = ? "
        "WHERE issue_key = ?", changes,
    )
    if changes:
        log(f"  Re-attributed {len(changes)} issues to current team roster.")


def upsert_issue(conn, rec, loaded_at):
    conn.execute(
        """
        INSERT OR REPLACE INTO dim_gh_issue (
            issue_key, repo_key, issue_number, title, url, issue_type,
            state, state_reason, created_at, updated_at, closed_at,
            first_in_progress_at, primary_assignee, team_name,
            assignees_json, parent_key, sub_issue_count, etl_loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            rec["issue_key"], rec["repo_key"], rec["issue_number"],
            rec["title"], rec["url"], rec["issue_type"], rec["state"],
            rec["state_reason"], rec["created_at"], rec["updated_at"],
            rec["closed_at"], rec["first_in_progress_at"],
            rec["primary_assignee"], rec["team_name"], rec["assignees_json"],
            rec["parent_key"], rec["sub_issue_count"], loaded_at,
        ),
    )
    conn.execute("DELETE FROM dim_gh_issue_status_change WHERE issue_key = ?",
                 (rec["issue_key"],))
    conn.executemany(
        "INSERT INTO dim_gh_issue_status_change "
        "(issue_key, from_status, to_status, changed_at) "
        "VALUES (?, ?, ?, ?)",
        [(rec["issue_key"], sc["from_status"], sc["to_status"],
          sc["changed_at"]) for sc in rec["status_changes"]],
    )
    conn.execute("DELETE FROM bridge_pr_gh_issue WHERE issue_key = ?",
                 (rec["issue_key"],))
    conn.executemany(
        "INSERT INTO bridge_pr_gh_issue (pr_key, issue_key, link_type) "
        "VALUES (?, ?, ?)",
        [(pr_key, rec["issue_key"], lt) for pr_key, lt in rec["pr_links"].items()],
    )


# ── Main ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="GitHub Issues ETL to SQLite")
    parser.add_argument("--since", type=str,
                        help="Only sync issues updated on/after YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print records instead of writing to the DB")
    parser.add_argument("--verbose", action="store_true",
                        help="Log every issue")
    args = parser.parse_args()

    issues_cfg = get_github_issues_config()
    if not issues_cfg:
        log("[github.issues] not configured — nothing to do")
        return

    token = get_github_token()
    db_path = get_db_path()
    in_progress = {s.lower() for s in issues_cfg["in_progress_statuses"]}
    repo_filter = " ".join(f"repo:{r}" for r in issues_cfg["repos"])
    since_filter = f" updated:>={args.since}" if args.since else ""

    log(f"Repos: {', '.join(issues_cfg['repos'])}")
    log(f"Assignees: {len(ALL_TEAM_MEMBERS)} team members")

    # Search per assignee; an issue with two team assignees shows up twice,
    # so dedupe on issue_key.
    records = {}
    for member in sorted(ALL_TEAM_MEMBERS):
        query_str = f"{repo_filter} is:issue assignee:{member}{since_filter}"
        n = 0
        for node in search_issues(token, query_str):
            rec = transform_issue(node, in_progress)
            records[rec["issue_key"]] = rec
            n += 1
        if args.verbose or n:
            log(f"  {member}: {n} issues")
        time.sleep(0.5)

    log(f"Fetched {len(records)} unique issues")

    if args.dry_run:
        for rec in list(records.values())[:3]:
            print(json.dumps(rec, indent=2, default=str))
        return

    conn = get_db_connection(db_path)
    ensure_tables(conn)
    loaded_at = datetime.now(timezone.utc).isoformat()

    def _write():
        for rec in records.values():
            upsert_issue(conn, rec, loaded_at)
        # A full sync is authoritative: drop issues no longer assigned to
        # the team (reassigned away, or the member left the roster).
        if not args.since:
            stale = [
                row[0] for row in conn.execute(
                    "SELECT issue_key FROM dim_gh_issue"
                ).fetchall()
                if row[0] not in records
            ]
            for key in stale:
                for table in ("dim_gh_issue", "dim_gh_issue_status_change",
                              "bridge_pr_gh_issue"):
                    conn.execute(f"DELETE FROM {table} WHERE issue_key = ?",
                                 (key,))
            if stale:
                log(f"  Removed {len(stale)} issues no longer team-assigned")
        reattribute_issues(conn)
        conn.commit()
    with_db_retry(_write)

    with_db_retry(lambda: update_watermark(
        conn, "etl_watermark_gh_issues", "github_issues_etl",
        loaded_at, len(records), "issues_processed",
    ))
    conn.close()

    closed = sum(1 for r in records.values() if r["state_reason"] == "COMPLETED")
    with_ip = sum(1 for r in records.values() if r["first_in_progress_at"])
    log(f"Done. {len(records)} issues ({closed} completed, "
        f"{with_ip} with an in-progress status event).")
    log(f"Database: {db_path}")
    print(json.dumps({
        "status": "success", "issues_processed": len(records),
        "completed": closed, "with_in_progress": with_ip,
        "dry_run": args.dry_run, "db_path": db_path,
    }))


if __name__ == "__main__":
    main()
