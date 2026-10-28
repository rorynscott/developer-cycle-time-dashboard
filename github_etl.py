#!/usr/bin/env python3
"""
GitHub PR ETL to SQLite

Extracts pull request data from GitHub REST API and loads it into a local
SQLite database. Captures PRs where team members are involved as author,
reviewer, or requested reviewer.

Repos are discovered from the token (every non-archived repo in
[github] org it can read) unless [github] repos lists them explicitly. Each
repo keeps its own watermark, so a newly visible repo is backfilled from
[github] history_start on its first run. Each run also snapshots the
members of every [[github.approval_gates]] team.

Usage:
    python3 github_etl.py                          # auto (each repo since its last sync)
    python3 github_etl.py --since 2026-01-01       # from a specific date
    python3 github_etl.py --since 2026-01-01 --until 2026-01-31  # date range
    python3 github_etl.py --dry-run --verbose      # preview without writing
    python3 github_etl.py --all-authors             # don't filter by team involvement
    python3 github_etl.py --since 2026-07-09 --repos org/a,org/b  # subset re-run
"""

import json
import sys
import os
import re
import time
import argparse
import sqlite3
from datetime import datetime, timezone

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    get_team_lookup, get_team_members, get_team_resolver,
    get_former_members, get_github_token, get_github_repo_config,
    get_history_start, get_approval_gates, get_db_path,
)
from lib.db import (
    log, is_error, get_db_connection, with_db_retry, update_watermark,
)
from lib.github import (
    API, github_api, paginate, resolve_repos, fetch_team_members,
)

# ── Configuration ────────────────────────────────────────────────────────────

TEAM_LOOKUP = get_team_lookup()
ALL_TEAM_MEMBERS = get_team_members()
TEAM_AT = get_team_resolver()

AI_COAUTHOR_PATTERNS = [
    (re.compile(r"co-authored-by:.*\bclaude\b", re.IGNORECASE), "Claude"),
    (re.compile(r"co-authored-by:.*\banthropic\b", re.IGNORECASE), "Claude"),
    (re.compile(r"co-authored-by:.*\bcopilot\b", re.IGNORECASE), "Copilot"),
    (re.compile(r"co-authored-by:.*\bgithub copilot\b", re.IGNORECASE), "Copilot"),
    (re.compile(r"co-authored-by:.*\bchatgpt\b", re.IGNORECASE), "ChatGPT"),
    (re.compile(r"co-authored-by:.*\bopenai\b", re.IGNORECASE), "ChatGPT"),
    (re.compile(r"co-authored-by:.*\bgemini\b", re.IGNORECASE), "Gemini"),
    (re.compile(r"co-authored-by:.*\bcursor\b", re.IGNORECASE), "Cursor"),
]

# ── GitHub data fetchers ─────────────────────────────────────────────────────


def fetch_prs_in_range(token, repo, since_iso, until_iso=None):
    """Fetch all PRs updated within the given date range.

    Raises on an API error: a partial page list would otherwise advance the
    repo's watermark past PRs that were never fetched.
    """
    all_prs = []
    page = 1
    while True:
        result = github_api(
            token, f"{API}/repos/{repo}/pulls?state=all&sort=updated"
                   f"&direction=desc&per_page=100&page={page}",
        )
        if is_error(result):
            raise RuntimeError(f"Fetching PRs from {repo} page {page}: {result}")
        if not result:
            break
        for pr in result:
            updated = pr["updated_at"]
            if updated < since_iso:
                return all_prs
            if until_iso and updated > until_iso:
                continue
            all_prs.append(pr)
        if len(result) < 100:
            break
        page += 1
        time.sleep(0.1)
    return all_prs


def has_team_involvement(pr):
    """Check if a team member is the author or a requested reviewer."""
    author = (pr.get("user") or {}).get("login", "")
    if author in ALL_TEAM_MEMBERS:
        return True
    for reviewer in pr.get("requested_reviewers", []):
        if reviewer.get("login", "") in ALL_TEAM_MEMBERS:
            return True
    return False


def fetch_pr_detail(token, repo, number):
    result = github_api(token, f"{API}/repos/{repo}/pulls/{number}")
    if is_error(result):
        return None
    return result


def fetch_pr_reviews(token, repo, number):
    return paginate(token, f"{API}/repos/{repo}/pulls/{number}/reviews")


def fetch_issue_comments(token, repo, number):
    return paginate(token, f"{API}/repos/{repo}/issues/{number}/comments")


def fetch_pr_commits(token, repo, number):
    # Commits only feed AI co-author detection; 300 is plenty.
    return paginate(token, f"{API}/repos/{repo}/pulls/{number}/commits",
                    max_pages=3)


def reviews_have_team_member(reviews):
    """Check if any team member submitted a review."""
    for r in reviews:
        login = (r.get("user") or {}).get("login", "")
        if login in ALL_TEAM_MEMBERS:
            return True
    return False


# ── Transform ────────────────────────────────────────────────────────────────


def parse_iso(ts):
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def iso_str(dt):
    if dt is None:
        return None
    return dt.isoformat()


def hours_between(a, b):
    if a is None or b is None:
        return None
    delta = (b - a).total_seconds() / 3600.0
    return round(delta, 2)


def compute_review_status(reviews):
    if not reviews:
        return "REVIEW_REQUIRED"
    latest_by_user = {}
    for review in reviews:
        user = (review.get("user") or {}).get("login", "")
        state = review.get("state", "")
        if state in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            latest_by_user[user] = state
    if not latest_by_user:
        return "REVIEW_REQUIRED"
    states = set(latest_by_user.values())
    if "CHANGES_REQUESTED" in states:
        return "CHANGES_REQUESTED"
    if "APPROVED" in states:
        return "APPROVED"
    return "REVIEW_REQUIRED"


def detect_ai_coauthor(commits, pr_body=""):
    tools_found = set()
    texts = [pr_body or ""]
    for c in commits:
        msg = (c.get("commit") or {}).get("message", "")
        texts.append(msg)
    full_text = "\n".join(texts)
    for pattern, tool_name in AI_COAUTHOR_PATTERNS:
        if pattern.search(full_text):
            tools_found.add(tool_name)
    return sorted(tools_found)


def extract_reviewers(reviews):
    reviewers = set()
    for r in reviews:
        state = r.get("state", "")
        login = (r.get("user") or {}).get("login", "")
        if login and state in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            reviewers.add(login)
    return sorted(reviewers)


def transform_pr(pr_list_item, detail, reviews, issue_comments, commits):
    repo = pr_list_item.get("base", {}).get("repo", {}).get("full_name", "")
    number = pr_list_item["number"]
    pr_key = f"{repo}#{number}"
    author = (pr_list_item.get("user") or {}).get("login", "")

    created_at = parse_iso(pr_list_item["created_at"])

    if detail and detail.get("merged"):
        state = "merged"
    elif pr_list_item.get("state") == "closed":
        state = "closed"
    else:
        state = "open"

    review_times = []
    approval_times = []
    for r in reviews:
        submitted = r.get("submitted_at")
        if not submitted:
            continue
        r_state = r.get("state", "")
        if r_state != "PENDING":
            review_times.append(parse_iso(submitted))
        if r_state == "APPROVED":
            approval_times.append(parse_iso(submitted))

    first_review_at = min(review_times) if review_times else None
    first_approval_at = min(approval_times) if approval_times else None
    merged_at = parse_iso(detail.get("merged_at")) if detail else None
    closed_at = parse_iso(detail.get("closed_at")) if detail else None

    pr_body = pr_list_item.get("body", "") or ""
    ai_tools = detect_ai_coauthor(commits, pr_body)

    review_comment_count = detail.get("review_comments", 0) if detail else 0
    issue_comment_count = len(issue_comments)

    labels = [l["name"] for l in pr_list_item.get("labels", [])]

    return {
        "pr_key": pr_key,
        "pr_number": number,
        "repo_key": repo,
        "author_login": author,
        "team_name": TEAM_AT(author, merged_at or closed_at),
        "title": (pr_list_item.get("title") or "")[:2000],
        "state": state,
        "is_draft": pr_list_item.get("draft", False),
        "base_branch": pr_list_item.get("base", {}).get("ref"),
        "head_branch": pr_list_item.get("head", {}).get("ref"),
        "labels": json.dumps(labels),
        "created_at": iso_str(created_at),
        "updated_at": iso_str(parse_iso(pr_list_item["updated_at"])),
        "first_review_at": iso_str(first_review_at),
        "first_approval_at": iso_str(first_approval_at),
        "merged_at": iso_str(merged_at),
        "merged_by": ((detail or {}).get("merged_by") or {}).get("login"),
        "closed_at": iso_str(closed_at),
        "hours_to_first_review": hours_between(created_at, first_review_at),
        "hours_to_first_approval": hours_between(created_at, first_approval_at),
        "hours_to_merge": hours_between(created_at, merged_at),
        "review_comment_count": review_comment_count,
        "issue_comment_count": issue_comment_count,
        "total_comment_count": review_comment_count + issue_comment_count,
        "files_changed": detail.get("changed_files", 0) if detail else 0,
        "lines_added": detail.get("additions", 0) if detail else 0,
        "lines_removed": detail.get("deletions", 0) if detail else 0,
        "total_lines_changed": (
            (detail.get("additions", 0) + detail.get("deletions", 0))
            if detail else 0
        ),
        "has_ai_coauthor": len(ai_tools) > 0,
        "ai_coauthor_tools": ",".join(ai_tools) if ai_tools else None,
        "reviewers": json.dumps(extract_reviewers(reviews)),
        "review_status": compute_review_status(reviews),
    }


def transform_reviews(pr_key, reviews):
    records = []
    for r in reviews:
        review_id = r.get("id", 0)
        state = r.get("state", "")
        if state == "PENDING":
            continue
        records.append({
            "review_key": f"{pr_key}#{review_id}",
            "pr_key": pr_key,
            "reviewer_login": (r.get("user") or {}).get("login", ""),
            "review_state": state,
            "submitted_at": iso_str(parse_iso(r.get("submitted_at"))),
        })
    return records


# ── SQLite ───────────────────────────────────────────────────────────────────

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS fact_pr (
        pr_key              TEXT PRIMARY KEY,
        pr_number           INTEGER NOT NULL,
        repo_key            TEXT NOT NULL,
        author_login        TEXT NOT NULL,
        team_name           TEXT,
        title               TEXT,
        state               TEXT NOT NULL,
        is_draft            INTEGER,
        base_branch         TEXT,
        head_branch         TEXT,
        labels              TEXT,
        created_at          TEXT,
        updated_at          TEXT,
        first_review_at     TEXT,
        first_approval_at   TEXT,
        merged_at           TEXT,
        merged_by           TEXT,
        closed_at           TEXT,
        hours_to_first_review   REAL,
        hours_to_first_approval REAL,
        hours_to_merge          REAL,
        review_comment_count    INTEGER,
        issue_comment_count     INTEGER,
        total_comment_count     INTEGER,
        files_changed       INTEGER,
        lines_added         INTEGER,
        lines_removed       INTEGER,
        total_lines_changed INTEGER,
        has_ai_coauthor     INTEGER,
        ai_coauthor_tools   TEXT,
        reviewers           TEXT,
        review_status       TEXT,
        etl_loaded_at       TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_author (
        author_login    TEXT PRIMARY KEY,
        team_name       TEXT,
        is_tracked      INTEGER DEFAULT 0,
        left_on         TEXT
    )
    """,
    # is_active: the repo is currently tracked (visible to the token and not
    # excluded). synced_through: per-repo watermark (ISO timestamp).
    """
    CREATE TABLE IF NOT EXISTS dim_repo (
        repo_key        TEXT PRIMARY KEY,
        repo_owner      TEXT,
        repo_name       TEXT,
        is_active       INTEGER DEFAULT 1,
        synced_through  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_review (
        review_key      TEXT PRIMARY KEY,
        pr_key          TEXT NOT NULL,
        reviewer_login  TEXT NOT NULL,
        review_state    TEXT,
        submitted_at    TEXT,
        etl_loaded_at   TEXT
    )
    """,
    # Approval-gate team membership, one row per (team, login). Teams have
    # no membership history in the API, so first_seen/last_seen record
    # which daily snapshots included the login.
    """
    CREATE TABLE IF NOT EXISTS dim_gate_member (
        team_slug   TEXT NOT NULL,
        login       TEXT NOT NULL,
        first_seen  TEXT NOT NULL,
        last_seen   TEXT NOT NULL,
        PRIMARY KEY (team_slug, login)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS etl_watermark (
        pipeline_name       TEXT PRIMARY KEY,
        last_run_at         TEXT,
        last_updated_since  TEXT,
        prs_processed       INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fact_pr_author ON fact_pr(author_login)",
    "CREATE INDEX IF NOT EXISTS idx_fact_pr_repo ON fact_pr(repo_key)",
    "CREATE INDEX IF NOT EXISTS idx_fact_pr_state ON fact_pr(state)",
    "CREATE INDEX IF NOT EXISTS idx_fact_pr_created ON fact_pr(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_fact_pr_team ON fact_pr(team_name)",
    "CREATE INDEX IF NOT EXISTS idx_dim_review_pr ON dim_review(pr_key)",
    "CREATE INDEX IF NOT EXISTS idx_dim_review_reviewer ON dim_review(reviewer_login)",
]


def ensure_tables(conn):
    for ddl in DDL_STATEMENTS:
        conn.execute(ddl)
    # Migrations for existing databases
    cur = conn.execute("PRAGMA table_info(fact_pr)")
    if "merged_by" not in {row[1] for row in cur.fetchall()}:
        conn.execute("ALTER TABLE fact_pr ADD COLUMN merged_by TEXT")
        log("  Migrated: added merged_by column to fact_pr")
    cur = conn.execute("PRAGMA table_info(dim_author)")
    if "left_on" not in {row[1] for row in cur.fetchall()}:
        conn.execute("ALTER TABLE dim_author ADD COLUMN left_on TEXT")
        log("  Migrated: added left_on column to dim_author")
    cur = conn.execute("PRAGMA table_info(dim_repo)")
    repo_cols = {row[1] for row in cur.fetchall()}
    if "synced_through" not in repo_cols:
        conn.execute("ALTER TABLE dim_repo ADD COLUMN is_active INTEGER DEFAULT 1")
        conn.execute("ALTER TABLE dim_repo ADD COLUMN synced_through TEXT")
        # Repos synced under the old shared watermark carry it over, so
        # switching to per-repo watermarks doesn't trigger a re-backfill.
        row = conn.execute(
            "SELECT last_updated_since FROM etl_watermark "
            "WHERE pipeline_name = 'github_pr_etl'"
        ).fetchone()
        if row and row[0]:
            conn.execute("UPDATE dim_repo SET synced_through = ?", (row[0],))
        log("  Migrated: per-repo watermarks on dim_repo")
    conn.commit()
    log("  Tables ensured.")


def seed_dimensions(conn, repos):
    """Mark exactly ``repos`` as tracked and (re)seed the team roster."""
    conn.execute("UPDATE dim_repo SET is_active = 0")
    for repo in repos:
        owner, name = repo.split("/")
        conn.execute(
            "INSERT INTO dim_repo (repo_key, repo_owner, repo_name, is_active) "
            "VALUES (?, ?, ?, 1) "
            "ON CONFLICT(repo_key) DO UPDATE SET is_active = 1",
            (repo, owner, name),
        )
    # Untrack anyone dropped from the roster entirely, then (re)seed current
    # and former members. Former members carry left_on so review queries can
    # stop counting them after they left.
    lookup = get_team_lookup()
    former = get_former_members()
    placeholders = ",".join("?" for _ in lookup)
    conn.execute(
        f"UPDATE dim_author SET team_name = NULL, is_tracked = 0, left_on = NULL "
        f"WHERE is_tracked = 1 AND author_login NOT IN ({placeholders})",
        list(lookup),
    )
    for login, team_name in lookup.items():
        conn.execute(
            "INSERT OR REPLACE INTO dim_author "
            "(author_login, team_name, is_tracked, left_on) VALUES (?, ?, 1, ?)",
            (login, team_name, former.get(login)),
        )
    conn.commit()
    log("  Dimensions seeded.")


def snapshot_gate_members(conn, token):
    """Record today's membership of every approval-gate team."""
    today = datetime.now(timezone.utc).date().isoformat()
    for gate in get_approval_gates():
        org = gate["repo"].split("/")[0]
        try:
            members = fetch_team_members(token, org, gate["team"])
        except RuntimeError as e:
            # Needs org Members:read on the token. Keep going: PR data is
            # still worth loading, and old snapshots stay usable.
            log(f"  WARNING: can't read gate team {org}/{gate['team']}: {e}")
            continue
        conn.executemany(
            "INSERT INTO dim_gate_member (team_slug, login, first_seen, last_seen) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(team_slug, login) DO UPDATE SET last_seen = excluded.last_seen",
            [(gate["team"], m, today, today) for m in members],
        )
        log(f"  Gate {gate['team']}: {len(members)} members")
    conn.commit()


def reattribute_prs(conn):
    """Recompute fact_pr.team_name for every PR from the current roster.

    Team membership changes (merges, departures) then apply to history
    without a re-fetch. A PR belongs to its author's team as of when it
    merged/closed; open PRs use the author's current team.
    """
    rows = conn.execute(
        "SELECT pr_key, author_login, COALESCE(merged_at, closed_at), team_name "
        "FROM fact_pr"
    ).fetchall()
    changes = [
        (new, pr_key) for pr_key, author, finished, old in rows
        if (new := TEAM_AT(author, finished)) != old
    ]
    conn.executemany("UPDATE fact_pr SET team_name = ? WHERE pr_key = ?", changes)
    conn.commit()
    if changes:
        log(f"  Re-attributed {len(changes)} PRs to current team roster.")


def upsert_fact_pr(conn, record):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO fact_pr (
            pr_key, pr_number, repo_key, author_login, team_name,
            title, state, is_draft, base_branch, head_branch, labels,
            created_at, updated_at, first_review_at, first_approval_at,
            merged_at, merged_by, closed_at, hours_to_first_review,
            hours_to_first_approval, hours_to_merge,
            review_comment_count, issue_comment_count, total_comment_count,
            files_changed, lines_added, lines_removed, total_lines_changed,
            has_ai_coauthor, ai_coauthor_tools,
            reviewers, review_status,
            etl_loaded_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?
        )
        """,
        (
            record["pr_key"], record["pr_number"], record["repo_key"],
            record["author_login"], record["team_name"], record["title"],
            record["state"], int(record["is_draft"]),
            record["base_branch"], record["head_branch"], record["labels"],
            record["created_at"], record["updated_at"],
            record["first_review_at"], record["first_approval_at"],
            record["merged_at"], record["merged_by"], record["closed_at"],
            record["hours_to_first_review"], record["hours_to_first_approval"],
            record["hours_to_merge"], record["review_comment_count"],
            record["issue_comment_count"], record["total_comment_count"],
            record["files_changed"], record["lines_added"],
            record["lines_removed"], record["total_lines_changed"],
            int(record["has_ai_coauthor"]),
            record["ai_coauthor_tools"],
            record["reviewers"],
            record["review_status"], now,
        ),
    )


def upsert_dim_reviews(conn, pr_key, review_records):
    conn.execute("DELETE FROM dim_review WHERE pr_key = ?", (pr_key,))
    now = datetime.now(timezone.utc).isoformat()
    for r in review_records:
        conn.execute(
            "INSERT INTO dim_review (review_key, pr_key, reviewer_login, "
            "review_state, submitted_at, etl_loaded_at) VALUES (?, ?, ?, ?, ?, ?)",
            (r["review_key"], r["pr_key"], r["reviewer_login"],
             r["review_state"], r["submitted_at"], now),
        )


# ── Main ─────────────────────────────────────────────────────────────────────


def sync_repo(token, repo, since_iso, until_iso, args, db_conn):
    """Load every team-involved PR in ``repo`` updated in the range.

    Returns (processed, skipped).
    """
    prs = fetch_prs_in_range(token, repo, since_iso, until_iso)
    log(f"  Found {len(prs)} PRs in date range")

    # Phase 1: quick filter by author / requested reviewers
    if args.all_authors:
        candidates = prs
    else:
        candidates = [pr for pr in prs if has_team_involvement(pr)]
        log(f"  {len(candidates)} have team involvement "
            f"(author or requested reviewer), {len(prs) - len(candidates)} skipped")

        # Phase 2: for remaining PRs, fetch reviews to catch team reviewers
        candidate_numbers = {pr["number"] for pr in candidates}
        remaining = [pr for pr in prs if pr["number"] not in candidate_numbers]
        if remaining:
            log(f"  Checking {len(remaining)} remaining PRs for team reviewers...")
            for pr in remaining:
                if reviews_have_team_member(fetch_pr_reviews(token, repo, pr["number"])):
                    candidates.append(pr)
                time.sleep(0.1)
            log(f"  After review check: {len(candidates)} PRs with team involvement")

    log(f"  Enriching {len(candidates)} PRs...")

    for i, pr in enumerate(candidates):
        number = pr["number"]
        author = (pr.get("user") or {}).get("login", "")

        if args.verbose:
            log(f"  [{i+1}/{len(candidates)}] PR #{number} by {author}")

        detail = fetch_pr_detail(token, repo, number)
        reviews = fetch_pr_reviews(token, repo, number)
        issue_comments = fetch_issue_comments(token, repo, number)
        commits = fetch_pr_commits(token, repo, number)

        record = transform_pr(pr, detail, reviews, issue_comments, commits)
        review_records = transform_reviews(record["pr_key"], reviews)

        if args.dry_run:
            if args.verbose or i < 3:
                print(json.dumps(record, indent=2, default=str))
        else:
            def _write_pr():
                upsert_fact_pr(db_conn, record)
                upsert_dim_reviews(db_conn, record["pr_key"], review_records)
                if author not in TEAM_LOOKUP:
                    db_conn.execute(
                        "INSERT OR IGNORE INTO dim_author "
                        "(author_login, team_name, is_tracked) "
                        "VALUES (?, NULL, 0)",
                        (author,),
                    )
            with_db_retry(_write_pr)

            if (i + 1) % 50 == 0:
                with_db_retry(db_conn.commit)

        time.sleep(0.25)

        if (i + 1) % 50 == 0:
            log(f"  Progress: {i+1}/{len(candidates)} PRs enriched")

    return len(candidates), len(prs) - len(candidates)


def main():
    parser = argparse.ArgumentParser(description="GitHub PR ETL to SQLite")
    parser.add_argument("--since", type=str,
                        help="Start date (YYYY-MM-DD) for every repo. "
                             "Overrides the per-repo watermarks.")
    parser.add_argument("--until", type=str,
                        help="End date (YYYY-MM-DD). Defaults to now; "
                             "a bounded run doesn't advance watermarks.")
    parser.add_argument("--repos", type=str,
                        help="Comma-separated repo subset (org/repo)")
    parser.add_argument("--list-repos", action="store_true",
                        help="Print the repos that would be tracked, then exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract and transform only, no DB writes")
    parser.add_argument("--all-authors", action="store_true",
                        help="Include all authors (default: team involvement only)")
    parser.add_argument("--verbose", action="store_true",
                        help="Extra logging")
    args = parser.parse_args()

    gh_token = get_github_token()
    db_path = get_db_path()
    tracked = resolve_repos(gh_token, get_github_repo_config())
    log(f"Tracking {len(tracked)} repos: {', '.join(tracked)}")
    if args.list_repos:
        print("\n".join(tracked))
        return
    repos = tracked
    if args.repos:
        repos = [r.strip() for r in args.repos.split(",") if r.strip()]

    db_conn = None
    watermarks = {}
    if not args.dry_run:
        db_conn = get_db_connection(db_path)
        ensure_tables(db_conn)
        seed_dimensions(db_conn, tracked)
        snapshot_gate_members(db_conn, gh_token)
        reattribute_prs(db_conn)
        watermarks = dict(db_conn.execute(
            "SELECT repo_key, synced_through FROM dim_repo"
        ).fetchall())
    elif os.path.exists(db_path):
        # Dry run: read existing watermarks so it previews a normal run,
        # not a full backfill of every repo
        conn = get_db_connection(db_path)
        try:
            watermarks = dict(conn.execute(
                "SELECT repo_key, synced_through FROM dim_repo"
            ).fetchall())
        except sqlite3.OperationalError:  # pre-migration DB, no synced_through
            pass
        conn.close()

    until_iso = f"{args.until}T23:59:59Z" if args.until else None
    history_start = f"{get_history_start()}T00:00:00Z"

    total_processed = 0
    total_skipped = 0
    failed = []
    run_started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for repo in repos:
        prior = watermarks.get(repo)
        if args.since:
            since_iso = f"{args.since}T00:00:00Z"
        else:
            # Never synced: backfill from history_start
            since_iso = prior or history_start
        log(f"\nProcessing {repo} (since {since_iso})...")
        try:
            processed, skipped = sync_repo(
                gh_token, repo, since_iso, until_iso, args, db_conn,
            )
        except RuntimeError as e:
            # Leave this repo's watermark alone so the next run retries it
            log(f"  ERROR: {repo} failed, watermark not advanced: {e}")
            failed.append(repo)
            continue
        total_processed += processed
        total_skipped += skipped

        # Advance only when the run covered everything since the old
        # watermark up to now (no --until, and --since not after it).
        covers_gap = prior is None or since_iso <= prior
        if db_conn and not until_iso and covers_gap:
            with_db_retry(lambda: db_conn.execute(
                "UPDATE dim_repo SET synced_through = ? WHERE repo_key = ?",
                (run_started, repo),
            ))
        if db_conn:
            with_db_retry(db_conn.commit)

    if db_conn:
        # Run-level record, shown as "last updated" on the dashboard
        with_db_retry(lambda: update_watermark(
            db_conn, "etl_watermark", "github_pr_etl",
            run_started, total_processed, "prs_processed",
        ))
        db_conn.close()

    log(f"\nDone. Processed {total_processed} PRs, skipped {total_skipped}.")
    log(f"Database: {db_path}")
    print(json.dumps({
        "status": "failed" if failed else "success",
        "prs_processed": total_processed,
        "prs_skipped": total_skipped,
        "failed_repos": failed,
        "until": until_iso,
        "dry_run": args.dry_run,
        "db_path": db_path,
    }))
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
