#!/usr/bin/env python3
"""
GitHub PR ETL to SQLite

Extracts pull request data from GitHub REST API and loads it into a local
SQLite database. Captures PRs where team members are involved as author,
reviewer, or requested reviewer.

Usage:
    python3 github_etl.py                          # auto (since last run, or yesterday)
    python3 github_etl.py --since 2026-01-01       # from a specific date
    python3 github_etl.py --since 2026-01-01 --until 2026-01-31  # date range
    python3 github_etl.py --dry-run --verbose      # preview without writing
    python3 github_etl.py --all-authors             # don't filter by team involvement
"""

import json
import sys
import os
import re
import time
import sqlite3
import argparse
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    load_config, get_team_lookup, get_team_members, get_github_token,
    get_db_path,
)
from lib.db import log, is_error, get_db_connection, with_db_retry

# ── Configuration ────────────────────────────────────────────────────────────

TEAM_LOOKUP = get_team_lookup()
ALL_TEAM_MEMBERS = get_team_members()

JIRA_KEY_RE = re.compile(r"(?<![A-Za-z])([A-Z][A-Z0-9]+-\d+)(?=\b|[_/\s]|$)")

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

# ── GitHub API helpers ───────────────────────────────────────────────────────


def github_api(token, url, retries=3):
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req) as resp:
                remaining = resp.headers.get("X-RateLimit-Remaining")
                if remaining and int(remaining) < 50:
                    reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
                    wait = max(reset_ts - int(time.time()), 1)
                    log(f"  Rate limit low ({remaining}), sleeping {wait}s...")
                    time.sleep(wait)
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code in (403, 429):
                reset_ts = int(e.headers.get("X-RateLimit-Reset", 0))
                wait = max(reset_ts - int(time.time()), 5)
                log(f"  Rate limited ({e.code}), sleeping {wait}s... "
                    f"(attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            body = e.read().decode() if attempt == 0 else ""
            if attempt == retries - 1:
                return {"error": str(e), "status": e.code, "body": body}
            time.sleep(2)
    return {"error": "Max retries exceeded"}


# ── GitHub data fetchers ─────────────────────────────────────────────────────


def fetch_prs_page(token, repo, state="all", sort="updated", direction="desc",
                   per_page=100, page=1):
    url = (f"https://api.github.com/repos/{repo}/pulls"
           f"?state={state}&sort={sort}&direction={direction}"
           f"&per_page={per_page}&page={page}")
    return github_api(token, url)


def fetch_prs_in_range(token, repo, since_iso, until_iso=None):
    """Fetch all PRs updated within the given date range."""
    all_prs = []
    page = 1
    while True:
        result = fetch_prs_page(token, repo, page=page)
        if is_error(result):
            log(f"  Error fetching PRs from {repo} page {page}: {result}")
            break
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
    author = pr.get("user", {}).get("login", "")
    if author in ALL_TEAM_MEMBERS:
        return True
    for reviewer in pr.get("requested_reviewers", []):
        if reviewer.get("login", "") in ALL_TEAM_MEMBERS:
            return True
    return False


def fetch_pr_detail(token, repo, number):
    url = f"https://api.github.com/repos/{repo}/pulls/{number}"
    result = github_api(token, url)
    if is_error(result):
        return None
    return result


def fetch_pr_reviews(token, repo, number):
    all_reviews = []
    page = 1
    while True:
        url = (f"https://api.github.com/repos/{repo}/pulls/{number}/reviews"
               f"?per_page=100&page={page}")
        result = github_api(token, url)
        if is_error(result):
            break
        if not result:
            break
        all_reviews.extend(result)
        if len(result) < 100:
            break
        page += 1
    return all_reviews


def fetch_issue_comments(token, repo, number):
    all_comments = []
    page = 1
    while True:
        url = (f"https://api.github.com/repos/{repo}/issues/{number}/comments"
               f"?per_page=100&page={page}")
        result = github_api(token, url)
        if is_error(result):
            break
        if not result:
            break
        all_comments.extend(result)
        if len(result) < 100:
            break
        page += 1
    return all_comments


def fetch_pr_commits(token, repo, number):
    all_commits = []
    page = 1
    while True:
        url = (f"https://api.github.com/repos/{repo}/pulls/{number}/commits"
               f"?per_page=100&page={page}")
        result = github_api(token, url)
        if is_error(result):
            break
        if not result:
            break
        all_commits.extend(result)
        if len(result) < 100 or page >= 3:
            break
        page += 1
    return all_commits


def reviews_have_team_member(reviews):
    """Check if any team member submitted a review."""
    for r in reviews:
        login = r.get("user", {}).get("login", "")
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
        user = review.get("user", {}).get("login", "")
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
        msg = c.get("commit", {}).get("message", "")
        texts.append(msg)
    full_text = "\n".join(texts)
    for pattern, tool_name in AI_COAUTHOR_PATTERNS:
        if pattern.search(full_text):
            tools_found.add(tool_name)
    return sorted(tools_found)


def extract_jira_keys(title, head_branch, body=""):
    """Extract Jira ticket keys from PR title, branch name, and body."""
    originals = f"{title or ''} {head_branch or ''} {body or ''}"
    keys = set(JIRA_KEY_RE.findall(originals))
    return sorted(keys)


def extract_reviewers(reviews):
    reviewers = set()
    for r in reviews:
        state = r.get("state", "")
        login = r.get("user", {}).get("login", "")
        if login and state in ("APPROVED", "CHANGES_REQUESTED", "DISMISSED"):
            reviewers.add(login)
    return sorted(reviewers)


def transform_pr(pr_list_item, detail, reviews, issue_comments, commits):
    repo = pr_list_item.get("base", {}).get("repo", {}).get("full_name", "")
    number = pr_list_item["number"]
    pr_key = f"{repo}#{number}"
    author = pr_list_item["user"]["login"]

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
        "team_name": TEAM_LOOKUP.get(author),
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
        "jira_keys": extract_jira_keys(
            pr_list_item.get("title"), pr_list_item.get("head", {}).get("ref"), pr_body
        ),
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
            "reviewer_login": r.get("user", {}).get("login", ""),
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
        jira_keys           TEXT,
        reviewers           TEXT,
        review_status       TEXT,
        etl_loaded_at       TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_author (
        author_login    TEXT PRIMARY KEY,
        team_name       TEXT,
        is_tracked      INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_repo (
        repo_key    TEXT PRIMARY KEY,
        repo_owner  TEXT,
        repo_name   TEXT
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
    """
    CREATE TABLE IF NOT EXISTS bridge_pr_jira (
        pr_key      TEXT NOT NULL,
        jira_key    TEXT NOT NULL,
        PRIMARY KEY (pr_key, jira_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_bridge_pr_jira_jira ON bridge_pr_jira(jira_key)",
]


def ensure_tables(conn):
    for ddl in DDL_STATEMENTS:
        conn.execute(ddl)
    # Migrations for existing databases
    cur = conn.execute("PRAGMA table_info(fact_pr)")
    columns = {row[1] for row in cur.fetchall()}
    if "jira_keys" not in columns:
        conn.execute("ALTER TABLE fact_pr ADD COLUMN jira_keys TEXT")
        log("  Migrated: added jira_keys column to fact_pr")
    conn.commit()
    log("  Tables ensured.")


def seed_dimensions(conn):
    cfg = load_config()
    for repo in cfg["github"]["repos"]:
        owner, name = repo.split("/")
        conn.execute(
            "INSERT OR REPLACE INTO dim_repo (repo_key, repo_owner, repo_name) "
            "VALUES (?, ?, ?)",
            (repo, owner, name),
        )
    for team_name, members in [(t["name"], t["members"]) for t in cfg["teams"]]:
        for login in members:
            conn.execute(
                "INSERT OR REPLACE INTO dim_author (author_login, team_name, is_tracked) "
                "VALUES (?, ?, 1)",
                (login, team_name),
            )
    conn.commit()
    log("  Dimensions seeded.")


def upsert_fact_pr(conn, record):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO fact_pr (
            pr_key, pr_number, repo_key, author_login, team_name,
            title, state, is_draft, base_branch, head_branch, labels,
            created_at, updated_at, first_review_at, first_approval_at,
            merged_at, closed_at, hours_to_first_review,
            hours_to_first_approval, hours_to_merge,
            review_comment_count, issue_comment_count, total_comment_count,
            files_changed, lines_added, lines_removed, total_lines_changed,
            has_ai_coauthor, ai_coauthor_tools, jira_keys,
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
            record["merged_at"], record["closed_at"],
            record["hours_to_first_review"], record["hours_to_first_approval"],
            record["hours_to_merge"], record["review_comment_count"],
            record["issue_comment_count"], record["total_comment_count"],
            record["files_changed"], record["lines_added"],
            record["lines_removed"], record["total_lines_changed"],
            int(record["has_ai_coauthor"]),
            record["ai_coauthor_tools"],
            json.dumps(record["jira_keys"]) if record["jira_keys"] else None,
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


def main():
    parser = argparse.ArgumentParser(description="GitHub PR ETL to SQLite")
    parser.add_argument("--since", type=str,
                        help="Start date (YYYY-MM-DD). Overrides watermark.")
    parser.add_argument("--until", type=str,
                        help="End date (YYYY-MM-DD). Defaults to now.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Extract and transform only, no DB writes")
    parser.add_argument("--all-authors", action="store_true",
                        help="Include all authors (default: team involvement only)")
    parser.add_argument("--verbose", action="store_true",
                        help="Extra logging")
    args = parser.parse_args()

    cfg = load_config()
    gh_token = get_github_token()
    db_path = get_db_path()
    repos = cfg["github"]["repos"]

    # Set up DB
    db_conn = None
    if not args.dry_run:
        db_conn = get_db_connection(db_path)
        ensure_tables(db_conn)
        seed_dimensions(db_conn)

    # Determine date range
    if args.since:
        since_iso = f"{args.since}T00:00:00Z"
    elif db_conn:
        from lib.db import read_watermark
        since_iso = read_watermark(db_conn, "etl_watermark", "github_pr_etl")
    else:
        since_iso = None

    if not since_iso:
        since_dt = datetime.now(timezone.utc) - timedelta(days=1)
        since_iso = since_dt.strftime("%Y-%m-%dT00:00:00Z")
        log("No watermark found, defaulting to yesterday")

    until_iso = None
    if args.until:
        until_iso = f"{args.until}T23:59:59Z"

    log(f"Date range: {since_iso}" + (f" to {until_iso}" if until_iso else " to now"))

    total_processed = 0
    total_skipped = 0
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    for repo in repos:
        log(f"\nProcessing {repo}...")
        prs = fetch_prs_in_range(gh_token, repo, since_iso, until_iso)
        log(f"  Found {len(prs)} PRs in date range")

        # Phase 1: quick filter by author / requested reviewers
        if args.all_authors:
            candidates = prs
        else:
            candidates = []
            for pr in prs:
                if has_team_involvement(pr):
                    candidates.append(pr)
            log(f"  {len(candidates)} have team involvement "
                f"(author or requested reviewer), {len(prs) - len(candidates)} skipped")

        # Phase 2: for remaining PRs, fetch reviews to catch team reviewers
        if not args.all_authors:
            candidate_numbers = {pr["number"] for pr in candidates}
            remaining = [pr for pr in prs if pr["number"] not in candidate_numbers]
            if remaining:
                log(f"  Checking {len(remaining)} remaining PRs for team reviewers...")
                for pr in remaining:
                    reviews = fetch_pr_reviews(gh_token, repo, pr["number"])
                    if reviews_have_team_member(reviews):
                        candidates.append(pr)
                    time.sleep(0.1)
                log(f"  After review check: {len(candidates)} PRs with team involvement")

        log(f"  Enriching {len(candidates)} PRs...")

        for i, pr in enumerate(candidates):
            number = pr["number"]
            author = pr["user"]["login"]

            if args.verbose:
                log(f"  [{i+1}/{len(candidates)}] PR #{number} by {author}")

            detail = fetch_pr_detail(gh_token, repo, number)
            reviews = fetch_pr_reviews(gh_token, repo, number)
            issue_comments = fetch_issue_comments(gh_token, repo, number)
            commits = fetch_pr_commits(gh_token, repo, number)

            record = transform_pr(pr, detail, reviews, issue_comments, commits)
            review_records = transform_reviews(record["pr_key"], reviews)

            if args.dry_run:
                if args.verbose or total_processed < 3:
                    print(json.dumps(record, indent=2, default=str))
            else:
                def _write_pr():
                    upsert_fact_pr(db_conn, record)
                    upsert_dim_reviews(db_conn, record["pr_key"], review_records)
                    db_conn.execute(
                        "DELETE FROM bridge_pr_jira WHERE pr_key = ?",
                        (record["pr_key"],),
                    )
                    for jk in record["jira_keys"]:
                        db_conn.execute(
                            "INSERT INTO bridge_pr_jira (pr_key, jira_key) "
                            "VALUES (?, ?)",
                            (record["pr_key"], jk),
                        )
                    if author not in TEAM_LOOKUP:
                        db_conn.execute(
                            "INSERT OR IGNORE INTO dim_author "
                            "(author_login, team_name, is_tracked) "
                            "VALUES (?, NULL, 0)",
                            (author,),
                        )
                with_db_retry(_write_pr)

                if (total_processed + 1) % 50 == 0:
                    with_db_retry(db_conn.commit)

            total_processed += 1
            time.sleep(0.25)

            if (i + 1) % 50 == 0:
                log(f"  Progress: {i+1}/{len(candidates)} PRs enriched")

        total_skipped += len(prs) - len(candidates)

    if db_conn and not args.dry_run:
        from lib.db import update_watermark
        with_db_retry(db_conn.commit)
        with_db_retry(lambda: update_watermark(
            db_conn, "etl_watermark", "github_pr_etl",
            now_iso, total_processed, "prs_processed",
        ))
        db_conn.close()

    log(f"\nDone. Processed {total_processed} PRs, skipped {total_skipped}.")
    log(f"Database: {db_path}")
    print(json.dumps({
        "status": "success",
        "prs_processed": total_processed,
        "prs_skipped": total_skipped,
        "since": since_iso,
        "until": until_iso,
        "dry_run": args.dry_run,
        "db_path": db_path,
    }))


if __name__ == "__main__":
    main()
