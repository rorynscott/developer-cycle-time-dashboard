#!/usr/bin/env python3
"""
VictorOps (Splunk On-Call) ETL to SQLite.

Fetches incidents from the Reporting API scoped to a single team slug, then
classifies each by whether any team member was paged or actively responded.
This lets the dashboard distinguish real on-call impact from policy noise.

Usage:
    python3 victorops_etl.py                         # incremental
    python3 victorops_etl.py --since 2026-01-01      # backfill
    python3 victorops_etl.py --dry-run --verbose
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    get_victorops_config, get_victorops_credentials, get_db_path,
)
from lib.db import (
    log, is_error, get_db_connection, with_db_retry,
    read_watermark, update_watermark,
)

TEAM_MEMBERS_URL = "https://api.victorops.com/api-public/v1/team/{team}/members"
INCIDENTS_URL = "https://api.victorops.com/api-reporting/v2/incidents"

PAGE_SIZE = 100


# ── API ─────────────────────────────────────────────────────────────────────


def vo_get(creds, url, retries=5):
    """GET a VictorOps endpoint with retry/backoff on 429."""
    for attempt in range(retries):
        req = urllib.request.Request(url)
        req.add_header("X-VO-Api-Id", creds["api_id"])
        req.add_header("X-VO-Api-Key", creds["api_key"])
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                delay = 15 * (attempt + 1)
                log(f"  Rate limited; sleeping {delay}s (attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                continue
            body = e.read().decode()[:400]
            if attempt == retries - 1:
                return {"error": str(e), "status": e.code, "body": body}
            time.sleep(2)
    return {"error": "max retries exceeded"}


def fetch_team_members(creds, team_slug):
    """Return the set of VO usernames on the given team."""
    result = vo_get(creds, TEAM_MEMBERS_URL.format(team=team_slug))
    if is_error(result):
        log(f"ERROR: Could not fetch team members: {result}")
        sys.exit(1)
    return {m["username"] for m in result.get("members", []) if m.get("username")}


def fetch_all_incidents(creds, team_slug, started_after):
    """Paginate through all incidents since started_after, team-scoped."""
    all_items = []
    offset = 0
    while True:
        params = {
            "startedAfter": started_after,
            "teams": team_slug,
            "limit": PAGE_SIZE,
            "offset": offset,
        }
        url = INCIDENTS_URL + "?" + urllib.parse.urlencode(params)
        result = vo_get(creds, url)
        if is_error(result):
            log(f"ERROR fetching incidents at offset {offset}: {result}")
            break
        items = result.get("incidents", [])
        all_items.extend(items)
        total = result.get("total", 0)
        log(f"  Fetched {len(all_items)} / {total}")
        if len(items) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(1.5)  # polite pacing
    return all_items


# ── Transform ───────────────────────────────────────────────────────────────


def _transition(transitions, name):
    """Find first transition matching a name (case-insensitive)."""
    target = name.lower()
    for t in transitions or []:
        if (t.get("name") or "").lower() == target:
            return t
    return None


def transform_incident(inc, team_members, exclude_patterns, team_slug):
    """Flatten a VO incident into our DB shape with impact classification."""
    transitions = inc.get("transitions", []) or []
    triggered = _transition(transitions, "triggered")
    acked = _transition(transitions, "acknowledged")
    resolved = _transition(transitions, "resolved")

    paged_users = list(inc.get("pagedUsers") or [])
    paged_teams = list(inc.get("pagedTeams") or [])

    acker = acked.get("by") if acked else None
    resolver = resolved.get("by") if resolved else None

    paged_team_member = any(u in team_members for u in paged_users)
    responded_by_team = (
        (acker in team_members if acker else False)
        or (resolver in team_members if resolver else False)
    )

    service = inc.get("service") or inc.get("entityDisplayName") or ""
    is_test = any(
        re.search(p, service, re.IGNORECASE) for p in exclude_patterns
    ) if exclude_patterns else False

    return {
        "incident_number": str(inc.get("incidentNumber")),
        "entity_id": inc.get("entityId"),
        "service": service,
        "entity_display": inc.get("entityDisplayName"),
        "started_at": inc.get("startTime") or (triggered or {}).get("at"),
        "acked_at": (acked or {}).get("at"),
        "resolved_at": (resolved or {}).get("at"),
        "acker": acker,
        "resolver": resolver,
        "current_phase": inc.get("currentPhase"),
        "alert_count": inc.get("alertCount"),
        "routing_key": inc.get("routingKey"),
        "paged_users_json": json.dumps(paged_users) if paged_users else None,
        "paged_teams_json": json.dumps(paged_teams) if paged_teams else None,
        "team_slug": team_slug,
        "paged_team_member": int(paged_team_member),
        "responded_by_team": int(responded_by_team),
        "is_test": int(is_test),
    }


# ── SQLite ──────────────────────────────────────────────────────────────────

VO_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fact_vo_incident (
        incident_number     TEXT PRIMARY KEY,
        entity_id           TEXT,
        service             TEXT,
        entity_display      TEXT,
        started_at          TEXT NOT NULL,
        acked_at            TEXT,
        resolved_at         TEXT,
        acker               TEXT,
        resolver            TEXT,
        current_phase       TEXT,
        alert_count         INTEGER,
        routing_key         TEXT,
        paged_users_json    TEXT,
        paged_teams_json    TEXT,
        team_slug           TEXT,
        paged_team_member   INTEGER,
        responded_by_team   INTEGER,
        is_test             INTEGER,
        etl_loaded_at       TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_vo_started ON fact_vo_incident(started_at)",
    "CREATE INDEX IF NOT EXISTS idx_vo_responded ON fact_vo_incident(responded_by_team)",
    "CREATE INDEX IF NOT EXISTS idx_vo_routing ON fact_vo_incident(routing_key)",
    """
    CREATE TABLE IF NOT EXISTS etl_watermark_victorops (
        pipeline_name       TEXT PRIMARY KEY,
        last_run_at         TEXT,
        last_updated_since  TEXT,
        incidents_processed INTEGER
    )
    """,
]


def ensure_vo_tables(conn):
    for ddl in VO_DDL:
        conn.execute(ddl)
    conn.commit()


def upsert_incident(conn, r):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO fact_vo_incident (
            incident_number, entity_id, service, entity_display, started_at,
            acked_at, resolved_at, acker, resolver, current_phase,
            alert_count, routing_key, paged_users_json, paged_teams_json,
            team_slug, paged_team_member, responded_by_team, is_test,
            etl_loaded_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        (
            r["incident_number"], r["entity_id"], r["service"], r["entity_display"],
            r["started_at"], r["acked_at"], r["resolved_at"],
            r["acker"], r["resolver"], r["current_phase"], r["alert_count"],
            r["routing_key"], r["paged_users_json"], r["paged_teams_json"],
            r["team_slug"], r["paged_team_member"], r["responded_by_team"],
            r["is_test"], now,
        ),
    )


# ── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="VictorOps ETL to SQLite")
    parser.add_argument("--since", type=str,
                        help="Start date YYYY-MM-DD (default: watermark or last 7 days)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cfg = get_victorops_config()
    if not cfg:
        print("ERROR: [victorops] not configured", file=sys.stderr)
        sys.exit(1)
    creds = get_victorops_credentials()

    db_path = get_db_path()
    db_conn = None
    if not args.dry_run:
        db_conn = get_db_connection(db_path)
        ensure_vo_tables(db_conn)

    # Resolve since
    if args.since:
        since = f"{args.since}T00:00:00Z"
    elif db_conn:
        wm = read_watermark(db_conn, "etl_watermark_victorops", "victorops_etl")
        # Re-fetch the last 2 days on every run to catch late acks/resolutions.
        if wm:
            dt = datetime.fromisoformat(wm.replace("Z", "+00:00"))
            since = (dt - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            since = None
    else:
        since = None

    if not since:
        since_dt = datetime.now(timezone.utc) - timedelta(days=7)
        since = since_dt.strftime("%Y-%m-%dT00:00:00Z")
        log("No watermark; defaulting to last 7 days")

    log(f"Fetching VO incidents since {since} for team {cfg['team_slug']}")

    team_members = fetch_team_members(creds, cfg["team_slug"])
    log(f"Team members: {len(team_members)}")

    incidents = fetch_all_incidents(creds, cfg["team_slug"], since)
    log(f"Fetched {len(incidents)} incidents")

    total = 0
    skipped_test = 0
    for inc in incidents:
        record = transform_incident(
            inc, team_members, cfg["exclude_service_patterns"], cfg["team_slug"]
        )
        if record["is_test"]:
            skipped_test += 1
            if args.verbose:
                log(f"  SKIP (test): #{record['incident_number']} {record['service'][:60]}")
            continue

        if args.dry_run:
            if args.verbose or total < 3:
                print(json.dumps(record, indent=2, default=str))
        else:
            with_db_retry(lambda r=record: upsert_incident(db_conn, r))
        total += 1
        if not args.dry_run and total % 200 == 0:
            with_db_retry(db_conn.commit)

    if db_conn and not args.dry_run:
        with_db_retry(db_conn.commit)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with_db_retry(lambda: update_watermark(
            db_conn, "etl_watermark_victorops", "victorops_etl",
            now_iso, total, "incidents_processed",
        ))
        db_conn.close()

    log(f"Done. {total} incidents processed, {skipped_test} test incidents skipped.")
    print(json.dumps({
        "status": "success",
        "incidents_processed": total,
        "test_incidents_skipped": skipped_test,
        "since": since,
        "dry_run": args.dry_run,
        "db_path": db_path,
    }))


if __name__ == "__main__":
    main()
