#!/usr/bin/env python3
"""
FireHydrant ETL to SQLite.

Fetches incidents from the FireHydrant REST API scoped to a single team,
flattening severity / milestone / lifecycle measurements and role assignments
into dedicated tables.

Usage:
    python3 firehydrant_etl.py                         # incremental
    python3 firehydrant_etl.py --since 2026-01-01      # backfill
    python3 firehydrant_etl.py --dry-run --verbose
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
    get_firehydrant_config, get_firehydrant_token, get_db_path,
)
from lib.db import (
    log, is_error, get_db_connection, with_db_retry,
    read_watermark, update_watermark,
)

INCIDENTS_URL = "https://api.firehydrant.io/v1/incidents"
PAGE_SIZE = 50


# ── API ─────────────────────────────────────────────────────────────────────


def fh_get(token, url, retries=5):
    for attempt in range(retries):
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                delay = 10 * (attempt + 1)
                log(f"  Rate limited; sleeping {delay}s")
                time.sleep(delay)
                continue
            body = e.read().decode()[:400]
            if attempt == retries - 1:
                return {"error": str(e), "status": e.code, "body": body}
            time.sleep(2)
    return {"error": "max retries exceeded"}


def fetch_all_incidents(token, team_id, since_date):
    """Paginate through incidents scoped to a team, starting at since_date."""
    items = []
    page = 1
    while True:
        params = {
            "team_ids": team_id,
            "start_date": since_date,
            "per_page": PAGE_SIZE,
            "page": page,
        }
        url = INCIDENTS_URL + "?" + urllib.parse.urlencode(params)
        data = fh_get(token, url)
        if is_error(data):
            log(f"ERROR fetching page {page}: {data}")
            break
        page_items = data.get("data", [])
        items.extend(page_items)
        pagination = data.get("pagination") or {}
        log(f"  page {page}: fetched {len(page_items)}, total so far {len(items)}")
        if not page_items or not pagination.get("next"):
            break
        page = pagination["next"]
        time.sleep(0.5)
    return items


# ── Transform ───────────────────────────────────────────────────────────────


def _iso_duration_to_seconds(s):
    """Parse an ISO-8601 duration like 'PT1H30M5S' into seconds. None for invalid."""
    if not s or not isinstance(s, str):
        return None
    m = re.match(r"^PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?$", s)
    if not m:
        return None
    h, mi, se = m.groups()
    return float(h or 0) * 3600 + float(mi or 0) * 60 + float(se or 0)


def transform_incident(inc, team_id):
    """Flatten a FH incident record + derived fields."""
    # Lifecycle measurements — store all as a dict {slug: seconds}
    lm = {}
    for meas in inc.get("lifecycle_measurements") or []:
        slug = meas.get("slug")
        seconds = _iso_duration_to_seconds(meas.get("value"))
        if slug and seconds is not None:
            lm[slug] = seconds

    severity = inc.get("severity")
    current_milestone = inc.get("current_milestone")

    return {
        "incident_id": inc["id"],
        "number": inc.get("number"),
        "name": inc.get("name"),
        "summary": inc.get("summary"),
        "customer_impact_summary": inc.get("customer_impact_summary"),
        "severity": severity,
        "priority": inc.get("priority"),
        "current_milestone": current_milestone,
        "incident_type": (inc.get("incident_type") or {}).get("name"),
        "tag_list": json.dumps(inc.get("tag_list") or []) if inc.get("tag_list") else None,
        "labels": json.dumps(inc.get("labels") or {}) if inc.get("labels") else None,
        "team_id": team_id,
        "created_at": inc.get("created_at"),
        "started_at": inc.get("started_at"),
        "discarded_at": inc.get("discarded_at"),
        "lifecycle_seconds_json": json.dumps(lm) if lm else None,
        "time_to_detect_s": lm.get("time-to-detect"),
        "time_to_acknowledge_s": lm.get("time-to-acknowledge"),
        "time_to_mitigation_s": lm.get("time-to-mitigate"),
        "time_to_resolution_s": lm.get("time-to-resolve"),
    }


def transform_role_assignments(inc):
    """One row per (incident, role, user)."""
    out = []
    for ra in inc.get("role_assignments") or []:
        role = (ra.get("incident_role") or {}).get("name")
        user = ra.get("user") or {}
        out.append({
            "incident_id": inc["id"],
            "role_name": role,
            "user_id": user.get("id"),
            "user_name": user.get("name"),
            "user_email": user.get("email"),
        })
    return out


# ── SQLite ──────────────────────────────────────────────────────────────────

FH_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fact_fh_incident (
        incident_id                 TEXT PRIMARY KEY,
        number                      INTEGER,
        name                        TEXT,
        summary                     TEXT,
        customer_impact_summary     TEXT,
        severity                    TEXT,
        priority                    TEXT,
        current_milestone           TEXT,
        incident_type               TEXT,
        tag_list                    TEXT,
        labels                      TEXT,
        team_id                     TEXT,
        created_at                  TEXT,
        started_at                  TEXT,
        discarded_at                TEXT,
        lifecycle_seconds_json      TEXT,
        time_to_detect_s            REAL,
        time_to_acknowledge_s       REAL,
        time_to_mitigation_s        REAL,
        time_to_resolution_s        REAL,
        etl_loaded_at               TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fh_started ON fact_fh_incident(started_at)",
    "CREATE INDEX IF NOT EXISTS idx_fh_severity ON fact_fh_incident(severity)",
    "CREATE INDEX IF NOT EXISTS idx_fh_milestone ON fact_fh_incident(current_milestone)",
    """
    CREATE TABLE IF NOT EXISTS dim_fh_role_assignment (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        incident_id     TEXT NOT NULL,
        role_name       TEXT,
        user_id         TEXT,
        user_name       TEXT,
        user_email      TEXT,
        etl_loaded_at   TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fh_ra_incident ON dim_fh_role_assignment(incident_id)",
    "CREATE INDEX IF NOT EXISTS idx_fh_ra_user ON dim_fh_role_assignment(user_id)",
    """
    CREATE TABLE IF NOT EXISTS etl_watermark_firehydrant (
        pipeline_name       TEXT PRIMARY KEY,
        last_run_at         TEXT,
        last_updated_since  TEXT,
        incidents_processed INTEGER
    )
    """,
]


def ensure_fh_tables(conn):
    for ddl in FH_DDL:
        conn.execute(ddl)
    conn.commit()


def upsert_incident(conn, r):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO fact_fh_incident (
            incident_id, number, name, summary, customer_impact_summary,
            severity, priority, current_milestone, incident_type,
            tag_list, labels, team_id,
            created_at, started_at, discarded_at,
            lifecycle_seconds_json,
            time_to_detect_s, time_to_acknowledge_s,
            time_to_mitigation_s, time_to_resolution_s, etl_loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            r["incident_id"], r["number"], r["name"], r["summary"],
            r["customer_impact_summary"], r["severity"], r["priority"],
            r["current_milestone"], r["incident_type"],
            r["tag_list"], r["labels"], r["team_id"],
            r["created_at"], r["started_at"], r["discarded_at"],
            r["lifecycle_seconds_json"],
            r["time_to_detect_s"], r["time_to_acknowledge_s"],
            r["time_to_mitigation_s"], r["time_to_resolution_s"],
            now,
        ),
    )


def replace_role_assignments(conn, incident_id, assignments):
    """Role assignments can change; delete+insert rather than upsert."""
    conn.execute(
        "DELETE FROM dim_fh_role_assignment WHERE incident_id = ?", (incident_id,)
    )
    now = datetime.now(timezone.utc).isoformat()
    for a in assignments:
        conn.execute(
            """
            INSERT INTO dim_fh_role_assignment (
                incident_id, role_name, user_id, user_name, user_email,
                etl_loaded_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                a["incident_id"], a["role_name"], a["user_id"],
                a["user_name"], a["user_email"], now,
            ),
        )


# ── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="FireHydrant ETL to SQLite")
    parser.add_argument("--since", type=str)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cfg = get_firehydrant_config()
    if not cfg:
        print("ERROR: [firehydrant] not configured", file=sys.stderr)
        sys.exit(1)
    token = get_firehydrant_token()

    db_path = get_db_path()
    db_conn = None
    if not args.dry_run:
        db_conn = get_db_connection(db_path)
        ensure_fh_tables(db_conn)

    if args.since:
        since_date = args.since
    elif db_conn:
        wm = read_watermark(db_conn, "etl_watermark_firehydrant", "firehydrant_etl")
        if wm:
            # Re-fetch last 3 days to catch role changes + late milestone updates
            dt = datetime.fromisoformat(wm.replace("Z", "+00:00"))
            since_date = (dt - timedelta(days=3)).strftime("%Y-%m-%d")
        else:
            since_date = None
    else:
        since_date = None

    if not since_date:
        since_date = (
            datetime.now(timezone.utc) - timedelta(days=14)
        ).strftime("%Y-%m-%d")
        log("No watermark; defaulting to last 14 days")

    log(f"Fetching FH incidents since {since_date} for team {cfg['team_id']}")

    incidents = fetch_all_incidents(token, cfg["team_id"], since_date)
    log(f"Fetched {len(incidents)} incidents")

    total = 0
    for inc in incidents:
        record = transform_incident(inc, cfg["team_id"])
        roles = transform_role_assignments(inc)

        if args.dry_run:
            if args.verbose or total < 2:
                print(json.dumps(record, indent=2, default=str))
                for r in roles:
                    print(f"  role: {r['role_name']} -> {r['user_name']}")
        else:
            def _write(rec=record, ra=roles):
                upsert_incident(db_conn, rec)
                replace_role_assignments(db_conn, rec["incident_id"], ra)
            with_db_retry(_write)

        total += 1
        if not args.dry_run and total % 100 == 0:
            with_db_retry(db_conn.commit)

    if db_conn and not args.dry_run:
        with_db_retry(db_conn.commit)
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with_db_retry(lambda: update_watermark(
            db_conn, "etl_watermark_firehydrant", "firehydrant_etl",
            now_iso, total, "incidents_processed",
        ))
        db_conn.close()

    log(f"Done. Processed {total} incidents.")
    print(json.dumps({
        "status": "success",
        "incidents_processed": total,
        "since": since_date,
        "dry_run": args.dry_run,
        "db_path": db_path,
    }))


if __name__ == "__main__":
    main()
