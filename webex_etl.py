#!/usr/bin/env python3
"""
Webex ETL to SQLite — ingests messages from a single Webex room.

Uses an OAuth Integration refresh token (see scripts/webex_auth.py to get one).
Supports backfill via --since and incremental pulls via a watermark table.

Usage:
    python3 webex_etl.py                              # incremental
    python3 webex_etl.py --since 2026-04-01           # backfill from date
    python3 webex_etl.py --dry-run --verbose          # preview
"""

import argparse
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    get_webex_config, get_webex_integration,
    get_webex_email_team_lookup, get_db_path,
)
from lib.db import (
    log, is_error, get_db_connection, with_db_retry,
    read_watermark, update_watermark,
)

TOKEN_URL = "https://webexapis.com/v1/access_token"
MESSAGES_URL = "https://webexapis.com/v1/messages"
PEOPLE_URL = "https://webexapis.com/v1/people"

# Page size the Webex API accepts on /messages. Docs say max=1000 but in
# practice it caps at 100 for message lists — we'll use 100 to stay safe.
PAGE_SIZE = 100


# ── Auth ────────────────────────────────────────────────────────────────────


def refresh_access_token():
    """Exchange stored refresh token for a fresh access token.

    Webex rotates the refresh token on every exchange. We write the new one
    back to disk so subsequent runs don't use a stale value.
    """
    integ = get_webex_integration()
    cfg = get_webex_config() or {}
    path = os.path.expanduser(
        cfg.get("refresh_token_path") or "~/.webex_refresh_token"
    )
    if not os.path.exists(path):
        print(
            f"ERROR: No refresh token at {path}. "
            "Run scripts/webex_auth.py to create one.",
            file=sys.stderr,
        )
        sys.exit(1)
    with open(path) as f:
        refresh = f.read().strip()

    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "client_id": integ["client_id"],
        "client_secret": integ["client_secret"],
        "refresh_token": refresh,
    }).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req) as resp:
            tokens = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        print(
            f"ERROR: Token refresh failed ({e.code}): {e.read().decode()[:500]}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Persist rotated refresh token if it changed
    new_rt = tokens.get("refresh_token")
    if new_rt and new_rt != refresh:
        with open(path, "w") as f:
            f.write(new_rt)
        os.chmod(path, 0o600)
        log(f"  Refresh token rotated, saved to {path}")

    return tokens["access_token"]


# ── API ─────────────────────────────────────────────────────────────────────


def webex_get(access_token, url, retries=3):
    """GET a Webex endpoint with retry on 429."""
    for attempt in range(retries):
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {access_token}")
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read()), dict(resp.headers)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 10))
                log(f"  Rate limited; sleeping {retry_after}s...")
                time.sleep(retry_after)
                continue
            body = e.read().decode()[:400]
            if attempt == retries - 1:
                return {"error": str(e), "status": e.code, "body": body}, {}
            time.sleep(2)
    return {"error": "max retries"}, {}


def fetch_messages(access_token, room_id, before=None):
    """Fetch one page of messages, newest-first. Returns (items, next_before)."""
    params = {"roomId": room_id, "max": PAGE_SIZE}
    if before:
        params["beforeMessage"] = before
    url = MESSAGES_URL + "?" + urllib.parse.urlencode(params)
    data, _headers = webex_get(access_token, url)
    if is_error(data):
        log(f"  Error fetching messages: {data}")
        return [], None
    items = data.get("items", [])
    next_before = items[-1]["id"] if items else None
    return items, next_before


def fetch_all_messages_since(access_token, room_id, since_iso, verbose=False):
    """Walk the room newest → oldest, stopping when we've passed since_iso."""
    all_items = []
    before = None
    pages = 0
    while True:
        items, next_before = fetch_messages(access_token, room_id, before=before)
        pages += 1
        if not items:
            break
        # Messages come newest first; once the oldest in this page is
        # older than the cutoff, we can stop.
        kept = [m for m in items if m["created"] >= since_iso]
        all_items.extend(kept)
        if verbose:
            log(f"  page {pages}: fetched {len(items)}, kept {len(kept)}, "
                f"oldest in page {items[-1]['created']}")
        if len(kept) < len(items):
            break  # We crossed the cutoff in this page
        if not next_before:
            break
        before = next_before
        time.sleep(0.2)  # Be polite
    return all_items


# ── Transform ───────────────────────────────────────────────────────────────


# Bot emails we don't want polluting the volume/asks metrics. These generate
# mechanical responses (++ karma, reminders) rather than support activity.
IGNORED_BOT_EMAILS = {"thekarmabot@webex.bot"}


def transform_message(msg, email_team_lookup):
    """Flatten a Webex message record for SQLite."""
    email = (msg.get("personEmail") or "").lower()
    return {
        "message_id": msg["id"],
        "room_id": msg.get("roomId"),
        "parent_message_id": msg.get("parentId"),
        "person_id": msg.get("personId"),
        "person_email": email,
        "team_name": email_team_lookup.get(email, "Other"),
        "text": msg.get("text"),
        "mentions_json": (
            json.dumps(msg.get("mentionedPeople")) if msg.get("mentionedPeople") else None
        ),
        "created_at": msg.get("created"),
    }


# ── SQLite ──────────────────────────────────────────────────────────────────

WEBEX_DDL = [
    """
    CREATE TABLE IF NOT EXISTS fact_webex_message (
        message_id          TEXT PRIMARY KEY,
        room_id             TEXT NOT NULL,
        parent_message_id   TEXT,
        person_id           TEXT,
        person_email        TEXT,
        team_name           TEXT,
        text                TEXT,
        mentions_json       TEXT,
        created_at          TEXT NOT NULL,
        etl_loaded_at       TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_webex_msg_room ON fact_webex_message(room_id)",
    "CREATE INDEX IF NOT EXISTS idx_webex_msg_parent ON fact_webex_message(parent_message_id)",
    "CREATE INDEX IF NOT EXISTS idx_webex_msg_person ON fact_webex_message(person_email)",
    "CREATE INDEX IF NOT EXISTS idx_webex_msg_created ON fact_webex_message(created_at)",
    """
    CREATE TABLE IF NOT EXISTS etl_watermark_webex (
        pipeline_name       TEXT PRIMARY KEY,
        last_run_at         TEXT,
        last_updated_since  TEXT,
        messages_processed  INTEGER
    )
    """,
]


def ensure_webex_tables(conn):
    for ddl in WEBEX_DDL:
        conn.execute(ddl)
    conn.commit()


def upsert_message(conn, record):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR REPLACE INTO fact_webex_message (
            message_id, room_id, parent_message_id, person_id, person_email,
            team_name, text, mentions_json, created_at, etl_loaded_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["message_id"], record["room_id"],
            record["parent_message_id"], record["person_id"],
            record["person_email"], record["team_name"],
            record["text"], record["mentions_json"],
            record["created_at"], now,
        ),
    )


# ── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Webex ETL to SQLite")
    parser.add_argument("--since", type=str,
                        help="Start date YYYY-MM-DD. Overrides watermark.")
    parser.add_argument("--room", type=str,
                        help="Room ID override (defaults to config.webex.room_id).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Fetch and transform, but don't write to DB.")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    cfg = get_webex_config()
    if not cfg:
        print("ERROR: [webex] section missing from config.toml", file=sys.stderr)
        sys.exit(1)

    room_id = args.room or cfg.get("room_id")
    if not room_id:
        print("ERROR: webex.room_id not set in config.toml", file=sys.stderr)
        sys.exit(1)

    db_path = get_db_path()
    db_conn = None
    if not args.dry_run:
        db_conn = get_db_connection(db_path)
        ensure_webex_tables(db_conn)

    # Resolve 'since'
    if args.since:
        since_iso = f"{args.since}T00:00:00.000Z"
    elif db_conn:
        wm = read_watermark(db_conn, "etl_watermark_webex", "webex_etl")
        since_iso = wm if wm else None
    else:
        since_iso = None

    if not since_iso:
        # Default to last 7 days on a fresh install
        since_iso = (
            datetime.now(timezone.utc)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .isoformat().replace("+00:00", "Z")
        )
        log("No watermark, defaulting to today (no backfill). "
            "Use --since YYYY-MM-DD for a backfill.")

    log(f"Fetching messages since {since_iso} from room {room_id}")

    access_token = refresh_access_token()
    email_team_lookup = get_webex_email_team_lookup()

    messages = fetch_all_messages_since(
        access_token, room_id, since_iso, verbose=args.verbose,
    )
    log(f"Fetched {len(messages)} messages")

    total = 0
    skipped_bots = 0
    for msg in messages:
        email = (msg.get("personEmail") or "").lower()
        if email in IGNORED_BOT_EMAILS:
            skipped_bots += 1
            continue
        record = transform_message(msg, email_team_lookup)
        if args.dry_run:
            if args.verbose or total < 3:
                print(json.dumps(record, indent=2, default=str))
        else:
            with_db_retry(lambda r=record: upsert_message(db_conn, r))
        total += 1
        if not args.dry_run and total % 200 == 0:
            with_db_retry(db_conn.commit)

    if db_conn and not args.dry_run:
        with_db_retry(db_conn.commit)
        now_iso = datetime.now(timezone.utc).isoformat()
        with_db_retry(lambda: update_watermark(
            db_conn, "etl_watermark_webex", "webex_etl",
            now_iso, total, "messages_processed",
        ))
        db_conn.close()

    log(f"Done. Processed {total} messages ({skipped_bots} bot messages skipped).")
    print(json.dumps({
        "status": "success",
        "messages_processed": total,
        "bot_messages_skipped": skipped_bots,
        "room_id": room_id,
        "since": since_iso,
        "dry_run": args.dry_run,
        "db_path": db_path,
    }))


if __name__ == "__main__":
    main()
