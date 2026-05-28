#!/usr/bin/env python3
"""
Developer Cycle Time Dashboard — Streamlit app powered by the ETL SQLite DB.

Usage:
    streamlit run dashboard.py
"""

import json
import os
import sys
import sqlite3
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import numpy as np
import plotly.graph_objects as go
import streamlit as st

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import (
    load_config, get_teams, get_team_names, get_team_colors,
    get_db_path, get_dashboard_title, get_dashboard_default_days,
    get_jira_support_config,
)
from lib.categorize import categorize

# ── Config-driven constants ────────────────────────────────────────────────

DB_PATH = get_db_path()
TEAMS = get_team_names()
COLORS = get_team_colors()

# Build origin mapping: team_name → short_name for review burden chart
_TEAM_SHORT_NAMES = {t["name"]: t["short_name"] for t in get_teams()}

AI_COLORS = {
    "Human Only": "#636EFA",
    "Human + AI": "#AB63FA",
}

# ── Data loading ────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def load_prs():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                pr_key, pr_number, repo_key, author_login, team_name, title,
                state, is_draft, created_at, merged_at, closed_at,
                hours_to_first_review, hours_to_first_approval, hours_to_merge,
                review_comment_count, issue_comment_count, total_comment_count,
                files_changed, lines_added, lines_removed, total_lines_changed,
                has_ai_coauthor, ai_coauthor_tools, review_status
            FROM fact_pr
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame()
    conn.close()
    if df.empty:
        return df
    df["created_date"] = pd.to_datetime(df["created_at"]).dt.date
    df["merged_date"] = pd.to_datetime(df["merged_at"]).dt.date
    return df


@st.cache_data(ttl=300)
def load_task_cycle_times():
    """Load per-Jira-issue cycle time data joined with PR timing."""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                j.issue_key,
                j.issue_type,
                j.status,
                j.story_points,
                j.sprint_name,
                j.project_key,
                j.assignee_display_name,
                MAX(p.has_ai_coauthor) AS has_ai,
                p.team_name,
                j.in_progress_at,
                MIN(p.created_at) AS first_pr_created,
                MAX(p.merged_at) AS last_pr_merged,
                j.done_at,
                COUNT(DISTINCT p.pr_key) AS pr_count
            FROM fact_pr p
            JOIN bridge_pr_jira b ON p.pr_key = b.pr_key
            JOIN dim_jira_issue j ON b.jira_key = j.issue_key
            WHERE p.team_name IS NOT NULL
                AND p.state = 'merged'
                AND j.in_progress_at IS NOT NULL
                AND j.done_at IS NOT NULL
            GROUP BY j.issue_key
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame()
    conn.close()

    if df.empty:
        return df

    for col in ("in_progress_at", "first_pr_created", "last_pr_merged", "done_at"):
        df[col] = pd.to_datetime(df[col], utc=True)

    df["pre_pr_hours"] = (
        (df["first_pr_created"] - df["in_progress_at"]).dt.total_seconds() / 3600
    )
    df["pr_cycle_hours"] = (
        (df["last_pr_merged"] - df["first_pr_created"]).dt.total_seconds() / 3600
    )
    df["post_merge_hours"] = (
        (df["done_at"] - df["last_pr_merged"]).dt.total_seconds() / 3600
    )
    df["total_cycle_hours"] = (
        (df["done_at"] - df["in_progress_at"]).dt.total_seconds() / 3600
    )
    df["pr_pct"] = df["pr_cycle_hours"] / df["total_cycle_hours"] * 100

    df = df[
        (df["pr_cycle_hours"] > 0)
        & (df["total_cycle_hours"] > 0)
        & (df["pre_pr_hours"] >= 0)
        & (df["post_merge_hours"] >= 0)
        & (df["total_cycle_hours"] < 2000)
    ].copy()

    df["authoring"] = df["has_ai"].map({1: "Human + AI", 0: "Human Only"})
    df["done_date"] = df["done_at"].dt.date
    return df


@st.cache_data(ttl=300)
def load_throughput_load_daily():
    """Daily series of engineering throughput and support load, team-scoped.

    Throughput components:
      - merged_prs: PRs merged by our teams (from fact_pr)
      - jira_done:  Jira issues transitioned to Done/Closed/Resolved that had
                    at least one linked PR authored by one of our teams.
                    (We don't have a Jira-assignee → team mapping, so using
                    the linked-PR relationship scopes ownership cleanly.)

    Support load components:
      - ztce_created: ZTCE tickets created, scoped to configured support teams
      - webex_asks:   Top-level Webex messages (asks) in our room
      - fh_real:      FireHydrant S1-S4 incidents (drills/gamedays excluded)
    """
    conn = sqlite3.connect(DB_PATH)

    team_list = [
        t["name"] for t in get_teams()
    ]
    placeholders = ",".join("?" for _ in team_list)

    # 1. Merged PRs per day
    try:
        prs = pd.read_sql_query(
            f"""
            SELECT DATE(merged_at) AS d, COUNT(*) AS merged_prs
            FROM fact_pr
            WHERE team_name IN ({placeholders}) AND merged_at IS NOT NULL
            GROUP BY DATE(merged_at)
            """,
            conn, params=team_list,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        prs = pd.DataFrame(columns=["d", "merged_prs"])

    # 2. Jira done-transitions for issues with linked PR from our teams
    try:
        jira_done = pd.read_sql_query(
            f"""
            SELECT DATE(sc.changed_at) AS d,
                   COUNT(DISTINCT sc.issue_key) AS jira_done
            FROM dim_jira_status_change sc
            WHERE sc.to_status IN ('Done','Closed','Resolved')
              AND sc.issue_key IN (
                SELECT DISTINCT b.jira_key
                FROM bridge_pr_jira b
                JOIN fact_pr p ON b.pr_key = p.pr_key
                WHERE p.team_name IN ({placeholders})
              )
            GROUP BY DATE(sc.changed_at)
            """,
            conn, params=team_list,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        jira_done = pd.DataFrame(columns=["d", "jira_done"])

    # 3. ZTCE tickets created (scoped via support_team custom field)
    support_cfg = get_jira_support_config()
    ztce = pd.DataFrame(columns=["d", "ztce_created"])
    if support_cfg:
        try:
            ztce_raw = pd.read_sql_query(
                """
                SELECT created_at, custom_fields_json
                FROM dim_jira_issue
                WHERE project_key = ?
                """,
                conn, params=(support_cfg["project"],),
            )
            if not ztce_raw.empty:
                allowed = set(support_cfg.get("teams", []))
                def _team(row):
                    if not row: return None
                    try: return json.loads(row).get("support_team")
                    except (TypeError, ValueError): return None
                ztce_raw["team"] = ztce_raw["custom_fields_json"].apply(_team)
                if allowed:
                    ztce_raw = ztce_raw[ztce_raw["team"].isin(allowed)]
                ztce_raw["d"] = pd.to_datetime(ztce_raw["created_at"], utc=True).dt.date
                ztce = (
                    ztce_raw.groupby("d").size().reset_index(name="ztce_created")
                )
                ztce["d"] = ztce["d"].astype(str)
        except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
            pass

    # 4. Webex top-level asks per day
    try:
        webex_asks = pd.read_sql_query(
            """
            SELECT DATE(created_at) AS d, COUNT(*) AS webex_asks
            FROM fact_webex_message
            WHERE parent_message_id IS NULL
            GROUP BY DATE(created_at)
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        webex_asks = pd.DataFrame(columns=["d", "webex_asks"])

    conn.close()

    # 5. FireHydrant S1-S4 per day, scoped to incidents where a team
    #    member held a role OR a configured keyword matched. Uses the
    #    same filter logic as the Incidents tab for consistency.
    fh_all, _ = load_fh_incidents()
    if fh_all.empty:
        fh_real = pd.DataFrame(columns=["d", "fh_real"])
    else:
        scoped = fh_all[
            fh_all["severity"].isin(["S1", "S2", "S3", "S4"])
            & (fh_all["match_member"] | fh_all["match_keyword"])
        ].copy()
        if scoped.empty:
            fh_real = pd.DataFrame(columns=["d", "fh_real"])
        else:
            fh_real = (
                scoped.assign(d=scoped["started_at"].dt.date)
                .groupby("d").size()
                .reset_index(name="fh_real")
            )

    # Outer-join everything on date
    import functools
    frames = [prs, jira_done, ztce, webex_asks, fh_real]
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame()

    out = functools.reduce(
        lambda a, b: pd.merge(a, b, on="d", how="outer"), frames
    ).fillna(0)

    out["d"] = pd.to_datetime(out["d"]).dt.date
    out = out.sort_values("d").reset_index(drop=True)
    # Integer columns
    for col in ("merged_prs", "jira_done", "ztce_created", "webex_asks", "fh_real"):
        if col in out.columns:
            out[col] = out[col].astype(int)
        else:
            out[col] = 0

    # Composites
    out["throughput"] = out["merged_prs"] + out["jira_done"]
    out["support_load"] = out["ztce_created"] + out["webex_asks"] + out["fh_real"]
    return out


@st.cache_data(ttl=300)
def load_fh_incidents():
    """Load FireHydrant incidents + joined IC (first incident commander).

    Also computes per-row match flags against the [incidents] scope:
      - match_member: any role assignment has user_email in member_emails
      - match_keyword: any of name/summary/customer_impact_summary contains
        one of the configured keywords (case-insensitive)
    Rule A (routing key) does not apply to FH — those fields are VO-only.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT i.incident_id, i.number, i.name, i.severity, i.priority,
                   i.current_milestone, i.incident_type,
                   i.summary, i.customer_impact_summary, i.tag_list,
                   i.created_at, i.started_at, i.discarded_at,
                   i.time_to_detect_s, i.time_to_acknowledge_s,
                   i.time_to_mitigation_s, i.time_to_resolution_s
            FROM fact_fh_incident i
            """,
            conn,
        )
        roles = pd.read_sql_query(
            """
            SELECT incident_id, role_name, user_name, user_email
            FROM dim_fh_role_assignment
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame(), pd.DataFrame()
    conn.close()

    if df.empty:
        return df, roles

    df["started_at"] = pd.to_datetime(df["started_at"], utc=True, errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    df["started_date"] = df["started_at"].dt.date
    # Convert seconds → minutes for display
    for col in (
        "time_to_detect_s", "time_to_acknowledge_s",
        "time_to_mitigation_s", "time_to_resolution_s",
    ):
        df[col.replace("_s", "_min")] = df[col] / 60.0

    # ── Per-row scope match flags ─────────────────────────────────────────
    import re as _re
    from lib.config import get_incidents_scope
    scope = get_incidents_scope()

    member_emails = set(scope["member_emails"])
    keyword_re = _compile_keywords(scope["keywords"])

    # Rule B — a team member held any role on the incident
    if not roles.empty and member_emails:
        touched = (
            roles.assign(_email=roles["user_email"].fillna("").str.lower())
            .query("_email in @member_emails")
            ["incident_id"].unique()
        )
        df["match_member"] = df["incident_id"].isin(touched)
    else:
        df["match_member"] = False

    # Rule C — keyword regex match
    text_blob = (
        df["name"].fillna("") + " \n" +
        df["summary"].fillna("") + " \n" +
        df["customer_impact_summary"].fillna("")
    ).str.lower()
    if keyword_re:
        df["match_keyword"] = text_blob.str.contains(keyword_re, regex=True, na=False)
    else:
        df["match_keyword"] = False

    # Rule A does not apply to FH incidents (no routing key on this source).
    df["match_routing"] = False

    return df, roles


def _compile_keywords(keywords):
    """Combine a list of regex patterns into a single alternation regex.

    Returns a compiled pattern or None if the list is empty.
    """
    import re as _re
    if not keywords:
        return None
    # Each keyword is already lowercased by get_incidents_scope; combine.
    alt = "|".join(f"(?:{k})" for k in keywords)
    try:
        return _re.compile(alt)
    except _re.error as e:
        import streamlit as _st
        _st.warning(f"Invalid keyword regex in config.toml [incidents]: {e}")
        return None


@st.cache_data(ttl=300)
def load_vo_incidents():
    """Load VictorOps incidents with derived time-to-ack / time-to-resolve.

    Also computes per-row match flags against the [incidents] scope:
      - match_routing: routing_key is in the configured routing_keys list
      - match_member: a configured vo_username appears in paged_users_json,
        or is the acker/resolver
      - match_keyword: service / entity_display contains a keyword substring
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT incident_number, service, entity_display,
                   started_at, acked_at, resolved_at,
                   acker, resolver, current_phase, alert_count,
                   routing_key, paged_users_json,
                   paged_team_member, responded_by_team, is_test
            FROM fact_vo_incident
            WHERE is_test = 0
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame()
    conn.close()

    if df.empty:
        return df

    for col in ("started_at", "acked_at", "resolved_at"):
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    df["started_date"] = df["started_at"].dt.date
    df["mtta_minutes"] = (
        (df["acked_at"] - df["started_at"]).dt.total_seconds() / 60
    )
    df["mttr_minutes"] = (
        (df["resolved_at"] - df["started_at"]).dt.total_seconds() / 60
    )
    df["impacted_us"] = (
        (df["paged_team_member"] == 1) | (df["responded_by_team"] == 1)
    )

    # ── Per-row scope match flags ─────────────────────────────────────────
    from lib.config import get_incidents_scope
    scope = get_incidents_scope()
    routing_keys = set(scope["routing_keys"])
    vo_usernames = set(scope["vo_usernames"])
    keyword_re = _compile_keywords(scope["keywords"])

    # Rule A — routing key
    df["match_routing"] = (
        df["routing_key"].fillna("").str.lower().isin(routing_keys)
    )

    # Rule B — team member on the page. Check paged_users_json (JSON list)
    # and the raw acker / resolver strings.
    def _any_member_in_json(j):
        if not j or not isinstance(j, str):
            return False
        try:
            users = json.loads(j)
        except (ValueError, TypeError):
            return False
        return any((u or "").lower() in vo_usernames for u in users)

    paged_hit = df["paged_users_json"].apply(_any_member_in_json)
    acker_hit = df["acker"].fillna("").str.lower().isin(vo_usernames)
    resolver_hit = df["resolver"].fillna("").str.lower().isin(vo_usernames)
    df["match_member"] = paged_hit | acker_hit | resolver_hit

    # Rule C — keyword regex match against service + entity_display
    text_blob = (
        df["service"].fillna("") + " " + df["entity_display"].fillna("")
    ).str.lower()
    if keyword_re:
        df["match_keyword"] = text_blob.str.contains(keyword_re, regex=True, na=False)
    else:
        df["match_keyword"] = False

    return df


@st.cache_data(ttl=300)
def load_webex_messages():
    """Load all messages from the configured Webex room.

    Augments each row with category (applied to text at query time) and
    computed thread metadata: for every top-level message, reply_count and
    time-to-first-reply in minutes.
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT message_id, parent_message_id, person_email, team_name,
                   text, created_at
            FROM fact_webex_message
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame()
    conn.close()

    if df.empty:
        return df

    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["created_date"] = df["created_at"].dt.date
    df["is_top_level"] = df["parent_message_id"].isna()
    df["category"] = df["text"].fillna("").apply(categorize)
    return df


@st.cache_data(ttl=300)
def load_support_tickets():
    """Load ZTCE tickets scoped to configured support teams.

    Categorization is applied here (at query time) so updating config
    patterns reclassifies tickets on the next cache refresh without any
    re-ingest.
    """
    support_cfg = get_jira_support_config()
    if not support_cfg:
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                issue_key, summary, status, status_category, priority,
                assignee_display_name, reporter_display_name,
                labels, comment_count, custom_fields_json,
                created_at, updated_at, resolved_at
            FROM dim_jira_issue
            WHERE project_key = ?
            """,
            conn,
            params=(support_cfg["project"],),
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame()
    conn.close()

    if df.empty:
        return df

    # Unpack the support custom fields stored in custom_fields_json
    def _extract(row, key):
        if not row:
            return None
        try:
            return json.loads(row).get(key)
        except (TypeError, ValueError):
            return None

    df["support_team"] = df["custom_fields_json"].apply(lambda v: _extract(v, "support_team"))
    df["support_priority"] = df["custom_fields_json"].apply(lambda v: _extract(v, "support_priority"))
    df["support_severity"] = df["custom_fields_json"].apply(lambda v: _extract(v, "support_severity"))
    df["support_impact"] = df["custom_fields_json"].apply(lambda v: _extract(v, "support_impact"))

    # Scope to configured teams
    allowed = set(support_cfg.get("teams", []))
    if allowed:
        df = df[df["support_team"].isin(allowed)].copy()

    if df.empty:
        return df

    # Date columns
    df["created_date"] = pd.to_datetime(df["created_at"], utc=True).dt.date
    df["resolved_date"] = pd.to_datetime(df["resolved_at"], utc=True).dt.date
    df["ttr_hours"] = (
        (pd.to_datetime(df["resolved_at"], utc=True) -
         pd.to_datetime(df["created_at"], utc=True)).dt.total_seconds() / 3600
    )
    df["category"] = df["summary"].apply(categorize)
    df["is_open"] = df["resolved_at"].isna()
    return df


@st.cache_data(ttl=300)
def load_reviews():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                r.review_key, r.pr_key, r.reviewer_login, r.review_state,
                r.submitted_at, a.team_name AS reviewer_team,
                p.files_changed, p.lines_added, p.lines_removed,
                p.total_lines_changed,
                p.author_login AS pr_author,
                pa.team_name AS pr_author_team,
                pa.is_tracked AS pr_author_is_tracked
            FROM dim_review r
            LEFT JOIN dim_author a ON r.reviewer_login = a.author_login
            LEFT JOIN fact_pr p ON r.pr_key = p.pr_key
            LEFT JOIN dim_author pa ON p.author_login = pa.author_login
            """,
            conn,
        )
    except (pd.io.sql.DatabaseError, sqlite3.OperationalError):
        conn.close()
        return pd.DataFrame()
    conn.close()
    if df.empty:
        return df
    df["review_date"] = pd.to_datetime(df["submitted_at"]).dt.date
    return df


# ── Helpers ─────────────────────────────────────────────────────────────────


def filter_by_date(df, date_col, start, end):
    return df[(df[date_col] >= start) & (df[date_col] <= end)]


def daily_bar_chart(df, date_col, color_col, title, y_label="Count"):
    """Stacked bar chart of daily counts with linear trend line."""
    daily = (
        df.groupby([date_col, color_col])
        .size()
        .reset_index(name="count")
    )
    color_map = (
        COLORS if color_col == "team_name"
        else AI_COLORS if color_col == "authoring"
        else None
    )
    fig = px.bar(
        daily,
        x=date_col,
        y="count",
        color=color_col,
        title=title,
        labels={date_col: "Date", "count": y_label, color_col: ""},
        color_discrete_map=color_map,
        barmode="stack",
    )
    total_daily = df.groupby(date_col).size().reset_index(name="count")
    total_daily = total_daily.sort_values(date_col)
    if len(total_daily) >= 2:
        x_num = (pd.to_datetime(total_daily[date_col]) - pd.to_datetime(total_daily[date_col].iloc[0])).dt.days.values.astype(float)
        coeffs = np.polyfit(x_num, total_daily["count"].values, 1)
        trend_y = np.polyval(coeffs, x_num)
        fig.add_trace(go.Scatter(
            x=total_daily[date_col],
            y=trend_y,
            mode="lines",
            name="Trend",
            line=dict(color="#888", width=2, dash="dash"),
            hovertemplate="%{y:.1f}<extra>Trend</extra>",
        ))
    fig.update_layout(xaxis_tickformat="%b %d", legend=dict(orientation="h", y=-0.15))
    return fig


def stat_chart(df, date_col, value_col, group_col, title):
    """Line chart showing Avg and P90 of a metric per day."""
    agg = (
        df.groupby([date_col, group_col])[value_col]
        .agg(avg="mean", p90=lambda x: x.quantile(0.9))
        .reset_index()
    )
    fig = go.Figure()
    groups = agg[group_col].unique()
    for group in sorted(groups):
        g = agg[agg[group_col] == group]
        color = COLORS.get(group)
        fig.add_trace(go.Scatter(
            x=g[date_col], y=g["avg"], mode="lines+markers",
            name=f"{group} — Avg",
            line=dict(color=color),
        ))
        fig.add_trace(go.Scatter(
            x=g[date_col], y=g["p90"], mode="lines+markers",
            name=f"{group} — P90",
            line=dict(color=color, dash="dash"),
        ))
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=value_col.replace("_", " ").title(),
        xaxis_tickformat="%b %d",
        legend=dict(orientation="h", y=-0.2),
    )
    return fig


def volume_chart(df, date_col, value_col, group_col, title):
    """Line chart showing the daily sum of a metric per group."""
    agg = (
        df.groupby([date_col, group_col])[value_col]
        .sum()
        .reset_index(name="total")
    )
    fig = go.Figure()
    for group in sorted(agg[group_col].unique()):
        g = agg[agg[group_col] == group]
        color = COLORS.get(group)
        fig.add_trace(go.Scatter(
            x=g[date_col], y=g["total"], mode="lines+markers",
            name=group,
            line=dict(color=color),
        ))
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title=value_col.replace("_", " ").title(),
        xaxis_tickformat="%b %d",
        legend=dict(orientation="h", y=-0.2),
    )
    return fig


# ── App ─────────────────────────────────────────────────────────────────────


def main():
    st.set_page_config(page_title="Developer Effectiveness", layout="wide")
    st.title(get_dashboard_title())

    # Show last ETL run times
    conn = sqlite3.connect(DB_PATH)
    etl_sources = [
        ("GitHub", "etl_watermark", "github_pr_etl"),
        ("Jira", "etl_watermark_jira", "jira_etl"),
        ("Webex", "etl_watermark_webex", "webex_etl"),
        ("VictorOps", "etl_watermark_victorops", "victorops_etl"),
        ("FireHydrant", "etl_watermark_firehydrant", "firehydrant_etl"),
    ]
    from datetime import datetime, timezone as tz
    et = tz(timedelta(hours=-4))
    captions = []
    for label, table, pipeline in etl_sources:
        try:
            row = conn.execute(
                f"SELECT last_run_at FROM {table} WHERE pipeline_name = ?",
                (pipeline,),
            ).fetchone()
        except sqlite3.OperationalError:
            continue  # ETL hasn't run yet — table doesn't exist
        if row and row[0]:
            utc_dt = datetime.fromisoformat(row[0])
            captions.append(f"{label}: {utc_dt.astimezone(et).strftime('%Y-%m-%d %I:%M %p')} ET")
    conn.close()
    if captions:
        st.caption("Last updated — " + " · ".join(captions))

    prs = load_prs()
    reviews = load_reviews()
    task_cycles = load_task_cycle_times()
    support = load_support_tickets()
    webex = load_webex_messages()
    vo = load_vo_incidents()
    fh, fh_roles = load_fh_incidents()

    # ── Sidebar ─────────────────────────────────────────────────────────────

    st.sidebar.header("Filters")

    selected_teams = st.sidebar.multiselect(
        "Teams", TEAMS, default=TEAMS
    )

    default_days = get_dashboard_default_days()
    default_end = date.today()
    default_start = default_end - timedelta(days=default_days)
    date_range = st.sidebar.date_input(
        "Date range",
        value=(default_start, default_end),
        max_value=default_end,
    )
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = default_start, default_end

    drill_down = st.sidebar.toggle("Show individual user breakdown", value=False)
    color_col = "author_login" if drill_down else "team_name"
    review_color_col = "reviewer_login" if drill_down else "reviewer_team"

    # ── Filter data ─────────────────────────────────────────────────────────

    if prs.empty and reviews.empty:
        st.info(
            "No data yet. Run the GitHub ETL to populate PR data:\n\n"
            "```\npython3 github_etl.py --since 2025-01-01\n```"
        )
        return

    team_prs = prs[prs["team_name"].isin(selected_teams)] if not prs.empty else prs
    team_prs_ranged = filter_by_date(team_prs, "created_date", start_date, end_date) if not prs.empty else prs

    team_reviews = reviews[reviews["reviewer_team"].isin(selected_teams)] if not reviews.empty else reviews
    team_reviews_ranged = filter_by_date(
        team_reviews, "review_date", start_date, end_date
    ) if not reviews.empty else reviews

    # ── Tabs ───────────────────────────────────────────────────────────────

    (tab_overview, tab_reviews, tab_pr_size, tab_cycle_time,
     tab_support, tab_oncall, tab_incidents, tab_tvl) = st.tabs(
        ["Overview", "Reviews", "PR Size", "Cycle Time",
         "Support", "On-Call", "Incidents", "Throughput vs Load"]
    )

    # ── Overview tab ───────────────────────────────────────────────────────

    with tab_overview:

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("PRs Created", len(team_prs_ranged))
        c2.metric("Reviews Submitted", len(team_reviews_ranged))
        merged = team_prs_ranged[team_prs_ranged["state"] == "merged"]
        avg_merge_hrs = merged["hours_to_merge"].dropna().mean()
        c3.metric("Avg Hours to Merge", f"{avg_merge_hrs:.1f}" if pd.notna(avg_merge_hrs) else "—")
        ai_pct = (
            team_prs_ranged["has_ai_coauthor"].sum() / len(team_prs_ranged) * 100
            if len(team_prs_ranged) > 0 else 0
        )
        c4.metric("AI Co-authored", f"{ai_pct:.0f}%")

        st.header("PRs Created")
        st.plotly_chart(
            daily_bar_chart(
                team_prs_ranged, "created_date", color_col,
                "PRs Created by Day",
            ),
            use_container_width=True,
        )

        st.header("AI Co-authoring")
        ai_prs = team_prs_ranged.copy()
        ai_prs["authoring"] = ai_prs["has_ai_coauthor"].map(
            {1: "Human + AI", 0: "Human Only", True: "Human + AI", False: "Human Only"}
        )
        st.plotly_chart(
            daily_bar_chart(
                ai_prs, "created_date", "authoring",
                "PRs by Authoring Method",
            ),
            use_container_width=True,
        )

    # ── Reviews tab ────────────────────────────────────────────────────────

    with tab_reviews:

        st.header("Reviews Submitted")
        st.plotly_chart(
            daily_bar_chart(
                team_reviews_ranged, "review_date", review_color_col,
                "Reviews Submitted by Day",
            ),
            use_container_width=True,
        )

        st.header("Review Burden by Origin")
        st.caption(
            "All reviews submitted by your teams, grouped by the PR author's team. "
            '"Other" = authors outside your configured teams.'
        )

        origin_reviews = team_reviews_ranged.copy()
        # Map PR author's team to short name; everything else is "Other"
        origin_reviews["review_origin"] = origin_reviews["pr_author_team"].map(
            _TEAM_SHORT_NAMES
        ).fillna("Other")

        # Build color map from config team colors + orange for Other
        origin_color_map = {
            _TEAM_SHORT_NAMES[t["name"]]: t["color"] for t in get_teams()
        }
        origin_color_map["Other"] = "#FFA15A"

        # Category order: teams in config order, then Other
        origin_order = [t["short_name"] for t in get_teams()] + ["Other"]
        origin_reviews["review_origin"] = pd.Categorical(
            origin_reviews["review_origin"],
            categories=origin_order,
            ordered=True,
        )

        origin_daily = (
            origin_reviews.groupby(["review_date", "review_origin"])
            .size()
            .reset_index(name="count")
        )
        fig_origin = px.bar(
            origin_daily,
            x="review_date",
            y="count",
            color="review_origin",
            color_discrete_map=origin_color_map,
            title="Reviews by PR Author Origin",
            labels={"review_date": "Date", "count": "Reviews", "review_origin": ""},
            barmode="stack",
            category_orders={"review_origin": origin_order},
        )

        # Add % Other regression line
        total_daily = origin_reviews.groupby("review_date").size()
        other_daily = (
            origin_reviews[origin_reviews["review_origin"] == "Other"]
            .groupby("review_date").size()
        )
        pct_other = (other_daily / total_daily * 100).dropna().reset_index()
        pct_other.columns = ["review_date", "pct"]
        pct_other = pct_other.sort_values("review_date")
        if len(pct_other) >= 2:
            x_num = (pd.to_datetime(pct_other["review_date"]) - pd.to_datetime(pct_other["review_date"].iloc[0])).dt.days.values.astype(float)
            coeffs = np.polyfit(x_num, pct_other["pct"].values, 1)
            trend_y = np.polyval(coeffs, x_num)
            fig_origin.add_trace(go.Scatter(
                x=pct_other["review_date"],
                y=trend_y,
                mode="lines",
                name="% Other (trend)",
                line=dict(color="#FFA15A", width=2, dash="dash"),
                yaxis="y2",
                hovertemplate="%{y:.1f}%<extra>% Other trend</extra>",
            ))
            fig_origin.update_layout(
                yaxis2=dict(
                    title="% Other",
                    overlaying="y",
                    side="right",
                    range=[0, 100],
                    showgrid=False,
                ),
            )

        fig_origin.update_layout(
            xaxis_tickformat="%b %d",
            legend=dict(orientation="h", y=-0.15),
        )
        st.plotly_chart(fig_origin, use_container_width=True)

        # Summary metrics
        total = len(origin_reviews)
        if total > 0:
            counts = origin_reviews["review_origin"].value_counts()
            other_count = counts.get("Other", 0)
            oc1, oc2, oc3 = st.columns(3)
            oc1.metric("Total Reviews", total)
            oc2.metric("External (Other)", f"{other_count} ({other_count / total * 100:.0f}%)")
            oc3.metric("Internal", f"{total - other_count} ({(total - other_count) / total * 100:.0f}%)")

        st.header("Who Are We Reviewing?")
        st.caption("PR authors outside your teams whose PRs your teams reviewed")

        all_team_members = set()
        for t in TEAMS:
            all_team_members.update(prs[prs["team_name"] == t]["author_login"].unique())

        external_reviews = team_reviews_ranged[
            ~team_reviews_ranged["pr_author"].isin(all_team_members)
        ].copy()

        if len(external_reviews) > 0:
            ext_by_author = (
                external_reviews
                .groupby("pr_author")
                .agg(
                    reviews=("review_key", "count"),
                    unique_prs=("pr_key", "nunique"),
                )
                .reset_index()
                .sort_values("reviews", ascending=False)
            )

            top_n = ext_by_author.head(15)
            fig = px.bar(
                top_n,
                x="pr_author",
                y="reviews",
                title="Top External Authors Reviewed by Your Teams",
                labels={"pr_author": "PR Author", "reviews": "Reviews"},
                text="unique_prs",
            )
            fig.update_traces(texttemplate="%{text} PRs", textposition="outside")
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

            with st.expander("Full table"):
                ext_by_author.columns = ["PR Author", "Reviews", "Unique PRs"]
                st.dataframe(ext_by_author, hide_index=True, use_container_width=True)
        else:
            st.info("No external reviews found in this date range.")

    # ── PR Size tab ────────────────────────────────────────────────────────

    with tab_pr_size:

        st.header("PR Size — Authored by Team")
        col1, col2 = st.columns(2)
        with col1:
            st.plotly_chart(
                volume_chart(
                    team_prs_ranged, "created_date", "files_changed",
                    color_col if not drill_down else "team_name",
                    "Files Changed (Daily Total)",
                ),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                volume_chart(
                    team_prs_ranged, "created_date", "total_lines_changed",
                    color_col if not drill_down else "team_name",
                    "Lines Changed (Daily Total)",
                ),
                use_container_width=True,
            )

        st.header("PR Size — Reviewed by Team")

        review_prs = (
            team_reviews_ranged
            .drop_duplicates(subset=["pr_key", "reviewer_login"])
            .dropna(subset=["files_changed"])
        )

        col3, col4 = st.columns(2)
        with col3:
            st.plotly_chart(
                volume_chart(
                    review_prs, "review_date", "files_changed",
                    review_color_col if not drill_down else "reviewer_team",
                    "Files Changed on Reviewed PRs (Daily Total)",
                ),
                use_container_width=True,
            )
        with col4:
            st.plotly_chart(
                volume_chart(
                    review_prs, "review_date", "total_lines_changed",
                    review_color_col if not drill_down else "reviewer_team",
                    "Lines Changed on Reviewed PRs (Daily Total)",
                ),
                use_container_width=True,
            )

    # ── Cycle Time tab ─────────────────────────────────────────────────────

    with tab_cycle_time:

        st.header("Task Cycle Time — In Progress → Done")
        st.caption(
            "For Jira issues linked to merged PRs: three-phase breakdown from "
            "In Progress → first PR opened → last PR merged → Done. "
            "Uses Jira changelog timestamps for accurate lifecycle tracking."
        )

        if not task_cycles.empty:
            tc = task_cycles[task_cycles["team_name"].isin(selected_teams)]
            tc = tc[
                (tc["done_date"] >= start_date) & (tc["done_date"] <= end_date)
            ]

            if not tc.empty:
                tc_single = tc[tc["pr_count"] == 1]
                tc_multi = tc[tc["pr_count"] > 1]

                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Linked Issues", f"{len(tc_single)} single-PR",
                           delta=f"{len(tc_multi)} multi-PR", delta_color="off")
                k2.metric("Median Pre-PR (hrs)",
                           f"{tc_single['pre_pr_hours'].median():.1f}",
                           delta=f"mean: {tc_single['pre_pr_hours'].mean():.1f}",
                           delta_color="off")
                k3.metric("Median PR Cycle (hrs)",
                           f"{tc_single['pr_cycle_hours'].median():.1f}",
                           delta=f"mean: {tc_single['pr_cycle_hours'].mean():.1f}",
                           delta_color="off")
                k4.metric("Median Total (hrs)",
                           f"{tc_single['total_cycle_hours'].median():.1f}",
                           delta=f"PR is {tc_single['pr_pct'].median():.0f}% of total",
                           delta_color="off")

                col_a, col_b = st.columns(2)

                with col_a:
                    agg = (
                        tc_single.groupby("authoring")
                        .agg(
                            issues=("issue_key", "count"),
                            med_pre_pr=("pre_pr_hours", "median"),
                            med_pr=("pr_cycle_hours", "median"),
                            med_post=("post_merge_hours", "median"),
                        )
                        .reset_index()
                    )
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=agg["authoring"], y=agg["med_pre_pr"],
                        name="Pre-PR (In Progress → first PR)",
                        marker_color="#19D3F3",
                        text=agg["med_pre_pr"].round(1),
                        textposition="inside",
                    ))
                    fig.add_trace(go.Bar(
                        x=agg["authoring"], y=agg["med_pr"],
                        name="PR Review (first PR → merged)",
                        marker_color="#AB63FA",
                        text=agg["med_pr"].round(1),
                        textposition="inside",
                    ))
                    fig.add_trace(go.Bar(
                        x=agg["authoring"], y=agg["med_post"],
                        name="Post-merge (merged → Done)",
                        marker_color="#636EFA",
                        text=agg["med_post"].round(1),
                        textposition="inside",
                    ))
                    fig.update_layout(
                        title="Median Cycle Breakdown (Single-PR Issues)",
                        barmode="stack",
                        yaxis_title="Hours",
                        legend=dict(orientation="h", y=-0.2),
                    )
                    st.plotly_chart(fig, use_container_width=True)

                with col_b:
                    fig2 = px.box(
                        tc, x="authoring", y="pr_pct",
                        color="authoring",
                        color_discrete_map=AI_COLORS,
                        title="PR Time as % of Total Cycle",
                        labels={"pr_pct": "% of Cycle in PR", "authoring": ""},
                    )
                    fig2.update_layout(showlegend=False)
                    st.plotly_chart(fig2, use_container_width=True)

                tc_monthly = tc_single.copy()
                tc_monthly["month"] = pd.to_datetime(tc_monthly["done_date"]).dt.to_period("M").dt.to_timestamp()
                monthly_agg = (
                    tc_monthly.groupby(["month", "authoring"])
                    .agg(
                        med_pr_hrs=("pr_cycle_hours", "median"),
                        med_total_hrs=("total_cycle_hours", "median"),
                        count=("issue_key", "count"),
                    )
                    .reset_index()
                )
                if len(monthly_agg["month"].unique()) > 1:
                    fig3 = px.line(
                        monthly_agg, x="month", y="med_pr_hrs",
                        color="authoring",
                        color_discrete_map=AI_COLORS,
                        markers=True,
                        title="Median PR Cycle Time by Month (Single-PR Issues)",
                        labels={"month": "Month", "med_pr_hrs": "Hours", "authoring": ""},
                    )
                    fig3.update_layout(
                        xaxis_tickformat="%b %Y",
                        legend=dict(orientation="h", y=-0.15),
                    )
                    st.plotly_chart(fig3, use_container_width=True)

                with st.expander("Raw data"):
                    display_cols = [
                        "issue_key", "authoring", "team_name", "pr_count",
                        "pre_pr_hours", "pr_cycle_hours", "post_merge_hours",
                        "total_cycle_hours", "pr_pct", "story_points", "sprint_name",
                    ]
                    show = tc[display_cols].copy()
                    for c in ("pre_pr_hours", "pr_cycle_hours", "post_merge_hours",
                              "total_cycle_hours", "pr_pct"):
                        show[c] = show[c].round(1)
                    show.columns = [
                        "Issue", "Authoring", "Team", "PRs",
                        "Pre-PR Hrs", "PR Hrs", "Post-merge Hrs",
                        "Total Hrs", "PR %", "Story Pts", "Sprint",
                    ]
                    st.dataframe(
                        show.sort_values("Total Hrs", ascending=False),
                        hide_index=True, use_container_width=True,
                    )
            else:
                st.info("No linked Jira issues found in this date range.")
        else:
            st.info(
                "No Jira data available. Run the Jira ETL with --backfill "
                "to load Jira issues linked to PRs."
            )

    # ── Support tab ────────────────────────────────────────────────────────

    with tab_support:
        st.header("Support Escalations")
        st.caption(
            "Tickets from the configured support project (e.g. ZTCE) scoped "
            "to the teams in `jira.support.teams`. Categorization is applied "
            "at query time from the shared `[[categories]]` patterns in config."
        )

        if support.empty:
            st.info(
                "No support ticket data yet. Make sure `[jira.support]` is "
                "configured in `config.toml` and run:\n\n"
                "```\npython3 jira_etl.py --since 2026-01-01\n```"
            )
        else:
            s_range = support[
                (support["created_date"] >= start_date) &
                (support["created_date"] <= end_date)
            ].copy()

            # ── Top-line metrics ─────────────────────────────────────────────
            open_now = support[support["is_open"]]
            resolved_range = s_range[~s_range["is_open"]]

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Tickets Created (range)", len(s_range))
            m2.metric("Resolved (range)", len(resolved_range))
            m3.metric("Currently Open (all time)", len(open_now))
            median_ttr = resolved_range["ttr_hours"].median()
            m4.metric(
                "Median TTR (hrs)",
                f"{median_ttr:.1f}" if pd.notna(median_ttr) else "—",
            )

            # ── Volume vs PR volume ──────────────────────────────────────────
            st.subheader("Ticket Volume vs PR Volume")
            st.caption("Daily counts — is there a correlation?")

            daily_tickets = (
                s_range.groupby("created_date").size()
                .reset_index(name="tickets")
            )
            daily_prs = (
                team_prs_ranged.groupby("created_date").size()
                .reset_index(name="prs")
            ) if not team_prs_ranged.empty else pd.DataFrame(columns=["created_date", "prs"])

            fig_vol = go.Figure()
            fig_vol.add_trace(go.Bar(
                x=daily_tickets["created_date"],
                y=daily_tickets["tickets"],
                name="Support Tickets",
                marker_color="#EF553B",
            ))
            if not daily_prs.empty:
                fig_vol.add_trace(go.Scatter(
                    x=daily_prs["created_date"],
                    y=daily_prs["prs"],
                    name="PRs (secondary axis)",
                    mode="lines+markers",
                    line=dict(color="#636EFA"),
                    yaxis="y2",
                ))
                fig_vol.update_layout(
                    yaxis2=dict(title="PRs", overlaying="y", side="right", showgrid=False),
                )
            fig_vol.update_layout(
                yaxis_title="Tickets",
                xaxis_tickformat="%b %d",
                legend=dict(orientation="h", y=-0.2),
                barmode="group",
            )
            st.plotly_chart(fig_vol, use_container_width=True)

            # Pearson correlation on aligned daily counts
            if not daily_prs.empty and len(daily_tickets) > 2:
                merged = pd.merge(
                    daily_tickets, daily_prs,
                    on="created_date", how="outer",
                ).fillna(0)
                if len(merged) >= 3:
                    corr = merged["tickets"].corr(merged["prs"])
                    st.caption(
                        f"Pearson correlation (daily): **{corr:.2f}** "
                        f"across {len(merged)} days in range"
                    )

            # ── Category breakdown ───────────────────────────────────────────
            col_a, col_b = st.columns(2)

            with col_a:
                st.subheader("By Category")
                cat_counts = (
                    s_range.groupby("category").size()
                    .reset_index(name="tickets")
                    .sort_values("tickets", ascending=True)
                )
                fig_cat = px.bar(
                    cat_counts, x="tickets", y="category",
                    orientation="h",
                    labels={"tickets": "Tickets", "category": ""},
                    title="Tickets by Category",
                )
                st.plotly_chart(fig_cat, use_container_width=True)

            with col_b:
                st.subheader("By Priority")
                # support_priority is cleaner than plain priority (P1/P2/P3)
                priority_df = s_range.copy()
                priority_df["priority_label"] = (
                    priority_df["support_priority"].fillna("Unassigned")
                )
                prio_counts = (
                    priority_df.groupby("priority_label").size()
                    .reset_index(name="tickets")
                    .sort_values("priority_label")
                )
                fig_prio = px.bar(
                    prio_counts, x="priority_label", y="tickets",
                    labels={"priority_label": "Priority", "tickets": "Tickets"},
                    title="Tickets by Escalation Priority",
                    color="priority_label",
                )
                fig_prio.update_layout(showlegend=False)
                st.plotly_chart(fig_prio, use_container_width=True)

            # ── TTR distribution ────────────────────────────────────────────
            st.subheader("Time-to-Resolution")
            resolved_valid = resolved_range[resolved_range["ttr_hours"] > 0]
            if not resolved_valid.empty:
                col_c, col_d = st.columns(2)
                with col_c:
                    fig_ttr = px.box(
                        resolved_valid, x="category", y="ttr_hours",
                        title="TTR by Category (hours)",
                        labels={"ttr_hours": "TTR (hrs)", "category": ""},
                    )
                    fig_ttr.update_layout(xaxis_tickangle=-30)
                    st.plotly_chart(fig_ttr, use_container_width=True)
                with col_d:
                    fig_ttr_hist = px.histogram(
                        resolved_valid, x="ttr_hours",
                        nbins=30,
                        title="TTR Distribution (all resolved)",
                        labels={"ttr_hours": "TTR (hrs)"},
                    )
                    st.plotly_chart(fig_ttr_hist, use_container_width=True)
            else:
                st.info("No resolved tickets in this date range.")

            # ── Open backlog age ────────────────────────────────────────────
            st.subheader("Open Ticket Age")
            if not open_now.empty:
                now_ts = pd.Timestamp.now(tz="UTC")
                open_df = open_now.copy()
                open_df["age_days"] = (
                    (now_ts - pd.to_datetime(open_df["created_at"], utc=True))
                    .dt.total_seconds() / 86400
                )
                open_df["age_bucket"] = pd.cut(
                    open_df["age_days"],
                    bins=[-1, 1, 7, 14, 30, 90, 10_000],
                    labels=["<1d", "1-7d", "8-14d", "15-30d", "31-90d", ">90d"],
                )
                age_counts = (
                    open_df.groupby("age_bucket", observed=True).size()
                    .reset_index(name="tickets")
                )
                fig_age = px.bar(
                    age_counts, x="age_bucket", y="tickets",
                    labels={"age_bucket": "Age", "tickets": "Open Tickets"},
                    title=f"Open Backlog Age ({len(open_df)} total)",
                )
                st.plotly_chart(fig_age, use_container_width=True)
            else:
                st.caption("No open tickets.")

            # ── Assignee load ───────────────────────────────────────────────
            st.subheader("Assignee Load (open tickets)")
            if not open_now.empty:
                load_df = (
                    open_now.groupby("assignee_display_name", dropna=False)
                    .size().reset_index(name="open_tickets")
                    .sort_values("open_tickets", ascending=False)
                )
                load_df["assignee_display_name"] = (
                    load_df["assignee_display_name"].fillna("Unassigned")
                )
                st.dataframe(
                    load_df.rename(columns={
                        "assignee_display_name": "Assignee",
                        "open_tickets": "Open Tickets",
                    }),
                    hide_index=True,
                    use_container_width=True,
                )

            # ── Comment count distribution ──────────────────────────────────
            st.subheader("Thread Size (Comment Count)")
            cc_df = s_range.dropna(subset=["comment_count"])
            if not cc_df.empty:
                fig_cc = px.histogram(
                    cc_df, x="comment_count",
                    nbins=20,
                    title=f"Comment Count per Ticket (median: "
                          f"{cc_df['comment_count'].median():.0f})",
                    labels={"comment_count": "Comments"},
                )
                st.plotly_chart(fig_cc, use_container_width=True)

            # ── Webex panels ────────────────────────────────────────────────
            if not webex.empty:
                st.divider()
                st.header("Help Channel Activity (Webex)")
                st.caption(
                    "Messages from the configured Webex support room. "
                    "Top-level messages are treated as 'asks'; their "
                    "replies become thread metadata."
                )

                wx_range = webex[
                    (webex["created_date"] >= start_date) &
                    (webex["created_date"] <= end_date)
                ].copy()

                top_level = wx_range[wx_range["is_top_level"]].copy()
                replies = wx_range[~wx_range["is_top_level"]].copy()

                # Compute thread size (reply counts) and time-to-first-reply
                # from the full loaded dataset so we don't miss replies that
                # landed outside the visible date range.
                all_replies = webex[~webex["is_top_level"]].copy()
                thread_agg = (
                    all_replies.groupby("parent_message_id")
                    .agg(
                        reply_count=("message_id", "count"),
                        first_reply=("created_at", "min"),
                    )
                    .reset_index()
                    .rename(columns={"parent_message_id": "message_id"})
                )
                top_level = top_level.merge(
                    thread_agg, on="message_id", how="left"
                )
                top_level["reply_count"] = top_level["reply_count"].fillna(0).astype(int)
                top_level["ttfr_minutes"] = (
                    (top_level["first_reply"] - top_level["created_at"])
                    .dt.total_seconds() / 60
                )

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Asks (top-level)", len(top_level))
                m2.metric("Total messages", len(wx_range))
                median_thread = top_level["reply_count"].median()
                m3.metric(
                    "Median replies/ask",
                    f"{median_thread:.0f}" if pd.notna(median_thread) else "—",
                )
                median_ttfr = top_level["ttfr_minutes"].dropna().median()
                m4.metric(
                    "Median time-to-first-reply",
                    f"{median_ttfr:.0f} min" if pd.notna(median_ttfr) else "—",
                )

                st.subheader("Daily Volume")
                vol_daily = (
                    wx_range.assign(
                        kind=wx_range["is_top_level"].map(
                            {True: "Asks", False: "Replies"}
                        )
                    )
                    .groupby(["created_date", "kind"]).size()
                    .reset_index(name="count")
                )
                fig_wx_vol = px.bar(
                    vol_daily, x="created_date", y="count", color="kind",
                    title="Messages per Day",
                    labels={"created_date": "Date", "count": "Messages", "kind": ""},
                    color_discrete_map={"Asks": "#EF553B", "Replies": "#636EFA"},
                    barmode="stack",
                )
                fig_wx_vol.update_layout(
                    xaxis_tickformat="%b %d",
                    legend=dict(orientation="h", y=-0.2),
                )
                st.plotly_chart(fig_wx_vol, use_container_width=True)

                col_a, col_b = st.columns(2)

                with col_a:
                    st.subheader("Asks by Category")
                    cat_counts = (
                        top_level.groupby("category").size()
                        .reset_index(name="asks")
                        .sort_values("asks", ascending=True)
                    )
                    fig_wx_cat = px.bar(
                        cat_counts, x="asks", y="category", orientation="h",
                        labels={"asks": "Asks", "category": ""},
                    )
                    st.plotly_chart(fig_wx_cat, use_container_width=True)

                with col_b:
                    st.subheader("Asks by Asker Team")
                    team_counts = (
                        top_level.groupby("team_name").size()
                        .reset_index(name="asks")
                        .sort_values("asks", ascending=False)
                    )
                    fig_wx_team = px.bar(
                        team_counts, x="team_name", y="asks",
                        color="team_name",
                        color_discrete_map={**COLORS, "Other": "#FFA15A"},
                        labels={"team_name": "", "asks": "Asks"},
                    )
                    fig_wx_team.update_layout(showlegend=False)
                    st.plotly_chart(fig_wx_team, use_container_width=True)

                col_c, col_d = st.columns(2)

                with col_c:
                    st.subheader("Thread Size Distribution")
                    fig_ts = px.histogram(
                        top_level, x="reply_count", nbins=25,
                        title=f"Replies per Ask (max: {top_level['reply_count'].max()})",
                        labels={"reply_count": "Replies"},
                    )
                    st.plotly_chart(fig_ts, use_container_width=True)

                with col_d:
                    st.subheader("Time-to-First-Reply")
                    ttfr_valid = top_level.dropna(subset=["ttfr_minutes"])
                    ttfr_valid = ttfr_valid[ttfr_valid["ttfr_minutes"] < 24 * 60]
                    if not ttfr_valid.empty:
                        fig_ttfr = px.histogram(
                            ttfr_valid, x="ttfr_minutes", nbins=30,
                            title="TTFR Distribution (under 24h)",
                            labels={"ttfr_minutes": "Minutes"},
                        )
                        st.plotly_chart(fig_ttfr, use_container_width=True)
                    else:
                        st.info("No replied-to asks in this range.")

                st.subheader("Top Askers (External)")
                st.caption("People asking for help who are not on your teams.")
                external = top_level[top_level["team_name"] == "Other"]
                if not external.empty:
                    ext_counts = (
                        external.groupby("person_email").size()
                        .reset_index(name="asks")
                        .sort_values("asks", ascending=False)
                        .head(20)
                    )
                    st.dataframe(
                        ext_counts.rename(columns={
                            "person_email": "Email", "asks": "Asks",
                        }),
                        hide_index=True,
                        use_container_width=True,
                    )

                with st.expander("Raw asks (top-level messages)"):
                    show_cols = top_level[[
                        "created_at", "person_email", "team_name",
                        "category", "reply_count", "ttfr_minutes", "text",
                    ]].copy()
                    show_cols["ttfr_minutes"] = show_cols["ttfr_minutes"].round(1)
                    show_cols.columns = [
                        "Timestamp", "Asker", "Team", "Category",
                        "Replies", "TTFR (min)", "Text",
                    ]
                    st.dataframe(
                        show_cols.sort_values("Timestamp", ascending=False),
                        hide_index=True, use_container_width=True,
                    )

            # ── Raw table ───────────────────────────────────────────────────
            with st.expander("Raw tickets"):
                show = s_range[[
                    "issue_key", "summary", "category", "status",
                    "support_priority", "support_severity",
                    "assignee_display_name", "comment_count",
                    "created_date", "resolved_date", "ttr_hours",
                ]].copy()
                show["ttr_hours"] = show["ttr_hours"].round(1)
                show.columns = [
                    "Key", "Summary", "Category", "Status",
                    "Priority", "Severity", "Assignee", "Comments",
                    "Created", "Resolved", "TTR (hrs)",
                ]
                st.dataframe(
                    show.sort_values("Created", ascending=False),
                    hide_index=True, use_container_width=True,
                )

    # ── On-Call tab ────────────────────────────────────────────────────────

    with tab_oncall:
        st.header("On-Call (VictorOps)")
        st.caption(
            "Incidents paged to the configured team. **Impacting** means a "
            "team member was in pagedUsers OR a team member acked/resolved "
            "the page. Test incidents are filtered out at ETL time."
        )

        if vo.empty:
            st.info(
                "No VictorOps data yet. Run:\n\n"
                "```\npython3 victorops_etl.py --since 2026-01-01\n```"
            )
        else:
            vo_range = vo[
                (vo["started_date"] >= start_date) &
                (vo["started_date"] <= end_date)
            ].copy()

            impact = vo_range[vo_range["impacted_us"]].copy()
            noise = vo_range[~vo_range["impacted_us"]].copy()

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Impacting incidents", len(impact))
            m2.metric("Noise (escalation only)", len(noise))
            median_mtta = impact["mtta_minutes"].dropna().median()
            m3.metric(
                "Median MTTA",
                f"{median_mtta:.1f} min" if pd.notna(median_mtta) else "—",
            )
            median_mttr = impact["mttr_minutes"].dropna().median()
            m4.metric(
                "Median MTTR",
                f"{median_mttr:.1f} min" if pd.notna(median_mttr) else "—",
            )

            st.subheader("Daily Volume — Impacting vs Noise")
            vol_daily = (
                vo_range.assign(
                    kind=vo_range["impacted_us"].map(
                        {True: "Impacting", False: "Noise"}
                    )
                )
                .groupby(["started_date", "kind"]).size()
                .reset_index(name="incidents")
            )
            fig_vol = px.bar(
                vol_daily, x="started_date", y="incidents", color="kind",
                color_discrete_map={"Impacting": "#EF553B", "Noise": "#CCCCCC"},
                labels={"started_date": "Date", "incidents": "Incidents", "kind": ""},
                title="Paged per Day",
                barmode="stack",
            )
            fig_vol.update_layout(
                xaxis_tickformat="%b %d",
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig_vol, use_container_width=True)

            col_a, col_b = st.columns(2)

            with col_a:
                st.subheader("MTTA Distribution (impacting)")
                mtta_valid = impact.dropna(subset=["mtta_minutes"])
                mtta_valid = mtta_valid[mtta_valid["mtta_minutes"] < 120]
                if not mtta_valid.empty:
                    fig_mtta = px.histogram(
                        mtta_valid, x="mtta_minutes", nbins=24,
                        title="Time to Acknowledge (< 2 hours)",
                        labels={"mtta_minutes": "Minutes"},
                    )
                    st.plotly_chart(fig_mtta, use_container_width=True)
                else:
                    st.info("No acked incidents in range.")

            with col_b:
                st.subheader("MTTR Distribution (impacting)")
                mttr_valid = impact.dropna(subset=["mttr_minutes"])
                mttr_valid = mttr_valid[mttr_valid["mttr_minutes"] < 60 * 8]
                if not mttr_valid.empty:
                    fig_mttr = px.histogram(
                        mttr_valid, x="mttr_minutes", nbins=24,
                        title="Time to Resolve (< 8 hours)",
                        labels={"mttr_minutes": "Minutes"},
                    )
                    st.plotly_chart(fig_mttr, use_container_width=True)
                else:
                    st.info("No resolved incidents in range.")

            st.subheader("Responder Load")
            st.caption("Team members who acked or resolved an impacting incident.")
            responders = pd.concat([
                impact[["acker"]].rename(columns={"acker": "user"}).assign(action="ack"),
                impact[["resolver"]].rename(columns={"resolver": "user"}).assign(action="resolve"),
            ])
            responders = responders.dropna(subset=["user"])
            if not responders.empty:
                rl = (
                    responders.groupby(["user", "action"]).size()
                    .reset_index(name="count")
                )
                fig_rl = px.bar(
                    rl, x="user", y="count", color="action",
                    barmode="stack",
                    labels={"user": "Responder", "count": "Incidents", "action": ""},
                )
                fig_rl.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig_rl, use_container_width=True)

            st.subheader("Noise Trend")
            st.caption(
                "Policy-only escalations — incidents that named the team but "
                "no individual was paged and no team member responded. Growth "
                "here signals noisy tooling."
            )
            if not noise.empty:
                noise_daily = (
                    noise.groupby("started_date").size()
                    .reset_index(name="incidents")
                )
                noise_daily = noise_daily.sort_values("started_date")
                fig_noise = px.bar(
                    noise_daily, x="started_date", y="incidents",
                    labels={"started_date": "Date", "incidents": "Noise Incidents"},
                    title=f"Noise Floor ({len(noise)} total)",
                )
                # Overlay a linear trend
                if len(noise_daily) >= 2:
                    x_num = (
                        pd.to_datetime(noise_daily["started_date"]) -
                        pd.to_datetime(noise_daily["started_date"].iloc[0])
                    ).dt.days.values.astype(float)
                    coeffs = np.polyfit(x_num, noise_daily["incidents"].values, 1)
                    trend_y = np.polyval(coeffs, x_num)
                    fig_noise.add_trace(go.Scatter(
                        x=noise_daily["started_date"], y=trend_y,
                        mode="lines", name="Trend",
                        line=dict(color="#888", width=2, dash="dash"),
                    ))
                fig_noise.update_layout(xaxis_tickformat="%b %d")
                st.plotly_chart(fig_noise, use_container_width=True)

                st.caption("Top noise routing keys:")
                noise_rk = (
                    noise.groupby("routing_key").size()
                    .reset_index(name="incidents")
                    .sort_values("incidents", ascending=False)
                    .head(10)
                )
                st.dataframe(
                    noise_rk.rename(columns={
                        "routing_key": "Routing Key", "incidents": "Incidents",
                    }),
                    hide_index=True, use_container_width=True,
                )

            with st.expander("Impacting incidents — raw"):
                show = impact[[
                    "incident_number", "service", "started_at",
                    "acker", "resolver", "mtta_minutes", "mttr_minutes",
                    "routing_key", "current_phase",
                ]].copy()
                show["mtta_minutes"] = show["mtta_minutes"].round(1)
                show["mttr_minutes"] = show["mttr_minutes"].round(1)
                show.columns = [
                    "#", "Service", "Started", "Acker", "Resolver",
                    "MTTA (min)", "MTTR (min)", "Routing Key", "Phase",
                ]
                st.dataframe(
                    show.sort_values("Started", ascending=False),
                    hide_index=True, use_container_width=True,
                )

    # ── Incidents tab (FireHydrant + VictorOps, team-scoped) ──────────────

    with tab_incidents:
        st.header("Incidents (team-scoped)")
        st.caption(
            "FireHydrant incident records plus VictorOps pages, filtered to "
            "incidents relevant to your teams. An incident qualifies if ANY "
            "of the three scope rules match (routing key, team-member "
            "involvement, or keyword). Rules are configured in "
            "`config.toml` under `[incidents]`."
        )

        from lib.config import get_incidents_scope
        scope_cfg = get_incidents_scope()

        with st.expander("Scope filters", expanded=True):
            col1, col2, col3 = st.columns(3)
            with col1:
                apply_routing = st.checkbox(
                    "🔀 Routing key (VO)",
                    value=True,
                    help=(
                        f"VO routing_key in: "
                        f"{', '.join(scope_cfg['routing_keys']) or '— none configured'}"
                    ),
                )
            with col2:
                apply_member = st.checkbox(
                    "👤 Team member involved",
                    value=True,
                    help=(
                        f"VO paged_users/acker/resolver matches one of "
                        f"{len(scope_cfg['vo_usernames'])} VO usernames, or "
                        f"an FH role is held by one of "
                        f"{len(scope_cfg['member_emails'])} emails."
                    ),
                )
            with col3:
                apply_keyword = st.checkbox(
                    "🔎 Keyword match",
                    value=True,
                    help=(
                        f"{len(scope_cfg['keywords'])} keywords matched "
                        f"substring against VO service / FH name + summary."
                    ),
                )
            st.caption(
                "All three rules default ON. A row matches if any enabled "
                "rule fires. Turn rules off to see how much each one "
                "contributes."
            )

        # ── Helpers to apply rules ────────────────────────────────────────
        def _qualifies(row):
            if apply_routing and row.get("match_routing"):
                return True
            if apply_member and row.get("match_member"):
                return True
            if apply_keyword and row.get("match_keyword"):
                return True
            return False

        def _badges(row):
            parts = []
            if apply_member and row.get("match_member"):
                parts.append("👤")
            if apply_routing and row.get("match_routing"):
                parts.append("🔀")
            if apply_keyword and row.get("match_keyword"):
                parts.append("🔎")
            return "".join(parts)

        if fh.empty and vo.empty:
            st.info(
                "No VO or FH data yet. Run:\n\n"
                "```\npython3 firehydrant_etl.py --since 2026-01-01\n"
                "python3 victorops_etl.py --since 2026-01-01\n```"
            )
        elif fh.empty:
            st.info(
                "No FireHydrant data yet. Run:\n\n"
                "```\npython3 firehydrant_etl.py --since 2026-01-01\n```"
            )
        else:
            fh_range_all = fh[
                (fh["started_date"] >= start_date) &
                (fh["started_date"] <= end_date)
            ].copy()

            # Apply scope filter to FH rows
            if fh_range_all.empty:
                fh_range = fh_range_all
            else:
                fh_mask = fh_range_all.apply(_qualifies, axis=1)
                fh_range = fh_range_all[fh_mask].copy()

            # Apply same filter to VO rows for a combined view
            if vo.empty:
                vo_range = pd.DataFrame()
            else:
                vo_range_all = vo[
                    (vo["started_date"] >= start_date) &
                    (vo["started_date"] <= end_date)
                ].copy()
                if vo_range_all.empty:
                    vo_range = vo_range_all
                else:
                    vo_mask = vo_range_all.apply(_qualifies, axis=1)
                    vo_range = vo_range_all[vo_mask].copy()

            # ── Scope summary: how did we get here? ──────────────────────
            def _exclusive_counts(df_scoped):
                """Return (member_only, routing_only, keyword_only, overlap)
                based on the ENABLED rules. Used to explain the filtered set.
                """
                if df_scoped.empty:
                    return 0, 0, 0, 0
                m = df_scoped["match_member"] & apply_member
                r = df_scoped["match_routing"] & apply_routing
                k = df_scoped["match_keyword"] & apply_keyword
                member_only = (m & ~r & ~k).sum()
                routing_only = (~m & r & ~k).sum()
                keyword_only = (~m & ~r & k).sum()
                overlap = (m.astype(int) + r.astype(int) + k.astype(int) >= 2).sum()
                return int(member_only), int(routing_only), int(keyword_only), int(overlap)

            fh_mo, fh_ro, fh_ko, fh_ov = _exclusive_counts(fh_range)
            vo_mo, vo_ro, vo_ko, vo_ov = _exclusive_counts(vo_range)

            s1, s2, s3, s4 = st.columns(4)
            s1.metric("FH in scope", len(fh_range))
            s2.metric(
                "VO in scope", len(vo_range) if not vo_range.empty else 0,
            )
            s3.metric(
                "Keyword-only (not involved)",
                f"FH {fh_ko} · VO {vo_ko}",
                help=(
                    "Incidents matching a keyword but NOT a team member. "
                    "Signals incidents in your domain where your team "
                    "wasn't pulled in."
                ),
            )
            s4.metric(
                "Routing-only (VO, not involved)",
                vo_ro,
                help=(
                    "VO pages on a team routing key where no team member "
                    "was paged or responded."
                ),
            )

            st.subheader("FireHydrant — filtered")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Incidents", len(fh_range))
            sev_real = fh_range[fh_range["severity"].isin(["S1", "S2", "S3", "S4"])]
            m2.metric("Real (S1–S4)", len(sev_real))
            m_ttr = fh_range["time_to_resolution_min"].dropna().median()
            m3.metric(
                "Median TTR",
                f"{m_ttr:.0f} min" if pd.notna(m_ttr) else "—",
            )
            m_tta = fh_range["time_to_acknowledge_min"].dropna().median()
            m4.metric(
                "Median TTA",
                f"{m_tta:.0f} min" if pd.notna(m_tta) else "—",
            )

            if fh_range.empty:
                st.info(
                    "No FireHydrant incidents match the current scope "
                    "filters. Adjust the filters above or widen the "
                    "sidebar date range. (VO-scoped view below may still "
                    "have data.)"
                )

            st.subheader("Incidents by Severity over Time")
            daily_sev = (
                fh_range.groupby(["started_date", "severity"]).size()
                .reset_index(name="incidents")
            )
            # Assign colors — S1/S2 red family, S3/S4 blue, drills gray
            sev_colors = {
                "S1": "#B00020",
                "S2": "#EF553B",
                "S3": "#FFA15A",
                "S4": "#FFE168",
                "QR": "#19D3F3",
                "GAMEDAY": "#AAAAAA",
                "PERFORMANCE-GATING": "#CCCCCC",
                "FIRE-DRILL-SSO": "#888888",
            }
            fig_sev = px.bar(
                daily_sev, x="started_date", y="incidents", color="severity",
                color_discrete_map=sev_colors,
                labels={"started_date": "Date", "incidents": "Incidents", "severity": ""},
                title="Daily Incident Volume",
                barmode="stack",
            )
            fig_sev.update_layout(
                xaxis_tickformat="%b %d",
                legend=dict(orientation="h", y=-0.2),
            )
            st.plotly_chart(fig_sev, use_container_width=True)

            col_a, col_b = st.columns(2)

            with col_a:
                st.subheader("Severity Mix")
                sev_counts = (
                    fh_range.groupby("severity").size()
                    .reset_index(name="incidents")
                    .sort_values("incidents", ascending=False)
                )
                fig_mix = px.bar(
                    sev_counts, x="severity", y="incidents",
                    color="severity", color_discrete_map=sev_colors,
                    labels={"severity": "", "incidents": "Incidents"},
                )
                fig_mix.update_layout(showlegend=False)
                st.plotly_chart(fig_mix, use_container_width=True)

            with col_b:
                st.subheader("Current Milestone")
                ms_counts = (
                    fh_range.groupby("current_milestone").size()
                    .reset_index(name="incidents")
                    .sort_values("incidents", ascending=False)
                )
                fig_ms = px.bar(
                    ms_counts, x="current_milestone", y="incidents",
                    labels={"current_milestone": "", "incidents": "Incidents"},
                )
                st.plotly_chart(fig_ms, use_container_width=True)

            st.subheader("Lifecycle Measurement Distributions")
            st.caption(
                "Median and P90 across each lifecycle transition for "
                "incidents started in the selected range."
            )
            lifecycle_cols = [
                ("time_to_detect_min", "Detect"),
                ("time_to_acknowledge_min", "Acknowledge"),
                ("time_to_mitigation_min", "Mitigate"),
                ("time_to_resolution_min", "Resolve"),
            ]
            stats = []
            for col, label in lifecycle_cols:
                vals = fh_range[col].dropna()
                if len(vals):
                    stats.append({
                        "Phase": label,
                        "Count": len(vals),
                        "Median (min)": round(vals.median(), 1),
                        "P90 (min)": round(vals.quantile(0.9), 1),
                        "Max (min)": round(vals.max(), 1),
                    })
            if stats:
                st.dataframe(pd.DataFrame(stats), hide_index=True, use_container_width=True)

            # Long-form box plot for each phase (real incidents only)
            real_only = fh_range[fh_range["severity"].isin(["S1", "S2", "S3", "S4"])]
            if not real_only.empty:
                phase_long = pd.concat([
                    real_only[[col]].rename(columns={col: "minutes"}).assign(Phase=label)
                    for col, label in lifecycle_cols
                ]).dropna(subset=["minutes"])
                # Cap very long tails for readability (> 24h)
                phase_long = phase_long[phase_long["minutes"] < 24 * 60]
                if not phase_long.empty:
                    fig_box = px.box(
                        phase_long, x="Phase", y="minutes",
                        category_orders={"Phase": [lbl for _, lbl in lifecycle_cols]},
                        labels={"minutes": "Minutes"},
                        title="Lifecycle Timing — S1–S4 only, capped at 24h",
                    )
                    st.plotly_chart(fig_box, use_container_width=True)

            st.subheader("Incident Commander Leaderboard")
            st.caption(
                "Number of incidents where each person held the IC role. "
                "Same incident can have multiple IC assignments (handoff)."
            )
            # Scope roles to incidents in the current range
            ids_in_range = set(fh_range["incident_id"])
            roles_in_range = fh_roles[fh_roles["incident_id"].isin(ids_in_range)]
            ic = roles_in_range[roles_in_range["role_name"] == "Incident Commander"]
            if not ic.empty:
                ic_counts = (
                    ic.groupby("user_name").size()
                    .reset_index(name="incidents")
                    .sort_values("incidents", ascending=False)
                    .head(15)
                )
                fig_ic = px.bar(
                    ic_counts, x="user_name", y="incidents",
                    labels={"user_name": "IC", "incidents": "Incidents"},
                )
                fig_ic.update_layout(xaxis_tickangle=-30)
                st.plotly_chart(fig_ic, use_container_width=True)

            st.subheader("All Roles Breakdown")
            if not roles_in_range.empty:
                role_counts = (
                    roles_in_range.groupby(["role_name", "user_name"])
                    .size().reset_index(name="incidents")
                    .sort_values("incidents", ascending=False)
                )
                st.dataframe(
                    role_counts.rename(columns={
                        "role_name": "Role", "user_name": "User",
                        "incidents": "Incidents",
                    }),
                    hide_index=True, use_container_width=True,
                )

            with st.expander("FireHydrant — raw incidents (filtered)"):
                if fh_range.empty:
                    st.info("No FH incidents match the current scope filters.")
                else:
                    show = fh_range[[
                        "number", "name", "severity", "current_milestone",
                        "started_at", "time_to_acknowledge_min",
                        "time_to_mitigation_min", "time_to_resolution_min",
                    ]].copy()
                    show["Match"] = fh_range.apply(_badges, axis=1).values
                    for c in (
                        "time_to_acknowledge_min", "time_to_mitigation_min",
                        "time_to_resolution_min",
                    ):
                        show[c] = show[c].round(1)
                    show.columns = [
                        "#", "Name", "Severity", "Milestone", "Started",
                        "TTA (min)", "TTM (min)", "TTR (min)", "Match",
                    ]
                    st.dataframe(
                        show.sort_values("Started", ascending=False),
                        hide_index=True, use_container_width=True,
                    )

            # ── VictorOps view (scope-filtered) ──────────────────────────
            if not vo_range.empty:
                st.subheader("VictorOps — filtered pages")
                st.caption(
                    "VO pages matching the same scope filters. Useful "
                    "context because most real incidents start as a VO "
                    "page before a FireHydrant record is opened."
                )
                v1, v2, v3, v4 = st.columns(4)
                v1.metric("Pages", len(vo_range))
                v_mtta = vo_range["mtta_minutes"].dropna().median()
                v2.metric(
                    "Median MTTA",
                    f"{v_mtta:.1f} min" if pd.notna(v_mtta) else "—",
                )
                v_mttr = vo_range["mttr_minutes"].dropna().median()
                v3.metric(
                    "Median MTTR",
                    f"{v_mttr:.1f} min" if pd.notna(v_mttr) else "—",
                )
                top_rk = (
                    vo_range.groupby("routing_key").size()
                    .reset_index(name="n")
                    .sort_values("n", ascending=False)
                )
                v4.metric(
                    "Top routing key",
                    top_rk.iloc[0]["routing_key"] if not top_rk.empty else "—",
                    help=(
                        f"{int(top_rk.iloc[0]['n'])} pages"
                        if not top_rk.empty else ""
                    ),
                )

                with st.expander("VictorOps — raw pages (filtered)"):
                    show_vo = vo_range[[
                        "incident_number", "service", "routing_key",
                        "started_at", "acker", "resolver",
                        "mtta_minutes", "mttr_minutes", "current_phase",
                    ]].copy()
                    show_vo["Match"] = vo_range.apply(_badges, axis=1).values
                    show_vo["mtta_minutes"] = show_vo["mtta_minutes"].round(1)
                    show_vo["mttr_minutes"] = show_vo["mttr_minutes"].round(1)
                    show_vo.columns = [
                        "#", "Service", "Routing Key", "Started",
                        "Acker", "Resolver", "MTTA (min)", "MTTR (min)",
                        "Phase", "Match",
                    ]
                    st.dataframe(
                        show_vo.sort_values("Started", ascending=False),
                        hide_index=True, use_container_width=True,
                    )

    # ── Throughput vs Load tab ─────────────────────────────────────────────

    with tab_tvl:
        st.header("Throughput vs Support Load")
        st.caption(
            "Does our team's output drive the support load we see? "
            "Throughput = merged PRs + Jira issues transitioned to Done "
            "(scoped to issues with a linked PR from our teams). "
            "Support load = ZTCE tickets + Webex asks + FireHydrant "
            "S1–S4 incidents scoped via the Incidents-tab rules "
            "(team-member role OR keyword match). A lag peak in the "
            "correlation chart would suggest the relationship is real; "
            "a flat chart means it isn't."
        )

        tvl = load_throughput_load_daily()
        if tvl.empty:
            st.info("Not enough data yet — run the ETLs first.")
        else:
            tvl_range = tvl[
                (tvl["d"] >= start_date) & (tvl["d"] <= end_date)
            ].copy()

            if tvl_range.empty:
                st.info("No data in the selected date range.")
            else:
                # ── Weekly rollup, indexed ──────────────────────────────
                tvl_weekly = tvl_range.copy()
                tvl_weekly["week"] = (
                    pd.to_datetime(tvl_weekly["d"])
                    .dt.to_period("W-MON").dt.start_time
                )
                weekly = tvl_weekly.groupby("week").agg({
                    "merged_prs": "sum",
                    "jira_done": "sum",
                    "ztce_created": "sum",
                    "webex_asks": "sum",
                    "fh_real": "sum",
                    "throughput": "sum",
                    "support_load": "sum",
                }).reset_index()

                # Index both composites to 100 at their mean so they're
                # visually comparable on one axis.
                def _indexed(series):
                    m = series.mean()
                    return (series / m * 100) if m else series

                weekly["throughput_idx"] = _indexed(weekly["throughput"])
                weekly["support_idx"] = _indexed(weekly["support_load"])

                st.subheader("Weekly Indexed Trend")
                st.caption(
                    "Both series indexed to 100 at their own mean. "
                    "Values above 100 mean a busier-than-average week."
                )
                fig_idx = go.Figure()
                fig_idx.add_trace(go.Scatter(
                    x=weekly["week"], y=weekly["throughput_idx"],
                    mode="lines+markers", name="Throughput (PRs + Jira done)",
                    line=dict(color="#636EFA", width=2),
                ))
                fig_idx.add_trace(go.Scatter(
                    x=weekly["week"], y=weekly["support_idx"],
                    mode="lines+markers", name="Support load (ZTCE + Webex + FH)",
                    line=dict(color="#EF553B", width=2),
                ))
                fig_idx.add_hline(y=100, line_dash="dot", line_color="#888",
                                  annotation_text="avg")
                fig_idx.update_layout(
                    yaxis_title="Indexed (100 = average)",
                    xaxis_tickformat="%b %d",
                    legend=dict(orientation="h", y=-0.2),
                )
                st.plotly_chart(fig_idx, use_container_width=True)

                st.subheader("Weekly Totals (raw)")
                st.caption("Absolute counts behind the indexed view above.")
                disp = weekly[[
                    "week", "merged_prs", "jira_done",
                    "throughput", "ztce_created", "webex_asks", "fh_real",
                    "support_load",
                ]].copy()
                disp["week"] = pd.to_datetime(disp["week"]).dt.strftime("%Y-%m-%d")
                disp.columns = [
                    "Week", "Merged PRs", "Jira Done", "Throughput",
                    "ZTCE", "Webex Asks", "FH S1-S4", "Support Load",
                ]
                st.dataframe(disp, hide_index=True, use_container_width=True)

                # ── Lag correlation (weekly) ──────────────────────────────
                st.subheader("Lag Correlation (weekly)")
                st.caption(
                    "For each lag from 0 to 4 weeks, Pearson correlation "
                    "between weekly throughput and weekly support load "
                    "shifted forward by that lag. Weekly aggregation "
                    "avoids the weekend-zero artifact that inflates daily "
                    "correlation on workweek-patterned data."
                )
                corrs = []
                for lag in range(0, 5):
                    support_shifted = weekly["support_load"].shift(-lag)
                    paired = pd.concat(
                        [weekly["throughput"], support_shifted], axis=1
                    ).dropna()
                    if len(paired) < 4:
                        continue
                    r = paired["throughput"].corr(paired.iloc[:, 1])
                    corrs.append({"lag_weeks": lag, "correlation": r, "n": len(paired)})
                corrs_df = pd.DataFrame(corrs)

                if not corrs_df.empty:
                    peak = corrs_df.loc[corrs_df["correlation"].idxmax()]
                    fig_corr = px.bar(
                        corrs_df, x="lag_weeks", y="correlation",
                        labels={"lag_weeks": "Lag (weeks)", "correlation": "Pearson r"},
                        title=(
                            f"Lag correlation — peak at {int(peak['lag_weeks'])}w "
                            f"(r={peak['correlation']:.2f}, "
                            f"n={int(peak['n'])} weeks)"
                        ),
                        text=corrs_df["correlation"].round(2),
                    )
                    fig_corr.update_traces(textposition="outside")
                    fig_corr.add_hline(y=0, line_color="#888", line_dash="dot")
                    fig_corr.add_hline(y=0.3, line_color="#FFA15A", line_dash="dot",
                                       annotation_text="r=0.3 (weak)")
                    fig_corr.add_hline(y=0.5, line_color="#EF553B", line_dash="dot",
                                       annotation_text="r=0.5 (meaningful)")
                    fig_corr.update_layout(yaxis_range=[-1, 1])
                    st.plotly_chart(fig_corr, use_container_width=True)

                    n_weeks = len(weekly)
                    if n_weeks < 12:
                        st.warning(
                            f"Only {n_weeks} weeks of data in range — "
                            f"too few for a confident conclusion. "
                            f"Widen the date range for more robust signal."
                        )
                    elif peak["correlation"] < 0.3:
                        st.warning(
                            "Peak correlation is weak (r < 0.3). The "
                            "data does not support a throughput → "
                            "support-load relationship in this range."
                        )
                    elif peak["correlation"] < 0.5:
                        st.info(
                            "Modest correlation. Suggestive but "
                            "inconclusive — gather more history."
                        )
                    else:
                        st.success(
                            f"Meaningful correlation (r={peak['correlation']:.2f}) "
                            f"at {int(peak['lag_weeks'])}-week lag."
                        )

                # ── Ratio over time ────────────────────────────────────
                st.subheader("Support / Throughput Ratio (weekly)")
                st.caption(
                    "Support load divided by throughput, week over week. "
                    "A climbing ratio would mean you're delivering fewer "
                    "units of work per unit of incoming support — either "
                    "throughput is flagging or quality is declining."
                )
                ratio = weekly[weekly["throughput"] > 0].copy()
                ratio["ratio"] = ratio["support_load"] / ratio["throughput"]
                if not ratio.empty:
                    fig_ratio = px.bar(
                        ratio, x="week", y="ratio",
                        labels={"week": "Week", "ratio": "Support / Throughput"},
                    )
                    # Trend line
                    if len(ratio) >= 2:
                        xn = (
                            pd.to_datetime(ratio["week"]) -
                            pd.to_datetime(ratio["week"].iloc[0])
                        ).dt.days.values.astype(float)
                        coeffs = np.polyfit(xn, ratio["ratio"].values, 1)
                        fig_ratio.add_trace(go.Scatter(
                            x=ratio["week"],
                            y=np.polyval(coeffs, xn),
                            mode="lines", name="Trend",
                            line=dict(color="#888", width=2, dash="dash"),
                        ))
                    fig_ratio.update_layout(xaxis_tickformat="%b %d")
                    st.plotly_chart(fig_ratio, use_container_width=True)

    # ── Drill-down tables ───────────────────────────────────────────────────

    if drill_down:
        st.header("Individual Breakdown")

        tab1, tab2 = st.tabs(["By Author", "By Reviewer"])

        with tab1:
            author_stats = (
                team_prs_ranged
                .groupby(["author_login", "team_name"])
                .agg(
                    prs_created=("pr_key", "count"),
                    avg_files=("files_changed", "mean"),
                    avg_loc=("total_lines_changed", "mean"),
                    avg_hrs_to_merge=("hours_to_merge", "mean"),
                    ai_coauthor_pct=("has_ai_coauthor", "mean"),
                )
                .reset_index()
                .sort_values("prs_created", ascending=False)
            )
            author_stats["avg_files"] = author_stats["avg_files"].round(1)
            author_stats["avg_loc"] = author_stats["avg_loc"].round(0).astype(int)
            author_stats["avg_hrs_to_merge"] = author_stats["avg_hrs_to_merge"].round(1)
            author_stats["ai_coauthor_pct"] = (author_stats["ai_coauthor_pct"] * 100).round(0).astype(int).astype(str) + "%"
            author_stats.columns = [
                "Author", "Team", "PRs Created", "Avg Files",
                "Avg LOC", "Avg Hrs to Merge", "AI Co-author %",
            ]
            st.dataframe(author_stats, hide_index=True, use_container_width=True)

        with tab2:
            reviewer_stats = (
                team_reviews_ranged
                .groupby(["reviewer_login", "reviewer_team"])
                .agg(
                    reviews=("review_key", "count"),
                    unique_prs=("pr_key", "nunique"),
                    avg_files_reviewed=("files_changed", "mean"),
                    avg_loc_reviewed=("total_lines_changed", "mean"),
                )
                .reset_index()
                .sort_values("reviews", ascending=False)
            )
            reviewer_stats["avg_files_reviewed"] = reviewer_stats["avg_files_reviewed"].round(1)
            reviewer_stats["avg_loc_reviewed"] = reviewer_stats["avg_loc_reviewed"].round(0).astype(int)
            reviewer_stats.columns = [
                "Reviewer", "Team", "Reviews", "Unique PRs",
                "Avg Files Reviewed", "Avg LOC Reviewed",
            ]
            st.dataframe(reviewer_stats, hide_index=True, use_container_width=True)


if __name__ == "__main__":
    main()
