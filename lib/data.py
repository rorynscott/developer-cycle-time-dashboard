"""
Cached data loaders for the dashboard. Each returns an empty DataFrame
(with the expected columns where callers need them) when the ETL that
owns the table hasn't run yet.
"""

import sqlite3

import numpy as np
import pandas as pd
import streamlit as st

from lib.config import (
    get_db_path, get_approval_gates, get_github_issues_config,
)

DB_PATH = get_db_path()


def _query(sql, params=()):
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql_query(sql, conn, params=params)
    except (pd.errors.DatabaseError, sqlite3.OperationalError):
        return pd.DataFrame()
    finally:
        conn.close()


def _repo_short(keys):
    return keys.str.split("/").str[-1]


@st.cache_data(ttl=300)
def load_repos():
    """Tracked repos (short names), sorted — the stable color order."""
    df = _query("SELECT repo_key FROM dim_repo WHERE is_active = 1")
    if df.empty:
        return []
    return sorted(_repo_short(df["repo_key"]).tolist(), key=str.lower)


@st.cache_data(ttl=300)
def load_prs():
    """Team-authored PRs (team_name set = authored while on the team)."""
    df = _query(
        """
        SELECT pr_key, repo_key, author_login, team_name, title, state,
               created_at, merged_at, merged_by, closed_at,
               files_changed, total_lines_changed, has_ai_coauthor,
               hours_to_merge, total_comment_count
        FROM fact_pr
        WHERE team_name IS NOT NULL
        """
    )
    if df.empty:
        return df
    df["repo"] = _repo_short(df["repo_key"])
    for col in ("created_at", "merged_at", "closed_at"):
        df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
    df["created_date"] = df["created_at"].dt.date
    df["merged_date"] = df["merged_at"].dt.date
    df["has_ai_coauthor"] = df["has_ai_coauthor"].fillna(0).astype(bool)
    return df


@st.cache_data(ttl=300)
def load_reviews():
    """Reviews submitted by team members (while on the team)."""
    df = _query(
        """
        SELECT r.review_key, r.pr_key, r.reviewer_login, r.review_state,
               r.submitted_at, a.team_name AS reviewer_team,
               p.repo_key, p.files_changed, p.total_lines_changed,
               p.author_login AS pr_author, p.team_name AS pr_author_team
        FROM dim_review r
        JOIN dim_author a ON r.reviewer_login = a.author_login
        JOIN fact_pr p ON r.pr_key = p.pr_key
        WHERE a.team_name IS NOT NULL
          -- Former members stop counting for the team after left_on
          AND (a.left_on IS NULL OR r.submitted_at < a.left_on)
          -- Reviews on your own PR are replies, not reviews
          AND r.reviewer_login != p.author_login
        """
    )
    if df.empty:
        return df
    df["repo"] = _repo_short(df["repo_key"])
    df["submitted_at"] = pd.to_datetime(df["submitted_at"], utc=True, format="ISO8601")
    df["review_date"] = df["submitted_at"].dt.date
    df["origin"] = np.where(
        df["pr_author_team"].notna(), "Our team's PRs", "Other teams' PRs",
    )
    return df


@st.cache_data(ttl=300)
def load_approvals():
    """APPROVED reviews on team-authored PRs, with who gave them.

    approver_kind: "Teammate" (on the team at review time), "Approval gate"
    (a member of the PR repo's gate team in any membership snapshot), or
    "Other".
    """
    df = _query(
        """
        SELECT r.pr_key, r.reviewer_login, r.submitted_at, p.repo_key,
               CASE WHEN a.team_name IS NOT NULL
                     AND (a.left_on IS NULL OR r.submitted_at < a.left_on)
                    THEN 1 ELSE 0 END AS is_teammate
        FROM dim_review r
        JOIN fact_pr p ON r.pr_key = p.pr_key
        LEFT JOIN dim_author a ON r.reviewer_login = a.author_login
        WHERE r.review_state = 'APPROVED' AND p.team_name IS NOT NULL
        """
    )
    if df.empty:
        return df
    df["submitted_at"] = pd.to_datetime(df["submitted_at"], utc=True, format="ISO8601")
    gate_team = {g["repo"]: g["team"] for g in get_approval_gates()}
    members = load_gate_members()
    gate_pairs = set(zip(members["team_slug"], members["login"]))
    df["is_gate"] = [
        (gate_team.get(repo), login) in gate_pairs
        for repo, login in zip(df["repo_key"], df["reviewer_login"])
    ]
    df["approver_kind"] = np.select(
        [df["is_teammate"] == 1, df["is_gate"]],
        ["Teammate", "Approval gate"], "Other",
    )
    return df


@st.cache_data(ttl=300)
def load_gate_members():
    df = _query("SELECT team_slug, login, first_seen, last_seen FROM dim_gate_member")
    if df.empty:
        return pd.DataFrame(columns=["team_slug", "login", "first_seen", "last_seen"])
    return df


def pr_flow(prs, approvals):
    """Per-PR approval timeline in hours.

    Stages: open → first approval; first approval → first approval from
    the repo's gate team (gated PRs only: gate repo, created on/after the
    gate's effective date); required approval (the gate approval when
    gated, else the first approval) → merge; and open → merge overall.
    Approvals submitted after the merge are ignored.
    """
    flow = prs.copy()
    gates = {g["repo"]: pd.Timestamp(g["effective"], tz="UTC")
             for g in get_approval_gates()}
    flow["gated"] = [
        repo in gates and created >= gates[repo]
        for repo, created in zip(flow["repo_key"], flow["created_at"])
    ]

    appr = approvals.merge(prs[["pr_key", "merged_at"]], on="pr_key")
    appr = appr[appr["merged_at"].isna() | (appr["submitted_at"] <= appr["merged_at"])]
    first = appr.groupby("pr_key")["submitted_at"].min().rename("first_approval_at")
    first_gate = (
        appr[appr["is_gate"]].groupby("pr_key")["submitted_at"].min()
        .rename("gate_approval_at")
    )
    flow = flow.join(first, on="pr_key").join(first_gate, on="pr_key")
    flow.loc[~flow["gated"], "gate_approval_at"] = pd.NaT

    def hours(a, b):
        return (flow[b] - flow[a]).dt.total_seconds() / 3600

    flow["h_to_first_approval"] = hours("created_at", "first_approval_at")
    flow["h_first_to_gate"] = hours("first_approval_at", "gate_approval_at")
    flow["required_approval_at"] = flow["gate_approval_at"].where(
        flow["gated"], flow["first_approval_at"],
    )
    flow["h_approval_to_merge"] = hours("required_approval_at", "merged_at")
    flow["h_open_to_merge"] = hours("created_at", "merged_at")
    return flow


@st.cache_data(ttl=300)
def load_tasks():
    """Team-assigned GitHub issues, excluding containers.

    A container is an issue with sub-issues or a type in
    github.issues.exclude_types; most issues have no type set, so the
    sub-issue rule catches untyped epics.
    """
    df = _query(
        """
        SELECT i.issue_key, i.repo_key, i.title, i.url, i.issue_type,
               i.state, i.state_reason, i.created_at, i.first_in_progress_at,
               i.closed_at, i.primary_assignee, i.parent_key,
               i.sub_issue_count,
               COUNT(DISTINCT p.pr_key) AS merged_pr_count
        FROM dim_gh_issue i
        LEFT JOIN bridge_pr_gh_issue b ON b.issue_key = i.issue_key
        LEFT JOIN fact_pr p ON p.pr_key = b.pr_key AND p.state = 'merged'
        WHERE i.team_name IS NOT NULL
        GROUP BY i.issue_key
        """
    )
    if df.empty:
        return df
    cfg = get_github_issues_config()
    excluded = set(cfg["exclude_types"]) if cfg else set()
    df = df[
        ~df["issue_type"].isin(excluded) & (df["sub_issue_count"].fillna(0) == 0)
    ].copy()
    df["repo"] = _repo_short(df["repo_key"])
    for col in ("created_at", "first_in_progress_at", "closed_at"):
        df[col] = pd.to_datetime(df[col], utc=True, format="ISO8601")
    df["completed"] = df["state_reason"].eq("COMPLETED")
    df["closed_date"] = df["closed_at"].dt.date
    day = 86400
    df["created_to_close_days"] = (
        (df["closed_at"] - df["created_at"]).dt.total_seconds() / day
    )
    df["ip_to_close_days"] = (
        (df["closed_at"] - df["first_in_progress_at"]).dt.total_seconds() / day
    )
    # Status set after closing (board cleanup) isn't a real start time
    df.loc[df["ip_to_close_days"] < 0, "ip_to_close_days"] = np.nan
    return df


@st.cache_data(ttl=300)
def load_epics():
    df = _query(
        """
        SELECT issue_key, repo_key, title, url, issue_type, state,
               created_at, closed_at, children_total, children_completed,
               parent_title
        FROM dim_gh_epic
        """
    )
    if df.empty:
        return df
    df["repo"] = _repo_short(df["repo_key"])
    return df


@st.cache_data(ttl=300)
def load_last_runs():
    """{source label: last_run_at ISO string} for the header caption."""
    out = {}
    for label, table, pipeline in (
        ("PRs", "etl_watermark", "github_pr_etl"),
        ("Issues", "etl_watermark_gh_issues", "github_issues_etl"),
    ):
        df = _query(f"SELECT last_run_at FROM {table} WHERE pipeline_name = ?",
                    (pipeline,))
        if not df.empty and df.iloc[0, 0]:
            out[label] = df.iloc[0, 0]
    return out
