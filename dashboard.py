#!/usr/bin/env python3
"""
Developer Cycle Time Dashboard — Streamlit app powered by the ETL SQLite DB.

Usage:
    streamlit run dashboard.py
"""

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
)

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


# ── App ─────────────────────────────────────────────────────────────────────


def main():
    st.set_page_config(page_title="PR Dashboard", layout="wide")
    st.title(get_dashboard_title())

    # Show last ETL run times
    conn = sqlite3.connect(DB_PATH)
    gh_row = None
    try:
        gh_row = conn.execute(
            "SELECT last_run_at FROM etl_watermark WHERE pipeline_name = 'github_pr_etl'"
        ).fetchone()
    except sqlite3.OperationalError:
        pass  # GitHub ETL hasn't run yet — table doesn't exist
    jira_row = None
    try:
        jira_row = conn.execute(
            "SELECT last_run_at FROM etl_watermark_jira WHERE pipeline_name = 'jira_etl'"
        ).fetchone()
    except sqlite3.OperationalError:
        pass  # Jira ETL hasn't run yet — table doesn't exist
    conn.close()
    from datetime import datetime, timezone as tz
    et = tz(timedelta(hours=-4))
    captions = []
    if gh_row and gh_row[0]:
        utc_dt = datetime.fromisoformat(gh_row[0])
        captions.append(f"GitHub: {utc_dt.astimezone(et).strftime('%Y-%m-%d %I:%M %p')} ET")
    if jira_row and jira_row[0]:
        utc_dt = datetime.fromisoformat(jira_row[0])
        captions.append(f"Jira: {utc_dt.astimezone(et).strftime('%Y-%m-%d %I:%M %p')} ET")
    if captions:
        st.caption("Last updated — " + " · ".join(captions))

    prs = load_prs()
    reviews = load_reviews()
    task_cycles = load_task_cycle_times()

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

    tab_overview, tab_reviews, tab_pr_size, tab_cycle_time = st.tabs(
        ["Overview", "Reviews", "PR Size", "Cycle Time"]
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
                stat_chart(
                    team_prs_ranged, "created_date", "files_changed",
                    color_col if not drill_down else "team_name",
                    "Files Changed (Avg + P90)",
                ),
                use_container_width=True,
            )
        with col2:
            st.plotly_chart(
                stat_chart(
                    team_prs_ranged, "created_date", "total_lines_changed",
                    color_col if not drill_down else "team_name",
                    "Lines Changed (Avg + P90)",
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
                stat_chart(
                    review_prs, "review_date", "files_changed",
                    review_color_col if not drill_down else "reviewer_team",
                    "Files Changed on Reviewed PRs (Avg + P90)",
                ),
                use_container_width=True,
            )
        with col4:
            st.plotly_chart(
                stat_chart(
                    review_prs, "review_date", "total_lines_changed",
                    review_color_col if not drill_down else "reviewer_team",
                    "Lines Changed on Reviewed PRs (Avg + P90)",
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
