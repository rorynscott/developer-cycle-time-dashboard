"""Delivery tab: team tasks (GitHub issues) and the epics they roll up to."""

import plotly.express as px
import streamlit as st

from lib.charts import SINGLE
from views.common import in_range, breakout_chart, median_table

DURATIONS = [
    ("created_to_close_days", "Created → closed (days)"),
    ("ip_to_close_days", "In progress → closed (days)"),
]


def render(tasks, epics, f):
    st.caption(
        "GitHub issues assigned to a team member, credited to the first "
        "team-member assignee. Epics and other containers (an issue type in "
        "`github.issues.exclude_types`, or any issue with sub-issues) are "
        "rolled up below instead of counted as tasks. **In progress** is the "
        "earliest Projects status change matching "
        "`github.issues.in_progress_statuses`; issues that never got one "
        "are left out of that metric only."
    )
    if tasks.empty:
        st.info("No GitHub issue data. Run `python3 github_issues_etl.py`.")
        return

    done = in_range(tasks[tasks["completed"]], "closed_date", f)
    scoped = tasks[tasks["repo"].isin(f.repos)]
    open_tasks = scoped[scoped["state"] == "OPEN"]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tasks completed", len(done), border=True)
    c2.metric("Open tasks", len(open_tasks),
              f"{open_tasks['first_in_progress_at'].notna().sum()} in progress",
              delta_color="off", delta_arrow="off", border=True)
    ip = done["ip_to_close_days"].dropna()
    c3.metric("Median in progress → closed",
              f"{ip.median():.1f} d" if len(ip) else "—",
              f"n={len(ip)} of {len(done)}", delta_color="off",
              delta_arrow="off", border=True)
    linked = (done["merged_pr_count"] > 0).sum()
    c4.metric("With a linked merged PR",
              f"{linked / len(done):.0%}" if len(done) else "—",
              f"{linked} of {len(done)}", delta_color="off",
              delta_arrow="off", border=True)
    if 0 < len(ip) < 20:
        st.warning(f"Only {len(ip)} completed tasks have an in-progress "
                   "timestamp — treat that median as anecdotal.")

    breakout_chart(done, f, "closed_date", "Tasks completed per week",
                   "Tasks", "primary_assignee")
    st.dataframe(
        median_table(done, f, "primary_assignee", DURATIONS),
        hide_index=True, width="stretch",
    )

    render_epics(scoped, done, epics)

    with st.expander("Completed tasks"):
        show = done[[
            "url", "title", "repo", "primary_assignee",
            "created_to_close_days", "ip_to_close_days", "merged_pr_count",
        ]].round(1).sort_values("created_to_close_days", ascending=False)
        st.dataframe(
            show, hide_index=True, width="stretch",
            column_config={
                "url": st.column_config.LinkColumn(
                    "Issue", display_text=r"([^/]+/issues/\d+)$"),
                "title": "Title", "repo": "Repo",
                "primary_assignee": "Assignee",
                "created_to_close_days": "Created → closed (d)",
                "ip_to_close_days": "In progress → closed (d)",
                "merged_pr_count": "Merged PRs",
            },
        )


def render_epics(scoped, done, epics):
    st.subheader("Epics")
    st.caption(
        "Parents of team tasks. **Progress** counts every sub-issue, "
        "whoever it's assigned to; the team columns count only team tasks. "
        "Shown: open epics, plus closed epics with a team task completed in "
        "the range."
    )
    with_parent = scoped[scoped["parent_key"].notna()]
    if with_parent.empty or epics.empty:
        st.info("No team tasks with a parent issue.")
        return
    team = with_parent.groupby("parent_key").agg(
        team_open=("state", lambda s: (s == "OPEN").sum()),
        team_assignees=("primary_assignee",
                        lambda s: ", ".join(sorted(set(s.dropna())))),
    )
    team["team_done_in_range"] = (
        done[done["parent_key"].notna()].groupby("parent_key").size()
    )
    team = team.fillna({"team_done_in_range": 0})
    table = epics.join(team, on="issue_key", how="inner")
    table = table[(table["state"] == "OPEN") | (table["team_done_in_range"] > 0)]
    if table.empty:
        st.info("No open epics, and none with team work completed in range.")
        return
    table["progress"] = (
        table["children_completed"] / table["children_total"].where(
            table["children_total"] > 0)
    )
    table["children"] = (table["children_completed"].fillna(0).astype(int).astype(str)
                         + " / " + table["children_total"].fillna(0).astype(int).astype(str))
    table = table.sort_values(["state", "team_done_in_range", "team_open"],
                              ascending=[False, False, False])

    open_epics = table[table["state"] == "OPEN"].head(12)
    if not open_epics.empty:
        chart = open_epics.assign(
            pct=open_epics["progress"].fillna(0) * 100,
            # Unique axis labels: truncated titles could otherwise collide
            title=open_epics["title"].str.slice(0, 55) + " #"
                  + open_epics["issue_key"].str.split("#").str[-1],
        )
        chart = chart.iloc[::-1]
        fig = px.bar(chart, x="pct", y="title", orientation="h",
                     text=chart["children"],
                     title="Open epics — sub-issues completed",
                     labels={"pct": "% of sub-issues completed", "title": ""},
                     range_x=[0, 100])
        fig.update_traces(marker_color=SINGLE, textposition="outside",
                          cliponaxis=False,
                          hovertemplate="%{y}<br>%{text} sub-issues "
                                        "(%{x:.0f}%)<extra></extra>")
        fig.update_layout(height=max(260, 34 * len(chart) + 110),
                          xaxis_showgrid=True, yaxis_automargin=True)
        st.plotly_chart(fig, width="stretch")

    st.dataframe(
        table[["url", "title", "repo", "state", "progress", "children",
               "team_done_in_range", "team_open", "team_assignees",
               "parent_title"]],
        hide_index=True, width="stretch",
        column_config={
            "url": st.column_config.LinkColumn(
                "Epic", display_text=r"([^/]+/issues/\d+)$"),
            "title": "Title", "repo": "Repo", "state": "State",
            "progress": st.column_config.ProgressColumn(
                "Progress", format="percent", min_value=0, max_value=1),
            "children": "Sub-issues done",
            "team_done_in_range": st.column_config.NumberColumn(
                "Team done (range)", format="%d"),
            "team_open": st.column_config.NumberColumn("Team open", format="%d"),
            "team_assignees": "Team assignees",
            "parent_title": "Parent",
        },
    )
