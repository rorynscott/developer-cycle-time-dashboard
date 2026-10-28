#!/usr/bin/env python3
"""
Developer Cycle Time Dashboard — Streamlit app powered by the ETL SQLite DB.

Usage:
    streamlit run dashboard.py
"""

import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import streamlit as st

# Add project root to path for lib imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib.config import get_dashboard_title, get_dashboard_default_days
from lib import charts  # noqa: F401  (registers the Magnetic plotly template)
from lib.data import (
    load_repos, load_prs, load_reviews, load_approvals, load_gate_members,
    load_tasks, load_epics, load_last_runs,
)
from views import output, reviews, flow, size, delivery
from views.common import Filters, BREAKOUTS

ET = ZoneInfo("America/New_York")


def sidebar(all_repos):
    st.sidebar.header("Filters")
    default_end = date.today()
    default_start = default_end - timedelta(days=get_dashboard_default_days())
    picked = st.sidebar.date_input(
        "Date range", value=(default_start, default_end), max_value=default_end,
    )
    start, end = picked if len(picked) == 2 else (default_start, default_end)
    breakout = st.sidebar.segmented_control(
        "Break out by", BREAKOUTS, default="None", required=True,
    )
    repos = st.sidebar.multiselect("Repos", all_repos, default=all_repos)
    return Filters(start=start, end=end, breakout=breakout, repos=repos,
                   all_repos=all_repos)


def main():
    st.set_page_config(page_title="Developer Cycle Time", layout="wide")
    st.title(get_dashboard_title())

    runs = load_last_runs()
    if runs:
        st.caption("Last updated — " + " · ".join(
            f"{label}: {datetime.fromisoformat(ts).astimezone(ET):%Y-%m-%d %I:%M %p %Z}"
            for label, ts in runs.items()
        ))

    prs = load_prs()
    if prs.empty:
        st.info(
            "No data yet. Run the GitHub ETL to populate PR data:\n\n"
            "```\npython3 github_etl.py\n```"
        )
        return

    f = sidebar(load_repos())
    if not f.repos:
        st.warning("Select at least one repo.")
        return
    review_data = load_reviews()

    tabs = st.tabs(["Output", "Reviews", "PR Flow", "PR Size", "Delivery"])
    with tabs[0]:
        output.render(prs, f)
    with tabs[1]:
        reviews.render(review_data, f)
    with tabs[2]:
        flow.render(prs, load_approvals(), load_gate_members(), f)
    with tabs[3]:
        size.render(prs, review_data, f)
    with tabs[4]:
        delivery.render(load_tasks(), load_epics(), f)


if __name__ == "__main__":
    main()
