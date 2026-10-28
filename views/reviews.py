"""Reviews tab: volume, review burden by origin, and where we review."""

import streamlit as st

from lib.charts import CATEGORICAL, weekly_bars, stacked_share_bars
from views.common import in_range, breakout_chart

ORIGIN_ORDER = ["Our team's PRs", "Other teams' PRs"]
ORIGIN_COLORS = dict(zip(ORIGIN_ORDER, CATEGORICAL[:2]))


def render(reviews, f):
    rv = in_range(reviews, "review_date", f)

    st.caption(
        "Every review a team member submitted (approve, request changes, or "
        "comment), excluding replies on their own PRs. A PR reviewed three "
        "times counts three times."
    )
    breakout_chart(rv, f, "review_date", "Reviews submitted per week",
                   "Reviews", "reviewer_login")

    st.subheader("Review burden by origin")
    if rv.empty:
        st.info("No reviews in this range.")
        return
    other = (rv["origin"] == ORIGIN_ORDER[1]).sum()
    c1, c2, c3 = st.columns(3)
    c1.metric("Reviews", len(rv), border=True)
    c2.metric("On other teams' PRs", f"{other} ({other / len(rv):.0%})",
              border=True)
    c3.metric("Distinct PRs reviewed", rv["pr_key"].nunique(), border=True)

    # entities=ORIGIN_ORDER gives the same two colors as ORIGIN_COLORS
    st.plotly_chart(
        weekly_bars(rv, "review_date", "Reviews per week, by PR author's team",
                    "Reviews", group_col="origin", entities=ORIGIN_ORDER),
        width="stretch",
    )

    st.subheader("Which repos we review")
    st.plotly_chart(
        stacked_share_bars(rv, "repo", "origin", "Reviews per repo",
                           "Reviews", ORIGIN_ORDER, ORIGIN_COLORS),
        width="stretch",
    )
