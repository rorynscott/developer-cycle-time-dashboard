"""Output tab: PRs created and AI co-authoring."""

import plotly.express as px
import streamlit as st

from lib.charts import CATEGORICAL, week_start, ranked_bars
from views.common import in_range, breakout_chart, group_col

AI_ORDER = ["Human only", "Human + AI"]
AI_COLORS = dict(zip(AI_ORDER, CATEGORICAL[:2]))


def render(prs, f):
    created = in_range(prs, "created_date", f)
    merged = in_range(prs, "merged_date", f)

    c1, c2, c3 = st.columns(3)
    c1.metric("PRs created", len(created), border=True)
    c2.metric("PRs merged", len(merged), border=True)
    ai_pct = created["has_ai_coauthor"].mean() if len(created) else None
    c3.metric("AI co-authored (created)",
              f"{ai_pct:.0%}" if ai_pct is not None else "—", border=True)

    breakout_chart(created, f, "created_date", "PRs created per week", "PRs",
                   "author_login")

    st.subheader("AI co-authoring")
    st.caption("A PR counts as AI co-authored when its body or a commit "
               "carries a `Co-authored-by:` trailer for an AI tool.")
    if created.empty:
        st.info("No PRs created in this range.")
        return
    ai = created.assign(
        week=week_start(created["created_date"]),
        authoring=created["has_ai_coauthor"].map({True: AI_ORDER[1],
                                                   False: AI_ORDER[0]}),
    )
    weekly = ai.groupby(["week", "authoring"]).size().reset_index(name="prs")
    fig = px.bar(weekly, x="week", y="prs", color="authoring",
                 color_discrete_map=AI_COLORS,
                 category_orders={"authoring": AI_ORDER},
                 title="PRs created per week, by authoring",
                 labels={"week": "", "prs": "PRs", "authoring": ""})
    fig.update_traces(marker_line_color="#ffffff", marker_line_width=2,
                      hovertemplate="Week of %{x|%b %d}<br>"
                                    "%{fullData.name}: %{y}<extra></extra>")
    fig.update_layout(barmode="stack",
                      xaxis=dict(tickformat="%b %d", tick0=weekly["week"].min(),
                                 dtick=7 * 86400000))

    key = group_col(f, "author_login")
    if not key:
        st.plotly_chart(fig, width="stretch")
        return
    left, right = st.columns([3, 2])
    left.plotly_chart(fig, width="stretch")
    share = ai.groupby(key)["has_ai_coauthor"].mean() * 100
    right.plotly_chart(
        ranked_bars(share, f"AI co-authored share — by {f.breakout.lower()}",
                    "% of PRs created", "{:.0f}%"),
        width="stretch",
    )
