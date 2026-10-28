"""Shared filter state and the breakout-aware chart section."""

from dataclasses import dataclass, field
from datetime import date

import pandas as pd
import streamlit as st

from lib.charts import weekly_bars, ranked_bars

BREAKOUTS = ["None", "Engineer", "Repo"]


@dataclass
class Filters:
    start: date
    end: date
    breakout: str                    # one of BREAKOUTS
    repos: list = field(default_factory=list)      # selected repo short names
    all_repos: list = field(default_factory=list)  # stable color order


def in_range(df, date_col, f):
    """Rows whose ``date_col`` falls in the date range and selected repos."""
    if df.empty:
        return df
    mask = (df[date_col] >= f.start) & (df[date_col] <= f.end)
    if "repo" in df.columns:
        mask &= df["repo"].isin(f.repos)
    return df[mask]


def breakout_chart(df, f, date_col, title, y_label, engineer_col,
                   value_col=None, value_fmt="{:,.0f}"):
    """Weekly bars with the sidebar breakout applied.

    None: one series. Repo: stacked by repo (fixed colors). Engineer: the
    weekly total beside a ranked bar per engineer — the roster is bigger
    than the color palette, so engineers are compared by length, not hue.
    """
    if df.empty:
        st.info(f"No data for {title.lower()} in this range.")
        return
    if f.breakout == "Repo":
        st.plotly_chart(
            weekly_bars(df, date_col, title, y_label, value_col,
                        group_col="repo", entities=f.all_repos),
            width="stretch",
        )
        return
    fig = weekly_bars(df, date_col, title, y_label, value_col)
    if f.breakout != "Engineer":
        st.plotly_chart(fig, width="stretch")
        return
    left, right = st.columns([3, 2])
    left.plotly_chart(fig, width="stretch")
    per = (df.groupby(engineer_col)[value_col].sum() if value_col
           else df.groupby(engineer_col).size())
    right.plotly_chart(
        ranked_bars(per, f"{title} — by engineer", y_label, value_fmt),
        width="stretch",
    )


def group_col(f, engineer_col):
    """The column to group tables by for the current breakout, or None."""
    return {"Engineer": engineer_col, "Repo": "repo"}.get(f.breakout)


def median_table(df, f, engineer_col, metrics):
    """Long-format stats: one row per (breakout group, metric).

    ``metrics`` is [(column, label)]. Each row carries its own n because
    metrics cover different subsets (e.g. only gated PRs have a gate stage).
    """
    key = group_col(f, engineer_col)
    groups = df.groupby(key) if key else [(None, df)]
    rows = []
    for name, g in groups:
        for col, label in metrics:
            vals = g[col].dropna()
            row = {f.breakout: name} if key else {}
            row.update({
                "Metric": label,
                "n": len(vals),
                "Median": round(vals.median(), 1) if len(vals) else None,
                "P90": round(vals.quantile(0.9), 1) if len(vals) else None,
            })
            rows.append(row)
    return pd.DataFrame(rows)
