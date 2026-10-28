"""PR Flow tab: time through approval stages, and who approves our PRs."""

import pandas as pd
import streamlit as st

from lib.charts import CATEGORICAL, ranked_bars, stacked_share_bars
from lib.config import get_approval_gates
from lib.data import pr_flow
from views.common import in_range, group_col, median_table

STAGES = [
    ("h_to_first_approval", "Open → 1st approval"),
    ("h_first_to_gate", "1st approval → gate"),
    ("h_approval_to_merge", "Approval → merge"),
    ("h_open_to_merge", "Open → merge"),
]
KIND_ORDER = ["Teammate", "Approval gate", "Other"]
KIND_COLORS = dict(zip(KIND_ORDER, CATEGORICAL[:3]))


def _fmt_hours(h):
    if pd.isna(h):
        return "—"
    return f"{h:.1f} h" if h < 48 else f"{h / 24:.1f} d"


def render(prs, approvals, gate_members, f):
    gates = get_approval_gates()
    gate_lines = "; ".join(
        f"**{g['repo'].split('/')[-1]}** needs an approval from "
        f"`{g['team']}` (PRs created from {g['effective']})" for g in gates
    ) or "none configured"
    st.caption(
        "Team-authored PRs **created** in the range. Stages: open → first "
        "approval from anyone; first approval → first approval from the "
        "repo's approval gate; the required approval (the gate approval on "
        "gated PRs, otherwise the first approval) → merge; and open → merge. "
        "Stages overlap, so the medians don't add up. Time spent as a draft "
        "counts as open (GitHub doesn't record when a draft became ready). "
        f"Approval gates: {gate_lines}."
    )

    created = in_range(prs, "created_date", f)
    if created.empty:
        st.info("No PRs created in this range.")
        return
    flow = pr_flow(created, approvals)

    gated = flow[flow["gated"]]
    cols = st.columns(len(STAGES))
    for (col, label), cell in zip(STAGES, cols):
        vals = flow[col].dropna()
        cell.metric(label, _fmt_hours(vals.median()),
                    f"median · n={len(vals)}", delta_color="off",
                    delta_arrow="off", border=True)
    bypassed = gated[gated["merged_at"].notna() & gated["gate_approval_at"].isna()]
    if len(bypassed):
        st.warning(
            f"{len(bypassed)} gated PR(s) merged without an approval from the "
            "gate team (per the membership snapshots): "
            + ", ".join(bypassed["pr_key"].str.split("/").str[-1])
        )

    key = group_col(f, "author_login")
    if key:
        cells = st.columns(2) + st.columns(2)
        for (col, label), cell in zip(STAGES, cells):
            med = flow.groupby(key)[col].median()
            cell.plotly_chart(
                ranked_bars(med, f"Median {label} (h)", "Hours", "{:.1f}"),
                width="stretch",
            )
    st.dataframe(
        median_table(flow, f, "author_login", STAGES),
        hide_index=True, width="stretch",
        column_config={"Median": "Median (h)", "P90": "P90 (h)"},
    )

    render_approvers(created, approvals, flow, gate_members, f)


def render_approvers(created, approvals, flow, gate_members, f):
    st.subheader("Who approves our PRs")
    appr = approvals[approvals["pr_key"].isin(created["pr_key"])].copy()
    if appr.empty:
        st.info("No approvals on PRs created in this range.")
        return
    appr["repo"] = appr["repo_key"].str.split("/").str[-1]
    gates = {g["repo"].split("/")[-1]: g for g in get_approval_gates()}

    repos = sorted(appr["repo"].unique(), key=str.lower)
    options = ["All repos"] + repos
    default = next((r for r in repos if r in gates), "All repos")
    repo = st.selectbox("Repo", options, index=options.index(default),
                        key="approver_repo")
    if repo != "All repos":
        appr = appr[appr["repo"] == repo]

    # One row per (PR, approver): re-approvals after new pushes count once
    per_pr = appr.drop_duplicates(subset=["pr_key", "reviewer_login"])
    by_approver = per_pr.groupby("reviewer_login")["pr_key"].nunique()
    approved_prs = per_pr["pr_key"].nunique()
    top2 = by_approver.nlargest(2).index
    top2_share = per_pr[per_pr["reviewer_login"].isin(top2)]["pr_key"].nunique()

    c1, c2, c3 = st.columns(3)
    c1.metric("Approved PRs", approved_prs, border=True)
    c2.metric("Distinct approvers", len(by_approver), border=True)
    c3.metric("Approved by the top 2", f"{top2_share / approved_prs:.0%}",
              ", ".join(top2), delta_color="off", delta_arrow="off",
              border=True)

    st.plotly_chart(
        stacked_share_bars(per_pr, "reviewer_login", "approver_kind",
                           "PRs approved, by approver", "PRs approved",
                           KIND_ORDER, KIND_COLORS),
        width="stretch",
    )

    gate = gates.get(repo)
    if not gate:
        return
    st.markdown(f"**Who clears the {repo} gate**")
    members = gate_members[gate_members["team_slug"] == gate["team"]]
    snap = (f"{len(members)} members; snapshots from {members['first_seen'].min()} "
            f"to {members['last_seen'].max()}" if len(members)
            else "no membership snapshot yet — run the PR ETL")
    st.caption(
        f"Gated PRs only, credited to whoever gave the **first** approval "
        f"from `{gate['team']}` ({snap}). GitHub keeps no membership history, "
        "so someone who left the team before the first snapshot isn't "
        "recognized as a gate approver."
    )
    gated = flow[flow["gated"] & (flow["repo"] == repo)
                 & flow["gate_approval_at"].notna()]
    if gated.empty:
        st.info("No gated PRs with a gate approval in this range.")
        return
    clears = appr[appr["is_gate"]].merge(
        gated[["pr_key", "gate_approval_at"]], on="pr_key",
    )
    clears = clears[clears["submitted_at"] == clears["gate_approval_at"]]
    clears = clears.drop_duplicates(subset=["pr_key"])
    counts = clears["reviewer_login"].value_counts()
    top2 = counts.head(2)
    c1, c2 = st.columns([1, 2])
    c1.metric("Gated PRs cleared", len(gated), border=True)
    c1.metric("Cleared by the top 2", f"{top2.sum() / len(gated):.0%}",
              ", ".join(top2.index), delta_color="off", delta_arrow="off",
              border=True)
    c2.plotly_chart(
        ranked_bars(counts, "First gate approval, by approver", "PRs",
                    color=KIND_COLORS["Approval gate"]),
        width="stretch",
    )
