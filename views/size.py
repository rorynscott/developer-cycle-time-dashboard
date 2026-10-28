"""PR Size tab: weekly lines/files changed, authored and reviewed."""

import streamlit as st

from views.common import in_range, breakout_chart


def render(prs, reviews, f):
    st.caption("Weekly totals (sums) of lines and files changed.")

    st.subheader("Authored by the team")
    created = in_range(prs, "created_date", f)
    breakout_chart(created, f, "created_date", "Lines changed per week",
                   "Lines", "author_login", value_col="total_lines_changed")
    breakout_chart(created, f, "created_date", "Files changed per week",
                   "Files", "author_login", value_col="files_changed")

    st.subheader("Reviewed by the team")
    st.caption("Each PR counts once per reviewer, on the day of their "
               "first review.")
    rv = in_range(reviews, "review_date", f)
    reviewed = (rv.sort_values("submitted_at")
                .drop_duplicates(subset=["pr_key", "reviewer_login"])
                .dropna(subset=["files_changed"]))
    breakout_chart(reviewed, f, "review_date", "Lines changed on reviewed PRs per week",
                   "Lines", "reviewer_login", value_col="total_lines_changed")
    breakout_chart(reviewed, f, "review_date", "Files changed on reviewed PRs per week",
                   "Files", "reviewer_login", value_col="files_changed")
