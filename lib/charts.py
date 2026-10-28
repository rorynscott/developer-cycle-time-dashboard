"""
Chart styling and shared chart builders.

Colors are Magnetic light-theme tokens. CATEGORICAL is Magnetic's charting
order (accent.a … accent.g), cut at 7 slots: that prefix passes the dataviz
palette checks (lightness, chroma, CVD separation, contrast) on the white
chart surface, and adding accent.h fails the chroma floor. A breakout with
more entities than slots is shown as ranked bars instead of more colors.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

SURFACE = "#ffffff"       # base.bg.weak.default
TEXT = "#23282e"          # base.text.default
TEXT_MUTED = "#596069"    # base.text.medium.default
GRID = "#e1e4e8"          # base.border.default
NEUTRAL = "#889099"       # data-viz dormant: "Other" buckets, trend lines

CATEGORICAL = [
    "#7d8aff",  # accent.a
    "#b02863",  # accent.b
    "#f2638c",  # accent.c
    "#753bcc",  # accent.d
    "#7da11b",  # accent.e
    "#ad3907",  # accent.f
    "#04a4b0",  # accent.g
]
SINGLE = CATEGORICAL[0]

pio.templates["magnetic"] = go.layout.Template(
    layout=dict(
        font=dict(family="Inter, sans-serif", color=TEXT, size=13),
        title=dict(font=dict(size=15, color=TEXT), x=0, xanchor="left"),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        colorway=CATEGORICAL,
        xaxis=dict(gridcolor=GRID, linecolor=GRID, zeroline=False,
                   showgrid=False, ticks="", title_font_color=TEXT_MUTED,
                   tickfont_color=TEXT_MUTED),
        yaxis=dict(gridcolor=GRID, linecolor=GRID, zeroline=False,
                   ticks="", title_font_color=TEXT_MUTED,
                   tickfont_color=TEXT_MUTED),
        legend=dict(orientation="h", y=-0.18, x=0, title_text="",
                    font_color=TEXT_MUTED),
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=GRID,
                        font=dict(color=TEXT, family="Inter, sans-serif")),
        bargap=0.3,
        barcornerradius=4,
        margin=dict(t=48, l=8, r=8, b=8),
    ),
)
pio.templates.default = "magnetic"


def color_map(entities):
    """Fixed entity → color mapping (color follows the entity, not its rank).

    ``entities`` is the full, stably ordered universe (e.g. every tracked
    repo), not just what's in the current filter, so filtering never
    repaints survivors. Entities past the last slot map to NEUTRAL; callers
    fold them into "Other".
    """
    return {
        e: CATEGORICAL[i] if i < len(CATEGORICAL) else NEUTRAL
        for i, e in enumerate(entities)
    }


def week_start(dates):
    """Monday of each date's week, as a Timestamp."""
    return pd.to_datetime(dates).dt.to_period("W-SUN").dt.start_time


def _style_bars(fig):
    # 2px surface gap between stacked segments and adjacent bars
    fig.update_traces(marker_line_color=SURFACE, marker_line_width=2,
                      selector=dict(type="bar"))
    return fig


def weekly_bars(df, date_col, title, y_label, value_col=None,
                group_col=None, entities=None):
    """Weekly totals as bars: a count of rows, or the sum of ``value_col``.

    With ``group_col``, bars stack by group using the fixed colors of
    ``entities``; groups beyond the palette fold into "Other".
    """
    data = df.assign(week=week_start(df[date_col]))
    if group_col:
        cmap = color_map(entities)
        data[group_col] = data[group_col].where(
            data[group_col].map(cmap).ne(NEUTRAL) & data[group_col].isin(cmap),
            "Other",
        )
        keys = ["week", group_col]
    else:
        keys = ["week"]
    agg = (
        data.groupby(keys)[value_col].sum() if value_col
        else data.groupby(keys).size()
    ).reset_index(name="value")

    if group_col:
        cmap = {**color_map(entities), "Other": NEUTRAL}
        order = [e for e in list(entities) + ["Other"] if e in set(agg[group_col])]
        fig = px.bar(
            agg, x="week", y="value", color=group_col,
            color_discrete_map=cmap, category_orders={group_col: order},
            title=title, labels={"week": "", "value": y_label, group_col: ""},
        )
        fig.update_traces(hovertemplate="Week of %{x|%b %d}<br>"
                                        "%{fullData.name}: %{y:,}<extra></extra>")
    else:
        fig = px.bar(agg, x="week", y="value", title=title,
                     labels={"week": "", "value": y_label})
        fig.update_traces(marker_color=SINGLE,
                          hovertemplate="Week of %{x|%b %d}: %{y:,}<extra></extra>")
    # Ticks on the Mondays the bars represent (plotly defaults to Sundays)
    fig.update_layout(barmode="stack", hovermode="closest",
                      xaxis=dict(tickformat="%b %d", tick0=agg["week"].min(),
                                 dtick=7 * 86400000))
    return _style_bars(fig)


def ranked_bars(series, title, x_label, text_fmt="{:,.0f}", color=SINGLE,
                max_rows=15):
    """Horizontal bars for one value per entity, largest at the top."""
    s = series.dropna().sort_values(ascending=False).head(max_rows)[::-1]
    fig = go.Figure(go.Bar(
        x=s.values, y=s.index.astype(str), orientation="h",
        marker_color=color, text=[text_fmt.format(v) for v in s.values],
        textposition="outside", cliponaxis=False,
        hovertemplate="%{y}: %{text}<extra></extra>",
    ))
    # Headroom so the outside value labels aren't clipped
    top = s.max() if len(s) and s.max() > 0 else 1
    fig.update_layout(
        title=title, xaxis_title=x_label, xaxis_showgrid=True,
        xaxis_range=[0, top * 1.18], yaxis_showgrid=False,
        height=max(240, 32 * len(s) + 90),
    )
    return _style_bars(fig)


def stacked_share_bars(df, cat_col, group_col, title, x_label, order,
                       colors, max_rows=15):
    """Horizontal stacked bars: count per ``cat_col``, split by ``group_col``.

    ``order``/``colors`` fix the segment order and colors (≤ palette size).
    """
    counts = df.groupby([cat_col, group_col]).size().reset_index(name="n")
    totals = counts.groupby(cat_col)["n"].sum().sort_values(ascending=False)
    # px puts the first category at the top, so largest-first reads top-down
    top = totals.head(max_rows).index.tolist()
    counts = counts[counts[cat_col].isin(top)]
    fig = px.bar(
        counts, y=cat_col, x="n", color=group_col, orientation="h",
        color_discrete_map=colors,
        category_orders={group_col: order, cat_col: top},
        title=title, labels={cat_col: "", "n": x_label, group_col: ""},
    )
    fig.update_traces(hovertemplate="%{y} · %{fullData.name}: %{x:,}<extra></extra>")
    fig.update_layout(barmode="stack", xaxis_showgrid=True,
                      height=max(260, 34 * len(top) + 110))
    return _style_bars(fig)
