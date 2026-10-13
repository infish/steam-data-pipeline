"""Visual system for the dashboard: palette, chart styling, layout pieces.

The dashboard is black and orange (.streamlit/config.toml), so orange leads
the series. Colors come from a palette validated for colour-vision
deficiency on the dark surface (all-pairs checks for the first three slots,
and the blue/red pair). Colors follow meaning: a job, a series role, or a
direction. They never encode rank, and status colors are reserved for state.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'


@dataclass(frozen=True)
class Tokens:
    surface: str
    ink: str
    ink_secondary: str
    muted: str
    grid: str
    axis: str
    border: str
    series: tuple
    rise: str
    fall: str


DARK = Tokens(
    surface="#141414",
    ink="#ffffff",
    ink_secondary="#c8c3bc",
    muted="#8c867e",
    grid="#262321",
    axis="#3a3632",
    border="rgba(255,255,255,0.10)",
    series=("#dd7516", "#3987e5", "#199e70"),
    rise="#3987e5",
    fall="#e66767",
)

# A job keeps its color everywhere it appears.
JOB_SLOTS = {"catalog_daily": 0, "players_hourly": 1, "reviews_daily": 2}

# How often each job should succeed, for the sidebar's "late" badge.
JOB_EXPECTED_HOURS = {
    "players_hourly": 1,
    "catalog_daily": 24,
    "reviews_daily": 24,
}


def tokens():
    # One dark theme; st.context.theme.type does not reliably report a
    # custom theme, so charts do not follow it.
    return DARK


def job_color(job_name):
    series = tokens().series
    return series[JOB_SLOTS.get(job_name, 0) % len(series)]


def style(figure, height=340, legend=True, y_title=None, x_title=None):
    """Apply the shared chart style and return the figure."""
    t = tokens()
    figure.update_layout(
        height=height,
        # Transparent, so charts sit on the card or page behind them.
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": FONT, "size": 13, "color": t.ink_secondary},
        colorway=list(t.series),
        margin={"l": 8, "r": 16, "t": 36 if legend else 12, "b": 8},
        showlegend=legend,
        legend={
            "orientation": "h",
            "x": 0,
            "xanchor": "left",
            "y": 1.02,
            "yanchor": "bottom",
            "title": {"text": ""},
            "font": {"color": t.ink_secondary},
        },
        hoverlabel={
            "bgcolor": t.surface,
            "bordercolor": t.axis,
            "font": {"family": FONT, "color": t.ink},
        },
        bargap=0.35,
        bargroupgap=0.1,
        separators=".,",
    )
    axis = {
        "gridcolor": t.grid,
        "gridwidth": 1,
        "zeroline": False,
        "linecolor": t.axis,
        "tickfont": {"color": t.muted},
        "title": {"font": {"color": t.ink_secondary}},
        "automargin": True,
    }
    figure.update_xaxes(showgrid=False, showline=True, **axis)
    figure.update_yaxes(showgrid=True, showline=False, **axis)

    if y_title is not None:
        figure.update_yaxes(title_text=y_title)

    if x_title is not None:
        figure.update_xaxes(title_text=x_title)

    figure.update_traces(
        marker={"cornerradius": 4, "line": {"width": 0}},
        selector={"type": "bar"},
    )
    figure.update_traces(
        line={"width": 2},
        marker={"size": 8, "line": {"width": 2, "color": t.surface}},
        selector={"type": "scatter"},
    )
    return figure


def show(figure):
    st.plotly_chart(
        figure,
        theme=None,
        use_container_width=True,
        config={"displayModeBar": False},
    )


def line(df, x, series, height=320, y_title=None, x_title=None,
         y_format=",.0f", x_format=None):
    """A multi-series line chart; series maps column -> display name.

    Rows with NaN values break the line, so gaps stay visible.
    """
    t = tokens()
    figure = go.Figure()

    for index, (column, name) in enumerate(series.items()):
        figure.add_trace(go.Scatter(
            x=df[x],
            y=df[column],
            name=name,
            mode="lines+markers",
            connectgaps=False,
            line={"color": t.series[index % len(t.series)]},
            marker={"color": t.series[index % len(t.series)]},
            hovertemplate=f"%{{y:{y_format}}}<extra>{name}</extra>",
        ))

    style(figure, height=height, legend=len(series) > 1,
          y_title=y_title, x_title=x_title)
    figure.update_layout(hovermode="x unified")
    figure.update_yaxes(tickformat=y_format)

    if x_format:
        figure.update_xaxes(tickformat=x_format, hoverformat=x_format)

    # A flat series (often the first day after a deploy) would otherwise get
    # sub-unit ticks that all print the same label.
    values = pd.concat([df[column] for column in series]).dropna()

    if values.nunique() <= 1:
        figure.update_yaxes(rangemode="tozero")

    if (df[x].nunique() <= 1
            and pd.api.types.is_datetime64_any_dtype(df[x])):
        figure.update_xaxes(dtick=24 * 3600 * 1000)

    return figure


def hbar(df, value, label, height=None, x_title=None, value_format=",.0f",
         color=None, hover_columns=()):
    """A horizontal bar chart, largest at the top, one color."""
    t = tokens()
    df = df.sort_values(value)
    customdata = df[list(hover_columns)] if hover_columns else None
    hover_extra = "".join(
        f"<br>{column.replace('_', ' ')}: %{{customdata[{i}]}}"
        for i, column in enumerate(hover_columns)
    )
    figure = go.Figure(go.Bar(
        x=df[value],
        y=df[label],
        orientation="h",
        marker={"color": color or t.series[0]},
        customdata=customdata,
        hovertemplate=(
            f"<b>%{{y}}</b><br>%{{x:{value_format}}}{hover_extra}"
            "<extra></extra>"
        ),
    ))
    style(figure, height=height or max(220, 30 * len(df) + 40),
          legend=False, x_title=x_title)
    figure.update_xaxes(showgrid=True, **count_ticks(df[value].max()))
    figure.update_yaxes(showgrid=False)
    return figure


def count_ticks(max_value):
    """Axis settings for counts: SI ticks (500k, 1M) for large values, whole
    numbers for small ones, where SI would print 0.5 as "500m"."""
    if max_value is not None and not pd.isna(max_value) and max_value >= 1000:
        return {"tickformat": "~s", "nticks": 5}

    small = max_value is None or pd.isna(max_value) or max_value <= 5
    return {"tickformat": ",d", "nticks": 5, "dtick": 1 if small else None}


def compact(value):
    """1186957 -> '1.19M'; for tiles where the exact figure is in help."""
    if value is None or pd.isna(value):
        return "N/A"

    value = float(value)

    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if abs(value) >= limit:
            return f"{value / limit:.3g}{suffix}"

    return f"{value:,.0f}"


ACCENT = "#ff8c1a"


def page_header(title, description):
    st.title(title)
    # A short orange rule under each title, the dashboard's signature.
    st.markdown(
        f"<div style='height:3px;width:56px;background:{ACCENT};"
        "border-radius:2px;margin:-0.9rem 0 0.9rem'></div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        f"<p style='font-size:1.05rem;margin-top:-0.6rem;opacity:0.75'>"
        f"{description}</p>",
        unsafe_allow_html=True,
    )


def card(title, caption=None):
    """A bordered section; use as `with card(...):`."""
    container = st.container(border=True)
    container.markdown(f"**{title}**")

    if caption:
        container.caption(caption)

    return container


def time_ago(value, now=None):
    if value is None or pd.isna(value):
        return "never"

    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    seconds = (now - pd.Timestamp(value).to_pydatetime()).total_seconds()

    if seconds < 90:
        return "just now"

    if seconds < 90 * 60:
        return f"{round(seconds / 60)} min ago"

    if seconds < 36 * 3600:
        return f"{round(seconds / 3600)} h ago"

    return f"{round(seconds / 86400)} days ago"


def run_badge(status, warning, finished_at, job_name, now=None):
    """Markdown badge for a job's latest run: icon + label, never color alone.
    """
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    expected = JOB_EXPECTED_HOURS.get(job_name)
    late = (
        expected is not None
        and finished_at is not None
        and not pd.isna(finished_at)
        and (now - pd.Timestamp(finished_at).to_pydatetime()).total_seconds()
        > expected * 1.5 * 3600
    )

    if status == "FAILED":
        return ":red-badge[:material/error: failed]"

    if status == "RUNNING":
        return ":blue-badge[:material/progress_activity: running]"

    if late:
        return ":orange-badge[:material/schedule: late]"

    if warning:
        return ":orange-badge[:material/warning: warnings]"

    return ":green-badge[:material/check_circle: ok]"


STATUS_LABELS = {
    "pass": "✅ pass",
    "warn": "⚠️ warn",
    "fail": "❌ fail",
}


def status_label(status):
    return STATUS_LABELS.get(status, status)
