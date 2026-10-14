"""
Step 6: Streamlit dashboard for the RUCO oil tracker (visual version).

Reads the CSV files written by the Spark job (output/clean_readings.csv, output/alerts.csv)
and refreshes every 2 seconds:
  - KPI tiles            : fryers to discard, alerts, oil to RUCO, biodiesel, CO2 avoided
  - TPC gauges           : one per fryer, green / amber / red bands at 22% and 25%
  - Hours-left bar chart : hatched bar = prediction extrapolated beyond the lab data
  - Time-in-status chart : how long each fryer spent OK / Warning / Discard
  - Line charts          : TPC, acid value and carbonyl value over frying time
  - ESG + data quality   : oil diverted per fryer, cumulative biodiesel, outliers / missing

Run:  py -m streamlit run dashboard/app.py
"""
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

OUTPUT = Path(__file__).parent.parent / "output"
CLEAN_CSV = OUTPUT / "clean_readings.csv"
ALERTS_CSV = OUTPUT / "alerts.csv"
REFRESH_SECONDS = 2
STEP_HOURS = 0.1           # one reading = 0.1 frying hour (see producer.py)

TPC_LIMIT = 25.0
TPC_WARNING = 22.0

# Fryers in fixed order, each with a fixed colour (same in every chart) and its oil volume
FRYERS = {
    "F160": {"name": "Bakery",          "litres": 10, "color": "#2a78d6"},
    "F175": {"name": "Cafe",            "litres": 8,  "color": "#eb6834"},
    "F190": {"name": "College Canteen", "litres": 25, "color": "#1baf7a"},
    "F205": {"name": "Restaurant",      "litres": 20, "color": "#eda100"},
    "F220": {"name": "Street Stall",    "litres": 15, "color": "#e87ba4"},
}
FRYER_COLOR = {f["name"]: f["color"] for f in FRYERS.values()}

# Status colours are reserved for status only (never reused for a fryer) and always
# shown together with a text label, so colour is never the only signal.
# Icons are Google Material Symbols, built into Streamlit as ":material/<name>:"
# (browse them at https://fonts.google.com/icons). `md` = Streamlit markdown colour name.
STATUS = {
    "WARMING_UP": {"label": "Warming up", "icon": ":material/hourglass_top:", "md": "gray",
                   "color": "#b8bcc2"},
    "OK":         {"label": "OK",         "icon": ":material/check_circle:",  "md": "green",
                   "color": "#2e9e5b"},
    "WARNING":    {"label": "Warning",    "icon": ":material/warning:",       "md": "orange",
                   "color": "#e0a100"},
    "DISCARD":    {"label": "Discard",    "icon": ":material/dangerous:",     "md": "red",
                   "color": "#d03b3b"},
}


def status_badge(status):
    """Coloured icon + text label, e.g. a red 'dangerous' icon followed by 'Discard'."""
    s = STATUS[status]
    return f":{s['md']}[{s['icon']} **{s['label']}**]"


def chart_heading(icon, text):
    """Heading above a chart, with a Material icon (Plotly titles can't render icon fonts)."""
    st.markdown(f"##### :material/{icon}: {text}")

BIODIESEL_YIELD = 0.85     # litres of biodiesel per litre of used cooking oil (assumption)
CO2_PER_L_BIODIESEL = 2.2  # kg CO2 avoided per litre of biodiesel replacing diesel (assumption)

PLOT_CONFIG = {"displayModeBar": False}

st.set_page_config(page_title="RUCO Oil Tracker", page_icon=":material/oil_barrel:", layout="wide")


# ---------------------------------------------------------------- data helpers
def read_csv(path):
    """Read a CSV that Spark may be appending to right now (skip a half-written last line)."""
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, on_bad_lines="skip")
    except (pd.errors.EmptyDataError, PermissionError):
        return pd.DataFrame()


def current_cycle(df):
    """Keep only each fryer's current oil cycle (hours restart from 0 after an oil change)."""
    df = df.copy()
    df["cycle"] = df.groupby("fryer_id")["data_hour"].transform(lambda h: (h.diff() < 0).cumsum())
    return df[df["cycle"] == df.groupby("fryer_id")["cycle"].transform("max")]


def base_layout(fig, height):
    """Shared look for all charts: white background, light grid, legend on top.
    (Chart titles are Streamlit headings with icons, see chart_heading().)"""
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=40, b=10),
                      plot_bgcolor="white", paper_bgcolor="white",
                      legend=dict(orientation="h", yanchor="bottom", y=1.0, x=0, title=None),
                      hoverlabel=dict(bgcolor="white"))
    fig.update_xaxes(gridcolor="#eeeeee", zeroline=False)
    fig.update_yaxes(gridcolor="#eeeeee", zeroline=False)
    return fig


# ---------------------------------------------------------------- charts
def gauge(row):
    """Speedometer-style gauge of the rolling TPC with the warning / FSSAI bands."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=row.tpc_rolling,
        number=dict(suffix=" %", valueformat=".1f", font=dict(size=30)),
        gauge=dict(
            axis=dict(range=[0, 50], tickvals=[0, 10, 20, 30, 40, 50], tickfont=dict(size=10)),
            bar=dict(color=FRYERS[row.fryer_id]["color"], thickness=0.3),
            steps=[dict(range=[0, TPC_WARNING], color="#e3f4e9"),
                   dict(range=[TPC_WARNING, TPC_LIMIT], color="#fdf0c8"),
                   dict(range=[TPC_LIMIT, 50], color="#f8dada")],
            threshold=dict(line=dict(color="#d03b3b", width=3), thickness=0.85, value=TPC_LIMIT),
        )))
    fig.update_layout(height=170, margin=dict(l=25, r=25, t=25, b=5), paper_bgcolor="white")
    return fig


def hours_left_chart(latest):
    """Horizontal bars: frying hours left per fryer. Hatched = extrapolated beyond lab data."""
    df = latest.iloc[::-1]   # first fryer on top
    hours = df["hours_left"].fillna(0)
    labels = [("     Discard now" if s == "DISCARD"
               else f"{h:.1f} h · {'live' if src == 'live' else 'model'}")
              for h, s, src in zip(hours, df["status"], df["rate_source"])]
    fig = go.Figure(go.Bar(
        x=hours, y=df["fryer_name"], orientation="h",
        marker=dict(color=[STATUS[s]["color"] for s in df["status"]],
                    pattern=dict(shape=["/" if e and s != "DISCARD" else ""
                                        for e, s in zip(df["extrapolated"], df["status"])],
                                 fgcolor="white", size=6)),
        text=labels, textposition="outside", cliponaxis=False,
        hovertemplate="%{y}: %{x:.1f} h left<extra></extra>"))
    # Fryers already past the limit have a 0-length bar: mark them with a red dot instead
    done = df[df["status"] == "DISCARD"]
    fig.add_scatter(x=[0] * len(done), y=done["fryer_name"], mode="markers", cliponaxis=False,
                    marker=dict(size=12, color=STATUS["DISCARD"]["color"]), hoverinfo="skip")
    fig.update_layout(showlegend=False)
    fig.update_xaxes(title="Frying hours left until TPC reaches 25%",
                     range=[0, max(hours.max() * 1.3, 5)])
    fig.add_annotation(text="Hatched bar = extrapolated beyond lab data (>30 h)",
                       xref="paper", yref="paper", x=1, y=1.04, yanchor="bottom", showarrow=False,
                       font=dict(size=11, color="#777"), xanchor="right")
    return base_layout(fig, 300)


def status_time_chart(cycle):
    """Stacked bars: how many frying hours each fryer has spent in each status."""
    counts = (cycle.groupby(["fryer_name", "status"]).size() * STEP_HOURS).unstack(fill_value=0)
    names = [f["name"] for f in FRYERS.values() if f["name"] in counts.index][::-1]
    fig = go.Figure()
    for status, info in STATUS.items():
        if status in counts.columns:
            fig.add_bar(y=names, x=counts.loc[names, status], orientation="h",
                        name=info["label"], marker_color=info["color"],
                        hovertemplate="%{y}: %{x:.1f} h<extra>" + info["label"] + "</extra>")
    fig.update_layout(barmode="stack", bargap=0.35, legend_traceorder="normal")
    fig.update_xaxes(title="Frying hours in this oil cycle")
    return base_layout(fig, 300)


def trend_chart(cycle, column, y_title, show_limits=False, show_raw=False, height=380):
    """Line per fryer over frying time (fixed 0-30 h axis so lines grow across the chart)."""
    fig = go.Figure()
    for fid, info in FRYERS.items():
        d = cycle[cycle["fryer_id"] == fid]
        if d.empty:
            continue
        if show_raw:
            fig.add_scatter(x=d["data_hour"], y=d["tpc_raw"], mode="markers", showlegend=False,
                            marker=dict(size=4, color=info["color"], opacity=0.3),
                            hoverinfo="skip")
        fig.add_scatter(x=d["data_hour"], y=d[column], mode="lines", name=info["name"],
                        line=dict(color=info["color"], width=2.5),
                        hovertemplate=f"{info['name']}<br>hour %{{x:.1f}}<br>{y_title}: %{{y:.2f}}<extra></extra>")
    if show_raw:
        out = cycle[cycle["is_outlier"]]
        fig.add_scatter(x=out["data_hour"], y=out["tpc_raw"], mode="markers", name="Outlier removed",
                        marker=dict(symbol="x", size=9, color="#d03b3b"),
                        hovertemplate="Outlier %{y:.1f}% at hour %{x:.1f}<extra></extra>")
    if show_limits:
        fig.add_hrect(y0=TPC_LIMIT, y1=60, fillcolor="#d03b3b", opacity=0.06, line_width=0)
        fig.add_hline(y=TPC_LIMIT, line=dict(color="#d03b3b", dash="dash"),
                      annotation_text="FSSAI discard limit 25%", annotation_position="top left")
        fig.add_hline(y=TPC_WARNING, line=dict(color="#e0a100", dash="dot"),
                      annotation_text="Warning 22%", annotation_position="bottom left")
    fig.update_xaxes(title="Frying time (hours)", range=[0, 30], dtick=2)
    fig.update_yaxes(title=y_title)
    if show_limits:
        top = max(cycle[["tpc_rolling"] + (["tpc_raw"] if show_raw else [])].max().max(), 30)
        fig.update_yaxes(range=[8, top + 2])
    return base_layout(fig, height)


def esg_by_fryer_chart(alerts):
    """Bars: litres of used oil sent to RUCO per fryer (each alert = one full fryer of oil)."""
    names = [f["name"] for f in FRYERS.values()]
    litres = [FRYERS[fid]["litres"] * int((alerts.get("fryer_id", pd.Series(dtype=str)) == fid).sum())
              for fid in FRYERS]
    fig = go.Figure(go.Bar(x=names, y=litres, marker_color=[FRYER_COLOR[n] for n in names],
                           text=[f"{v} L" if v else "" for v in litres], textposition="outside",
                           cliponaxis=False, hovertemplate="%{x}: %{y} L<extra></extra>"))
    fig.update_yaxes(title="Litres of used oil", range=[0, max(max(litres) * 1.25, 10)])
    return base_layout(fig, 300)


def esg_cumulative_chart(alerts):
    """Step line: biodiesel potential growing with every discard alert."""
    df = alerts.copy()
    df["time"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("time")
    df["biodiesel"] = (df["fryer_id"].map(lambda f: FRYERS[f]["litres"]) * BIODIESEL_YIELD).cumsum()
    fig = go.Figure(go.Scatter(
        x=df["time"], y=df["biodiesel"], mode="lines+markers", line_shape="hv",
        line=dict(color="#2e7d5b", width=2.5), marker=dict(size=9),
        fill="tozeroy", fillcolor="rgba(46,125,91,0.12)",
        text=df["fryer_name"], hovertemplate="%{text} discarded<br>total %{y:.1f} L biodiesel<extra></extra>"))
    fig.update_xaxes(title="Time")
    fig.update_yaxes(title="Litres of biodiesel (cumulative)", rangemode="tozero")
    return base_layout(fig, 300)


def data_quality_chart(readings):
    """Grouped bars: outliers removed and missing values filled by Spark, per fryer."""
    names = [f["name"] for f in FRYERS.values()]
    grouped = readings.groupby("fryer_name")
    outliers = grouped["is_outlier"].sum().reindex(names, fill_value=0)
    missing = grouped["was_missing"].sum().reindex(names, fill_value=0)
    fig = go.Figure()
    fig.add_bar(x=names, y=outliers, name="Outliers removed (z-score)", marker_color="#4a5560",
                text=outliers, textposition="outside")
    fig.add_bar(x=names, y=missing, name="Missing values filled", marker_color="#a9b3bc",
                text=missing, textposition="outside")
    fig.update_layout(barmode="group", bargap=0.3)
    fig.update_yaxes(title="Readings", range=[0, max(outliers.max(), missing.max(), 1) * 1.3])
    return base_layout(fig, 300)


# ---------------------------------------------------------------- page
st.title("RUCO Used Cooking Oil Tracker", anchor=False)
st.caption(":material/stream: Live oil quality per fryer · Kafka → Spark Structured Streaming → "
           "this dashboard")


@st.fragment(run_every=REFRESH_SECONDS)
def live_view():
    readings = read_csv(CLEAN_CSV)
    alerts = read_csv(ALERTS_CSV)
    if readings.empty:
        st.info("Waiting for data… start Spark (`docker compose up -d`) and the producer "
                "(`py producer/producer.py`).")
        return

    cycle = current_cycle(readings)
    latest = (cycle.groupby("fryer_id").tail(1).set_index("fryer_id")
              .reindex(list(FRYERS)).dropna(subset=["fryer_name"]).reset_index())

    last_ts = pd.to_datetime(readings["timestamp"].iloc[-1])
    age = (datetime.now(timezone.utc) - last_ts).total_seconds()
    st.caption(f":material/sync: Updates every {REFRESH_SECONDS} s · last reading {age:.0f} s ago · "
               f"{len(readings)} readings processed")

    # ---- KPI tiles ----
    litres = sum(FRYERS[f]["litres"] for f in alerts.get("fryer_id", []))
    biodiesel = litres * BIODIESEL_YIELD
    k = st.columns(5)
    k[0].metric("Fryers to discard now", int((latest["status"] == "DISCARD").sum()),
                icon=":material/dangerous:", border=True)
    k[1].metric("Discard alerts raised", len(alerts),
                icon=":material/notifications_active:", border=True)
    k[2].metric("Oil sent to RUCO", f"{litres} L", icon=":material/recycling:", border=True)
    k[3].metric("Biodiesel potential", f"{biodiesel:.1f} L",
                icon=":material/local_gas_station:", border=True)
    k[4].metric("CO₂ avoided", f"{biodiesel * CO2_PER_L_BIODIESEL:.1f} kg",
                icon=":material/eco:", border=True)

    # ---- Gauges ----
    st.subheader("Oil quality now: Total Polar Compounds (rolling average)",
                 icon=":material/speed:", anchor=False)
    for col, (_, row) in zip(st.columns(len(latest)), latest.iterrows()):
        with col:
            st.markdown(f"**{row.fryer_name}**  \n:gray[:material/device_thermostat: "
                        f"{row.temperature} °C · :material/water_drop: "
                        f"{FRYERS[row.fryer_id]['litres']} L]", text_alignment="center")
            st.plotly_chart(gauge(row), width="stretch", config=PLOT_CONFIG,
                            key=f"gauge_{row.fryer_id}")
            source = "live slope" if row.rate_source == "live" else "model prior"
            st.markdown(f"{status_badge(row.status)}  \n:gray[:material/trending_up: "
                        f"{row.rate_per_hour:.2f} %/h ({source})]", text_alignment="center")

    # ---- Hours left + time in status ----
    left, right = st.columns(2)
    with left:
        chart_heading("timer", "Hours of frying left")
        st.plotly_chart(hours_left_chart(latest), width="stretch", config=PLOT_CONFIG,
                        key="hours_left")
    with right:
        chart_heading("traffic", "Time spent in each status")
        st.plotly_chart(status_time_chart(cycle), width="stretch", config=PLOT_CONFIG,
                        key="status_time")

    # ---- Main TPC trend ----
    show_raw = st.session_state.get("show_raw", False)
    chart_heading("show_chart", "TPC over frying time (current oil cycle)")
    st.plotly_chart(trend_chart(cycle, "tpc_rolling", "TPC % (rolling avg)",
                                show_limits=True, show_raw=show_raw),
                    width="stretch", config=PLOT_CONFIG, key="tpc_trend")

    # ---- Other quality indicators ----
    a, c = st.columns(2)
    with a:
        chart_heading("science", "Acid value")
        st.plotly_chart(trend_chart(cycle, "acid_value", "mg KOH/g", height=300),
                        width="stretch", config=PLOT_CONFIG, key="acid")
    with c:
        chart_heading("science", "Carbonyl value")
        st.plotly_chart(trend_chart(cycle, "carbonyl_value", "µmol/g", height=300),
                        width="stretch", config=PLOT_CONFIG, key="carbonyl")

    # ---- ESG impact ----
    st.subheader("ESG impact: oil diverted to biodiesel (RUCO)", icon=":material/eco:",
                 anchor=False)
    e1, e2 = st.columns(2)
    with e1:
        chart_heading("recycling", "Used oil sent to RUCO, by fryer")
        st.plotly_chart(esg_by_fryer_chart(alerts), width="stretch", config=PLOT_CONFIG,
                        key="esg_fryer")
    with e2:
        chart_heading("local_gas_station", "Biodiesel potential over time")
        if alerts.empty:
            st.info("No oil collected yet: the first discard alert starts the biodiesel count.",
                    icon=":material/info:")
        else:
            st.plotly_chart(esg_cumulative_chart(alerts), width="stretch",
                            config=PLOT_CONFIG, key="esg_time")

    # ---- Data quality + alert log ----
    q1, q2 = st.columns(2)
    with q1:
        chart_heading("cleaning_services", "Spark data cleaning, by fryer")
        st.plotly_chart(data_quality_chart(readings), width="stretch",
                        config=PLOT_CONFIG, key="quality")
    with q2:
        chart_heading("notifications", "Discard alert log")
        if alerts.empty:
            st.write("No fryer has reached the 25% limit yet.")
        else:
            st.dataframe(alerts.iloc[::-1][["fryer_name", "data_hour", "tpc_rolling"]]
                         .rename(columns={"fryer_name": "Fryer", "data_hour": "Hour",
                                          "tpc_rolling": "TPC %"}),
                         hide_index=True, width="stretch", height=260)


st.toggle("Show raw sensor readings on the TPC chart (red × = outlier removed by Spark)",
          key="show_raw")
live_view()

st.caption(f"ESG assumptions: oil volume per fryer as shown on each gauge; 1 L used oil → "
           f"{BIODIESEL_YIELD} L biodiesel; {CO2_PER_L_BIODIESEL} kg CO₂ avoided per L biodiesel "
           f"(diesel ≈ 2.68 kg CO₂/L, UCO biodiesel cuts lifecycle emissions by ~80%). "
           f"Acid and carbonyl values are shown as measured (only TPC is used for alerts).")
