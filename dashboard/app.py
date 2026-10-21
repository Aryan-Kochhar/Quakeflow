"""QuakeFlow dashboard: reads the dbt marts from DuckDB (read-only)."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(os.environ.get("QUAKEFLOW_DB", Path.cwd() / "data" / "warehouse.duckdb"))

st.set_page_config(page_title="QuakeFlow", page_icon="🌍", layout="wide")


@st.cache_data(ttl=300)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        return con.execute(sql, list(params)).df()


if not DB_PATH.exists():
    st.error(f"No warehouse found at `{DB_PATH}`. Run `quakeflow run` first.")
    st.stop()

bounds = query("select min(event_date) as lo, max(event_date) as hi from marts.fct_earthquakes")
lo, hi = bounds.loc[0, "lo"], bounds.loc[0, "hi"]
if pd.isna(lo):
    st.warning("The warehouse is empty. Run `quakeflow run` to load data.")
    st.stop()

st.title("🌍 QuakeFlow — global earthquake activity")
st.caption("Source: USGS FDSN event API · pipeline: Python → DuckDB → dbt → Streamlit")

with st.sidebar:
    st.header("Filters")
    start, end = st.date_input(
        "Date range (UTC)", value=(max(lo, hi - timedelta(days=30)), hi), min_value=lo, max_value=hi
    )
    min_mag = st.slider("Minimum magnitude for map", 0.0, 7.0, 2.5, 0.5)

range_params = (start, end)

kpi = query(
    """
    select count(*) as quakes,
           count(*) filter (where magnitude >= 4.5) as m45,
           max(magnitude) as max_mag,
           count(distinct region) as regions
    from marts.fct_earthquakes where event_date between ? and ?
    """,
    range_params,
).iloc[0]
biggest = query(
    """
    select magnitude, place, event_time from marts.fct_earthquakes
    where event_date between ? and ? order by magnitude desc limit 1
    """,
    range_params,
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Earthquakes", f"{int(kpi.quakes):,}")
c2.metric("M4.5+", f"{int(kpi.m45):,}")
c3.metric("Largest", f"M{kpi.max_mag:.1f}" if pd.notna(kpi.max_mag) else "—")
c4.metric("Regions", f"{int(kpi.regions):,}")
if not biggest.empty:
    b = biggest.iloc[0]
    st.caption(
        f"Largest event: **M{b.magnitude:.1f}** — {b.place} at {b.event_time:%Y-%m-%d %H:%M} UTC"
    )

daily = query(
    "select * from marts.mart_daily_activity where event_date between ? and ? order by event_date",
    range_params,
)
left, right = st.columns([2, 1])
with left:
    st.subheader("Daily activity")
    fig = px.bar(
        daily,
        x="event_date",
        y=["m45_plus", "m25_plus"],
        barmode="overlay",
        labels={"value": "earthquakes", "event_date": "", "variable": ""},
    )
    fig.for_each_trace(lambda t: t.update(name={"m45_plus": "M4.5+", "m25_plus": "M2.5+"}[t.name]))
    st.plotly_chart(fig, use_container_width=True)
with right:
    st.subheader("By magnitude band")
    bands = query(
        """
        select magnitude_band, count(*) as quakes, min(magnitude) as sort_key
        from marts.fct_earthquakes where event_date between ? and ?
        group by 1 order by sort_key
        """,
        range_params,
    )
    st.plotly_chart(
        px.bar(
            bands,
            x="quakes",
            y="magnitude_band",
            orientation="h",
            log_x=True,
            labels={"magnitude_band": "", "quakes": "earthquakes (log)"},
        ),
        use_container_width=True,
    )

st.subheader("Where they happened")
points = query(
    """
    select latitude, longitude, magnitude, depth_km, place, event_time
    from marts.fct_earthquakes
    where event_date between ? and ? and magnitude >= ?
    """,
    (*range_params, min_mag),
)
geo = px.scatter_geo(
    points,
    lat="latitude",
    lon="longitude",
    color="depth_km",
    size=points["magnitude"].clip(lower=0.1),
    hover_name="place",
    hover_data={"magnitude": True, "event_time": True},
    color_continuous_scale="Viridis_r",
    projection="natural earth",
    size_max=18,
)
geo.update_layout(height=520, margin=dict(l=0, r=0, t=0, b=0))
st.plotly_chart(geo, use_container_width=True)

st.subheader("Most energetic regions (all time)")
st.dataframe(
    query(
        """
        select energy_rank as rank, region, earthquakes, m45_plus, max_magnitude,
               median_depth_km, energy_share_pct as "energy share %", last_event_at
        from marts.mart_region_activity order by energy_rank limit 15
        """
    ),
    hide_index=True,
    use_container_width=True,
)

with st.expander("Pipeline health"):
    st.dataframe(
        query(
            """
            select run_id, status, started_at, window_start, window_end, fetched,
                   inserted, updated, unchanged, quarantined, error
            from raw.pipeline_runs order by started_at desc limit 20
            """
        ),
        hide_index=True,
        use_container_width=True,
    )
