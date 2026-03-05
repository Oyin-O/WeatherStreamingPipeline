import time
from datetime import datetime, timezone
import pytz

import streamlit as st

st.set_page_config(
    page_title="Weather Stream",
    page_icon="🌦",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# local imports (must come after set_page_config)
from sqldatabase import (
    get_aggregate_stats,
    get_active_alerts,
    get_current_weather,
    get_weather_history
)

from streamlit_ui import (
    inject_global_styles,
    render_header,
    render_stats,
    render_city_cards,
    render_alerts,
    render_temperature_chart,
    render_wind_chart,
    render_map,
    render_city_comparison,
    render_download_button
)

REFRESH_INTERVAL = 10

# ── styles ─────────────────────────────────────────────────────
inject_global_styles()

# ── sidebar ────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Controls")
    st.markdown("---")
    hours = st.slider("History window (hours)", 1, 24, 6)
    cities_input = st.text_input(
        "Filter cities (comma-separated)",
        placeholder="Lagos, Abuja, Kano",
        help="Leave blank to show all cities"
    )
    selected_cities = (
        [c.strip() for c in cities_input.split(",") if c.strip()]
        if cities_input else None
    )
    st.markdown("---")
    auto_refresh = st.toggle("Auto-refresh (10s)", value=True)
    st.markdown("""
    <div style="font-size:.7rem; color:#64748b; margin-top:8px;">
        Pipeline: Open-Meteo → Kafka → Spark → Supabase (PostgreSQL) → Streamlit
    </div>
    """, unsafe_allow_html=True)

# ── data fetch ─────────────────────────────────────────────────
now_str = datetime.now(timezone.utc)
stats = get_aggregate_stats()
current_df = get_current_weather()
alerts_df = get_active_alerts()
history_df = get_weather_history(cities=selected_cities, hours=hours)

# ── layout ─────────────────────────────────────────────────────
render_header(last_updated=now_str)
render_stats(stats)

st.markdown("---")

# city cards + alerts
col_cards, col_alerts = st.columns([2, 1], gap="large")
with col_cards:
    render_city_cards(current_df)
with col_alerts:
    render_alerts(alerts_df)

st.markdown("---")

# charts row
col_temp, col_wind = st.columns([3, 2], gap="large")
with col_temp:
    render_temperature_chart(history_df)
with col_wind:
    render_wind_chart(current_df)

st.markdown("---")

# map
render_map(current_df)

st.markdown("---")

# city comparison
render_city_comparison(current_df, history_df)

st.markdown('---')
render_download_button(current_df, history_df)

# ── auto refresh ───────────────────────────────────────────────
if auto_refresh:
    time.sleep(REFRESH_INTERVAL)
    st.rerun()
