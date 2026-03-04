import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
from datetime import datetime

# ── colour tokens ──────────────────────────────────────────────
BG = "#0a0e1a"
SURFACE = "#111827"
SURFACE2 = "#1a2235"
BORDER = "#1e2d45"
ACCENT = "#00d4ff"
ACCENT2 = "#0080ff"
TEXT = "#e2e8f0"
MUTED = "#64748b"
WARN = "#f59e0b"
DANGER = "#ef4444"
SUCCESS = "#10b981"

ALERT_COLORS = {
    "normal": SUCCESS,
    "advisory": ACCENT,
    "warning": WARN,
    "severe": DANGER,
    "extreme": DANGER,
}

ALERT_ICONS = {
    "normal": "✅",
    "advisory": "🔵",
    "warning": "⚠️",
    "severe": "🔴",
    "extreme": "🚨",
}

# folium marker colours (must be valid folium colours)
FOLIUM_ALERT_COLORS = {
    "normal": "green",
    "advisory": "blue",
    "warning": "orange",
    "severe": "red",
    "extreme": "darkred",
}


def inject_global_styles():
    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

      html, body, [class*="css"] {{
        font-family: 'Syne', sans-serif;
        background-color: {BG};
        color: {TEXT};
      }}
      .stApp {{ background-color: {BG}; }}

      ::-webkit-scrollbar {{ width: 4px; }}
      ::-webkit-scrollbar-track {{ background: {SURFACE}; }}
      ::-webkit-scrollbar-thumb {{ background: {BORDER}; border-radius: 2px; }}

      [data-testid="metric-container"] {{
        background: {SURFACE};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 16px 20px;
      }}
      [data-testid="stMetricValue"] {{
        font-family: 'JetBrains Mono', monospace;
        color: {ACCENT};
        font-size: 1.6rem !important;
      }}
      [data-testid="stMetricLabel"] {{ color: {MUTED}; font-size: .75rem; letter-spacing:.08em; text-transform:uppercase; }}

      h1 {{ font-weight:800; letter-spacing:-.02em; }}
      h2, h3 {{ font-weight:700; color:{TEXT}; }}
      hr {{ border-color: {BORDER}; }}
    </style>
    """, unsafe_allow_html=True)


# ── header banner ──────────────────────────────────────────────
def render_header(last_updated: str = ""):
    st.markdown(f"""
    <div style="
        background: linear-gradient(135deg, {SURFACE} 0%, {SURFACE2} 100%);
        border: 1px solid {BORDER};
        border-left: 4px solid {ACCENT};
        border-radius: 16px;
        padding: 28px 32px;
        margin-bottom: 24px;
    ">
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
            <div>
                <div style="font-size:.7rem; letter-spacing:.15em; color:{MUTED}; text-transform:uppercase; margin-bottom:4px;">
                    LIVE PIPELINE
                </div>
                <h1 style="margin:0; font-size:2rem; color:{TEXT};">
                    🌦 Nigeria Weather Stream
                </h1>
                <div style="color:{MUTED}; font-size:.85rem; margin-top:6px;">
                    Kafka → Spark → Supabase (PostgreSQL) · 10 cities · auto-refresh 10s
                </div>
            </div>
            <div style="text-align:right;">
                <div style="
                    display:inline-flex; align-items:center; gap:8px;
                    background:{BG}; border:1px solid {BORDER};
                    border-radius:20px; padding:6px 14px;
                    font-family:'JetBrains Mono',monospace; font-size:.75rem; color:{ACCENT};
                ">
                    <span style="width:7px;height:7px;border-radius:50%;background:{SUCCESS};
                                 box-shadow:0 0 6px {SUCCESS}; display:inline-block;"></span>
                    LIVE &nbsp;·&nbsp; {last_updated}
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ── aggregate stat row ─────────────────────────────────────────
def render_stats(stats: dict):
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏙️ Cities", stats.get("city_count", 0))
    c2.metric("🌡️ Avg Temp", f"{stats.get('avg_temp_c', 0)}°C")
    c3.metric("💨 Max Wind", f"{stats.get('max_wind_kmh', 0)} km/h")
    c4.metric("🚨 Active Alerts", stats.get("active_alerts", 0))
    c5.metric("📊 Data Points", stats.get("data_points", 0))


# ── city weather cards ─────────────────────────────────────────
def _card(row: dict) -> str:
    level = row.get("alert_level", "normal")
    color = ALERT_COLORS.get(level, SUCCESS)
    icon = ALERT_ICONS.get(level, "✅")
    desc = row.get("weather_description", "—")
    temp = row.get("temperature_c")
    feels = row.get("apparent_temperature_c")
    wind = row.get("wind_speed_kmh")
    precip = row.get("precipitation_mm")
    humid = row.get("humidity_pct")

    temp_str = f"{temp:.1f}°C" if temp is not None else "—"
    feels_str = f"{feels:.1f}°C" if feels is not None else "—"
    wind_str = f"{wind:.1f}" if wind is not None else "—"
    precip_str = f"{precip:.1f}" if precip is not None else "—"
    humid_str = f"{humid:.0f}%" if humid is not None else "—"

    return f"""
    <div style="
        background:{SURFACE};
        border:1px solid {BORDER};
        border-top:3px solid {color};
        border-radius:14px;
        padding:18px 20px;
        height:100%;
    ">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">
            <div>
                <div style="font-size:1rem;font-weight:700;color:{TEXT};">{row.get('city', '—')}</div>
                <div style="font-size:.7rem;color:{MUTED};margin-top:2px;">{row.get('country', '')}</div>
            </div>
            <span style="font-size:1.2rem;">{icon}</span>
        </div>
        <div style="font-family:'JetBrains Mono',monospace;font-size:1.8rem;font-weight:600;color:{ACCENT};margin:8px 0;">
            {temp_str}
        </div>
        <div style="font-size:.75rem;color:{MUTED};margin-bottom:12px;">{desc}</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:.72rem;">
            <div style="color:{MUTED};">Feels like</div><div style="color:{TEXT};text-align:right;">{feels_str}</div>
            <div style="color:{MUTED};">Wind</div><div style="color:{TEXT};text-align:right;">{wind_str} km/h</div>
            <div style="color:{MUTED};">Precip</div><div style="color:{TEXT};text-align:right;">{precip_str} mm</div>
            <div style="color:{MUTED};">Humidity</div><div style="color:{TEXT};text-align:right;">{humid_str}</div>
        </div>
        <div style="
            margin-top:12px;
            padding:4px 10px;
            border-radius:20px;
            background:{color}22;
            border:1px solid {color}55;
            font-size:.65rem;
            color:{color};
            text-transform:uppercase;
            letter-spacing:.08em;
            display:inline-block;
        ">{level}</div>
    </div>
    """


def render_city_cards(df: pd.DataFrame):
    if df.empty:
        st.info("No current weather data yet. Make sure the pipeline is running.")
        return

    st.markdown(f"### 🏙️ Current Conditions")
    rows = df.to_dict("records")
    cols = st.columns(3)
    for i, row in enumerate(rows):
        with cols[i % 3]:
            st.markdown(_card(row), unsafe_allow_html=True)
            st.markdown("<div style='margin-bottom:12px'></div>", unsafe_allow_html=True)


# ── alerts panel ───────────────────────────────────────────────
def render_alerts(alerts_df: pd.DataFrame):
    st.markdown("### 🚨 Active Alerts")
    if alerts_df.empty:
        st.markdown(f"""
        <div style="background:{SURFACE};border:1px solid {BORDER};border-radius:12px;
                    padding:20px;text-align:center;color:{MUTED};font-size:.85rem;">
            ✅ No active alerts — all cities reporting normal conditions
        </div>
        """, unsafe_allow_html=True)
        return

    for _, row in alerts_df.iterrows():
        level = row.get("alert_level", "normal")
        color = ALERT_COLORS.get(level, SUCCESS)
        icon = ALERT_ICONS.get(level, "✅")
        message = row.get("alert_message", "")
        city = row.get("city", "")
        ts = row.get("timestamp", "")
        st.markdown(f"""
        <div style="
            background:{SURFACE};
            border:1px solid {color}44;
            border-left:4px solid {color};
            border-radius:10px;
            padding:14px 18px;
            margin-bottom:8px;
            display:flex; align-items:flex-start; gap:12px;
        ">
            <span style="font-size:1.2rem;">{icon}</span>
            <div style="flex:1;">
                <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;">
                    <span style="font-weight:700;color:{color};">{city} — {level.upper()}</span>
                    <span style="font-size:.7rem;color:{MUTED};font-family:'JetBrains Mono',monospace;">{ts}</span>
                </div>
                <div style="font-size:.8rem;color:{MUTED};margin-top:4px;">{message}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)


# ── temperature chart ──────────────────────────────────────────
def render_temperature_chart(history_df: pd.DataFrame):
    st.markdown("### 🌡️ Temperature Over Time")
    if history_df.empty:
        st.info("No history data yet.")
        return

    fig = px.line(
        history_df,
        x="timestamp",
        y="temperature_c",
        color="city",
        template="plotly_dark",
        labels={"temperature_c": "Temperature (°C)", "timestamp": "Time", "city": "City"},
    )
    fig.update_layout(
        paper_bgcolor=SURFACE,
        plot_bgcolor=BG,
        font_family="Syne",
        font_color=TEXT,
        legend=dict(bgcolor=SURFACE2, bordercolor=BORDER, borderwidth=1),
        margin=dict(l=0, r=0, t=10, b=0),
        xaxis=dict(gridcolor=BORDER, showgrid=True),
        yaxis=dict(gridcolor=BORDER, showgrid=True),
        hovermode="x unified",
    )
    fig.update_traces(line_width=2)
    st.plotly_chart(fig, use_container_width=True)


# ── wind speed chart ───────────────────────────────────────────
def render_wind_chart(current_df: pd.DataFrame):
    st.markdown("### 💨 Wind Speed by City")
    if current_df.empty:
        st.info("No wind data yet.")
        return

    df_sorted = current_df.dropna(subset=['wind_speed_kmh'])
    if df_sorted.empty:
        st.info("No wind speed data available.")
        return

    df_sorted = df_sorted.sort_values("wind_speed_kmh", ascending=True)
    fig = go.Figure(go.Bar(
        x=df_sorted["wind_speed_kmh"],
        y=df_sorted["city"],
        orientation="h",
        marker=dict(
            color=df_sorted["wind_speed_kmh"],
            colorscale=[[0, ACCENT2], [0.5, ACCENT], [1, DANGER]],
            showscale=False,
        ),
        text=df_sorted["wind_speed_kmh"].apply(lambda v: f"{v:.1f} km/h"),
        textposition="outside",
        textfont=dict(color=TEXT, size=11),
    ))
    fig.update_layout(
        paper_bgcolor=SURFACE,
        plot_bgcolor=BG,
        font_family="Syne",
        font_color=TEXT,
        margin=dict(l=0, r=60, t=10, b=0),
        xaxis=dict(gridcolor=BORDER, title="km/h"),
        yaxis=dict(gridcolor="rgba(0,0,0,0)"),
        height=380,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── interactive map ────────────────────────────────────────────
def render_map(current_df: pd.DataFrame):
    st.markdown("### 🗺️ City Weather Map")
    if current_df.empty:
        st.info("No data yet.")
        return

    # centre map on Nigeria
    m = folium.Map(location=[5.0, 20.0], zoom_start=3,
                   tiles="CartoDB dark_matter")

    for _, row in current_df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")
        city = row.get("city", "—")
        temp = row.get("temperature_c")
        wind = row.get("wind_speed_kmh")
        desc = row.get("weather_description", "—")
        level = row.get("alert_level", "normal")
        color = FOLIUM_ALERT_COLORS.get(level, "green")

        if lat is None or lon is None:
            continue

        temp_str = f"{temp:.1f}°C" if temp is not None else "—"
        wind_str = f"{wind:.1f} km/h" if wind is not None else "—"

        popup_html = f"""
        <div style="font-family:sans-serif;min-width:160px;">
            <div style="font-size:14px;font-weight:700;margin-bottom:6px;">📍 {city}</div>
            <div style="font-size:12px;color:#555;margin-bottom:8px;">{desc}</div>
            <table style="font-size:12px;width:100%;">
                <tr><td>🌡️ Temperature</td><td><b>{temp_str}</b></td></tr>
                <tr><td>💨 Wind Speed</td><td><b>{wind_str}</b></td></tr>
                <tr><td>⚠️ Alert</td><td><b>{level.upper()}</b></td></tr>
            </table>
        </div>
        """

        folium.CircleMarker(
            location=[lat, lon],
            radius=12,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=220),
            tooltip=f"{city} — {temp_str}",
        ).add_to(m)

        # city name label
        folium.Marker(
            location=[lat, lon],
            icon=folium.DivIcon(
                html=f'<div style="font-size:10px;font-weight:700;color:white;'
                     f'text-shadow:0 0 3px black;margin-top:14px;white-space:nowrap;">{city}</div>',
                icon_size=(80, 20),
                icon_anchor=(40, 0),
            )
        ).add_to(m)

    st_folium(m, use_container_width=True, height=480)


def render_download_button(current_df: pd.DataFrame, history_df: pd.DataFrame):
    st.markdown("### 📥 Download Data")

    col1, col2 = st.columns(2)

    with col1:
        if not current_df.empty:
            st.download_button(
                label="📥 Download Current Weather",
                data=current_df.to_csv(index=False),
                file_name=f"current_weather_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("No current weather data available.")

    with col2:
        if not history_df.empty:
            st.download_button(
                label="📥 Download Weather History",
                data=history_df.to_csv(index=False),
                file_name=f"weather_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        else:
            st.info("No history data available.")


# ── city comparison ────────────────────────────────────────────
def render_city_comparison(current_df: pd.DataFrame, history_df: pd.DataFrame):
    st.markdown("### ⚖️ City Comparison")

    if current_df.empty:
        st.info("No data yet.")
        return

    cities = sorted(current_df["city"].tolist())

    col_a, col_b = st.columns(2)
    with col_a:
        city_a = st.selectbox("Select City A", cities, index=0, key="city_a")
    with col_b:
        city_b = st.selectbox("Select City B", cities, index=min(1, len(cities) - 1), key="city_b")

    if city_a == city_b:
        st.warning("Please select two different cities.")
        return

    row_a = current_df[current_df["city"] == city_a].iloc[0].to_dict()
    row_b = current_df[current_df["city"] == city_b].iloc[0].to_dict()

    # ── current conditions side by side ───────────────────────
    st.markdown("#### Current Conditions")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(_card(row_a), unsafe_allow_html=True)
    with col2:
        st.markdown(_card(row_b), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── temperature history side by side ──────────────────────
    if not history_df.empty:
        st.markdown("#### 🌡️ Temperature History")
        hist_a = history_df[history_df["city"] == city_a]
        hist_b = history_df[history_df["city"] == city_b]

        col3, col4 = st.columns(2)

        with col3:
            if not hist_a.empty:
                fig_a = px.line(
                    hist_a, x="timestamp", y="temperature_c",
                    title=city_a, template="plotly_dark",
                    labels={"temperature_c": "°C", "timestamp": "Time"},
                    color_discrete_sequence=[ACCENT]
                )
                fig_a.update_layout(
                    paper_bgcolor=SURFACE, plot_bgcolor=BG,
                    font_color=TEXT, margin=dict(l=0, r=0, t=30, b=0),
                    xaxis=dict(gridcolor=BORDER),
                    yaxis=dict(gridcolor=BORDER),
                    showlegend=False
                )
                st.plotly_chart(fig_a, use_container_width=True)

        with col4:
            if not hist_b.empty:
                fig_b = px.line(
                    hist_b, x="timestamp", y="temperature_c",
                    title=city_b, template="plotly_dark",
                    labels={"temperature_c": "°C", "timestamp": "Time"},
                    color_discrete_sequence=[WARN]
                )
                fig_b.update_layout(
                    paper_bgcolor=SURFACE, plot_bgcolor=BG,
                    font_color=TEXT, margin=dict(l=0, r=0, t=30, b=0),
                    xaxis=dict(gridcolor=BORDER),
                    yaxis=dict(gridcolor=BORDER),
                    showlegend=False
                )
                st.plotly_chart(fig_b, use_container_width=True)

        # ── wind speed comparison bar ──────────────────────────
        st.markdown("#### 💨 Wind Speed Comparison")
        wind_data = pd.DataFrame({
            "City": [city_a, city_b],
            "Wind Speed (km/h)": [
                row_a.get("wind_speed_kmh") or 0,
                row_b.get("wind_speed_kmh") or 0
            ]
        })
        fig_wind = px.bar(
            wind_data, x="City", y="Wind Speed (km/h)",
            template="plotly_dark",
            color="City",
            color_discrete_sequence=[ACCENT, WARN],
            text="Wind Speed (km/h)"
        )
        fig_wind.update_traces(texttemplate='%{text:.1f} km/h', textposition='outside')
        fig_wind.update_layout(
            paper_bgcolor=SURFACE, plot_bgcolor=BG,
            font_color=TEXT, margin=dict(l=0, r=0, t=10, b=0),
            xaxis=dict(gridcolor=BORDER),
            yaxis=dict(gridcolor=BORDER),
            showlegend=False, height=300
        )
        st.plotly_chart(fig_wind, use_container_width=True)
