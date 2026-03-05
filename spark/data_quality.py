import logging
from datetime import datetime, timezone
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import streamlit as st

from producer.config import CITIES
from spark.email_alerts import send_pipeline_error
from spark.transformation_config import DATA_QUALITY
from dashboard.dbconfig import DB_URL

LOG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'data_quality.log'
)

logger = logging.getLogger('data_quality')
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


# ── Data Quality Checks ────────────────────────────────────────
EXPECTED_CITY_COUNT = len(CITIES)
TEMP_MIN = DATA_QUALITY['temp_min']
TEMP_MAX = DATA_QUALITY['temp_max']
MAX_LAG_MINUTES = DATA_QUALITY['max_lag_minutes']
CRITICAL_FIELDS = DATA_QUALITY['critical_fields']

load_dotenv()

CRITICAL_FIELDS = [
    'temperature_c', 'wind_speed_kmh',
    'weather_code', 'humidity_pct',
    'apparent_temperature_c'
]


def run_data_quality_checks(records: list[dict], batch_id: int) -> list[dict]:
    """Run data quality checks on a batch of records before saving."""
    issues = []

    # ── Check 1: Record Count ──────────────────────────────────
    if len(records) < EXPECTED_CITY_COUNT:
        msg = f"Batch {batch_id}: Only {len(records)}/{EXPECTED_CITY_COUNT} cities received"
        logger.warning(f"[Record Count] {msg}")
        issues.append({"check": "Record Count", "severity": "critical", "message": msg})
    else:
        logger.info(f"[Record Count] {len(records)}/{EXPECTED_CITY_COUNT} cities ✓")

    # ── Check 2: Null Fields ───────────────────────────────────
    for record in records:
        city = record.get('city', 'Unknown')
        for field in CRITICAL_FIELDS:
            if record.get(field) is None:
                msg = f"NULL {field} for {city} in batch {batch_id}"
                logger.warning(f"[Null Fields] {msg}")
                issues.append({"check": "Null Fields", "severity": "warning", "message": msg})

    if not any(i['check'] == 'Null Fields' for i in issues):
        logger.info(f"[Null Fields] No nulls detected in batch {batch_id} ✓")

    # ── Check 3: Duplicates ────────────────────────────────────
    cities = [r.get('city') for r in records]
    seen = set()
    for city in cities:
        if city in seen:
            msg = f"Duplicate city {city} in batch {batch_id}"
            logger.warning(f"[Duplicates] {msg}")
            issues.append({"check": "Duplicates", "severity": "warning", "message": msg})
        seen.add(city)

    if not any(i['check'] == 'Duplicates' for i in issues):
        logger.info(f"[Duplicates] No duplicates in batch {batch_id} ✓")

    # ── Check 4: Anomalous Temperatures ───────────────────────
    for record in records:
        city = record.get('city', 'Unknown')
        temp = record.get('temperature_c')
        if temp is not None and (temp < TEMP_MIN or temp > TEMP_MAX):
            msg = f"{city} has anomalous temperature: {temp}°C (expected {TEMP_MIN}–{TEMP_MAX}°C)"
            logger.warning(f"[Temperature Anomaly] {msg}")
            issues.append({"check": "Temperature Anomaly", "severity": "warning", "message": msg})

    if not any(i['check'] == 'Temperature Anomaly' for i in issues):
        logger.info(f"[Temperature Anomaly] All temperatures within range ✓")

    return issues


def check_pipeline_lag():
    logger.info("🔍 Checking pipeline lag / data freshness...")
    try:
        conn = psycopg2.connect(DB_URL)
        df = pd.read_sql_query(
            "SELECT city, timestamp FROM current_weather ORDER BY timestamp ASC",
            conn
        )
        conn.close()

        if df.empty:
            logger.critical("🔴 [Pipeline Lag] No data in current_weather table!")
            return

        now_utc = datetime.now(timezone.utc)
        stale_cities = []

        for _, row in df.iterrows():
            ts = row['timestamp']
            try:
                # ✅ Simple UTC comparison — no timezone conversion needed!
                ts_dt = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
                lag_minutes = (now_utc - ts_dt).total_seconds() / 60
                if lag_minutes > MAX_LAG_MINUTES:
                    stale_cities.append(f"{row['city']} ({lag_minutes:.0f}m ago)")
            except Exception:
                continue

        if stale_cities:
            logger.warning(f"⚠️ [Pipeline Lag] Stale data: {', '.join(stale_cities)}")
            try:
                from spark.email_alerts import send_pipeline_error
                send_pipeline_error(f"Stale data detected:\n{chr(10).join(stale_cities)}")
            except Exception as e:
                logger.error(f"Failed to send email: {e}")
        else:
            logger.info(f"✅ [Pipeline Lag] All cities updated within {MAX_LAG_MINUTES} minutes ✓")

    except Exception as e:
        logger.error(f"🔴 [Pipeline Lag] Check failed: {e}")