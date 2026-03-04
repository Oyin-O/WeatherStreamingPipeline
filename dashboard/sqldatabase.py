import psycopg2
import pandas as pd
from dbconfig import DB_URL
from dotenv import load_dotenv
import os

load_dotenv()

print(DB_URL)

def _get_connection():
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        return None


def get_current_weather() -> pd.DataFrame:
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query("SELECT * FROM current_weather ORDER BY city", conn)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def get_weather_history(cities: list[str] = None, hours: int = 6) -> pd.DataFrame:
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        query = """
            SELECT * FROM weather_history
            WHERE timestamp >= NOW() - INTERVAL '%s hours'
        """
        params = [hours]

        if cities:
            placeholders = ','.join('%s' for _ in cities)
            query += f" AND city IN ({placeholders})"
            params.extend(cities)

        df = pd.read_sql_query(query, conn, params=params)
        return df

    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def get_active_alerts() -> pd.DataFrame:
    conn = _get_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query("""
            SELECT * FROM weather_alerts
            WHERE timestamp >= NOW() - INTERVAL '6 hours'
            ORDER BY timestamp DESC
        """, conn)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def get_aggregate_stats() -> dict:
    conn = _get_connection()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*) as city_count,
                ROUND(AVG(temperature_c)::numeric, 1) as avg_temp_c,
                ROUND(MAX(wind_speed_kmh)::numeric, 1) as max_wind_kmh,
                SUM(CASE WHEN alert_level != 'normal' THEN 1 ELSE 0 END) as active_alerts
            FROM current_weather
        """)
        row = cur.fetchone()

        cur.execute("SELECT COUNT(*) FROM weather_history")
        history_count = cur.fetchone()[0]
        cur.close()

        return {
            "city_count": row[0],
            "avg_temp_c": row[1],
            "max_wind_kmh": row[2],
            "active_alerts": row[3] or 0,
            "data_points": history_count
        }
    except Exception:
        return {}
    finally:
        conn.close()