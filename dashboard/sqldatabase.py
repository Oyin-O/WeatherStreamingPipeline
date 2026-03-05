import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

DB_URL = os.getenv('DB_URL')


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
            SELECT *, CAST(timestamp AS TIMESTAMP) as parsed_time
            FROM weather_history
            WHERE CAST(timestamp AS TIMESTAMP) >= NOW() - (%s * INTERVAL '1 hour')
        """
        params = [hours]

        if cities:
            placeholders = ','.join('%s' for _ in cities)
            query += f" AND city IN ({placeholders})"
            params.extend(cities)

        query += " ORDER BY parsed_time ASC"

        df = pd.read_sql_query(query, conn, params=params)
        df['timestamp'] = pd.to_datetime(df['parsed_time'])
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
                    SELECT DISTINCT ON (city) 
                        city, timestamp, alert_level, alert_message
                    FROM weather_alerts
                    WHERE CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '6 hours'
                    AND alert_level != 'normal'
                    ORDER BY city, CAST(timestamp AS TIMESTAMP) DESC
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
