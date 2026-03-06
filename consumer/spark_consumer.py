import logging
import os
import sys
from datetime import datetime, timedelta, timezone
import psycopg2
from dotenv import load_dotenv


from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType
)

from consumer.transformations import transform_record
from consumer.email_alerts import send_weather_alert, send_pipeline_error
from consumer.data_quality import run_data_quality_checks


load_dotenv()

DB_URL = os.getenv('DB_URL')

sys.path.insert(0, '.')

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHECKPOINT_DIR = os.path.join(PROJECT_ROOT, 'data', 'spark-checkpoints')

KAFKA_SCHEMA = StructType([
    StructField('city', StringType(), True),
    StructField('country', StringType(), True),
    StructField('latitude', DoubleType(), True),
    StructField('longitude', DoubleType(), True),
    StructField('timestamp', StringType(), True),
    StructField('temperature_c', DoubleType(), True),
    StructField('humidity_pct', DoubleType(), True),
    StructField('apparent_temperature_c', DoubleType(), True),
    StructField('precipitation_mm', DoubleType(), True),
    StructField('wind_speed_kmh', DoubleType(), True),
    StructField('wind_gusts_kmh', DoubleType(), True),
    StructField('weather_code', IntegerType(), True),
    StructField('pressure_hpa', DoubleType(), True)
])


def init_db():
    try:
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()

        cur.execute("""
        CREATE TABLE IF NOT EXISTS current_weather (
        city TEXT PRIMARY KEY,
        country TEXT,
        latitude REAL,
        longitude REAL,
        timestamp TEXT,
        local_timestamp TEXT,
        temperature_c REAL,
        humidity_pct REAL,
        apparent_temperature_c REAL,
        precipitation_mm REAL,
        wind_speed_kmh REAL,
        wind_gusts_kmh REAL,
        weather_code INTEGER,
        weather_description TEXT,
        pressure_hpa REAL,
        alert_level TEXT,
        alert_message TEXT
        )
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_history(
        id SERIAL PRIMARY KEY,
        city TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        timestamp TEXT,
        local_timestamp TEXT,
        temperature_c REAL,
        humidity_pct REAL,
        apparent_temperature_c REAL,
        precipitation_mm REAL,
        wind_speed_kmh REAL,
        wind_gusts_kmh REAL,
        weather_code INTEGER,
        weather_description TEXT,
        pressure_hpa REAL,
        alert_level TEXT
        ) 
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_alerts(
        id SERIAL PRIMARY KEY, 
        city TEXT,
        timestamp TEXT,
        local_timestamp TEXT,
        alert_level TEXT,
        alert_message TEXT
        )
        """)

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Postgres database initialised")
    except Exception as e:
        logger.error(f'Failed to initialise database: {e}')
        send_pipeline_error(f"Database initialisation failed: {e}")
        raise


def cleanup_old_data(cur):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cur.execute("DELETE FROM weather_history WHERE timestamp < %s", (cutoff,))
    cur.execute("DELETE FROM weather_alerts WHERE timestamp < %s", (cutoff,))


_sent_alerts = set()


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    records = [row.asDict() for row in rows]
    logger.info(f"Processing batch {batch_id} with {len(rows)} rows")

    logger.info("─" * 40)

    issues = run_data_quality_checks(records, batch_id)

    # ✅ Handle issues
    critical_issues = [i for i in issues if i['severity'] == 'critical']
    warning_issues = [i for i in issues if i['severity'] == 'warning']

    if critical_issues:
        logger.critical(f"🔴 {len(critical_issues)} critical issue(s) found in batch {batch_id}!")
        summary = "\n".join([f"[CRITICAL] {i['check']}: {i['message']}" for i in critical_issues])
        send_pipeline_error(f"Batch {batch_id} critical issues:\n\n{summary}", batch_id)

    if warning_issues:
        logger.warning(f"⚠️ {len(warning_issues)} warning(s) found in batch {batch_id}")
        summary = "\n".join([f"[WARNING] {i['check']}: {i['message']}" for i in warning_issues])
        send_pipeline_error(f"Batch {batch_id} warnings:\n\n{summary}", batch_id)

    if not issues:
        logger.info(f"✅ Batch {batch_id} passed all quality checks")

    logger.info("─" * 40)

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        for record in records:
            transformed = transform_record(record)

            cur.execute("""
                INSERT INTO current_weather (
                    city, country, latitude, longitude, timestamp, local_timestamp,
                    temperature_c, humidity_pct, apparent_temperature_c,
                    precipitation_mm, wind_speed_kmh, wind_gusts_kmh,
                    weather_code, weather_description, pressure_hpa, alert_level, alert_message
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)
                ON CONFLICT (city) DO UPDATE SET
                    country = EXCLUDED.country,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timestamp = EXCLUDED.timestamp,
                    local_timestamp = EXCLUDED.local_timestamp,
                    temperature_c = EXCLUDED.temperature_c,
                    humidity_pct = EXCLUDED.humidity_pct,
                    apparent_temperature_c = EXCLUDED.apparent_temperature_c,
                    precipitation_mm = EXCLUDED.precipitation_mm,
                    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                    wind_gusts_kmh = EXCLUDED.wind_gusts_kmh,
                    weather_code = EXCLUDED.weather_code,
                    weather_description = EXCLUDED.weather_description,
                    pressure_hpa = EXCLUDED.pressure_hpa,
                    alert_level = EXCLUDED.alert_level,
                    alert_message = EXCLUDED.alert_message
            """, (
                transformed['city'], transformed['country'], transformed['latitude'],
                transformed['longitude'], transformed['timestamp'], transformed['local_timestamp'],
                transformed['temperature_c'], transformed['humidity_pct'],
                transformed['apparent_temperature_c'], transformed['precipitation_mm'],
                transformed['wind_speed_kmh'], transformed['wind_gusts_kmh'],
                transformed['weather_code'], transformed['weather_description'],
                transformed['pressure_hpa'], transformed['alert_level'], transformed['alert_message']
            ))

            cur.execute("""
                INSERT INTO weather_history (
                    city, country, latitude, longitude, timestamp,local_timestamp,
                    temperature_c, humidity_pct, apparent_temperature_c,
                    precipitation_mm, wind_speed_kmh, wind_gusts_kmh,
                    weather_code, weather_description, pressure_hpa, alert_level
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                transformed['city'], transformed['country'], transformed['latitude'],
                transformed['longitude'], transformed['timestamp'], transformed['local_timestamp'],
                transformed['temperature_c'],
                transformed['humidity_pct'], transformed['apparent_temperature_c'],
                transformed['precipitation_mm'], transformed['wind_speed_kmh'],
                transformed['wind_gusts_kmh'], transformed['weather_code'],
                transformed['weather_description'], transformed['pressure_hpa'],
                transformed['alert_level']
            ))

            if transformed['alert_level'] != 'normal' and transformed['alert_message']:
                cur.execute("""
                    INSERT INTO weather_alerts (city, timestamp, alert_level, alert_message)
                    SELECT %s, %s, %s, %s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM weather_alerts
                        WHERE city = %s
                        AND alert_level = %s
                        AND CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '1 hour'
                    )
                """, (
                    transformed['city'], transformed['timestamp'],
                    transformed['alert_level'], transformed['alert_message'],
                    transformed['city'], transformed['alert_level']
                ))

            level = transformed['alert_level']
            city = transformed['city']
            country = transformed['country']
            alert_key = f"{city}_{level}"

            if level in ('severe', 'warning') and alert_key not in _sent_alerts:
                send_weather_alert(
                    city=city,
                    country=country,
                    alert_level=level,
                    alert_message=transformed['alert_message'],
                    temperature_c=transformed['temperature_c'],
                    wind_speed_kmh=transformed['wind_speed_kmh']
                )
                _sent_alerts.add(alert_key)

            # clear sent alerts when condition returns to normal
            if level == 'normal':
                _sent_alerts.discard(f"{city}_severe")
                _sent_alerts.discard(f"{city}_warning")

        cleanup_old_data(cur)
        conn.commit()
        logger.info(f'Batch {batch_id} written to Supabase successfully')

    except Exception as e:
        conn.rollback()
        logger.error(f'Error writing batch {batch_id}: {e}')
        send_pipeline_error(str(e), batch_id)
        raise
    finally:
        cur.close()
        conn.close()


def main():
    init_db()
    spark = (
        SparkSession.builder
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true')
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.jars.repositories", "https://repos.spark-packages.org/,https://repo1.maven.org/maven2/")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')

    logger.info("Reading from kafka topic raw_weather")

    kafka_df = (
        spark.readStream
        .format('kafka')
        .option('kafka.bootstrap.servers', "127.0.0.1:9092")
        .option('subscribe', 'raw_weather')
        .option('startingOffsets', 'latest')
        .option('failOnDataLoss', 'false')
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col('json_str'), KAFKA_SCHEMA).alias('data'))
        .select('data.*')
    )

    query = (
        parsed_df.writeStream.foreachBatch(write_batch_to_postgres)  # ← updated
        .option('checkpointLocation', CHECKPOINT_DIR)
        .trigger(processingTime='30 seconds')
        .start()
    )

    logger.info('streaming query started. Waiting for termination ...')
    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f'Streaming query failed: {e}')
        send_pipeline_error(f"Spark streaming query failed: {e}")
        raise


if __name__ == "__main__":
    main()
