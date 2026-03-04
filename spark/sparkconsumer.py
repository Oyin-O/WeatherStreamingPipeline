import logging
import os
import sys
from datetime import datetime, timedelta, timezone
import psycopg2
from dashboard.dbconfig import DB_URL

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType
)

from spark.transformations import transform_record

sys.path.insert(0, '.')

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'weather.db')
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
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS current_weather (
    city TEXT PRIMARY KEY,
    country TEXT,
    latitude REAL,
    longitude REAL,
    timestamp TEXT,
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
    CREATE TABLE IF NOT EXISTS weather_history(
    id SERIAL PRIMARY KEY,
    city TEXT,
    country TEXT,
    latitude REAL,
    longitude REAL,
    timestamp TEXT,
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
    alert_level TEXT,
    alert_message TEXT
    )
    """)

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Postgres database initialised")


def cleanup_old_data(cur):
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cur.execute("DELETE FROM weather_history WHERE timestamp < %s", (cutoff,))
    cur.execute("DELETE FROM weather_alerts WHERE timestamp < %s", (cutoff,))


def write_batch_to_postgres(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    rows = batch_df.collect()
    logger.info(f"Processing batch {batch_id} with {len(rows)} rows")

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        for row in rows:
            record = row.asDict()
            transformed = transform_record(record)

            cur.execute("""
                INSERT INTO current_weather (
                    city, country, latitude, longitude, timestamp,
                    temperature_c, humidity_pct, apparent_temperature_c,
                    precipitation_mm, wind_speed_kmh, wind_gusts_kmh,
                    weather_code, weather_description, pressure_hpa, alert_level
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (city) DO UPDATE SET
                    country = EXCLUDED.country,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    timestamp = EXCLUDED.timestamp,
                    temperature_c = EXCLUDED.temperature_c,
                    humidity_pct = EXCLUDED.humidity_pct,
                    apparent_temperature_c = EXCLUDED.apparent_temperature_c,
                    precipitation_mm = EXCLUDED.precipitation_mm,
                    wind_speed_kmh = EXCLUDED.wind_speed_kmh,
                    wind_gusts_kmh = EXCLUDED.wind_gusts_kmh,
                    weather_code = EXCLUDED.weather_code,
                    weather_description = EXCLUDED.weather_description,
                    pressure_hpa = EXCLUDED.pressure_hpa,
                    alert_level = EXCLUDED.alert_level
            """, (
                transformed['city'], transformed['country'], transformed['latitude'],
                transformed['longitude'], transformed['timestamp'], transformed['temperature_c'],
                transformed['humidity_pct'], transformed['apparent_temperature_c'],
                transformed['precipitation_mm'], transformed['wind_speed_kmh'],
                transformed['wind_gusts_kmh'], transformed['weather_code'],
                transformed['weather_description'], transformed['pressure_hpa'],
                transformed['alert_level']
            ))

            cur.execute("""
                INSERT INTO weather_history (
                    city, country, latitude, longitude, timestamp,
                    temperature_c, humidity_pct, apparent_temperature_c,
                    precipitation_mm, wind_speed_kmh, wind_gusts_kmh,
                    weather_code, weather_description, pressure_hpa, alert_level
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                transformed['city'], transformed['country'], transformed['latitude'],
                transformed['longitude'], transformed['timestamp'], transformed['temperature_c'],
                transformed['humidity_pct'], transformed['apparent_temperature_c'],
                transformed['precipitation_mm'], transformed['wind_speed_kmh'],
                transformed['wind_gusts_kmh'], transformed['weather_code'],
                transformed['weather_description'], transformed['pressure_hpa'],
                transformed['alert_level']
            ))

            if transformed['alert_level'] != 'normal' and transformed['alert_message']:
                cur.execute("""
                    INSERT INTO weather_alerts (city, timestamp, alert_level, alert_message)
                    VALUES (%s, %s, %s, %s)
                """, (
                    transformed['city'], transformed['timestamp'],
                    transformed['alert_level'], transformed['alert_message']
                ))

        cleanup_old_data(cur)
        conn.commit()
        logger.info(f'Batch {batch_id} written to Supabase successfully')

    except Exception as e:
        conn.rollback()
        logger.error(f'Error writing batch {batch_id}: {e}')
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
    query.awaitTermination()


if __name__ == "__main__":
    main()
