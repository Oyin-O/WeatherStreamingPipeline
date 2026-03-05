import json
import logging
import sys
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from producer.config import KAFKA_BOOTSTRAP_SERVERS, CITIES, KAFKA_TOPIC, \
    API_BASE_URL, POLL_INTERVAL_SECONDS, WEATHER_PARAMS

from spark.email_alerts import send_pipeline_error

sys.path.insert(0, '.')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_producer(max_retries: int = 30, retry_delay: int = 5):
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f'CONNECTED to kafka at {KAFKA_BOOTSTRAP_SERVERS}')
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f'RETRY Kafka unavailable ATTEMPT {attempt}/{max_retries} .. retry in {retry_delay}s'
            )
            time.sleep(retry_delay)

    raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts")


def fetch_weather():
    longitude = ','.join(str(c['lon']) for c in CITIES)
    latitude = ','.join(str(c['lat']) for c in CITIES)

    PARAMS = {
        'latitude': latitude,
        'longitude': longitude,
        'current': ','.join(WEATHER_PARAMS),
        'timezone': 'auto'
    }

    response = requests.get(API_BASE_URL, params=PARAMS, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not isinstance(data, list):
        data = [data]

    results = []
    fetch_time = datetime.now(timezone.utc).isoformat()

    for i, city in enumerate(CITIES):
        current = data[i].get('current', {})
        results.append({
            'city': city['name'],
            'country': city['country'],
            'latitude': city['lat'],
            'longitude': city['lon'],
            'timestamp': fetch_time,
            'temperature_c': current.get('temperature_2m'),
            'apparent_temperature_c': current.get('apparent_temperature'),
            'precipitation_mm': current.get('precipitation'),
            'wind_speed_kmh': current.get('wind_speed_10m'),
            'wind_gusts_kmh': current.get('wind_gusts_10m'),
            'weather_code': current.get('weather_code'),
            'pressure_hpa': current.get('surface_pressure'),
            'humidity_pct': current.get('relative_humidity_2m')
        })

    return results


def is_valid_record(record: dict) -> bool:
    required_fields = [
        'city', 'timestamp', 'temperature_c',
        'wind_speed_kmh', 'weather_code'
    ]
    for field in required_fields:
        if record.get(field) is None:
            logger.warning(f"Missing {field} for {record.get('city', 'unknown')}, skipping")
            return False
    return True


def run():
    producer = create_producer()

    logger.info(f'starting weather producer polling every {POLL_INTERVAL_SECONDS}s')

    while True:
        try:
            records = fetch_weather()
            published = 0
            for record in records:
                if not is_valid_record(record):
                    continue
                producer.send(
                    KAFKA_TOPIC,
                    key=record['city'],
                    value=record
                )
                published += 1
            producer.flush()
            logger.info(f'Published weather of {published}/{len(records)} cities')
        except requests.RequestException as e:
            logger.error(f'API request failed: {e}')
            send_pipeline_error(f"API request failed: {e}")
        except Exception as e:
            logger.error(f'Unexpected Error: {e}')
            send_pipeline_error(f"Producer unexpected error: {e}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == '__main__':
    run()
