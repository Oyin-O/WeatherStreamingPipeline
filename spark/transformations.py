from datetime import datetime, timezone
import pytz
from spark.transformation_config import (
    WMO_CODES,
    ALERT_THRESHOLDS,
    ALERT_MESSAGE_THRESHOLDS,
    CITY_TIMEZONES,
    DEFAULT_TZ
)


def get_local_timestamp(raw_ts: str, city: str) -> str:
    if not raw_ts:
        return raw_ts
    try:
        tz_name = CITY_TIMEZONES.get(city, DEFAULT_TZ)
        tz = pytz.timezone(tz_name)
        utc_dt = datetime.fromisoformat(raw_ts.replace('Z', '+00:00'))
        local_dt = utc_dt.astimezone(tz)
        tz_abbr = local_dt.strftime('%Z')  # e.g. WAT, EAT, SAST
        return local_dt.strftime(f'%Y-%m-%d %H:%M:%S {tz_abbr}')
    except Exception:
        return raw_ts


def get_weather_description(code):
    if code is None:
        return 'Unknown'
    return WMO_CODES.get(code, 'Unknown')


def classify_alert(temperature_c: float, wind_speed_kmh: float,
                   precipitation_mm: float, weather_code: int):
    if temperature_c is not None and (temperature_c > 45 or temperature_c < -30):
        return 'severe'
    if wind_speed_kmh is not None and wind_speed_kmh > 100:
        return 'severe'
    if weather_code is not None and weather_code in (96, 99):
        return 'severe'

    if temperature_c is not None and (temperature_c > 40 or temperature_c < -20):
        return 'warning'
    if wind_speed_kmh is not None and wind_speed_kmh > 70:
        return 'warning'
    if precipitation_mm is not None and precipitation_mm > 10:
        return 'warning'
    if weather_code is not None and weather_code == 95:
        return 'warning'

    if temperature_c is not None and (temperature_c > 35 or temperature_c < -10):
        return 'advisory'
    if wind_speed_kmh is not None and wind_speed_kmh > 50:
        return 'advisory'
    if precipitation_mm is not None and precipitation_mm > 5:
        return 'advisory'
    if weather_code is not None and weather_code in (65, 67, 75, 82, 86):
        return 'advisory'

    return 'normal'


def get_alert_message(
        city: str,
        alert_level: str,
        temperature_c: float,
        wind_speed_kmh: float,
        precipitation_mm: float,
        weather_description: str
):
    parts = [f'{city}: {alert_level.upper()} - {weather_description}']

    if temperature_c is not None:
        if temperature_c > 35:
            parts.append(f'Extreme heat {temperature_c:.1f}°C')
        elif temperature_c < -10:
            parts.append(f'Extreme cold {temperature_c:.1f}°C')

    if wind_speed_kmh is not None and wind_speed_kmh > 50:
        parts.append(f'High winds {wind_speed_kmh:.1f} km/h')

    if precipitation_mm is not None and wind_speed_kmh > 50:
        parts.append(f'Heavy precipitation {precipitation_mm:.1f}°C')

    if precipitation_mm is not None and precipitation_mm > 5:
        parts.append(f'Heavy preciptation {precipitation_mm:.1f} mm')

    return ' | '.join(parts)


def transform_record(record):
    temp_c = record.get('temperature_c')
    wind_kmh = record.get('wind_speed_kmh')
    precip_mm = record.get('precipitation_mm')
    code = record.get('weather_code')
    city = record.get('city', 'Unknown')

    # ✅ Convert UTC timestamp to local time
    local_timestamp = get_local_timestamp(record.get('timestamp'), city)

    weather_desc = get_weather_description(code)
    alert_level = classify_alert(temp_c, wind_kmh, precip_mm, code)

    return {
        **record,
        'timestamp': local_timestamp,
        'weather_description': weather_desc,
        'alert_level': alert_level,
        'alert_message': get_alert_message(
            record.get('city', 'Unknown'),
            alert_level, temp_c, wind_kmh, precip_mm, weather_desc
        )
    }
