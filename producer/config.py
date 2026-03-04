
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = "raw_weather"
API_BASE_URL = "https://api.open-meteo.com/v1/forecast"
POLL_INTERVAL_SECONDS = 60

WEATHER_PARAMS = [
    "temperature_2m",
    "relative_humidity_2m",
    "apparent_temperature",
    "is_day",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_gusts_10m",
    "surface_pressure"
]

# Major Nigerian Cities/States with Country Tag
CITIES = [
    {"name": "Lagos", "country": "Nigeria", "lat": 6.4550, "lon": 3.3841},
    {"name": "Abuja", "country": "Nigeria", "lat": 9.0667, "lon": 7.4833},
    {"name": "Kano", "country": "Nigeria", "lat": 12.0000, "lon": 8.5167},
    {"name": "Ibadan", "country": "Nigeria", "lat": 7.3964, "lon": 3.9167},
    {"name": "Port Harcourt", "country": "Nigeria", "lat": 4.8242, "lon": 7.0336},
    {"name": "Benin City", "country": "Nigeria", "lat": 6.3333, "lon": 5.6222},
    {"name": "Maiduguri", "country": "Nigeria", "lat": 11.8333, "lon": 13.1500},
    {"name": "Jos", "country": "Nigeria", "lat": 9.8965, "lon": 8.8583},
    {"name": "Enugu", "country": "Nigeria", "lat": 6.4525, "lon": 7.5105},
    {"name": "Kaduna", "country": "Nigeria", "lat": 10.5105, "lon": 7.4165},
    {"name": "Accra", "country": "Ghana", "lat": 5.6037, "lon": -0.1870},
    {"name": "Nairobi", "country": "Kenya", "lat": -1.2921, "lon": 36.8219},
    {"name": "Cape Town", "country": "South Africa", "lat": -33.9249, "lon": 18.4241},
    {"name": "Johannesburg", "country": "South Africa", "lat": -26.2041, "lon": 28.0473},
    {"name": "Cairo", "country": "Egypt", "lat": 30.0444, "lon": 31.2357},
    {"name": "Addis Ababa", "country": "Ethiopia", "lat": 9.0320, "lon": 38.7469},
    {"name": "Dakar", "country": "Senegal", "lat": 14.7167, "lon": -17.4677}
]