# ── Weather Code Descriptions (WMO standard) ───────────────────
WMO_CODES = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Foggy", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    95: "Thunderstorm", 96: "Thunderstorm w/ hail", 99: "Thunderstorm w/ heavy hail"
}

# ── Alert Thresholds ───────────────────────────────────────────
ALERT_THRESHOLDS = {
    "severe": {
        "temp_high": 45,
        "temp_low": -30,
        "wind_speed": 100,
        "weather_codes": [96, 99]
    },
    "warning": {
        "temp_high": 40,
        "temp_low": -20,
        "wind_speed": 70,
        "precipitation": 10,
        "weather_codes": [95]
    },
    "advisory": {
        "temp_high": 35,
        "temp_low": -10,
        "wind_speed": 50,
        "precipitation": 5,
        "weather_codes": [65, 67, 75, 82, 86]
    }
}

# ── Alert Message Thresholds ───────────────────────────────────
ALERT_MESSAGE_THRESHOLDS = {
    "temp_high": 35,
    "temp_low": -10,
    "wind_speed": 50,
    "precipitation": 5,
}

# ── City Timezones ─────────────────────────────────────────────
CITY_TIMEZONES = {
    # Nigeria (WAT - UTC+1)
    "Lagos": "Africa/Lagos",
    "Abuja": "Africa/Lagos",
    "Kano": "Africa/Lagos",
    "Ibadan": "Africa/Lagos",
    "Port Harcourt": "Africa/Lagos",
    "Benin City": "Africa/Lagos",
    "Maiduguri": "Africa/Lagos",
    "Jos": "Africa/Lagos",
    "Enugu": "Africa/Lagos",
    "Kaduna": "Africa/Lagos",

    # Ghana (GMT - UTC+0)
    "Accra": "Africa/Accra",

    # Kenya (EAT - UTC+3)
    "Nairobi": "Africa/Nairobi",

    # South Africa (SAST - UTC+2)
    "Cape Town": "Africa/Johannesburg",
    "Johannesburg": "Africa/Johannesburg",

    # Egypt (EET - UTC+2)
    "Cairo": "Africa/Cairo",

    # Ethiopia (EAT - UTC+3)
    "Addis Ababa": "Africa/Addis_Ababa",

    # Senegal (GMT - UTC+0)
    "Dakar": "Africa/Dakar",
}

DEFAULT_TZ = "UTC"