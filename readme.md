# 🌦 Weather Streaming Pipeline

A real-time data engineering pipeline that streams live weather data for cities across Africa, built with **Apache Kafka**, **Apache Spark**, **Supabase (PostgreSQL)**, and **Streamlit**.

![Architecture Diagram](architecture.png)

## Project Overview

This project demonstrates a production-grade streaming data pipeline that:

- Polls the **Open-Meteo API** every 60 seconds for live weather data across 17 African cities
- Publishes weather records to a **Kafka** topic as JSON messages
- Consumes and transforms records using **Apache Spark Structured Streaming**
- Persists current conditions, historical data, and weather alerts to **Supabase (PostgreSQL)**
- Visualises everything on a live **Streamlit dashboard** that auto-refreshes every 10 seconds

The pipeline runs entirely on a local machine with Kafka containerised via Docker


1. **Producer** : fetches weather from Open-Meteo for 17 cities and publishes each city as a JSON message to the `raw_weather` Kafka topic
2. **Kafka** : acts as a message buffer, decoupling the producer from the consumer
3. **Spark Streaming** : reads from Kafka every 30 seconds, transforms each record (adds weather description, alert level, local timezone), and writes to Supabase
4. **Supabase** : stores three tables: current conditions (upserted per city), weather_history (appended), and weather_alerts 
5. **Streamlit** : reads from Supabase and renders a live dashboard with city cards, charts, an interactive map, and city comparisons

## 🛠️ Technologies Used

| Technology | Role | Version           |
|---|---|-------------------|
| **Python** | Core language | 3.11              |
| **Apache Kafka** | Message broker | 7.4.0 (Confluent) |
| **Apache Spark** | Stream processing | 3.5.1             |
| **Docker** | Kafka containerisation | Latest            |
| **Supabase** | Cloud PostgreSQL database | —                 |
| **PostgreSQL** | Database engine | 15                |
| **Streamlit** | Dashboard framework | 1.54.0            |
| **Plotly** | Interactive charts | 6.6.0             |
| **Folium** | Interactive map | 0.20.0            |
| **Open-Meteo** | Weather API (free, no key needed) | —                 |
| **psycopg2** | PostgreSQL Python driver | 2.9.11            |
| **kafka-python** | Kafka Python client | 2.3.0             |
| **pytz** | Timezone conversion | 2025.2            |

---

## 📁 Project Structure

```
WeatherStreamingPipeline/
├── producer/
│   ├── config.py                # Kafka config, API settings, city list
│   └── weather_producer.py      # Fetches weather & publishes to Kafka
├── consumer/
│   ├── spark_consumer.py        # Spark Streaming engine + foreachBatch
│   ├── transformations.py       # Weather description, alerts, timezone logic
│   ├── transformations_config.py  # Alert thresholds, city timezones, WMO codes
│   ├── email_alerts.py          # Email notification system
│   └── scheduler.py             # Daily summary scheduler
├── dashboard/
│   ├── comps.py                # Reusable Streamlit UI components
│   ├── sql_database.py         # PostgreSQL query functions
│   ├── db_config.py            # Database connection using env variables or streamlit secrets
│   └── Streamlitdashboard.py   # Main dashboard entry point
├── .streamlit
│   └── config.toml             # Streamlit server configuration for cloud deployment
├── app.py                      # Root entry point for Streamlit Cloud deployment
├── docker-compose.yml          # Kafka + Zookeeper setup
├── start.sh                    # One-command pipeline startup
├── stop.sh                     # One-command pipeline shutdown
├── requirements.txt            # Python dependencies
└── .env                        # Secrets (gitignored)
```

## 🗺️ Cities Tracked

| City | Country | Timezone |
|---|---|---|
| Lagos, Abuja, Kano, Ibadan, Port Harcourt, Benin City, Maiduguri, Jos, Enugu, Kaduna | Nigeria | WAT (UTC+1) |
| Accra | Ghana | GMT (UTC+0) |
| Nairobi | Kenya | EAT (UTC+3) |
| Cape Town, Johannesburg | South Africa | SAST (UTC+2) |
| Cairo | Egypt | EET (UTC+2) |
| Addis Ababa | Ethiopia | EAT (UTC+3) |
| Dakar | Senegal | GMT (UTC+0) |

### Configuration

Create a `.env` file in the project root:
```bash
DB_URL=postgresql://postgres.xxxx:YOUR_PASSWORD@aws-x-eu-west-x.pooler.supabase.com:5432/postgres
GMAIL_SENDER=your.email@gmail.com
GMAIL_PASSWORD=your-app-password
GMAIL_RECIPIENT=recipient@email.com
```

### Running the Pipeline

```bash
# Start everything with one command
./start.sh
```

Inspiration : https://www.youtube.com/watch?v=gS5ELKQo5cA&t=1115s