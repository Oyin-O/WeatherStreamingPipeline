import schedule
import time
import logging
import psycopg2
import pandas as pd
from dashboard.db_config import DB_URL
from consumer.email_alerts import send_daily_summary

logger = logging.getLogger(__name__)


def run_daily_summary():
    try:
        conn = psycopg2.connect(DB_URL)
        df = pd.read_sql_query("SELECT * FROM current_weather", conn)
        conn.close()
        records = df.to_dict("records")
        send_daily_summary(records)
        logger.info("Daily summary email sent")
    except Exception as e:
        logger.error(f"Failed to send daily summary: {e}")


# send every day at 8am WAT
schedule.every().day.at("10:35").do(run_daily_summary)

if __name__ == "__main__":
    logger.info("Scheduler started — daily summary at 08:00 WAT")
    while True:
        schedule.run_pending()
        time.sleep(60)