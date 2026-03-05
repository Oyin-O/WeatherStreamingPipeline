import schedule
import time
import logging
from spark.data_quality import check_pipeline_lag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


schedule.every(5).minutes.do(check_pipeline_lag)

if __name__ == "__main__":
    logger.info("🔍 Pipeline Lag Monitor started — checking every 5 minutes")
    check_pipeline_lag()
    while True:
        schedule.run_pending()
        time.sleep(30)