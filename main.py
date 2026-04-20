import logging
from extract import fetch_data
from transform import transform_data
from load import load_data
from load import run_post_load_procedure
import time


# Настройка логирования
logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_etl():
    """
    Полный ETL процесс:
    1. Extract
    2. Transform
    3. Load
    """
    logging.info("ETL started")

    # Extract
    raw_data = fetch_data()
    # Transform
    df = transform_data(raw_data)
    # Load
    load_data(df)
    # Run procedue
    run_post_load_procedure()

    logging.info("ETL finished")

if __name__ == "__main__":
    # try:
    #     while True:
    #         run_etl()
    #         time.sleep(3600)
    #
    # except KeyboardInterrupt:
    #     print("ETL stopped by user")
    run_etl()