import logging
import sys
from API_coingecko.extract import fetch_data
from transform import transform_data
from API_coingecko.load import load_data

# Настройка логирования
logging.basicConfig(
    filename="../logs/etl.log",
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
    # Фиксация начала run
    logging.info("ETL started")

    # Extract
    raw_data = fetch_data()
    # Transform
    df = transform_data(raw_data)
    # Load
    load_data(df)

    logging.info("ETL finished")

if __name__ == "__main__":
    # Fail-fast entrypoint: при любой критической ошибке завершаем процесс с non-zero exit code.
    try:
        run_etl()
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        sys.exit(1)
    # raw_data = fetch_data()
    # import json
    #
    # print(json.dumps(raw_data[:1], indent=2, ensure_ascii=False))