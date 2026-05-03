import logging
import sys
from json_large_data_reader import json_reader as jr_large
from json_reader import json_reader as jr
from API_coingecko.load import get_engine
from sqlalchemy import text

# Настройка логирования
logging.basicConfig(
    filename=r"D:\PycharmProjects\crypto-etl\logs\etl_json_load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def start_readers():
    """
    Полный ETL процесс:
    1. Loading data from json using ijson
    2. Loading data from jsonl
    """
    logging.info("ETL started")

    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stg.purchases_history"))

    # Reading large ijson file
    jr_large()
    # Reading jsonl
    jr()

    with engine.begin() as conn:
        conn.execute(text("CALL dwh.load_purchases_history()"))

    logging.info("ETL finished")

if __name__ == "__main__":
    # Fail-fast entrypoint: при любой критической ошибке завершаем процесс с non-zero exit code.
    try:
        start_readers()
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        sys.exit(1)