import logging
from pathlib import Path
import ijson
import pandas as pd
from load import get_engine
import sys

# Настройка логирования
logging.basicConfig(
    filename=r"D:\PycharmProjects\crypto-etl\logs\etl_json_load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def _flush_batch(batch: list[dict], engine) -> int:
    if not batch:
        return 0

    df = pd.DataFrame(batch)
    df["source"] = "json large"
    with engine.begin() as conn:
        df.to_sql(
            name="purchases_history",
            schema="stg",
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
    return len(df)

def json_reader() -> None:
    batch_size = 5000
    file_path = Path(__file__).with_name("crypto_nested_large.jsonl")
    loaded_rows = 0
    invalid_rows = 0
    batch: list[dict] = []
    engine = get_engine()

    logging.info(
        "JSON-large load started | file=%s ",
        file_path,
    )

    file_date = 0
    with open("crypto_nested_large.json", "r", encoding="utf-8") as f:
        for prefix, event, value in ijson.parse(f):
            if prefix == "file_date":
                file_date = value
                break

    with open("crypto_nested_large.json", "r", encoding="utf-8") as f:
        users = ijson.items(f, "users.item")

        for user in users:
            for tx in user["transactions"]:
                record = {
                    'transaction_id': tx["transaction_id"],
                    'full_name':user["full_name"],
                    'email': user["email"],
                    'city': user["city"],
                    'symbol': tx["symbol"],
                    'amount': tx["amount"],
                    'exchange': tx["exchange"],
                    'file_date': file_date
                }
            batch.append(record)
            if len(batch) >= batch_size:
                inserted = _flush_batch(batch, engine)
                loaded_rows += inserted
                logging.info("Batch inserted: %s rows (total=%s)", inserted, loaded_rows)
                batch = []

    if batch:
        inserted = _flush_batch(batch, engine)
        loaded_rows += inserted
        logging.info("Final batch inserted: %s rows (total=%s)", inserted, loaded_rows)

    logging.info(
        "JSON load finished | loaded=%s | invalid=%s",
        loaded_rows,
        invalid_rows,
    )


if __name__ == "__main__":
    # При любой критической ошибке завершаем процесс с non-zero exit code.
    try:
        json_reader()
    except Exception as e:
        logging.exception("ETL failed: %s", e)
        sys.exit(1)