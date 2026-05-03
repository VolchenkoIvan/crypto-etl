from pathlib import Path
import ijson
import pandas as pd
from API_coingecko.load import get_engine

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
    file_path = Path(__file__).with_name("crypto_nested_large.json")
    loaded_rows = 0
    invalid_rows = 0
    batch: list[dict] = []
    engine = get_engine()

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

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
                    'date_id': file_date
                }
            batch.append(record)
            if len(batch) >= batch_size:
                inserted = _flush_batch(batch, engine)
                loaded_rows += inserted
                batch = []

    if batch:
        inserted = _flush_batch(batch, engine)
        loaded_rows += inserted