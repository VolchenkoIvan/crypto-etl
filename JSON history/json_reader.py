import json
from pathlib import Path
import pandas as pd
import logging
from API_coingecko.load import get_engine

def _flush_batch(batch: list[dict], engine) -> int:
    if not batch:
        return 0

    df = pd.DataFrame(batch)
    df["source"] = "jsonl"
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
    file_path = Path(__file__).with_name("crypto_purchases.jsonl")
    loaded_rows = 0
    invalid_rows = 0
    batch: list[dict] = []
    engine = get_engine()

    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    logging.info(
        "JSON load started | file=%s ",
        file_path,
    )
    with file_path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                invalid_rows += 1
                logging.warning("Skip line %s: invalid JSON (%s)", line_no, exc)
                continue

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