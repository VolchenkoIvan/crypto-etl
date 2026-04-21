import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from sqlalchemy import text


load_dotenv()

def get_engine():
    """
    Создаем подключение к PostgreSQL через SQLAlchemy
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = int(os.getenv("DB_PORT"))
    database = os.getenv("DB_NAME")
    db_url = f"postgresql+psycopg2://{user}:{password}@" \
             f"{host}:{port}/{database}"

    engine = create_engine(db_url)
    return engine

def load_data(df):
    """
    Загружаем DataFrame в PostgreSQL
    """
    if df.empty:
        logging.warning("No data to load")
        return

    try:
        engine = get_engine()

        df.to_sql(
            name="crypto_prices",
            schema="stg",
            con=engine,
            if_exists="append",
            index=False,
            method="multi"
        )

        logging.info(f"Loaded {len(df)} rows into database")

    except Exception as e:
        logging.error(f"Error while loading data: {e}")

def run_post_load_procedure():
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text("CALL dwh.load_crypto_prices()"))