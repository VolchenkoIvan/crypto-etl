import pandas as pd
import logging
from datetime import datetime

def transform_data(data):
    """
    Преобразует JSON в DataFrame и очищает данные.
    """
    if not data:
        logging.warning("No data to transform")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Выбираем нужные поля
    df = df[[
        "name",
        "symbol",
        "current_price",
        "market_cap"
    ]]

    # Приводим к числовому типа данных
    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["market_cap"] = pd.to_numeric(df["market_cap"], errors="coerce")

    # Переименовываем столбцы
    df.columns = [
        "name",
        "symbol",
        "price",
        "market_cap"
    ]

    # Добавляем timestamp загрузки
    df["load_date_id"] = int(datetime.now().strftime("%Y%m%d"))
    df["load_date_id"] = df["load_date_id"].astype("int32")

    # Очистка данных
    df = df.drop_duplicates()

    # Удаляем строки с null price
    df = df[df["price"].notnull()]

    # Проверка: цена должна быть > 0
    df = df[df["price"] > 0]

    logging.info(f"Transformed data: {len(df)} rows after cleaning")

    return df