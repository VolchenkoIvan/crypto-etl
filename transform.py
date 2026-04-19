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
        "id",
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
        "id",
        "name",
        "symbol",
        "price",
        "market_cap"
    ]

    # Добавляем timestamp загрузки
    df["load_time"] = datetime.now()

    # Очистка данных
    df = df.drop_duplicates()

    # Удаляем строки с null price
    df = df[df["price"].notnull()]

    # Проверка: цена должна быть > 0
    df = df[df["price"] > 0]

    logging.info(f"Transformed data: {len(df)} rows after cleaning")

    return df