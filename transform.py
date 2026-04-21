import pandas as pd
import logging
from datetime import datetime


def transform_data(data):
    """
    Преобразуем JSON в DataFrame и очищаем данные.
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

    # Приводим к числовому типу данных
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
    df["date_id"] = int(datetime.now().strftime("%Y%m%d"))
    df["date_id"] = df["date_id"].astype("int32")
    df["hour_id"] = int(datetime.now().strftime("%H"))
    df["hour_id"] = df["hour_id"].astype("int16")

    # Очистка данных
    df = df.drop_duplicates()
    # Удаляем строки с null price
    df = df[df["price"].notnull()]
    # Проверка: цена должна быть > 0
    df = df[df["price"] > 0]

    logging.info(f"Transformed data: {len(df)} rows after cleaning")

    return df