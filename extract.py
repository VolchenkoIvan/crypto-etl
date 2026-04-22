import requests
import logging
from dotenv import load_dotenv
import os

load_dotenv()


def fetch_data():
    """
    Получаем данные с API и возвращаем JSON.
    """
    api_url = os.getenv("API_URL")
    params = {
        "vs_currency": "usd"
    }
    try:
        # Фиксируем старт шага Extract
        logging.info(f"Extract step started")
        # Fail-fast: сразу валим джобу при некорректной конфигурации API, чтобы не маскировать проблему пустыми данными.
        if not api_url:
            raise ValueError("API_URL is not set")

        response = requests.get(api_url, params=params)
        response.raise_for_status()  # если ошибка — исключение

        data = response.json()

        # Фиксируем финиш шага Extract
        logging.info(f"Extract step finished: rows={len(data)}")
        return data

    except Exception as e:
        # Fail-fast: логируем traceback и пробрасываем исключение, чтобы ETL завершился с ошибкой и корректным статусом.
        logging.exception(f"Error while fetching data: {e}")
        raise