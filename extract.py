import requests
import logging
from dotenv import load_dotenv
import os

load_dotenv()


def fetch_data():
    """
    Получает данные с API и возвращает JSON.
    """
    api_url = os.getenv("API_URL")
    params = {
        "vs_currency": "usd"
    }
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # если ошибка — исключение

        data = response.json()

        logging.info(f"Fetched {len(data)} records from API")
        return data

    except Exception as e:
        logging.error(f"Error while fetching data: {e}")
        return []