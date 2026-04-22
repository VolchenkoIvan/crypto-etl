import time
import requests
import logging
from dotenv import load_dotenv
import os

load_dotenv()
# Конфигурация надежности Extract: выносим timeout/retry/backoff в env-параметры для управляемой эксплуатации без правки кода.
API_TIMEOUT_SEC = int(os.getenv("API_TIMEOUT_SEC"))
API_MAX_RETRIES = int(os.getenv("API_MAX_RETRIES"))
API_BACKOFF_BASE_SEC = float(os.getenv("API_BACKOFF_BASE_SEC"))
# Явно фиксируем набор retryable HTTP-статусов: это временные ошибки, при которых повтор запроса обычно оправдан.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def fetch_data():
    """
    Получаем данные с API и возвращаем JSON.
    """
    api_url = os.getenv("API_URL")
    params = {
        "vs_currency": "usd"
    }
    # Фиксируем старт шага Extract
    logging.info(f"Extract step started")
    try:
        # Fail-fast: сразу валим джобу при некорректной конфигурации API, чтобы не маскировать проблему пустыми данными.
        if not api_url:
            raise ValueError("API_URL is not set")
    except Exception as e:
        # Fail-fast: логируем полный traceback и пробрасываем ошибку, чтобы не получать ложный "успех" ETL.
        logging.exception(f"Error while Extract data: {e}")
        raise

    # Retry-цикл с backoff: повышаем устойчивость к временным сетевым и серверным сбоям внешнего API.
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            logging.info(
                "Extract request attempt %s/%s (timeout=%ss)",
                attempt,
                API_MAX_RETRIES,
                API_TIMEOUT_SEC
            )

            response = requests.get(api_url, params=params, timeout=API_TIMEOUT_SEC)

            # Явная обработка retryable статусов: переводим их в контролируемый retry-сценарий, а не в общий fail.
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise requests.exceptions.HTTPError(
                    f"Retryable HTTP status: {response.status_code}",
                    response=response
                )

            # Неретраимые 4xx/прочие ошибки пробрасываем сразу: это обычно проблема запроса/доступа, а не временный сбой.
            response.raise_for_status()
            data = response.json()

            # Операционный лог финиша шага: фиксируем полезный объем данных для контроля качества загрузки.
            logging.info("Extract step finished: rows=%s", len(data))
            return data

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Временные инфраструктурные ошибки сети: обрабатываем через retry до исчерпания лимита.
            retry_reason = f"{type(e).__name__}: {e}"
        except requests.exceptions.HTTPError as e:
            # Разделяем retryable и non-retryable HTTP ошибки, чтобы не делать бессмысленные повторы.
            status_code = e.response.status_code if e.response is not None else None
            if status_code not in RETRYABLE_STATUS_CODES:
                logging.exception("Extract failed with non-retryable HTTP status: %s", status_code)
                raise
            retry_reason = f"retryable_http_status={status_code}"
        except requests.exceptions.RequestException as e:
            # Прочие RequestException считаем non-retryable: фиксируем traceback и завершаем Extract fail-fast.
            logging.exception("Extract failed with non-retryable request error: %s", e)
            raise
        except ValueError as e:
            # Некорректный JSON-ответ считаем критической проблемой контракта API и завершаем шаг без повторов.
            logging.exception("Extract failed while parsing API JSON payload: %s", e)
            raise

        # После исчерпания попыток пробрасываем ошибку: оркестратор получит non-zero и сможет отреагировать по политике.
        if attempt == API_MAX_RETRIES:
            logging.error("Extract failed after %s attempts: %s", API_MAX_RETRIES, retry_reason)
            raise RuntimeError(f"Extract failed after retries: {retry_reason}")

        # Экспоненциальный backoff снижает нагрузку на API и повышает шанс успешного ответа на следующей попытке.
        sleep_seconds = API_BACKOFF_BASE_SEC * (2 ** (attempt - 1))
        logging.warning(
            "Extract retry in %.1fs (attempt %s/%s, reason=%s)",
            sleep_seconds,
            attempt,
            API_MAX_RETRIES,
            retry_reason
        )
        time.sleep(sleep_seconds)
    return None