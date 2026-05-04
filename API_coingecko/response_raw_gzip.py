import requests
import os
import ijson
from dotenv import load_dotenv
load_dotenv()
import gzip

def fetch_data():
    """
    This is only the example of byte reading API requests
    """
    api_url = os.getenv("API_URL")
    print(api_url)
    params = {
        "vs_currency": "usd"
    }
    response = requests.get(api_url, params=params, stream=True)
    response.raise_for_status()


    stream = gzip.GzipFile(fileobj=response.raw)

    buffer = []
    # I did with gzip because coingecko returns gzip instead of json
    for item in ijson.items(stream, "item"):
        buffer.append(item)

        if len(buffer) == 20:
            print(f'The byte API request is read')
            buffer.clear()

if __name__ == '__main__':
    fetch_data()