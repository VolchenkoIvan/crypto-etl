import json
import random
from datetime import datetime
import uuid

OUTPUT_FILE = "crypto_purchases.jsonl"
NUM_RECORDS = 100000

symbols = [
    "wld","cc","buidl","trump","dexe","siren","usdt","btc","trx","jup","figr_heloc",
    "bcap","xaut","hash","gho","ltc","etc","usdd","hype","algo","leo","usde","pi",
    "xmr","rlusd","xrp","doge","flr","apt","eth","bch","arb","htx","shib","kas",
    "usdy","sol","pol","usdg","bgb","atom","jtrsy","ousg","ustb","usd1","ton","bdx",
    "uni","qnt","ylds","u","ada","stable","bonk","usdc","m","bnb","zec","xdc","near",
    "usyc","nexo","sky","tao","aster","usd0","night","kcs","pengu","bfusd","usds",
    "xlm","pump","usdf","pyusd","mnt","wlfi","jst","dai","paxg","cake","hbar",
    "aave","link","eutbl","rain","avax","vet","usdtb","morpho","sui","icp","fil",
    "ena","dot","pepe","okb","cro","wbt","gt","render","ondo"
]
cities = ["Madrid", "Belgrade", "Paris", "London", "Athens"]
exchanges = ["binance", "coinbase", "kraken"]

first_names = [
    "Ivan","Polina","John","Anna","Mark","Elena","Alex","Maria","Daniel","Olga",
    "Sergey","Nina","Victor","Irina","Pavel","Daria","Mikhail","Ekaterina","Roman","Tatiana",
    "Andrey","Sofia","Denis","Victoria","Kirill","Alina","Oleg","Yulia","Maxim","Natalia"
]

last_names = [
    "Ivanov","Petrov","Sidorov","Smirnov","Kuznetsov","Popov","Vasiliev","Sokolov","Mikhailov","Novikov",
    "Fedorov","Morozov","Volkov","Alekseev","Lebedev","Semenov","Egorov","Pavlov","Kozlov","Stepanov",
    "Nikolaev","Orlov","Andreev","Makarov","Zakharov","Borisov","Grigoriev","Romanov","Voronin","Tarasov"
]

file_date = int(datetime.now().strftime("%Y%m%d"))

def generate_record():
    first = random.choice(first_names)
    last = random.choice(last_names)

    return {
        "transaction_id": str(uuid.uuid4()),
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@mail.com",
        "city": random.choice(cities),
        "symbol": random.choice(symbols),
        "amount": round(random.uniform(0.01, 5), 4),
        "exchange": random.choice(exchanges),
        "date_id": file_date,
    }

# 🔥 запись JSONL
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    for i in range(NUM_RECORDS):
        record = generate_record()
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

        if i % 10000 == 0:
            print(f"{i} records generated")

print("✅ JSONL файл готов!")