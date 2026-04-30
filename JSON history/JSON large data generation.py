import json
import random
from datetime import datetime
import uuid
import os

OUTPUT_FILE = "crypto_nested_large.json"
TARGET_SIZE_MB = 100

symbols = [
    "wld", "cc", "buidl", "trump", "dexe", "siren", "usdt", "btc", "trx", "jup", "figr_heloc",
    "bcap", "xaut", "hash", "gho", "ltc", "etc", "usdd", "hype", "algo", "leo", "usde", "pi",
    "xmr", "rlusd", "xrp", "doge", "flr", "apt", "eth", "bch", "arb", "htx", "shib", "kas",
    "usdy", "sol", "pol", "usdg", "bgb", "atom", "jtrsy", "ousg", "ustb", "usd1", "ton", "bdx",
    "uni", "qnt", "ylds", "u", "ada", "stable", "bonk", "usdc", "m", "bnb", "zec", "xdc", "near",
    "usyc", "nexo", "sky", "tao", "aster", "usd0", "night", "kcs", "pengu", "bfusd", "usds",
    "xlm", "pump", "usdf", "pyusd", "mnt", "wlfi", "jst", "dai", "paxg", "cake", "hbar",
    "aave", "link", "eutbl", "rain", "avax", "vet", "usdtb", "morpho", "sui", "icp", "fil",
    "ena", "dot", "pepe", "okb", "cro", "wbt", "gt", "render", "ondo"
]

cities = ["Madrid", "Belgrade", "Paris", "London", "Athens"]
exchanges = ["binance", "coinbase", "kraken"]

first_names = [
    "Ivan", "Polina", "John", "Anna", "Mark", "Elena", "Alex", "Maria", "Daniel", "Olga",
    "Sergey", "Nina", "Victor", "Irina", "Pavel", "Daria", "Mikhail", "Ekaterina", "Roman", "Tatiana",
    "Andrey", "Sofia", "Denis", "Victoria", "Kirill", "Alina", "Oleg", "Yulia", "Maxim", "Natalia"
]

last_names = [
    "Ivanov", "Petrov", "Sidorov", "Smirnov", "Kuznetsov", "Popov", "Vasiliev", "Sokolov", "Mikhailov", "Novikov",
    "Fedorov", "Morozov", "Volkov", "Alekseev", "Lebedev", "Semenov", "Egorov", "Pavlov", "Kozlov", "Stepanov",
    "Nikolaev", "Orlov", "Andreev", "Makarov", "Zakharov", "Borisov", "Grigoriev", "Romanov", "Voronin", "Tarasov"
]

file_date = int(datetime.now().strftime("%Y%m%d"))
TARGET_SIZE_BYTES = TARGET_SIZE_MB * 1024 * 1024


def generate_user():
    first = random.choice(first_names)
    last = random.choice(last_names)

    user = {
        "full_name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}@mail.com",
        "city": random.choice(cities),
        "wallet": {
            "balance_usd": round(random.uniform(100, 100000), 2),
            "is_verified": random.choice([True, False])
        },
        "transactions": []
    }

    # 5–15 транзакций на пользователя
    for _ in range(random.randint(5, 15)):
        user["transactions"].append({
            "transaction_id": str(uuid.uuid4()),
            "symbol": random.choice(symbols),
            "amount": round(random.uniform(0.01, 5), 4),
            "exchange": random.choice(exchanges),
            "date_id": file_date
        })

    return user


# 🔥 streaming запись
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    f.write('{\n')
    f.write(f'"file_date": {file_date},\n')
    f.write('"users": [\n')

    first_record = True
    count = 0

    while os.path.getsize(OUTPUT_FILE) < TARGET_SIZE_BYTES:
        user = generate_user()

        if not first_record:
            f.write(',\n')
        else:
            first_record = False

        json.dump(user, f, ensure_ascii=False)

        count += 1
        if count % 1000 == 0:
            size_mb = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
            print(f"{count} users | {size_mb:.2f} MB")

    f.write('\n]\n}')

print(f"✅ Готово! ~{TARGET_SIZE_MB}MB JSON создан")