import ijson

i = 0

with open("crypto_nested_large.json", "r", encoding="utf-8") as f:
    users = ijson.items(f, "users.item")

    for user in users:
        print(user["email"])

        for tx in user["transactions"]:
            print(tx["symbol"], tx["amount"])
            i += 1
