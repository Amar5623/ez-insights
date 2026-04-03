from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from faker import Faker
from bson import ObjectId
import random
import time

fake = Faker()

MONGO_URI = "mongodb://aabb22063:mongodb1234@ac-cztlnzr-shard-00-00.f0id4p3.mongodb.net:27017,ac-cztlnzr-shard-00-01.f0id4p3.mongodb.net:27017,ac-cztlnzr-shard-00-02.f0id4p3.mongodb.net:27017/?ssl=true&replicaSet=atlas-pm8qkz-shard-0&authSource=admin&appName=Cluster0"

client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=15000,
    socketTimeoutMS=60000,
    connectTimeoutMS=15000,
    retryWrites=True,
)
db = client["ecom_analytics_v3"]

users_col    = db["users"]
products_col = db["products"]
orders_col   = db["orders"]

# ── RESET ──────────────────────────────────────────────
users_col.delete_many({})
products_col.delete_many({})
orders_col.delete_many({})
print("Collections cleared.")

# ── USERS (10) ─────────────────────────────────────────
users = [
    {
        "_id": ObjectId(),          # pre-assign _id
        "name": fake.name(),
        "email": fake.unique.email(),
        "country": fake.country(),
        "created_at": fake.date_time_this_year(),
    }
    for _ in range(10)
]
user_ids = users_col.insert_many(users).inserted_ids
print(f"Users inserted: {len(user_ids)}")

# ── PRODUCTS (20) ──────────────────────────────────────
categories = ["Electronics", "Fashion", "Home", "Sports", "Books"]
products_docs = [
    {
        "_id": ObjectId(),          # pre-assign _id
        "name": fake.word().capitalize(),
        "category": random.choice(categories),
        "price": round(random.uniform(50, 2000), 2),
        "stock": random.randint(5, 100),
    }
    for _ in range(20)
]
products_col.insert_many(products_docs)
products = list(products_col.find())
print(f"Products inserted: {len(products)}")

# ── ORDERS (100) ───────────────────────────────────────
statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
orders = []

for _ in range(100):
    num_items = random.randint(1, min(3, len(products)))
    selected  = random.sample(products, num_items)

    items        = []
    total_amount = 0.0

    for p in selected:
        qty = random.randint(1, 2)
        items.append({
            "product_id": p["_id"],
            "name":       p["name"],
            "quantity":   qty,
            "price":      p["price"],
        })
        total_amount += qty * p["price"]

        orders.append({
            "_id":          ObjectId(),     # pre-assign _id — prevents mutation issues
            "user_id":      random.choice(user_ids),
            "order_date":   fake.date_time_this_year(),
            "status":       random.choice(statuses),
            "items":        items,
            "total_amount": round(total_amount, 2),
        })

        # ── Insert orders one-by-one with retry + skip duplicates ──
        total_inserted = 0
        total_skipped  = 0

        for order in orders:
            for attempt in range(3):             # retry up to 3 times on network error
                try:
                    orders_col.insert_one(order)
                    total_inserted += 1
                    break                        # success → move to next order
                except DuplicateKeyError:
                    total_skipped += 1           # already inserted → skip silently
                    break
                except Exception as e:
                    if attempt < 2:
                        print(f"  Retrying order {order['_id']} (attempt {attempt+1}): {e}")
                        time.sleep(1)            # wait 1s before retry
                    else:
                        print(f"  Failed to insert order {order['_id']} after 3 attempts: {e}")

                        print(f"Orders inserted: {total_inserted}  |  Skipped (duplicates): {total_skipped}")
                        print("Done.")