from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Sample data
users = [f"user_{i}" for i in range(1000)]
products = [f"product_{i}" for i in range(500)]
locations = ["US", "UK", "DE", "FR", "IN", "JP", "BR", "CA", "AU"]

def generate_transaction():
    return {
        "transaction_id": f"txn_{int(time.time() * 1000)}",
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": "USD",
        "location": random.choice(locations),
        "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        "timestamp": datetime.utcnow().isoformat(),
        # 5% chance of being fraudulent
        "is_fraud": random.random() < 0.05  
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

while True:
    transaction = generate_transaction()
    producer.send('ecommerce-transactions', value=transaction)
    print(f"Sent transaction: {transaction['transaction_id']}")
    time.sleep(random.uniform(0.1, 0.5))  # Simulate variable rate
