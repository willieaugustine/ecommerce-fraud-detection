from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transaction-alerts',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for alerts...")
for message in consumer:
    alert = message.value
    if alert['is_anomaly']:
        print(f"🚨 ALERT: Suspicious transaction detected 🚨")
        print(f"Transaction ID: {alert['transaction_id']}")
        print(f"User: {alert['user_id']}")
        print(f"Amount: ${alert['amount']:.2f}")
        print(f"Anomaly Score: {alert['anomaly_score']:.2f}")
        print(f"Timestamp: {alert['timestamp']}")
        print("-" * 50)
