import json
import uuid
from datetime import datetime
from confluent_kafka import Producer

print("🔌 Opening explicit connection to Kafka broker...")

config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'diagnostic-producer',
    'acks': 'all' # Guarantee acknowledgment
}

producer = Producer(config)
topic = 'payment_transactions'

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Success! Sent to partition {msg.partition()} at offset {msg.offset()}")

print("📦 Creating diagnostic payment event...")
sample_payload = {
    "transaction_id": str(uuid.uuid4()),
    "account_id": "ACC-TEST-9999",
    "amount": 2500.50,
    "currency": "INR",
    "merchant": "Verification Test Lab",
    "location": "MUMBAI, IN",
    "device_ip": "127.0.0.1",
    "ts_string": datetime.utcnow().isoformat()
}

print(f"🚀 Pushing payload synchronously to topic '{topic}'...")
producer.produce(
    topic=topic,
    key=sample_payload["account_id"],
    value=json.dumps(sample_payload).encode('utf-8'),
    callback=delivery_report
)

# Block until the message is safely transmitted over the socket
print("⏳ Flushing memory buffers to network...")
producer.flush(timeout=5.0)
print("🏁 Diagnostics round complete.")
