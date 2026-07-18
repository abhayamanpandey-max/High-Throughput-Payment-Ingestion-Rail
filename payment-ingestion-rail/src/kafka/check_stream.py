import json
import time
from confluent_kafka import Consumer, KafkaError

print("🚀 Starting Stream Analyzer Engine...")

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'live-firehose-debugger-group',
    'auto.offset.reset': 'earliest', # Read from the beginning so we always see data instantly
    'enable.auto.commit': False
}

consumer = Consumer(consumer_config)
consumer.subscribe(['payment_transactions'])

print("⏳ Waiting for Kafka coordinator to assign partitions (takes a few seconds)...")

# Give the cluster a quick moment to handle group rebalance
time.sleep(3)

print("✅ Connection verified! Listening for live blocks (Press CTRL+C to quit)...\n")

try:
    empty_polls = 0
    while True:
        msg = consumer.poll(timeout=1.0) # Wait up to 1 second for data to hit
        
        if msg is None:
            empty_polls += 1
            if empty_polls % 5 == 0:
                print("   [~] Still listening... (Producer running in other window?)")
            continue
            
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"[-] Broker Error: {msg.error()}")
            continue

        # Data found! Reset empty counter
        empty_polls = 0
        tx = json.loads(msg.value().decode('utf-8'))
        print(f"⚡ DATA -> ID: {tx.get('transaction_id')[:8]}... | Account: {tx.get('account_id')} | Amt: ₹{tx.get('amount')} | Merchant: {tx.get('merchant')}")

except KeyboardInterrupt:
    print("\n🛑 Diagnostic tool closing down gracefully.")
finally:
    consumer.close()
    