import json
import uuid
import time
import random
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from confluent_kafka import Producer
from pydantic import BaseModel, Field

# 1. Strict Schema Enforcement
class PaymentTransaction(BaseModel):
    transaction_id: str
    account_id: str
    amount: float = Field(gt=0)
    currency: str
    merchant: str
    location: str
    device_ip: str
    ts_string: str

# 2. Throughput Tracker
class ThroughputTracker:
    def __init__(self, window_size=10):
        self.window_size = window_size
        self.timestamps = deque()
    
    def record(self):
        current_time = time.time()
        self.timestamps.append(current_time)
        
        while self.timestamps and self.timestamps[0] < current_time - self.window_size:
            self.timestamps.popleft()
        
        if len(self.timestamps) > 1:
            time_span = self.timestamps[-1] - self.timestamps[0]
            if time_span > 0:
                return len(self.timestamps) / time_span
        return 0

# PROFILING: Add timing to each operation
class OperationTimer:
    def __init__(self):
        self.pydantic_time = 0
        self.kafka_time = 0
        self.poll_time = 0
        self.other_time = 0
        self.count = 0
    
    def report(self):
        if self.count == 0:
            return
        print(f"\n⏱️  TIMING BREAKDOWN (per 1000 messages):")
        print(f"   Pydantic validation: {self.pydantic_time/self.count*1000:.3f}ms")
        print(f"   Kafka produce: {self.kafka_time/self.count*1000:.3f}ms")
        print(f"   Kafka poll: {self.poll_time/self.count*1000:.3f}ms")
        print(f"   Other/overhead: {self.other_time/self.count*1000:.3f}ms")
        print(f"   TOTAL per message: {(self.pydantic_time+self.kafka_time+self.poll_time+self.other_time)/self.count*1000:.3f}ms\n")

# 3. Kafka Configuration (Optimized for High Throughput)
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'payment-ingestion-driver',
    'compression.type': 'lz4',
    'linger.ms': 5,                    # Reduced from 10ms
    'batch.num.messages': 1000,        # Reduced from 5000 (faster flush)
    'batch.size': 32768,               # 32KB batches
    'queue.buffering.max.messages': 50000,  # Reduced from 100000
    'queue.buffering.max.kbytes': 10240,     # 10MB max
    'socket.nagle.disable': True,      # Disable Nagle's algorithm
    'acks': 1                          # Don't wait for all replicas (faster)
}

producer = Producer(kafka_config)
TOPIC_NAME = 'payment_transactions'
tracker = ThroughputTracker()
timer = OperationTimer()

def delivery_report(err, msg):
    if err is not None:
        print(f"[-] Delivery failed: {err}")

# 4. Worker Engine Task Loop
def transaction_worker_loop(worker_id: int, duration_seconds: int = 60):
    print(f"[+] Worker-{worker_id} started for {duration_seconds}s")
    
    merchants = ["Amazon", "Target", "Walmart", "Netflix", "Uber", "Apple Store"]
    locations = ["NEW_DELHI, IN", "MUMBAI, IN", "BANGALORE, IN", "NEW_YORK, US", "LONDON, UK"]
    
    message_count = 0
    validation_errors = 0
    send_errors = 0
    start_time = time.time()
    
    while time.time() - start_time < duration_seconds:
        try:
            # Time Pydantic validation
            t1 = time.time()
            tx_data = PaymentTransaction(
                transaction_id=str(uuid.uuid4()),
                account_id=f"ACC-{random.randint(10000, 99999)}",
                amount=round(random.uniform(10.0, 5000.0), 2),
                currency="INR",
                merchant=random.choice(merchants),
                location=random.choice(locations),
                device_ip=f"192.168.1.{random.randint(2, 254)}",
                ts_string=datetime.now(timezone.utc).isoformat()
            )
            t2 = time.time()
            timer.pydantic_time += (t2 - t1)
            
             # Time Kafka produce
            t1 = time.time()
            producer.produce(
                topic=TOPIC_NAME,
                key=tx_data.account_id,
                value=tx_data.model_dump_json().encode('utf-8'),
                callback=delivery_report
            )
            t2 = time.time()
            timer.kafka_time += (t2 - t1)

            # Time poll
            t1 = time.time()
            producer.poll(0)  # Non-blocking
            t2 = time.time()
            timer.poll_time += (t2 - t1)

            message_count += 1
            timer.count += 1
            tracker.record()
            producer.poll(0)
            
            # Print metrics every 5000 messages
            if message_count % 5000 == 0:
                tps = tracker.record()
                elapsed = time.time() - start_time
                print(f"[✓] Worker-{worker_id}: {message_count} msgs | TPS: {tps:.2f} | Elapsed: {elapsed:.1f}s")
            
            # time.sleep(0.001)  # 1ms = 1000 TPS per thread
            
        except ValueError as e:  # Pydantic validation error
            validation_errors += 1
        except Exception as e:
            send_errors += 1
            print(f"[-] Worker-{worker_id} error: {e}")
    
    elapsed = time.time() - start_time
    actual_tps = message_count / elapsed if elapsed > 0 else 0
    print(f"[✓] Worker-{worker_id} DONE: {message_count} msgs, {actual_tps:.2f} TPS, "
          f"{validation_errors} validation errors, {send_errors} send errors")
    
    return message_count

def run_driver(duration_seconds=60, num_threads=4):
    print(f"[+] Starting high-throughput ingestion driver ({num_threads} threads, {duration_seconds}s duration)...")
    print(f"[+] Target: {num_threads * 1000} TPS")
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [
            executor.submit(transaction_worker_loop, i, duration_seconds) 
            for i in range(num_threads)
        ]
        
        total_messages = sum(f.result() for f in futures)
    
    actual_tps = total_messages / duration_seconds if duration_seconds > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"📊 INGESTION METRICS")
    print(f"{'='*60}")
    print(f"Total Messages Produced: {total_messages}")
    print(f"Duration: {duration_seconds}s")
    print(f"Actual TPS: {actual_tps:.2f}")
    print(f"Target TPS: {num_threads * 1000}")
    print(f"Success Rate: {(actual_tps / (num_threads * 1000) * 100):.2f}%")
    print(f"{'='*60}\n")

    # Show timing breakdown
    timer.report()
    
    producer.flush(timeout=5.0)

if __name__ == '__main__':
    try:
        run_driver(duration_seconds=60, num_threads=4)
    except KeyboardInterrupt:
        print("\n🛑 Stopping ingestion driver...")
        producer.flush(timeout=5.0)
        print("🏁 System offline.")
