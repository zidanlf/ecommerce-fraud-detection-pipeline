import time, json, random, os
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

fake = Faker('id_ID')
HOST = os.getenv('KAFKA_HOST', 'kafka')

# Setup Producer
producer = KafkaProducer(
    bootstrap_servers=[f'{HOST}:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks=1, retries=3
)

print("\n" + "="*50)
print(f"PRODUCER STARTED | Target: {HOST}:9092")
print("="*50 + "\n")

try:
    while True:
        # Generate Data
        hour = random.randint(0, 23)
        voucher = f"HEMAT{random.choice([10, 20])}_XYZ" if random.random() < 0.2 else None
        
        data = {
            "order_id": str(fake.random_number(digits=6)),
            "user_id": f"U{random.randint(1, 100)}",
            "product_id": f"P{random.randint(1, 50)}",
            "quantity": random.randint(1, 150),
            "amount": random.randint(50000, 200000000),
            "voucher_code": voucher,
            "country": random.choice(['ID']*3 + ['US', 'SG']),
            "created_date": datetime.now().replace(hour=hour).strftime("%Y-%m-%dT%H:%M:%S")
        }

        # Send Data
        producer.send('orders', value=data)
        
        # Print Log Rapih (Satu baris per transaksi)
        print(f"[SENT] Order: {data['order_id']:<8} | User: {data['user_id']:<5} | Amt: {data['amount']:<12} | Ctry: {data['country']}")
        
        time.sleep(2)

except KeyboardInterrupt:
    print("\n[STOP] Producer stopped by user.\n")
    producer.flush()
    producer.close()