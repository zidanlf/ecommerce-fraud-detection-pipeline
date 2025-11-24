import time
import json
import random
import os # Tambahan import os
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

fake = Faker('id_ID')

# --- KONFIGURASI KONEKSI ---
# Mengambil alamat dari Environment Variable. 
# Jika tidak ada, default ke 'kafka' (karena kita akan jalankan di dalam Docker)
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka')

print(f" PRODUCER INIT: Menghubungkan ke Kafka di {KAFKA_HOST}:9092 ...")

# Setup Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[f'{KAFKA_HOST}:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

try:
    print(" PRODUCER START: Mulai mengirim data...")
    while True:
        # 1. GENERATE DATA DUMMY
        # Kita asumsi User U1-U100 & Product P1-P50 sudah ada dari Batch Processing
        user_id = f"U{random.randint(1, 100)}"
        product_id = f"P{random.randint(1, 50)}"
        
        # Logic Diskon (20% user pakai voucher)
        voucher_code = None
        if random.random() < 0.2: 
            voucher_code = f"HEMAT{random.choice([10, 20, 50])}_XYZ"

        # Logic Waktu (Acak jam biar bisa ngetes rule Fraud jam 00-04 pagi)
        # Kita mainkan jam-nya secara random untuk simulasi
        fake_hour = random.randint(0, 23)
        fake_time = datetime.now().replace(hour=fake_hour, minute=random.randint(0, 59))

        data = {
            "order_id": str(fake.random_number(digits=6)),
            "user_id": user_id,
            "product_id": product_id,
            "quantity": random.randint(1, 150),
            "amount": random.randint(50000, 200000000),
            "voucher_code": voucher_code,
            "country": random.choice(['ID', 'ID', 'ID', 'US', 'SG']),
            "created_date": fake_time.strftime("%Y-%m-%dT%H:%M:%S")
        }

        # 2. KIRIM KE KAFKA
        producer.send('orders', value=data)
        producer.flush()
        print(f"Sent: {data['order_id']} | User: {data['user_id']} | Jam: {fake_hour} | Country: {data['country']}")
        
        # Jeda 2 detik biar tidak flooding
        time.sleep(2)

except KeyboardInterrupt:
    print("\nStop Producer.")
except Exception as e:
    print(f"\n Error Producer: {e}")