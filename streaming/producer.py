# import time
# import json
# import random
# import os # Tambahan import os
# from kafka import KafkaProducer
# from faker import Faker
# from kafka.errors import NoBrokersAvailable
# from datetime import datetime

# fake = Faker('id_ID')

# # --- KONFIGURASI KONEKSI ---
# # Mengambil alamat dari Environment Variable. 
# # Jika tidak ada, default ke 'kafka' (karena kita akan jalankan di dalam Docker)
# KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka')

# print(f" PRODUCER INIT: Menghubungkan ke Kafka di {KAFKA_HOST}:9092 ...")

# # Setup Kafka Producer
# # Tambahkan sebelum while True loop
# max_retries = 5
# for i in range(max_retries):
#     try:
#         producer = KafkaProducer(
#             bootstrap_servers=[f'{KAFKA_HOST}:9092'],
#             value_serializer=lambda x: json.dumps(x).encode('utf-8')
#         )
#         print("✅ Producer connected to Kafka")
#         break
#     except NoBrokersAvailable:
#         print(f"⏳ Retry {i+1}/{max_retries} - Kafka not ready...")
#         time.sleep(5)

# try:
#     print(" PRODUCER START: Mulai mengirim data...")
#     while True:
#         # 1. GENERATE DATA DUMMY
#         # Kita asumsi User U1-U100 & Product P1-P50 sudah ada dari Batch Processing
#         user_id = f"U{random.randint(1, 100)}"
#         product_id = f"P{random.randint(1, 50)}"
        
#         # Logic Diskon (20% user pakai voucher)
#         voucher_code = None
#         if random.random() < 0.2: 
#             voucher_code = f"HEMAT{random.choice([10, 20, 50])}_XYZ"

#         # Logic Waktu (Acak jam biar bisa ngetes rule Fraud jam 00-04 pagi)
#         # Kita mainkan jam-nya secara random untuk simulasi
#         fake_hour = random.randint(0, 23)
#         fake_time = datetime.now().replace(hour=fake_hour, minute=random.randint(0, 59))

#         data = {
#             "order_id": str(fake.random_number(digits=6)),
#             "user_id": user_id,
#             "product_id": product_id,
#             "quantity": random.randint(1, 150),
#             "amount": random.randint(50000, 200000000),
#             "voucher_code": voucher_code,
#             "country": random.choice(['ID', 'ID', 'ID', 'US', 'SG']),
#             "created_date": fake_time.strftime("%Y-%m-%dT%H:%M:%S")
#         }

#         # 2. KIRIM KE KAFKA
#         producer.send('orders', value=data)
#         producer.flush()
#         print(f"Sent: {data['order_id']} | User: {data['user_id']} | Jam: {fake_hour} | Country: {data['country']}")
        
#         # Jeda 2 detik biar tidak flooding
#         time.sleep(2)

# except KeyboardInterrupt:
#     print("\nStop Producer.")
# except Exception as e:
#     print(f"\n Error Producer: {e}")

# import time
# import json
# import random
# import os
# from kafka import KafkaProducer
# from faker import Faker
# from datetime import datetime

# fake = Faker('id_ID')

# KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka')

# print(f"🔵 PRODUCER INIT: Menghubungkan ke Kafka di {KAFKA_HOST}:9092 ...")

# # Konfigurasi Producer yang lebih permissive untuk development
# producer = KafkaProducer(
#     bootstrap_servers=[f'{KAFKA_HOST}:9092'],
#     value_serializer=lambda x: json.dumps(x).encode('utf-8'),
#     acks=1,
#     retries=3,
#     max_in_flight_requests_per_connection=5,
#     compression_type=None,
#     linger_ms=10,
#     batch_size=16384
# )

# print("✅ Producer connected to Kafka")

# try:
#     print("🟢 PRODUCER START: Mulai mengirim data...")
#     counter = 0
    
#     while True:
#         # 1. GENERATE DATA DUMMY
#         user_id = f"U{random.randint(1, 100)}"
#         product_id = f"P{random.randint(1, 50)}"
        
#         # Logic Diskon (20% user pakai voucher)
#         voucher_code = None
#         if random.random() < 0.2: 
#             voucher_code = f"HEMAT{random.choice([10, 20, 50])}_XYZ"

#         # Logic Waktu (Acak jam biar bisa ngetes rule Fraud jam 00-04 pagi)
#         fake_hour = random.randint(0, 23)
#         fake_time = datetime.now().replace(hour=fake_hour, minute=random.randint(0, 59))

#         data = {
#             "order_id": str(fake.random_number(digits=6)),
#             "user_id": user_id,
#             "product_id": product_id,
#             "quantity": random.randint(1, 150),
#             "amount": random.randint(50000, 200000000),
#             "voucher_code": voucher_code,
#             "country": random.choice(['ID', 'ID', 'ID', 'US', 'SG']),
#             "created_date": fake_time.strftime("%Y-%m-%dT%H:%M:%S")
#         }

#         # 2. KIRIM KE KAFKA (fire-and-forget dengan callback)
#         future = producer.send('orders', value=data)
        
#         # Callback untuk tracking (non-blocking)
#         def on_success(metadata):
#             counter_val = counter
#             print(f"✅ [{counter_val}] Message delivered to {metadata.topic} partition {metadata.partition} offset {metadata.offset}")
        
#         def on_error(excp):
#             print(f"❌ Error sending message: {excp}")
        
#         future.add_callback(on_success)
#         future.add_errback(on_error)
        
#         counter += 1
#         print(f"📤 [{counter}] Sent: {data['order_id']} | User: {data['user_id']} | Jam: {fake_hour} | Country: {data['country']}")
        
#         # Jeda 2 detik
#         time.sleep(2)

# except KeyboardInterrupt:
#     print("\n⏹️  Stop Producer.")
# except Exception as e:
#     print(f"\n❌ Error Producer: {e}")
# finally:
#     producer.flush(timeout=10)  # Pastikan semua message terkirim
#     producer.close()
#     print("🔴 Producer closed.")

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