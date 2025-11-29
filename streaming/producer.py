import time, json, random, os, psycopg2
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

# --- CONFIGURATION ---
fake = Faker('id_ID')
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka')
REFRESH_INTERVAL = 50

# Connection String
DB_DSN = f"dbname={os.getenv('PG_DB','final_project_db')} " \
         f"user={os.getenv('PG_USER','airflow')} " \
         f"password={os.getenv('PG_PASS','airflow')} " \
         f"host={os.getenv('PG_HOST','postgres-project')} port=5432"

def fetch_references():
    """Fetches valid IDs from database to ensure referential integrity."""
    try:
        with psycopg2.connect(DB_DSN) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT user_id FROM users")
                users = [r[0] for r in cursor.fetchall()]

                cursor.execute("SELECT product_id FROM products")
                products = [r[0] for r in cursor.fetchall()]

                cursor.execute("SELECT voucher_code FROM vouchers WHERE valid_until >= CURRENT_DATE")
                vouchers = [r[0] for r in cursor.fetchall()]
                
                return users, products, vouchers
    except Exception as e:
        print(f"[ERROR] Database fetch failed: {e}")
        return [], [], []

# --- INIT PRODUCER ---
print("-" * 60)
print(f"PRODUCER STARTED | Target: {KAFKA_HOST}:9092")
print("-" * 60)

producer = KafkaProducer(
    bootstrap_servers=[f'{KAFKA_HOST}:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
    acks=1, retries=3
)

# Initial Load
user_ids, product_ids, voucher_codes = fetch_references()

if not user_ids or not product_ids:
    print("[FATAL] No users or products found in DB. Run Batch Ingestion first.")
    exit(1)

print(f"[INFO] Reference Data Loaded: {len(user_ids)} Users, {len(product_ids)} Products, {len(voucher_codes)} Vouchers\n")
print(f"{'TIMESTAMP':<20} | {'ORDER ID':<10} | {'USER':<6} | {'CTRY':<4} | {'AMT (IDR)':<12} | {'STATUS'}")
print("-" * 80)

try:
    counter = 0
    while True:
        # 1. Refresh References periodically
        if counter >= REFRESH_INTERVAL:
            user_ids, product_ids, voucher_codes = fetch_references()
            print(f"[INFO] Refreshed reference data from database.")
            counter = 0

        # 2. Generate Data Logic
        hour = random.randint(0, 23)
        voucher = random.choice(voucher_codes) if (voucher_codes and random.random() < 0.2) else None
        
        # Fraud Simulation Logic
        # 90% Normal, 10% Abnormal Quantity
        quantity = random.randint(1, 5) if random.random() < 0.9 else random.randint(100, 150)
        
        base_price = random.randint(50000, 2000000)
        amount = base_price * quantity
        if voucher: amount = int(amount * 0.9) # 10% Discount logic
        
        # Country Distribution (85% ID, 15% Others)
        country = random.choices(['ID', 'SG', 'US', 'MY'], weights=[85, 5, 5, 5])[0]

        data = {
            "order_id": str(fake.random_number(digits=8, fix_len=True)),
            "user_id": random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "quantity": quantity,
            "amount": amount,
            "voucher_code": voucher,
            "country": country,
            "created_date": datetime.now().replace(hour=hour).strftime("%Y-%m-%dT%H:%M:%S")
        }

        # 3. Send to Kafka
        producer.send('orders', value=data)

        # 4. Clean Logging (Columnar Format)
        log_status = "SENT"
        # Mark suspicious activity in log (High Amt or High Qty or Foreign)
        if quantity > 100 or amount > 100000000 or country != 'ID':
            log_status = "FLAGGED"

        print(f"{datetime.now().strftime('%H:%M:%S'):<20} | {data['order_id']:<10} | {data['user_id']:<6} | {country:<4} | {amount:<12} | {log_status}")

        counter += 1
        time.sleep(6)

except KeyboardInterrupt:
    print("\n[STOP] Producer stopped by user.")
    producer.flush()
    producer.close()
except Exception as e:
    print(f"\n[ERROR] Runtime error: {e}")