import json, psycopg2, requests, os
from kafka import KafkaConsumer
from datetime import datetime

# --- CONFIG ---
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL')
DB_DSN = f"dbname={os.getenv('PG_DB','final_project_db')} user={os.getenv('PG_USER','airflow')} password={os.getenv('PG_PASS','airflow')} host={os.getenv('PG_HOST','postgres-project')} port=5432"
SQL_INSERT = "INSERT INTO orders (order_id, user_id, product_id, quantity, amount, voucher_code, country, created_date, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING"

def check_fraud(data):
    hour = datetime.fromisoformat(data['created_date']).hour
    amt = int(str(data['amount']).replace("Rp.", "").replace(",", ""))
    
    rules = [
        (data['country'] != 'ID', "Foreign Location"),
        (data['quantity'] > 100 and 0 <= hour < 4, f"High Qty ({data['quantity']}) at Night"),
        (amt > 100_000_000 and 0 <= hour < 4, "High Amount at Night"),
        (data['voucher_code'] and amt < 5_000_000, "Voucher Abuse")
    ]
    reasons = [r for cond, r in rules if cond]
    return ("frauds", reasons) if reasons else ("genuine", [])

def alert_discord(data, reasons):
    payload = {
        "embeds": [{
            "title": f"FRAUD DETECTED: {data['order_id']}",
            "color": 15548997,
            "fields": [
                {"name": "User", "value": data['user_id'], "inline": True},
                {"name": "Amount", "value": str(data['amount']), "inline": True},
                {"name": "Reason", "value": ", ".join(reasons)}
            ]
        }]
    }
    try: requests.post(DISCORD_WEBHOOK_URL, json=payload)
    except: pass

# --- MAIN ---
print("\n" + "="*50)
print(f"CONSUMER STARTED | Kafka: {os.getenv('KAFKA_HOST', 'kafka')} | DB: {os.getenv('PG_HOST', 'postgres-project')}")
print("="*50 + "\n")

consumer = KafkaConsumer(
    'orders', bootstrap_servers=[f"{os.getenv('KAFKA_HOST', 'kafka')}:9092"],
    group_id='fraud-group', auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    conn = psycopg2.connect(DB_DSN)
    with conn.cursor() as cursor:
        print("[INFO] Connected to Database. Listening...\n")
        
        for msg in consumer:
            data = msg.value
            status, reasons = check_fraud(data)

            if status == 'frauds':
                print(f"   >>> [FRAUD]   Order: {data['order_id']} | User: {data['user_id']} | Reason: {', '.join(reasons)}")
                alert_discord(data, reasons)
            else:
                print(f"[OK]   Order: {data['order_id']} | Status: Genuine")

            cursor.execute(SQL_INSERT, (
                data['order_id'], data['user_id'], data['product_id'], 
                data['quantity'], data['amount'], data['voucher_code'],
                data['country'], data['created_date'], status
            ))
            conn.commit()

except KeyboardInterrupt: print("\n[STOP] Consumer stopped by user.\n")
except Exception as e: print(f"[ERROR] {e}")
finally: conn.close() if 'conn' in locals() else None