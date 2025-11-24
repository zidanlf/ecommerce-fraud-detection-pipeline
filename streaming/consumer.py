import json
import psycopg2
import requests 
import os # Tambahan import os
from kafka import KafkaConsumer
from datetime import datetime

# --- KONFIGURASI ---
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1441332811796578416/2YH6t5qs6qPpkFTFhqwh3_Nue49wpT2cWIljnSHOtwwk_yt-1LLdNDY5N3NioM5byd3Q"

# Ambil konfigurasi dari Environment Variable (Agar fleksibel)
# Default host kita set ke 'postgres-project' sesuai nama service di docker-compose baru
KAFKA_HOST = os.getenv('KAFKA_HOST', 'kafka')
PG_HOST = os.getenv('PG_HOST', 'postgres-project') 
PG_DB = os.getenv('PG_DB', 'final_project_db')
PG_USER = os.getenv('PG_USER', 'airflow')
PG_PASS = os.getenv('PG_PASS', 'airflow')

DB_CONFIG = {
    "dbname": PG_DB,
    "user": PG_USER,
    "password": PG_PASS,
    "host": PG_HOST, # PENTING: Mengarah ke container project
    "port": "5432"
}

# --- FUNGSI: KIRIM NOTIFIKASI KE DISCORD ---
def send_discord_alert(data, reasons):
    """
    Mengirim pesan peringatan ke Discord channel
    """
    reasons_str = ", ".join(reasons)
    
    # Format pesan (Embed style biar rapi)
    payload = {
        "username": "Fraud Detection System",
        "avatar_url": "https://cdn-icons-png.flaticon.com/512/1086/1086581.png",
        "embeds": [
            {
                "title": "🚨 FRAUD DETECTED!",
                "description": f"Transaksi mencurigakan ditemukan pada Order ID: **{data['order_id']}**",
                "color": 15548997, # Warna Merah
                "fields": [
                    {"name": "User ID", "value": data['user_id'], "inline": True},
                    {"name": "Amount", "value": f"Rp {data['amount']:,}", "inline": True},
                    {"name": "Country", "value": data['country'], "inline": True},
                    {"name": "Alasan Fraud", "value": f"⚠️ {reasons_str}"},
                    {"name": "Waktu", "value": data['created_date']}
                ]
            }
        ]
    }
    
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code == 204:
            print("✅ Notifikasi Discord terkirim.")
        else:
            print(f"❌ Gagal kirim notifikasi: {response.status_code}")
    except Exception as e:
        print(f"❌ Error Discord: {e}")

# --- FUNGSI: CEK RULES FRAUD ---
def check_fraud_rules(data):
    tx_time = datetime.fromisoformat(data['created_date'])
    hour = tx_time.hour
    
    is_fraud = False
    reasons = []

    # RULE 1: Transaksi dari Luar Negeri (Soal)
    if data['country'] != 'ID':
        is_fraud = True
        reasons.append("Lokasi Asing (Non-Indo)")

    # RULE 2: Qty > 100 di jam tidur (00:00 - 04:00) (Soal)
    if data['quantity'] > 100 and (0 <= hour < 4):
        is_fraud = True
        reasons.append(f"Qty Abnormal ({data['quantity']}) di Jam Malam")

    # RULE 3: Amount > 100 Juta di jam tidur (Soal)
    # Pastikan amount berupa integer
    amount_int = int(str(data['amount']).replace("Rp.", "").replace(",", "")) 
    if amount_int > 100000000 and (0 <= hour < 4):
        is_fraud = True
        reasons.append("Transaksi Jumbo (>100jt) di Jam Malam")
        
    # RULE 4 (BONUS): Voucher Abuse
    # Jika pakai voucher TAPI belanjanya sedikit (< 50rb)
    if data['voucher_code'] is not None and amount_int < 50000:
        is_fraud = True
        reasons.append("Penyalahgunaan Voucher (Nominal Kecil)")

    status = "frauds" if is_fraud else "genuine"
    return status, reasons

# --- MAIN PROGRAM ---
print(f"🟢 CONSUMER INIT: Kafka at {KAFKA_HOST} | DB at {PG_HOST}")

consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=[f'{KAFKA_HOST}:9092'],
    
    auto_offset_reset='earliest', 
    
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Koneksi ke DB
try:
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("🟢 DB Connected Successfully")
except Exception as e:
    print(f"🔴 DB Connection Failed: {e}")
    exit(1)

try:
    print("🟢 CONSUMER READY: Menunggu data...")
    for message in consumer:
        data = message.value
        
        # 1. Cek Rules
        status, reasons = check_fraud_rules(data)
        
        # 2. Jika Fraud, Kirim Notifikasi
        if status == 'frauds':
            print(f"⚠️ FRAUD DETECTED: {data['order_id']} | {reasons}")
            send_discord_alert(data, reasons)
        else:
            print(f"✅ Genuine: {data['order_id']}")

        # 3. Simpan ke Database
        sql = """
            INSERT INTO orders (order_id, user_id, product_id, quantity, amount, voucher_code, country, created_date, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """
        cursor.execute(sql, (
            data['order_id'], data['user_id'], data['product_id'], 
            data['quantity'], data['amount'], data['voucher_code'],
            data['country'], data['created_date'], status
        ))
        conn.commit()

except KeyboardInterrupt:
    print("Stop Consumer.")
except Exception as e:
    print(f"Error Consumer Loop: {e}")
finally:
    if 'cursor' in locals(): cursor.close()
    if 'conn' in locals(): conn.close()