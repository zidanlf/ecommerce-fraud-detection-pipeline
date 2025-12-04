import random
from faker import Faker
from datetime import datetime

fake = Faker('id_ID')

# --- GENERATE USERS ---
def generate_user_data(start_id=1, count=1):
    users = []
    for i in range(count):
        uid = f"U{start_id + i}"
        users.append({
            "user_id": uid,
            "name": fake.name(),
            "email": fake.email(),
            "created_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return users

# --- GENERATE PRODUCTS ---
def generate_product_data(start_id=1, count=1):
    products = []
    categories = ['Elektronik', 'Fashion', 'Dapur', 'Otomotif', 'Hobi']
    for i in range(count):
        pid = f"P{start_id + i}" 
        products.append({
            "product_id": pid,
            "product_name": f"{fake.word()} {fake.word()}",
            "category": random.choice(categories),
            "price": random.randint(50000, 5000000),
            "created_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    return products

# --- GENERATE VOUCHERS (DISKON) ---
def generate_voucher_data(count=1):
    vouchers = []
    types = ['PERCENT', 'FIXED']
    prefixes = ['SUPER', 'HEMAT', 'KILAT', 'WIB', 'GAJIAN'] 
    
    for i in range(count):
        disc_type = random.choice(types)
        if disc_type == 'PERCENT':
            val = random.choice([5, 10, 20, 50]) 
            suffix_val = val
        else:
            val = random.choice([10000, 25000, 50000]) 
            suffix_val = int(val/1000)
            
        code_name = f"{random.choice(prefixes)}{suffix_val}" 
        unique_suffix = fake.lexify(text='???').upper()
        
        vouchers.append({
            "voucher_code": f"{code_name}_{unique_suffix}",
            "voucher_name": f"Promo {code_name}",
            "discount_type": disc_type,
            "discount_value": val,
            "valid_until": fake.future_date(end_date="+30d")
        })
    return vouchers