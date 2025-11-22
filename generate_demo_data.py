import os, csv, json, random, struct, time
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

OUT = "sample_data"
os.makedirs(OUT, exist_ok=True)

def write_customers():
    rows = [
        ["C100","Amit Kumar",780,28.61,77.20,"D001","savings"],
        ["C101","Priya Sharma",690,19.07,72.87,"D002","current"],
        ["C102","Rohit Singh",640,22.57,88.36,"D003","savings"],
        ["C103","Sneha Patel",720,12.97,77.59,"D004","savings"]
    ]
    with open(f"{OUT}/customers.csv","w") as f:
        w = csv.writer(f)
        w.writerow(["customer_id","name","credit_score","home_lat","home_lon","registered_device","account_type"])
        w.writerows(rows)
    print("customers.csv created")

def write_transactions_json():
    folder = f"{OUT}/transactions_json"
    os.makedirs(folder, exist_ok=True)
    base = datetime.utcnow() - timedelta(days=2)

    for i in range(2):
        path = f"{folder}/batch_tx_{i}.json"
        with open(path,"w") as f:
            for j in range(30):
                cust = random.choice(["C100","C101","C102","C103"])
                device = random.choice(["D001","D002","D003","D004","D999"])
                rec = {
                    "tx_id": f"B{i}{j:04}",
                    "customer_id": cust,
                    "amount": round(random.uniform(10,5000),2),
                    "device_id": device,
                    "ip": f"192.168.{random.randint(0,255)}.{random.randint(1,255)}",
                    "merchant": random.choice(["Store","Food","Travel","Online"]),
                    "location": {
                        "lat": 28.61 + random.uniform(-0.05,0.05),
                        "lon": 77.20 + random.uniform(-0.05,0.05)
                    },
                    "timestamp": (base + timedelta(minutes=j*5)).isoformat()
                }
                f.write(json.dumps(rec)+"\n")
        print(path, "created")

def write_txt_notes():
    lines = [
        "C101|2025-09-10|Flagged for multiple failed logins",
        "C103|2025-10-05|After manual review: suspicious login pattern"
    ]
    with open(f"{OUT}/compliance_notes.txt","w") as f:
        for l in lines:
            f.write(l+"\n")
    print("compliance_notes.txt created")

def write_binary_fingerprints():
    path = f"{OUT}/device_fingerprints.bin"
    with open(path,"wb") as f:
        devices = ["D001","D002","D003","D004","D999"]
        for d in devices:
            d_pad = d.encode("utf-8") + b"\x00"*(8-len(d))
            f.write(struct.pack(">8sQdd", d_pad, int(time.time()), random.random(), random.random()))
    print("device_fingerprints.bin created")

def write_parquet_history():
    rows = []
    for i in range(10):
        rows.append({
            "case_id": f"F{i:03}",
            "customer_id": random.choice(["C100","C101","C102","C103"]),
            "tx_id": f"H{i:04}",
            "amount": round(random.uniform(100,3000),2),
            "fraud_type": random.choice(["fraud","stolen_card","unknown"]),
            "reported_ts": datetime.utcnow().isoformat()
        })
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, f"{OUT}/historical_fraud.parquet")
    print("historical_fraud.parquet created")

if __name__ == "__main__":
    write_customers()
    write_transactions_json()
    write_txt_notes()
    write_binary_fingerprints()
    write_parquet_history()
    print("All sample data created successfully!")
