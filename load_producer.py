from confluent_kafka import Producer
import json
import random
import sys
import time
from datetime import datetime

# Cara pakai: python load_producer.py <jumlah_event> <topic>
JUMLAH = int(sys.argv[1]) if len(sys.argv) > 1 else 100000
TOPIC = sys.argv[2] if len(sys.argv) > 2 else "stock-ticks"

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "linger.ms": 5,
    "batch.size": 65536,
})

# 100 saham simulasi supaya data tersebar rata ke semua partition
simbol_list = [f"STK{i:03d}" for i in range(100)]
harga = {s: random.randint(500, 10000) for s in simbol_list}

print(f"Mengirim {JUMLAH} event ke topic {TOPIC}...")
mulai = time.time()

for i in range(JUMLAH):
    s = simbol_list[i % len(simbol_list)]
    harga[s] = max(1, int(harga[s] * (1 + random.uniform(-0.005, 0.005))))
    data = {
        "symbol": s,
        "price": harga[s],
        "volume": random.randint(100, 5000),
        "timestamp": datetime.now().isoformat(),
    }
    while True:
        try:
            producer.produce(TOPIC, key=s, value=json.dumps(data))
            break
        except BufferError:
            producer.poll(0.1)   # antrian penuh, tunggu sebentar
    if i % 1000 == 0:
        producer.poll(0)

producer.flush()
selesai = time.time() - mulai
print(f"Selesai: {JUMLAH} event dalam {selesai:.2f} detik")
print(f"Throughput: {JUMLAH / selesai:,.0f} event/detik")