from confluent_kafka import Consumer
import json
import sys
import time

# Cara pakai: python consumer_bench.py <topic> <group_id>
TOPIC = sys.argv[1] if len(sys.argv) > 1 else "uji-p3"
GROUP = sys.argv[2] if len(sys.argv) > 2 else "bench"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
consumer.subscribe([TOPIC])

jumlah = 0
total_latency = 0.0
mulai = None
terakhir_dapat = time.time()
TIMEOUT_IDLE = 5   # berhenti kalau 5 detik tidak ada data baru

print("Menunggu partition & data...")

while True:
    pesan = consumer.poll(1.0)
    if pesan is None:
        if mulai is not None and time.time() - terakhir_dapat > TIMEOUT_IDLE:
            break
        continue
    if pesan.error():
        continue

    if mulai is None:
        mulai = time.time()          # timer mulai saat data pertama diterima
    terakhir_dapat = time.time()

    # Pekerjaan "analisis" sederhana (parse JSON) supaya consumer benar-benar bekerja
    data = json.loads(pesan.value())
    _ = data["price"] * data["volume"]
    jumlah += 1

durasi = terakhir_dapat - mulai if mulai else 0
consumer.close()

print("=" * 40)
print(f"Topic      : {TOPIC}")
print(f"Pesan      : {jumlah}")
print(f"Waktu      : {durasi:.2f} detik")
if durasi > 0:
    print(f"Throughput : {jumlah / durasi:,.0f} pesan/detik")