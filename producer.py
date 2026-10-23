from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Alamat Kafka (sudah kita buka lewat docker-compose)
producer = Producer({"bootstrap.servers": "localhost:9092"})

TOPIC = "stock-ticks"

# Harga awal beberapa saham (simulasi)
harga = {
    "BBCA": 9000,
    "BBRI": 4500,
    "TLKM": 3200,
    "ASII": 5000,
    "GOTO": 80,
}


def laporan(err, msg):
    """Dipanggil Kafka setelah pesan terkirim (atau gagal)."""
    if err:
        print("GAGAL kirim:", err)
    else:
        print(f"Terkirim -> partition {msg.partition()}, offset {msg.offset()}")


print("Producer jalan... tekan Ctrl+C untuk berhenti")

try:
    while True:
        for simbol in harga:
            # harga naik/turun acak sekitar 0,5%
            perubahan = random.uniform(-0.005, 0.005)
            harga[simbol] = max(1, round(harga[simbol] * (1 + perubahan)))

            data = {
                "symbol": simbol,
                "price": harga[simbol],
                "volume": random.randint(100, 5000),
                "timestamp": datetime.now().isoformat(),
            }

            # key = simbol saham, supaya data satu saham selalu di partition yang sama
            producer.produce(
                TOPIC,
                key=simbol,
                value=json.dumps(data),
                callback=laporan,
            )
            print("Kirim:", data)

        producer.poll(0)
        time.sleep(1)
except KeyboardInterrupt:
    print("Berhenti.")
finally:
    producer.flush()