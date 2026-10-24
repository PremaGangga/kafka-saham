import json
from collections import defaultdict, deque
from datetime import datetime

from confluent_kafka import Consumer, Producer

from kafka_config import kafka_config

# ---------- Pengaturan ----------
JENDELA_DETIK = 30       # rata-rata dihitung dari harga 30 detik terakhir
BATAS_LONJAKAN = 0.0002  # 0,02%; naikkan kalau alert terlalu sering
JEDA_ALERT = 10          # minimal 10 detik antar alert untuk simbol yang sama
MIN_DATA = 5             # butuh minimal 5 data di jendela sebelum menghitung

consumer = Consumer(kafka_config({
    "group.id": "analisis-forex",
    "auto.offset.reset": "latest",   # hanya data baru
}))
consumer.subscribe(["stock-ticks"])

alert_producer = Producer(kafka_config())

riwayat = defaultdict(deque)   # simbol -> deque[(waktu, harga)]
alert_terakhir = {}            # simbol -> waktu alert terakhir
diproses = 0

print("Consumer jalan... tekan Ctrl+C untuk berhenti")

try:
    while True:
        pesan = consumer.poll(1.0)
        if pesan is None:
            continue
        if pesan.error():
            print("Error:", pesan.error())
            continue

        try:
            data = json.loads(pesan.value().decode("utf-8"))
            simbol = data["symbol"]
            harga = float(data["price"])
            waktu = datetime.fromisoformat(data["timestamp"]).timestamp()
        except Exception:
            continue   # lewati pesan uji atau pesan rusak

        dq = riwayat[simbol]
        while dq and dq[0][0] < waktu - JENDELA_DETIK:
            dq.popleft()

        if len(dq) >= MIN_DATA:
            rata2 = sum(h for _, h in dq) / len(dq)
            selisih = (harga - rata2) / rata2

            diproses += 1
            if diproses % 200 == 0:
                print(f"{diproses} pesan diproses. {simbol}: harga={harga}, "
                      f"rata-rata={rata2:.5f}, selisih={selisih * 100:+.3f}%")

            if (abs(selisih) > BATAS_LONJAKAN
                    and waktu - alert_terakhir.get(simbol, 0) >= JEDA_ALERT):
                alert_terakhir[simbol] = waktu
                alert = {
                    "symbol": simbol,
                    "price": harga,
                    "average": round(rata2, 5),
                    "change_percent": round(selisih * 100, 3),
                    "type": "LONJAKAN NAIK" if selisih > 0 else "LONJAKAN TURUN",
                    "timestamp": data["timestamp"],
                }
                alert_producer.produce("stock-alerts", key=simbol, value=json.dumps(alert))
                alert_producer.poll(0)
                print(f">>> ALERT: {alert['type']} {simbol} ({alert['change_percent']}%)")

        dq.append((waktu, harga))

except KeyboardInterrupt:
    print("Berhenti.")
finally:
    consumer.close()
    alert_producer.flush()