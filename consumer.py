from kafka_config import kafka_config
from confluent_kafka import Consumer, Producer
from collections import defaultdict, deque
import json

# Pengaturan consumer (pembaca data)
consumer = Consumer(kafka_config({
    "group.id": "analisis-saham",
    "auto.offset.reset": "earliest",
}))
consumer.subscribe(["stock-ticks"])

alert_producer = Producer(kafka_config())

# Simpan 10 harga terakhir tiap saham
riwayat = defaultdict(lambda: deque(maxlen=10))

BATAS_LONJAKAN = 0.003   # 0,3%

print("Consumer jalan... tekan Ctrl+C untuk berhenti")

try:
    while True:
        pesan = consumer.poll(1.0)   # tunggu data maksimal 1 detik
        if pesan is None:
            continue
        if pesan.error():
            print("Error:", pesan.error())
            continue

        data = json.loads(pesan.value().decode("utf-8"))
        simbol = data["symbol"]
        harga = data["price"]

        # Hitung rata-rata dari harga sebelumnya (sebelum harga baru dimasukkan)
        if len(riwayat[simbol]) >= 5:
            rata2 = sum(riwayat[simbol]) / len(riwayat[simbol])
            selisih = (harga - rata2) / rata2

            print(f"{simbol}: harga={harga}, rata-rata={rata2:.1f}, selisih={selisih*100:+.2f}%")

            if abs(selisih) > BATAS_LONJAKAN:
                alert = {
                    "symbol": simbol,
                    "price": harga,
                    "average": round(rata2, 1),
                    "change_percent": round(selisih * 100, 2),
                    "type": "LONJAKAN NAIK" if selisih > 0 else "LONJAKAN TURUN",
                    "timestamp": data["timestamp"],
                }
                alert_producer.produce("stock-alerts", key=simbol, value=json.dumps(alert))
                alert_producer.poll(0)
                print(f"  >>> ALERT: {alert['type']} {simbol} ({alert['change_percent']}%)")

        riwayat[simbol].append(harga)

except KeyboardInterrupt:
    print("Berhenti.")
finally:
    consumer.close()
    alert_producer.flush()