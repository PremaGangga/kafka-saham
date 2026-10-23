import json
import time

from confluent_kafka import Producer

from kafka_config import kafka_config

cfg = kafka_config()
print("Mode koneksi :", "cloud" if "sasl.username" in cfg else "lokal")
print("Server       :", cfg["bootstrap.servers"])

producer = Producer(cfg)

try:
    meta = producer.list_topics(timeout=15)
except Exception as e:
    raise SystemExit(f"GAGAL terhubung: {e}")

print("Terhubung. Topic yang ada:", sorted(t for t in meta.topics if not t.startswith("_")))

hasil = {}


def laporan(err, msg):
    hasil["err"] = err
    hasil["waktu"] = time.time()
    hasil["msg"] = msg


pesan = {"symbol": "TEST", "price": 1, "volume": 1, "timestamp": "uji-koneksi"}
mulai = time.time()
producer.produce("stock-ticks", key="TEST", value=json.dumps(pesan), callback=laporan)
producer.flush(15)

if hasil.get("err"):
    raise SystemExit(f"GAGAL kirim: {hasil['err']}")
if "waktu" not in hasil:
    raise SystemExit("GAGAL kirim: tidak ada konfirmasi (cek topic stock-ticks sudah dibuat)")

m = hasil["msg"]
print(f"Pesan terkirim ke partition {m.partition()}, offset {m.offset()}")
print(f"Waktu kirim sampai konfirmasi: {(hasil['waktu'] - mulai) * 1000:.0f} ms")
print("BERHASIL.")