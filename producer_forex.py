"""Producer forex real-time dari Finnhub (WebSocket) ke Kafka.

Persiapan:
  pip install websocket-client
  Tambahkan baris ini di file .env:   FINNHUB_KEY=api_key_finnhub_kamu
Jalankan:
  python producer_forex.py

Catatan: pasar forex tutup di akhir pekan. BTC tetap bergerak 24 jam, jadi dashboard
tidak kosong saat forex tutup.
"""
import json
import os
import time
from datetime import datetime, timedelta, timezone

import websocket  # paket: websocket-client
from confluent_kafka import Producer

from kafka_config import kafka_config  # juga memuat file .env

API_KEY = os.environ.get("FINNHUB_KEY")
if not API_KEY:
    raise SystemExit("FINNHUB_KEY belum diisi (tambahkan di file .env atau di secret).")

TOPIC = "stock-ticks"
WIB = timezone(timedelta(hours=7))   # jam selalu WIB, di laptop maupun di server

SYMBOLS = {
    "OANDA:EUR_USD": "EURUSD",
    "OANDA:GBP_USD": "GBPUSD",
    "OANDA:USD_JPY": "USDJPY",
    "OANDA:XAU_USD": "XAUUSD",        # emas
    "BINANCE:BTCUSDT": "BTC",         # kripto, buka 24 jam
}

producer = Producer(kafka_config())
jumlah = 0


def on_open(ws):
    for s in SYMBOLS:
        ws.send(json.dumps({"type": "subscribe", "symbol": s}))
    print("Terhubung ke Finnhub. Menunggu data... (Ctrl+C untuk berhenti)")


def on_message(ws, message):
    global jumlah
    pesan = json.loads(message)
    if pesan.get("type") != "trade":
        return
    for t in pesan["data"]:
        nama = SYMBOLS.get(t["s"], t["s"])
        waktu = datetime.fromtimestamp(t["t"] / 1000, WIB).replace(tzinfo=None)
        data = {
            "symbol": nama,
            "price": t["p"],
            "volume": t["v"] if t["v"] else 1,   # forex tidak punya volume: dihitung 1 per tick
            "timestamp": waktu.isoformat(),
        }
        producer.produce(TOPIC, key=nama, value=json.dumps(data))
        jumlah += 1
        if jumlah % 100 == 0:
            print(f"Terkirim {jumlah} tick. Terakhir: {data}")
    producer.poll(0)


def on_error(ws, error):
    print("Error:", error)


try:
    while True:   # sambung ulang otomatis kalau koneksi putus
        ws = websocket.WebSocketApp(
            f"wss://ws.finnhub.io?token={API_KEY}",
            on_open=on_open, on_message=on_message, on_error=on_error,
        )
        ws.run_forever(ping_interval=20, ping_timeout=10)
        print("Koneksi putus, mencoba lagi dalam 5 detik...")
        time.sleep(5)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    print("Berhenti.")