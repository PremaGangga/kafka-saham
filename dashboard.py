import json
import uuid
from collections import defaultdict, deque

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from confluent_kafka import Consumer
from plotly.subplots import make_subplots
from streamlit_autorefresh import st_autorefresh

# ---------- Pengaturan ----------
BOOTSTRAP = "localhost:9092"
TOPIC_TICKS = "stock-ticks"
TOPIC_ALERTS = "stock-alerts"
WATCH = ["BBCA", "BBRI", "TLKM", "ASII", "GOTO"]
MAX_TICKS = 3000

PANEL, LINE, MUTED = "#10151c", "#1e2630", "#7b8794"
UP, DOWN, ACCENT = "#2de2c0", "#ff5c5c", "#e8a23a"
CFG = {"displayModeBar": False}

st.set_page_config(page_title="Stock Monitor", page_icon="📈", layout="wide",
                   initial_sidebar_state="collapsed")
st_autorefresh(interval=2000, key="refresh")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap');
:root{--bg:#0a0d12;--panel:#10151c;--line:#1e2630;--text:#d7dde5;--muted:#7b8794;
--up:#2de2c0;--down:#ff5c5c;--accent:#e8a23a;}
.stApp{background:var(--bg);color:var(--text);font-family:'IBM Plex Sans','Segoe UI',sans-serif;}
[data-testid="stHeader"],#MainMenu,footer{display:none;}
.block-container{padding:0.8rem 1.4rem 1.4rem;max-width:100%;}
[data-testid="stVerticalBlock"]{gap:0.7rem;}
[data-testid="stPlotlyChart"]{background:var(--panel);border:1px solid var(--line);border-radius:4px;overflow:hidden;}
label p{color:var(--muted)!important;font-size:12px!important;}
div[data-baseweb="select"]>div{background:var(--panel);border-color:var(--line);color:var(--text);}
.up{color:var(--up);} .down{color:var(--down);} .muted{color:var(--muted);}
.head{display:flex;justify-content:space-between;align-items:center;margin-bottom:0.2rem;}
.title{font-size:20px;font-weight:600;letter-spacing:0.2px;}
.live{font-size:12px;color:var(--muted);display:flex;align-items:center;gap:8px;}
.pulse{width:8px;height:8px;border-radius:50%;background:var(--up);animation:blink 1.6s infinite;}
@keyframes blink{50%{opacity:0.25;}}
@media (prefers-reduced-motion:reduce){.pulse{animation:none;}}
.ticker{display:flex;background:var(--panel);border:1px solid var(--line);border-radius:4px;overflow-x:auto;}
.tk{flex:1;min-width:170px;padding:10px 14px;display:flex;align-items:baseline;gap:10px;border-right:1px solid var(--line);font-size:13px;white-space:nowrap;}
.tk:last-child{border-right:none;}
.tk .dot{width:7px;height:7px;background:var(--accent);align-self:center;}
.tk .px{font-weight:600;font-variant-numeric:tabular-nums;margin-left:auto;}
.card{background:var(--panel);border:1px solid var(--line);border-radius:4px;padding:12px 14px;}
.ctitle{font-size:12px;color:var(--muted);margin-bottom:6px;}
.ctitle b{color:var(--text);font-weight:600;}
.big{font-size:22px;font-weight:600;font-variant-numeric:tabular-nums;display:flex;align-items:baseline;gap:10px;}
.big span{font-size:13px;font-weight:500;}
.spark{width:100%;height:64px;display:block;margin:8px 0;}
.kv{display:flex;justify-content:space-between;font-size:12px;padding:3px 0;border-top:1px solid var(--line);}
.kv span{color:var(--muted);} .kv b{font-weight:500;font-variant-numeric:tabular-nums;}
.wl{width:100%;border-collapse:collapse;font-size:12px;font-variant-numeric:tabular-nums;}
.wl th{color:var(--muted);font-weight:400;text-align:right;padding:6px 4px;border-bottom:1px solid var(--line);}
.wl td{text-align:right;padding:8px 4px;}
.wl th:first-child,.wl td:first-child{text-align:left;}
.wl tr.sel td{background:rgba(45,226,192,0.14);}
.rule{height:2px;width:38%;background:var(--accent);margin:0.1rem 0;}
.al{display:grid;grid-template-columns:56px 1fr auto auto 64px;gap:10px;align-items:center;font-size:12px;padding:7px 0;border-top:1px solid var(--line);font-variant-numeric:tabular-nums;}
.badge{font-size:11px;padding:2px 8px;border-radius:10px;text-align:center;}
.badge.up{background:rgba(45,226,192,0.15);} .badge.down{background:rgba(255,92,92,0.15);}
</style>
""", unsafe_allow_html=True)


# ---------- Kafka ----------
@st.cache_resource
def siapkan():
    """Dibuat sekali: consumer + penyimpanan data di memori."""
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "dashboard-" + uuid.uuid4().hex[:6],
        "auto.offset.reset": "latest",
    })
    c.subscribe([TOPIC_TICKS, TOPIC_ALERTS])
    ticks = defaultdict(lambda: deque(maxlen=MAX_TICKS))
    alerts = deque(maxlen=50)
    stats = {"n": 0}
    return c, ticks, alerts, stats


consumer, ticks, alerts, stats = siapkan()

for _ in range(2000):
    pesan = consumer.poll(0.005)
    if pesan is None:
        break
    if pesan.error():
        continue
    d = json.loads(pesan.value().decode("utf-8"))
    if pesan.topic() == TOPIC_TICKS:
        ticks[d["symbol"]].append((d["timestamp"], d["price"], d.get("volume", 0)))
        stats["n"] += 1
    else:
        alerts.appendleft(d)


# ---------- Helper ----------
def fmt(x):
    if abs(x) >= 1000:
        return f"{x:,.0f}"
    return f"{x:,.2f}".rstrip("0").rstrip(".")


def to_df(sym):
    df = pd.DataFrame(ticks[sym], columns=["time", "price", "volume"])
    df["time"] = pd.to_datetime(df["time"])
    return df


def change(df, n=60):
    base = df["price"].iloc[max(0, len(df) - n - 1)]
    return (df["price"].iloc[-1] / base - 1) * 100 if base else 0.0


def show(fig, key):
    try:
        st.plotly_chart(fig, key=key, width="stretch", config=CFG, theme=None)
    except Exception:
        st.plotly_chart(fig, key=key, use_container_width=True, config=CFG, theme=None)


def style(fig, title, h):
    fig.update_layout(
        title=dict(text=title, font=dict(size=13, color=MUTED), x=0.01),
        height=h, margin=dict(l=12, r=12, t=40, b=12),
        paper_bgcolor=PANEL, plot_bgcolor=PANEL, showlegend=False, bargap=0.25,
        font=dict(color=MUTED, family="IBM Plex Sans, Segoe UI, sans-serif"),
    )
    fig.update_xaxes(gridcolor=LINE, zeroline=False, rangeslider_visible=False)
    fig.update_yaxes(gridcolor=LINE, zeroline=False)


def spark_svg(vals, up):
    vals = list(vals)
    if len(vals) < 2:
        return ""
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1
    W, H = 200, 60
    line = " ".join(
        f"{i / (len(vals) - 1) * W:.1f},{H - 4 - (v - lo) / rng * (H - 8):.1f}"
        for i, v in enumerate(vals)
    )
    c = UP if up else DOWN
    return (f'<svg viewBox="0 0 {W} {H}" preserveAspectRatio="none" class="spark">'
            f'<polygon points="0,{H} {line} {W},{H}" fill="{c}" fill-opacity="0.16"/>'
            f'<polyline points="{line}" fill="none" stroke="{c}" stroke-width="1.6" '
            f'vector-effect="non-scaling-stroke"/></svg>')


def candle_fig(df, sec, sym):
    d = df.set_index("time")
    o = d["price"].resample(f"{sec}s").ohlc().dropna().tail(80)
    v = d["volume"].resample(f"{sec}s").sum().reindex(o.index)
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.8, 0.2], vertical_spacing=0.02)
    fig.add_trace(go.Candlestick(
        x=o.index, open=o["open"], high=o["high"], low=o["low"], close=o["close"],
        increasing_line_color=UP, increasing_fillcolor=UP,
        decreasing_line_color=DOWN, decreasing_fillcolor=DOWN, name=sym), row=1, col=1)
    warna = [UP if c >= op else DOWN for op, c in zip(o["open"], o["close"])]
    fig.add_trace(go.Bar(x=o.index, y=v, marker_color=warna, opacity=0.5, name="Volume"),
                  row=2, col=1)
    fig.add_hline(y=df["price"].iloc[-1], line_dash="dot", line_color=ACCENT,
                  line_width=1, row=1, col=1)
    style(fig, f"{sym}  candle {sec} detik", 520)
    fig.update_yaxes(side="right")
    return fig


def combo_fig(df, sym):
    d = df.set_index("time")
    b = d["price"].resample("10s").last().dropna().tail(60)
    v = d["volume"].resample("10s").sum().reindex(b.index)
    warna = ["#58d6a8" if i == 0 or b.iloc[i] >= b.iloc[i - 1] else "#1d8a78"
             for i in range(len(b))]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=b.index, y=v, marker_color=warna, name="Volume"),
                  secondary_y=False)
    fig.add_trace(go.Scatter(x=b.index, y=b, mode="lines+markers",
                             line=dict(color=DOWN, width=1.8),
                             marker=dict(size=5, color="#ffffff"), name="Harga"),
                  secondary_y=True)
    style(fig, f"Volume dan harga {sym} (per 10 detik)", 300)
    fig.update_yaxes(showgrid=False, secondary_y=True)
    return fig


# ---------- Tampilan ----------
st.markdown(
    f'<div class="head"><div class="title">Real-Time Stock Market Monitoring</div>'
    f'<div class="live"><span class="pulse"></span>Live, {stats["n"]:,} event diterima</div></div>',
    unsafe_allow_html=True)

symbols = [s for s in WATCH if s in ticks] or sorted(ticks)[:5]
if not symbols:
    st.info("Menunggu data. Pastikan producer.py (dan consumer.py untuk alert) sedang berjalan.")
    st.stop()

frames = {s: to_df(s) for s in symbols}

# Ticker bar
items = ""
for s in symbols:
    df = frames[s]
    ch = change(df)
    cls, arrow = ("up", "▲") if ch >= 0 else ("down", "▼")
    items += (f'<div class="tk"><span class="dot"></span><span>{s}</span>'
              f'<span class="px">{fmt(df["price"].iloc[-1])}</span>'
              f'<span class="{cls}">{arrow} {abs(ch):.2f}%</span></div>')
st.markdown(f'<div class="ticker">{items}</div>', unsafe_allow_html=True)

# Baris utama: watchlist + candlestick
left, right = st.columns([1.3, 4])
with right:
    c1, c2, _ = st.columns([1, 1, 3])
    sel = c1.selectbox("Saham", symbols)
    sec = c2.selectbox("Interval candle", [3, 5, 10, 30], index=1,
                       format_func=lambda x: f"{x} detik")

with left:
    rows = ""
    for s in symbols:
        df = frames[s]
        ch = change(df)
        vol = int(df["volume"].tail(60).sum())
        rows += (f'<tr class="{"sel" if s == sel else ""}"><td>{s}</td>'
                 f'<td>{fmt(df["price"].iloc[-1])}</td>'
                 f'<td class="{"up" if ch >= 0 else "down"}">{ch:+.2f}%</td>'
                 f'<td>{vol:,}</td></tr>')
    st.markdown(
        '<div class="card"><div class="ctitle"><b>Volume Watchlist</b></div>'
        '<table class="wl"><thead><tr><th>Simbol</th><th>Harga</th><th>Chg</th><th>Volume</th></tr></thead>'
        f'<tbody>{rows}</tbody></table></div>', unsafe_allow_html=True)

with right:
    show(candle_fig(frames[sel], sec, sel), "candle")

st.markdown('<div class="rule"></div>', unsafe_allow_html=True)

# Ringkasan per saham
cols = st.columns(len(symbols))
for col, s in zip(cols, symbols):
    df = frames[s]
    w = df.tail(60)
    ch = change(df)
    cls, arrow = ("up", "▲") if ch >= 0 else ("down", "▼")
    col.markdown(
        f'<div class="card"><div class="ctitle">Ringkasan : <b>{s}</b></div>'
        f'<div class="big">{fmt(df["price"].iloc[-1])}<span class="{cls}">{arrow} {abs(ch):.2f}%</span></div>'
        f'{spark_svg(df["price"].tail(120), ch >= 0)}'
        f'<div class="kv"><span>Tertinggi</span><b>{fmt(w["price"].max())}</b></div>'
        f'<div class="kv"><span>Terendah</span><b>{fmt(w["price"].min())}</b></div>'
        f'<div class="kv"><span>Volume</span><b>{int(w["volume"].sum()):,}</b></div></div>',
        unsafe_allow_html=True)

# Volume + harga, dan alert
bawah_kiri, bawah_kanan = st.columns([2.2, 1])
with bawah_kiri:
    show(combo_fig(frames[sel], sel), "combo")

with bawah_kanan:
    if alerts:
        baris = ""
        for a in list(alerts)[:8]:
            p = a.get("change_percent", 0)
            cls = "up" if p > 0 else "down"
            baris += (f'<div class="al"><span class="badge {cls}">{"Naik" if p > 0 else "Turun"}</span>'
                      f'<b>{a.get("symbol", "")}</b><span>{fmt(a.get("price", 0))}</span>'
                      f'<span class="{cls}">{p:+.2f}%</span>'
                      f'<span class="muted">{str(a.get("timestamp", ""))[11:19]}</span></div>')
    else:
        baris = '<div class="muted" style="font-size:12px;padding:8px 0;">Belum ada alert. Jalankan consumer.py.</div>'
    st.markdown(f'<div class="card"><div class="ctitle"><b>Alert terbaru</b></div>{baris}</div>',
                unsafe_allow_html=True)