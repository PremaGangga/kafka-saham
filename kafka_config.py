import os

from dotenv import load_dotenv

load_dotenv()


def _get(nama, default=None):
    nilai = os.environ.get(nama)
    if nilai:
        return nilai
    try:
        import streamlit as st
        return st.secrets.get(nama, default)
    except Exception:
        return default


def kafka_config(extra=None):
    mode = _get("KAFKA_MODE", "local")
    if mode == "cloud":
        cfg = {
            "bootstrap.servers": _get("KAFKA_BOOTSTRAP"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-256",
            "sasl.username": _get("KAFKA_USER", "avnadmin"),
            "sasl.password": _get("KAFKA_PASSWORD"),
            "ssl.ca.location": _get("KAFKA_CA", "ca.pem"),
        }
    else:
        cfg = {"bootstrap.servers": "localhost:9092"}
    if extra:
        cfg.update(extra)
    return cfg