"""Configuration via variables d'environnement (charge .env à la racine si présent)."""

import os
from pathlib import Path


def _load_dotenv() -> None:
    p = Path(__file__).resolve().parents[2] / ".env"
    if not p.is_file():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_CATALOG_DB = os.getenv("MONGO_CATALOG_DB", "bce_catalog")
MONGO_STATE_DB = os.getenv("MONGO_STATE_DB", "bce_state")

KBO_DATA_DIR = os.getenv("KBO_DATA_DIR", os.path.join(os.getcwd(), "data"))
BRONZE_ROOT = os.getenv("BRONZE_ROOT", "./bronze")
HDFS_WEBHDFS_URL = os.getenv("HDFS_WEBHDFS_URL", "").rstrip("/")
HDFS_USER = os.getenv("HDFS_USER", "hdfs")

TOR_SOCKS_HOST = os.getenv("TOR_SOCKS_HOST", "localhost")
TOR_SOCKS_PORT = int(os.getenv("TOR_SOCKS_PORT", "9050"))
TOR_CONTROL_HOST = os.getenv("TOR_CONTROL_HOST", TOR_SOCKS_HOST)
TOR_CONTROL_PORT = int(os.getenv("TOR_CONTROL_PORT", "9051"))
TOR_CONTROL_PASSWORD = os.getenv("TOR_CONTROL_PASSWORD", "")

BCE_BATCH_SIZE = int(os.getenv("BCE_BATCH_SIZE", "500"))
BCE_MAX_ATTEMPTS = int(os.getenv("BCE_MAX_ATTEMPTS", "5"))
BCE_PILOT_PREFIX = os.getenv("BCE_PILOT_PREFIX", "")
# Filtre optionnel pour les batches scrape (ex. "2" = personnes morales). Vide = tout le catalogue.
BCE_TYPE_FILTER = os.getenv("BCE_TYPE_FILTER", "")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
