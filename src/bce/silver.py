"""Silver layer — transformations propres du catalogue bronze → `entreprise_silver`.

Lit la collection source `enterprises` (jamais modifiée) et écrit les documents
transformés dans `entreprise_silver`. Chaque transformation s'ajoute dans `to_silver()`.

Transformations actuelles :
  - start_date : DD-MM-YYYY → YYYY-MM-DD (garde l'original dans start_date_raw)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne

from bce import config

SOURCE_COLLECTION = "enterprises"
SILVER_COLLECTION = "entreprise_silver"


def normalize_start_date(value: Any) -> str | None:
    """DD-MM-YYYY → YYYY-MM-DD. None si vide/invalide (déjà ISO accepté tel quel)."""
    if not value:
        return None
    s = str(value).strip()
    parts = s.split("-")
    if len(parts) == 3 and len(parts[0]) == 2 and len(parts[2]) == 4:
        d, m, y = parts
        try:
            datetime(int(y), int(m), int(d))
        except ValueError:
            return None
        return f"{y}-{m}-{d}"
    try:  # déjà au format ISO ?
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        return None


def to_silver(doc: dict) -> dict:
    """Transforme un document source en document silver."""
    out = dict(doc)
    out.pop("_id", None)
    raw = doc.get("start_date")
    out["start_date_raw"] = raw
    out["start_date"] = normalize_start_date(raw)
    return out


def build_silver(batch_size: int = 5000, limit: int | None = None) -> dict:
    client = MongoClient(config.MONGO_URI)
    db = client[config.MONGO_CATALOG_DB]
    src = db[SOURCE_COLLECTION]
    dst = db[SILVER_COLLECTION]
    dst.create_index("bce_number", unique=True)
    dst.create_index([("start_date", ASCENDING)])

    now = datetime.now(timezone.utc)
    processed = date_null = 0
    ops: list[UpdateOne] = []
    cursor = src.find({}, no_cursor_timeout=True)
    if limit:
        cursor = cursor.limit(limit)
    try:
        for doc in cursor:
            s = to_silver(doc)
            s["silver_updated_at"] = now
            if s["start_date"] is None:
                date_null += 1
            ops.append(UpdateOne({"bce_number": s["bce_number"]}, {"$set": s}, upsert=True))
            if len(ops) >= batch_size:
                dst.bulk_write(ops, ordered=False)
                processed += len(ops)
                ops = []
        if ops:
            dst.bulk_write(ops, ordered=False)
            processed += len(ops)
    finally:
        cursor.close()

    stats = {
        "processed": processed,
        "silver_count": dst.count_documents({}),
        "start_date_null": date_null,
    }
    client.close()
    return stats
