"""Silver layer — transformations propres du catalogue bronze → `entreprise_silver`.

Lit la collection source `enterprises` (jamais modifiée) et écrit les documents
transformés dans `entreprise_silver`. Chaque transformation s'ajoute dans `to_silver()`.

Transformations actuelles :
  - start_date : DD-MM-YYYY → YYYY-MM-DD (garde l'original dans start_date_raw)
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from pymongo import ASCENDING, MongoClient, UpdateOne

from bce import config
from bce.utils import normalize_bce

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


#Activités NACE


def dedup_activities(rows: list[dict]) -> list[dict]:
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for r in rows:
        code = (r.get("NaceCode") or "").strip()
        classification = (r.get("Classification") or "").strip()
        if not code:
            continue
        key = (code, classification)
        if key in seen:
            continue
        seen.add(key)
        out.append({
            "nace_code": code,
            "classification": classification,
            "nace_version": (r.get("NaceVersion") or "").strip() or None,
            "activity_group": (r.get("ActivityGroup") or "").strip() or None,
        })
    return out


def _iter_entity_groups(activity_path: Path) -> Iterator[tuple[str, list[dict]]]:
    """Streame activity.csv (trié par EntityNumber) et yield (entity, lignes)."""
    with open(activity_path, encoding="utf-8-sig", newline="") as f:
        current: str | None = None
        buf: list[dict] = []
        for row in csv.DictReader(f):
            ent = row["EntityNumber"]
            if current is None:
                current = ent
            if ent != current:
                yield current, buf
                current, buf = ent, []
            buf.append(row)
        if current is not None and buf:
            yield current, buf


def build_activities(kbo_dir: str | None = None, batch_size: int = 5000) -> dict:
    """Embarque un tableau `activities` dédupliqué dans chaque doc silver existant."""
    activity_path = Path(kbo_dir or config.KBO_DATA_DIR) / "activity.csv"
    if not activity_path.is_file():
        raise FileNotFoundError(f"Missing {activity_path}")

    client = MongoClient(config.MONGO_URI)
    dst = client[config.MONGO_CATALOG_DB][SILVER_COLLECTION]

    now = datetime.now(timezone.utc)
    entities = matched = total_acts = 0
    ops: list[UpdateOne] = []
    for entity, rows in _iter_entity_groups(activity_path):
        acts = dedup_activities(rows)
        if not acts:
            continue
        entities += 1
        total_acts += len(acts)
        bce = normalize_bce(entity)
        ops.append(UpdateOne(
            {"bce_number": bce},
            {"$set": {"activities": acts, "activities_updated_at": now}},
        ))
        if len(ops) >= batch_size:
            matched += dst.bulk_write(ops, ordered=False).matched_count
            ops = []
    if ops:
        matched += dst.bulk_write(ops, ordered=False).matched_count

    stats = {
        "entities_with_activities": entities,
        "matched_in_silver": matched,
        "activities_embedded": total_acts,
    }
    client.close()
    return stats
