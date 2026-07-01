"""Silver layer — transformations propres du bronze mergé → `entreprise_silver`.

Le bronze (`enterprises`) contient déjà toutes les infos liées (denominations, adresse,
contacts, activités brutes, établissements), mergées depuis les CSV KBO. La silver ne fait
que TRANSFORMER, sans re-joindre de source :

  1. start_date     : DD-MM-YYYY → YYYY-MM-DD (raw conservé dans start_date_raw)
  2. adresse        : on ne garde que TypeOfAddress = REGO (siège social)
  3. denominations  : dénomination officielle (type 001) en premier, autres ensuite
  4. codes → labels : statut / forme / situation / type / NACE → libellés FR (code.csv),
                      codes bruts conservés pour filtre/index
  5. activités      : dédup (NaceCode + Classification), toutes versions confondues
  6. établissements : start_date normalisée
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo import ASCENDING, MongoClient, UpdateOne

from bce import config

SOURCE_COLLECTION = "enterprises"
SILVER_COLLECTION = "entreprise_silver"

NACE_CATEGORY = {"2025": "Nace2025", "2008": "Nace2008", "2003": "Nace2003"}
OFFICIAL_DENOMINATION = "001"


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


def load_code_map(kbo_dir: str | None = None, language: str = "FR") -> dict[tuple[str, str], str]:
    """Charge code.csv → {(Category, Code): Description} pour la langue donnée."""
    path = Path(kbo_dir or config.KBO_DATA_DIR) / "code.csv"
    out: dict[tuple[str, str], str] = {}
    if not path.is_file():
        return out
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if r.get("Language") == language:
                out[(r.get("Category", ""), r.get("Code", ""))] = r.get("Description")
    return out


def _t(codes: dict, category: str, code: Any) -> str | None:
    if not code or not category:
        return None
    return codes.get((category, str(code)))


def dedup_activities(activities: list[dict]) -> list[dict]:
    """Dédup par (nace_code, classification) — on ignore la version NACE.

    Codes différents conservés (70220 vs 70200) ; MAIN/SECO/ANCI d'un même code conservés
    (classification différente) ; on garde la 1re occurrence.
    """
    seen: set[tuple[str, str]] = set()
    out: list[dict] = []
    for a in activities:
        code = (a.get("nace_code") or "").strip()
        classification = (a.get("classification") or "").strip()
        if not code:
            continue
        key = (code, classification)
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(a))
    return out


def order_denominations(denoms: list[dict]) -> list[dict]:
    """Dénomination officielle (type 001) en premier, les autres ensuite (ordre stable)."""
    official = [d for d in denoms if d.get("type") == OFFICIAL_DENOMINATION]
    others = [d for d in denoms if d.get("type") != OFFICIAL_DENOMINATION]
    return official + others


def to_silver(doc: dict, codes: dict) -> dict:
    """Transforme un doc bronze mergé en doc silver."""
    out = dict(doc)
    out.pop("_id", None)

    # 1) date normalisée
    raw = doc.get("start_date")
    out["start_date_raw"] = raw
    out["start_date"] = normalize_start_date(raw)

    # 2) adresse : REGO uniquement
    addr = doc.get("address")
    out["address"] = addr if (addr and addr.get("type") == "REGO") else None

    # 3) dénomination officielle en premier
    out["denominations"] = order_denominations(doc.get("denominations") or [])

    # 4) codes → labels (codes bruts conservés)
    out["status_label"] = _t(codes, "Status", doc.get("status"))
    out["juridical_situation_label"] = _t(codes, "JuridicalSituation", doc.get("juridical_situation"))
    out["juridical_form_label"] = _t(codes, "JuridicalForm", doc.get("juridical_form"))
    out["type_of_enterprise_label"] = _t(codes, "TypeOfEnterprise", doc.get("type_of_enterprise"))

    # 5) activités : dédup + labels NACE
    acts = dedup_activities(doc.get("activities") or [])
    for a in acts:
        a["nace_label"] = _t(codes, NACE_CATEGORY.get(a.get("nace_version") or "", ""), a.get("nace_code"))
        a["classification_label"] = _t(codes, "Classification", a.get("classification"))
        a["activity_group_label"] = _t(codes, "ActivityGroup", a.get("activity_group"))
    out["activities"] = acts

    # 6) établissements : date normalisée
    for e in doc.get("establishments") or []:
        e["start_date"] = normalize_start_date(e.get("start_date_raw"))

    return out


def build_silver(kbo_dir: str | None = None, batch_size: int = 5000, limit: int | None = None) -> dict:
    codes = load_code_map(kbo_dir)
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
            s = to_silver(doc, codes)
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
        "codes_loaded": len(codes),
    }
    client.close()
    return stats
