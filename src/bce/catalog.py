"""Catalogue KBO → MongoDB + partitionnement par préfixe BCE."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

from pymongo import ASCENDING, MongoClient, UpdateOne
from pymongo.collection import Collection

from bce import config
from bce.utils import format_bce, normalize_bce, tva_from_bce

TYPE_PERSONNE_MORALE = "2"
DENOMINATION_PRIMARY = "001"
BCE_PREFIXES = [f"{i:02d}" for i in range(100)]


class CatalogStore:
    def __init__(self, mongo_uri: str | None = None, db_name: str | None = None):
        self._client = MongoClient(mongo_uri or config.MONGO_URI)
        self._col: Collection = self._client[db_name or config.MONGO_CATALOG_DB]["enterprises"]
        self.ensure_indexes()

    def ensure_indexes(self) -> None:
        self._col.create_index("bce_number", unique=True)
        self._col.create_index([("status", ASCENDING)])
        self._col.create_index([("type_of_enterprise", ASCENDING)])

    def upsert_batch(self, docs: list[dict]) -> int:
        if not docs:
            return 0
        ops = [UpdateOne({"bce_number": d["bce_number"]}, {"$set": d}, upsert=True) for d in docs]
        result = self._col.bulk_write(ops, ordered=False)
        return result.upserted_count + result.modified_count

    def count(self, query: dict | None = None) -> int:
        return self._col.count_documents(query or {})

    def iter_bce_numbers(
        self,
        *,
        status: str | None = "AC",
        type_of_enterprise: str | None = None,
        prefix: str | None = None,
        limit: int | None = None,
        batch_size: int | None = None,
    ) -> Iterator[list[str]]:
        query: dict = {}
        if type_of_enterprise:
            query["type_of_enterprise"] = type_of_enterprise
        if status:
            query["status"] = status
        if prefix:
            query["bce_number"] = {"$regex": f"^{prefix}"}
        elif config.BCE_PILOT_PREFIX:
            query["bce_number"] = {"$regex": f"^{config.BCE_PILOT_PREFIX}"}

        batch_size = batch_size or config.BCE_BATCH_SIZE
        cursor = self._col.find(query, {"bce_number": 1}).sort("bce_number", ASCENDING)
        if limit:
            cursor = cursor.limit(limit)

        batch: list[str] = []
        for doc in cursor:
            batch.append(doc["bce_number"])
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def close(self) -> None:
        self._client.close()


# --- Merge des CSV liés dans le doc entreprise (bronze) ---

CONTACT_FIELD = {"TEL": "tel", "EMAIL": "email", "WEB": "web"}


def _f(row: dict, *keys: str) -> str | None:
    """Première valeur non vide parmi keys (préférence FR)."""
    for k in keys:
        v = (row.get(k) or "").strip()
        if v:
            return v
    return None


def _iter_groups(path: Path, key_col: str) -> Iterator[tuple[str, list[dict]]]:
    """Streame un CSV trié par key_col → yield (key, [rows])."""
    with open(path, encoding="utf-8-sig", newline="") as f:
        cur: str | None = None
        buf: list[dict] = []
        for row in csv.DictReader(f):
            k = row[key_col]
            if cur is None:
                cur = k
            if k != cur:
                yield cur, buf
                cur, buf = k, []
            buf.append(row)
        if cur is not None and buf:
            yield cur, buf


def _bulk_embed(col, path: Path, key_col: str, build_fields, batch_size: int = 5000) -> tuple[int, int]:
    """CSV trié par key_col → $set des champs de build_fields(rows) dans chaque doc entreprise."""
    if not path.is_file():
        return (0, 0)
    ops: list[UpdateOne] = []
    entities = matched = 0
    for key, rows in _iter_groups(path, key_col):
        fields = build_fields(rows)
        if not fields:
            continue
        entities += 1
        ops.append(UpdateOne({"bce_number": normalize_bce(key)}, {"$set": fields}))
        if len(ops) >= batch_size:
            matched += col.bulk_write(ops, ordered=False).matched_count
            ops = []
    if ops:
        matched += col.bulk_write(ops, ordered=False).matched_count
    return (entities, matched)


def _build_denominations(rows: list[dict]) -> dict:
    denoms = [
        {"language": r.get("Language"), "type": r.get("TypeOfDenomination"), "value": v}
        for r in rows if (v := (r.get("Denomination") or "").strip())
    ]
    primary = next((d["value"] for d in denoms if d["type"] == DENOMINATION_PRIMARY), None)
    return {"denominations": denoms, "denomination": primary}


def _build_address(rows: list[dict]) -> dict | None:
    r = next((x for x in rows if x.get("TypeOfAddress") == "REGO"), rows[0] if rows else None)
    if not r:
        return None
    return {"address": {
        "type": r.get("TypeOfAddress"),
        "street": _f(r, "StreetFR", "StreetNL"),
        "house_number": _f(r, "HouseNumber"),
        "box": _f(r, "Box"),
        "zipcode": _f(r, "Zipcode"),
        "municipality": _f(r, "MunicipalityFR", "MunicipalityNL"),
        "country": _f(r, "CountryFR", "CountryNL"),
    }}


def _build_contacts(rows: list[dict]) -> dict:
    out: dict[str, list[str]] = {"tel": [], "email": [], "web": []}
    for r in rows:
        field = CONTACT_FIELD.get(r.get("ContactType"))
        v = (r.get("Value") or "").strip()
        if field and v and v not in out[field]:
            out[field].append(v)
    return {"contacts": out}


def _build_activities_raw(rows: list[dict]) -> dict | None:
    """Activités BRUTES (toutes versions NACE, non dédupliquées) — la dédup se fait en silver."""
    acts = [{
        "nace_version": _f(r, "NaceVersion"),
        "nace_code": code,
        "classification": _f(r, "Classification"),
        "activity_group": _f(r, "ActivityGroup"),
    } for r in rows if (code := (r.get("NaceCode") or "").strip())]
    return {"activities": acts} if acts else None


def _embed_establishments(col, path: Path, batch_size: int = 5000) -> tuple[int, int]:
    """establishment.csv trié par EstablishmentNumber → agrégation mémoire par EnterpriseNumber."""
    if not path.is_file():
        return (0, 0)
    from collections import defaultdict

    agg: dict[str, list] = defaultdict(list)
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            agg[r["EnterpriseNumber"]].append(
                {"number": r["EstablishmentNumber"], "start_date_raw": _f(r, "StartDate")}
            )
    ops: list[UpdateOne] = []
    matched = 0
    for ent, estabs in agg.items():
        ops.append(UpdateOne(
            {"bce_number": normalize_bce(ent)},
            {"$set": {"establishments": estabs, "establishments_count": len(estabs)}},
        ))
        if len(ops) >= batch_size:
            matched += col.bulk_write(ops, ordered=False).matched_count
            ops = []
    if ops:
        matched += col.bulk_write(ops, ordered=False).matched_count
    return (len(agg), matched)


def ingest_kbo_catalog(
    kbo_dir: str | None = None,
    catalog_version: str | None = None,
    batch_size: int = 5000,
    *,
    all_types: bool = True,
    enrich: bool = True,
) -> dict:
    kbo_path = Path(kbo_dir or config.KBO_DATA_DIR)
    enterprise_path = kbo_path / "enterprise.csv"
    if not enterprise_path.is_file():
        raise FileNotFoundError(f"Missing {enterprise_path}")

    version = catalog_version or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    store = CatalogStore()
    col = store._col
    now = datetime.now(timezone.utc)

    # 1) Ancre : enterprise.csv (valeurs brutes, codes non traduits)
    total = 0
    batch: list[dict] = []
    with open(enterprise_path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            ent_type = row.get("TypeOfEnterprise") or ""
            if not all_types and ent_type != TYPE_PERSONNE_MORALE:
                continue
            bce_number = normalize_bce(row["EnterpriseNumber"])
            batch.append({
                "bce_number": bce_number,
                "bce_formatted": format_bce(bce_number),
                "tva": tva_from_bce(bce_number),
                "type_of_enterprise": ent_type,
                "status": row.get("Status"),
                "juridical_situation": row.get("JuridicalSituation"),
                "juridical_form": row.get("JuridicalForm"),
                "start_date": row.get("StartDate"),
                "catalog_version": version,
                "updated_at": now,
            })
            if len(batch) >= batch_size:
                total += store.upsert_batch(batch)
                batch = []
    if batch:
        total += store.upsert_batch(batch)

    stats: dict = {"enterprises": total, "catalog_version": version}

    # 2) Merge des CSV liés dans chaque doc entreprise
    if enrich:
        stats["denominations"] = _bulk_embed(col, kbo_path / "denomination.csv", "EntityNumber", _build_denominations, batch_size)
        stats["addresses"] = _bulk_embed(col, kbo_path / "address.csv", "EntityNumber", _build_address, batch_size)
        stats["contacts"] = _bulk_embed(col, kbo_path / "contact.csv", "EntityNumber", _build_contacts, batch_size)
        stats["activities"] = _bulk_embed(col, kbo_path / "activity.csv", "EntityNumber", _build_activities_raw, batch_size)
        stats["establishments"] = _embed_establishments(col, kbo_path / "establishment.csv", batch_size)

    stats["total_in_catalog"] = store.count()
    store.close()
    return stats


def all_bce_prefixes() -> list[str]:
    return list(BCE_PREFIXES)
