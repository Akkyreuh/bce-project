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


def _load_denominations(kbo_dir: Path) -> dict[str, str]:
    path = kbo_dir / "denomination.csv"
    if not path.is_file():
        return {}
    out: dict[str, str] = {}
    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("TypeOfDenomination") != DENOMINATION_PRIMARY:
                continue
            ent = row.get("EntityNumber", "")
            denom = (row.get("Denomination") or "").strip()
            if ent and denom and ent not in out:
                out[ent] = denom
    return out


def ingest_kbo_catalog(
    kbo_dir: str | None = None,
    catalog_version: str | None = None,
    batch_size: int = 5000,
    *,
    all_types: bool = True,
) -> dict:
    kbo_path = Path(kbo_dir or config.KBO_DATA_DIR)
    enterprise_path = kbo_path / "enterprise.csv"
    if not enterprise_path.is_file():
        raise FileNotFoundError(f"Missing {enterprise_path}")

    version = catalog_version or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    denominations = _load_denominations(kbo_path)
    store = CatalogStore()
    now = datetime.now(timezone.utc)
    total = 0
    batch: list[dict] = []

    with open(enterprise_path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            ent_type = row.get("TypeOfEnterprise") or ""
            if not all_types and ent_type != TYPE_PERSONNE_MORALE:
                continue
            formatted = row["EnterpriseNumber"]
            bce_number = normalize_bce(formatted)
            doc = {
                "bce_number": bce_number,
                "bce_formatted": format_bce(bce_number),
                "tva": tva_from_bce(bce_number),
                "type_of_enterprise": ent_type,
                "status": row.get("Status"),
                "juridical_situation": row.get("JuridicalSituation"),
                "juridical_form": row.get("JuridicalForm"),
                "start_date": row.get("StartDate"),
                "denomination": denominations.get(formatted),
                "catalog_version": version,
                "updated_at": now,
            }
            batch.append(doc)
            if len(batch) >= batch_size:
                total += store.upsert_batch(batch)
                batch = []

    if batch:
        total += store.upsert_batch(batch)

    stats = {
        "upserted_or_modified": total,
        "total_in_catalog": store.count(),
        "active": store.count({"status": "AC"}),
        "personnes_morales": store.count({"type_of_enterprise": TYPE_PERSONNE_MORALE}),
        "catalog_version": version,
    }
    store.close()
    return stats


def all_bce_prefixes() -> list[str]:
    return list(BCE_PREFIXES)
