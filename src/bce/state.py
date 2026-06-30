"""State DB idempotente + stockage bronze (local / WebHDFS)."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

import requests
from pymongo import ASCENDING, MongoClient, ReturnDocument
from pymongo.collection import Collection

from bce import config


# --- Bronze storage ---


class BronzeStorage:
    def __init__(
        self,
        webhdfs_url: str | None = None,
        user: str | None = None,
        local_root: str | None = None,
    ):
        self.webhdfs_url = (webhdfs_url or config.HDFS_WEBHDFS_URL).rstrip("/")
        self.user = user or config.HDFS_USER
        self.local_root = Path(local_root or config.BRONZE_ROOT)

    def _local_path(self, hdfs_path: str) -> Path:
        rel = hdfs_path.removeprefix("/bronze/").removeprefix("/")
        return self.local_root / rel

    def write(self, hdfs_path: str, data: bytes) -> str:
        if not hdfs_path.startswith("/bronze/"):
            hdfs_path = f"/bronze/{hdfs_path.lstrip('/')}"
        local = self._local_path(hdfs_path)
        local.parent.mkdir(parents=True, exist_ok=True)
        local.write_bytes(data)
        if self.webhdfs_url:
            self._ensure_webhdfs_dir(os.path.dirname(hdfs_path))
            self._webhdfs_put(hdfs_path, data)
        return hdfs_path

    def exists(self, hdfs_path: str) -> bool:
        if not hdfs_path.startswith("/bronze/"):
            hdfs_path = f"/bronze/{hdfs_path.lstrip('/')}"
        if self._local_path(hdfs_path).is_file():
            return True
        if not self.webhdfs_url:
            return False
        path = quote(hdfs_path, safe="/")
        url = f"{self.webhdfs_url}/webhdfs/v1{path}?op=GETFILESTATUS&user.name={self.user}"
        try:
            return requests.get(url, timeout=30).status_code == 200
        except requests.RequestException:
            return False

    def read_bytes(self, hdfs_path: str) -> bytes:
        local = self._local_path(hdfs_path)
        if local.is_file():
            return local.read_bytes()
        if not self.webhdfs_url:
            raise FileNotFoundError(hdfs_path)
        path = quote(hdfs_path, safe="/")
        url = f"{self.webhdfs_url}/webhdfs/v1{path}?op=OPEN&user.name={self.user}"
        r = requests.get(url, allow_redirects=True, timeout=120)
        r.raise_for_status()
        return r.content

    def _webhdfs_put(self, hdfs_path: str, data: bytes) -> None:
        path = quote(hdfs_path, safe="/")
        create_url = f"{self.webhdfs_url}/webhdfs/v1{path}?op=CREATE&overwrite=true&user.name={self.user}"
        r = requests.put(create_url, allow_redirects=False, timeout=120)
        r.raise_for_status()
        r2 = requests.put(r.headers["Location"], data=data, headers={"Content-Type": "application/octet-stream"}, timeout=300)
        r2.raise_for_status()

    def _ensure_webhdfs_dir(self, hdfs_dir: str) -> None:
        path = quote(hdfs_dir, safe="/")
        requests.put(f"{self.webhdfs_url}/webhdfs/v1{path}?op=MKDIRS&user.name={self.user}", timeout=30)

    def bronze_path_cbso_pdf(self, bce_number: str, year: int) -> str:
        return f"/bronze/nbb/pdfs/{bce_number}/{year}.pdf"

    def bronze_path_cbso_csv(self, bce_number: str, year: int) -> str:
        return f"/bronze/nbb/csvs/{bce_number}/{year}.csv"

    def bronze_path_stapor(self, bce_number: str, document_id: str) -> str:
        return f"/bronze/stapor/{bce_number}/{document_id}.pdf"

    def bronze_path_ejustice(self, bce_number: str, numac: str) -> str:
        return f"/bronze/ejustice/{bce_number}/{numac}.pdf"


# --- Artifact state ---


class ArtifactStatus(str, Enum):
    PENDING = "pending"
    DONE = "done"
    ERROR = "error"
    SKIPPED = "skipped"
    UNAVAILABLE = "unavailable"


def build_artifact_key(
    bce_number: str,
    source: str,
    artifact_type: str,
    *,
    deposit_id: str | None = None,
    document_id: str | None = None,
    publication_numac: str | None = None,
    year: int | None = None,
) -> str:
    parts = [bce_number, source]
    if deposit_id:
        parts.append(deposit_id)
    if document_id:
        parts.append(document_id)
    if publication_numac:
        parts.append(publication_numac)
    parts.append(artifact_type)
    if year is not None:
        parts.append(str(year))
    return "|".join(parts)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ArtifactStore:
    def __init__(self, mongo_uri: str | None = None, db_name: str | None = None):
        self._client = MongoClient(mongo_uri or config.MONGO_URI)
        self._col: Collection = self._client[db_name or config.MONGO_STATE_DB]["artifacts"]
        self.ensure_indexes()

    def ensure_indexes(self) -> None:
        self._col.create_index("artifact_key", unique=True)
        self._col.create_index([("source", ASCENDING), ("status", ASCENDING), ("bce_number", ASCENDING)])

    def upsert_pending(self, doc: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        key = doc["artifact_key"]
        on_insert = {
            "artifact_key": key,
            "bce_number": doc["bce_number"],
            "source": doc["source"],
            "artifact_type": doc["artifact_type"],
            "discovered_at": now,
            "attempts": 0,
            "status": ArtifactStatus.PENDING.value,
        }
        update = {
            "$setOnInsert": on_insert,
            "$set": {k: v for k, v in doc.items() if k not in on_insert},
        }
        return self._col.find_one_and_update(
            {"artifact_key": key}, update, upsert=True, return_document=ReturnDocument.AFTER
        )

    def mark_done(self, artifact_key: str, *, hdfs_path: str, checksum_sha256: str, file_size: int) -> None:
        self._col.update_one(
            {"artifact_key": artifact_key},
            {"$set": {
                "status": ArtifactStatus.DONE.value,
                "hdfs_path": hdfs_path,
                "checksum_sha256": checksum_sha256,
                "file_size": file_size,
                "completed_at": datetime.now(timezone.utc),
                "last_error": None,
            }},
        )

    def mark_error(self, artifact_key: str, error: str) -> None:
        self._col.update_one(
            {"artifact_key": artifact_key},
            {"$set": {"status": ArtifactStatus.ERROR.value, "last_error": error[:2000], "updated_at": datetime.now(timezone.utc)}, "$inc": {"attempts": 1}},
        )

    def get(self, artifact_key: str) -> dict | None:
        return self._col.find_one({"artifact_key": artifact_key})

    def is_done(self, artifact_key: str) -> bool:
        doc = self.get(artifact_key)
        return doc is not None and doc.get("status") == ArtifactStatus.DONE.value

    def iter_pending(
        self,
        source: str,
        *,
        bce_numbers: list[str] | None = None,
        max_attempts: int | None = None,
        limit: int | None = None,
    ) -> Iterator[dict]:
        query: dict[str, Any] = {
            "source": source,
            "status": {"$in": [ArtifactStatus.PENDING.value, ArtifactStatus.ERROR.value]},
            "attempts": {"$lt": max_attempts if max_attempts is not None else config.BCE_MAX_ATTEMPTS},
        }
        if bce_numbers:
            query["bce_number"] = {"$in": bce_numbers}
        cursor = self._col.find(query).sort("discovered_at", ASCENDING)
        if limit:
            cursor = cursor.limit(limit)
        yield from cursor

    def close(self) -> None:
        self._client.close()


def reconcile_local_bronze(store: ArtifactStore, bronze_root: str | None = None, source: str = "cbso") -> int:
    root = Path(bronze_root or config.BRONZE_ROOT)
    updated = 0
    patterns = [
        ("cbso", "pdf", root / "nbb" / "pdfs"),
        ("cbso", "csv", root / "nbb" / "csvs"),
        ("stapor", "statute_pdf", root / "stapor"),
        ("ejustice", "publication_pdf", root / "ejustice"),
    ]
    for src, artifact_type, base in patterns:
        if source != src or not base.exists():
            continue
        for bce_dir in base.iterdir():
            if not bce_dir.is_dir():
                continue
            for fpath in bce_dir.iterdir():
                if not fpath.is_file():
                    continue
                stem = fpath.stem
                year = int(stem) if stem.isdigit() and len(stem) == 4 else None
                key = build_artifact_key(
                    bce_dir.name, src, artifact_type,
                    deposit_id=stem, document_id=stem if artifact_type == "statute_pdf" else None,
                    publication_numac=stem if artifact_type == "publication_pdf" else None, year=year,
                )
                if store.get(key) and store.get(key).get("status") == "done":
                    continue
                data = fpath.read_bytes()
                store.upsert_pending({"artifact_key": key, "bce_number": bce_dir.name, "source": src, "artifact_type": artifact_type, "deposit_id": stem, "year": year})
                store.mark_done(key, hdfs_path=f"/bronze/{fpath.relative_to(root).as_posix()}", checksum_sha256=sha256_bytes(data), file_size=len(data))
                updated += 1
    return updated
