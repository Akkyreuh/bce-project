"""Pipeline bronze — discovery, download, orchestration par batch."""

from __future__ import annotations

import logging
from typing import Iterator

from bce import config
from bce.catalog import CatalogStore
from bce.scrapers import (
    CBSOScraper,
    EJusticeScraper,
    KBOWebScraper,
    StaporScraper,
    is_valid_csv_bytes,
    renew_tor_identity,
)
from bce.state import ArtifactStore, BronzeStorage, build_artifact_key, sha256_bytes

logger = logging.getLogger(__name__)


def _validate_download(artifact_type: str, data: bytes) -> str | None:
    """Retourne un message d'erreur si le contenu n'est pas l'artefact attendu, sinon None."""
    if not data or len(data) < 100:
        return f"réponse trop courte ({len(data)} octets)"
    head = data.lstrip()[:20]
    if head[:1] == b"{" or head.startswith(b"<!") or head.lower().startswith(b"<html"):
        return "réponse non-binaire (erreur JSON/HTML)"
    if artifact_type.endswith("pdf") and not data[:5].startswith(b"%PDF"):
        return "PDF invalide (pas d'en-tête %PDF)"
    if artifact_type == "csv" and not is_valid_csv_bytes(data):
        return "CSV invalide"
    return None


class BronzePipeline:
    """Discovery + download pour CBSO, Stapor, eJustice."""

    def __init__(self, use_tor: bool = True):
        self.use_tor = use_tor
        self.state = ArtifactStore()
        self.catalog = CatalogStore()
        self.storage = BronzeStorage()
        self.cbso = CBSOScraper(use_tor=use_tor)
        self.stapor = StaporScraper(use_tor=use_tor)
        self.ejustice = EJusticeScraper(use_tor=use_tor)
        self.kbo = KBOWebScraper(use_tor=use_tor)

    def iter_batches(self, *, status: str = "AC", prefix: str | None = None, limit: int | None = None, batch_size: int | None = None) -> Iterator[list[str]]:
        yield from self.catalog.iter_bce_numbers(
            status=status,
            type_of_enterprise=config.BCE_TYPE_FILTER or None,
            prefix=prefix or config.BCE_PILOT_PREFIX or None,
            limit=limit,
            batch_size=batch_size,
        )

    def discover(self, bce_number: str, sources: list[str] | None = None) -> dict[str, int]:
        sources = sources or ["cbso", "stapor", "ejustice"]
        counts: dict[str, int] = {}

        if "cbso" in sources:
            n = 0
            for art in self.cbso.discover_artifacts(bce_number):
                key = build_artifact_key(art.bce_number, "cbso", art.artifact_type, deposit_id=art.deposit_id, year=art.year)
                if self.state.is_done(key):
                    continue
                self.state.upsert_pending({"artifact_key": key, "bce_number": art.bce_number, "source": "cbso",
                    "artifact_type": art.artifact_type, "deposit_id": art.deposit_id, "year": art.year,
                    "model": art.model, "language": art.language, "deposit_date": art.deposit_date})
                n += 1
            counts["cbso"] = n

        if "stapor" in sources:
            n = 0
            for art in self.stapor.discover_statutes(bce_number):
                key = build_artifact_key(art.bce_number, "stapor", "statute_pdf", document_id=art.document_id)
                if self.state.is_done(key):
                    continue
                self.state.upsert_pending({"artifact_key": key, "bce_number": art.bce_number, "source": "stapor",
                    "artifact_type": "statute_pdf", "document_id": art.document_id, "title": art.title,
                    "statute_date": art.date, "download_url": art.download_url})
                n += 1
            counts["stapor"] = n

        if "ejustice" in sources:
            url = self.kbo.get_moniteur_url(bce_number)
            n = 0
            if url:
                for pub in self.ejustice.discover_publications(url, bce_number):
                    key = build_artifact_key(pub.bce_number, "ejustice", "publication_pdf", publication_numac=pub.numac)
                    if self.state.is_done(key):
                        continue
                    self.state.upsert_pending({"artifact_key": key, "bce_number": pub.bce_number, "source": "ejustice",
                        "artifact_type": "publication_pdf", "publication_numac": pub.numac,
                        "publication_date": pub.date, "publication_type": pub.pub_type, "pdf_url": pub.pdf_url})
                    n += 1
            counts["ejustice"] = n

        return counts

    def download_one(self, doc: dict) -> bool:
        key = doc["artifact_key"]
        if self.state.is_done(key):
            return False
        try:
            source, bce = doc["source"], doc["bce_number"]
            if source == "cbso":
                data = self.cbso.download_deposit_file(bce, doc["deposit_id"], doc["artifact_type"])
                path = self.storage.bronze_path_cbso_pdf(bce, doc["year"]) if doc["artifact_type"] == "pdf" else self.storage.bronze_path_cbso_csv(bce, doc["year"])
            elif source == "stapor":
                data = self.stapor.download_document(bce, doc["document_id"])
                path = self.storage.bronze_path_stapor(bce, doc["document_id"])
            elif source == "ejustice":
                data = self.ejustice.download_pdf(doc["pdf_url"])
                path = self.storage.bronze_path_ejustice(bce, doc["publication_numac"])
            else:
                self.state.mark_error(key, f"Unknown source {source}")
                return False
            err = _validate_download(doc["artifact_type"], data)
            if err:
                self.state.mark_error(key, err)
                return False
            written = self.storage.write(path, data)
            self.state.mark_done(key, hdfs_path=written, checksum_sha256=sha256_bytes(data), file_size=len(data))
            return True
        except Exception as exc:
            self.state.mark_error(key, str(exc))
            return False

    def download_pending(self, source: str, bce_numbers: list[str], limit: int | None = None) -> dict[str, int]:
        ok, fail, skip = 0, 0, 0
        for doc in self.state.iter_pending(source, bce_numbers=bce_numbers, limit=limit):
            if self.state.is_done(doc["artifact_key"]):
                skip += 1
                continue
            if self.download_one(doc):
                ok += 1
            else:
                fail += 1
        return {"downloaded": ok, "failed": fail, "skipped_done": skip}

    def process_batch(self, bce_numbers: list[str], sources: list[str], *, rotate_tor: bool = True) -> dict:
        if rotate_tor and self.use_tor:
            renew_tor_identity()
        discovered: dict[str, int] = {}
        for bce in bce_numbers:
            try:
                for src, n in self.discover(bce, sources=sources).items():
                    discovered[src] = discovered.get(src, 0) + n
            except Exception as exc:
                logger.warning("Discovery failed for %s: %s", bce, exc)
        downloaded = {src: self.download_pending(src, bce_numbers) for src in sources}
        return {"bce_count": len(bce_numbers), "discovered": discovered, "downloaded": downloaded}

    def run_source(self, source: str, *, max_batches: int | None = None, limit: int | None = None) -> list[dict]:
        results = []
        for i, batch in enumerate(self.iter_batches(limit=limit, batch_size=config.BCE_BATCH_SIZE)):
            if max_batches is not None and i >= max_batches:
                break
            logger.info("Processing batch %s (%s enterprises)", i + 1, len(batch))
            results.append(self.process_batch(batch, sources=[source]))
        return results

    def close(self) -> None:
        self.state.close()
        self.catalog.close()


# Alias rétrocompat DAGs
PipelineOrchestrator = BronzePipeline
