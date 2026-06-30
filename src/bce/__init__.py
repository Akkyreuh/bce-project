"""BCE pipeline — ingestion bronze."""

__version__ = "0.2.0"

from bce.catalog import CatalogStore, ingest_kbo_catalog
from bce.pipeline import BronzePipeline, PipelineOrchestrator
from bce.scrapers import CBSOScraper, is_valid_csv_bytes, select_deposits
from bce.state import ArtifactStore, BronzeStorage, build_artifact_key, reconcile_local_bronze, sha256_bytes
from bce.utils import format_bce, normalize_bce, tva_from_bce

__all__ = [
    "BronzePipeline",
    "PipelineOrchestrator",
    "CatalogStore",
    "ArtifactStore",
    "BronzeStorage",
    "ingest_kbo_catalog",
    "reconcile_local_bronze",
    "CBSOScraper",
    "select_deposits",
    "is_valid_csv_bytes",
    "build_artifact_key",
    "sha256_bytes",
    "normalize_bce",
    "format_bce",
    "tva_from_bce",
]
