"""Tests BCE pipeline."""

from bce.scrapers import is_valid_csv_bytes, select_deposits
from bce.state import BronzeStorage, build_artifact_key, sha256_bytes
from bce.utils import format_bce, normalize_bce, tva_from_bce


def test_normalize_bce():
    assert normalize_bce("878.065.378") == "0878065378"
    assert format_bce("878065378") == "0878.065.378"
    assert tva_from_bce("0878065378") == "BE0878065378"


def test_select_deposits_prefers_fr():
    deposits = [
        {"periodEndDateYear": 2025, "language": "NL", "depositDate": "2026-01-01", "modelId": "m82-f", "id": "a"},
        {"periodEndDateYear": 2025, "language": "FR", "depositDate": "2026-02-01", "modelId": "m82-f", "id": "b"},
        {"periodEndDateYear": 2025, "language": "FR", "depositDate": "2026-01-01", "modelId": "mc-full", "id": "c"},
    ]
    assert select_deposits(deposits)[2025]["id"] == "b"


def test_is_valid_csv_bytes():
    assert is_valid_csv_bytes(b'"Reference number","x"\n"70","1"\n')
    assert not is_valid_csv_bytes(b"<html>error</html>")


def test_artifact_key_unique():
    k1 = build_artifact_key("0878065378", "cbso", "pdf", deposit_id="dep1", year=2025)
    k2 = build_artifact_key("0878065378", "cbso", "csv", deposit_id="dep1", year=2025)
    assert k1 != k2


def test_sha256():
    assert len(sha256_bytes(b"test")) == 64


def test_bronze_storage_local_write(tmp_path):
    storage = BronzeStorage(local_root=str(tmp_path), webhdfs_url="")
    path = storage.bronze_path_cbso_csv("0878065378", 2025)
    storage.write(path, b"col1,col2\n1,2\n")
    assert storage.exists(path)
    assert storage.read_bytes(path) == b"col1,col2\n1,2\n"
