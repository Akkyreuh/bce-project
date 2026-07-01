"""Construit le bronze : enterprise.csv + merge de tous les CSV liés → enterprises.

    python scripts/build_bronze.py
    python scripts/build_bronze.py --no-enrich   # ancre seule (enterprise.csv)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bce.catalog import ingest_kbo_catalog


def main() -> int:
    ap = argparse.ArgumentParser(description="Build bronze (enterprises mergé)")
    ap.add_argument("--no-enrich", action="store_true", help="Ancre seule, sans merge des CSV liés")
    ap.add_argument("--batch-size", type=int, default=5000)
    args = ap.parse_args()

    print("Ingestion + merge KBO -> enterprises...")
    stats = ingest_kbo_catalog(batch_size=args.batch_size, enrich=not args.no_enrich)
    for k, v in stats.items():
        if isinstance(v, tuple):
            print(f"  {k:16}: entities={v[0]:,}  matched={v[1]:,}")
        elif isinstance(v, int):
            print(f"  {k:16}: {v:,}")
        else:
            print(f"  {k:16}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
