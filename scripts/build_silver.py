"""Construit la collection entreprise_silver depuis le catalogue.

    python scripts/build_silver.py            # tout le catalogue
    python scripts/build_silver.py --limit 1000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bce.silver import build_activities, build_silver


def _show(stats: dict) -> None:
    for k, v in stats.items():
        print(f"  {k:24}: {v:,}" if isinstance(v, int) else f"  {k:24}: {v}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build entreprise_silver")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=5000)
    ap.add_argument("--no-activities", action="store_true", help="Ne pas embarquer les activités NACE")
    args = ap.parse_args()

    print("1) Documents entreprise (start_date normalisée)…")
    _show(build_silver(batch_size=args.batch_size, limit=args.limit))

    if not args.no_activities:
        print("\n2) Activités NACE (dédup NaceCode+Classification)…")
        _show(build_activities(batch_size=args.batch_size))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
