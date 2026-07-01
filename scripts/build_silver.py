"""Construit entreprise_silver depuis le bronze mergé (transformations).

    python scripts/build_silver.py
    python scripts/build_silver.py --limit 1000
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

from bce.silver import build_silver


def main() -> int:
    ap = argparse.ArgumentParser(description="Build entreprise_silver")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--batch-size", type=int, default=5000)
    args = ap.parse_args()

    print("Transformation bronze -> entreprise_silver...")
    stats = build_silver(batch_size=args.batch_size, limit=args.limit)
    for k, v in stats.items():
        print(f"  {k:18}: {v:,}" if isinstance(v, int) else f"  {k:18}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
