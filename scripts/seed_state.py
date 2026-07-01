"""Initialise la State DB : discovery (sans téléchargement) → artefacts en `pending`.

Lit les entreprises cibles depuis MongoDB (catalogue) ou une liste de numéros BCE,
puis peuple bce_state.artifacts avec un doc par fichier découvert (CBSO/Stapor/eJustice)
en status=pending. Idempotent : ne réinsère pas ce qui est déjà `done`.

Exemples :
    python scripts/seed_state.py 0878065378 0836157420 0203430576
    python scripts/seed_state.py --prefix 0878 --limit 50
    python scripts/seed_state.py --limit 100 --tor
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bce.pipeline import BronzePipeline

SOURCES = ["cbso", "stapor", "ejustice"]


def _targets(pipe: BronzePipeline, args: argparse.Namespace) -> list[str]:
    if args.bce_numbers:
        from bce.utils import normalize_bce
        return [normalize_bce(n) for n in args.bce_numbers]
    targets: list[str] = []
    for batch in pipe.catalog.iter_bce_numbers(
        status="AC", prefix=args.prefix or None, limit=args.limit, batch_size=args.limit or 500
    ):
        targets.extend(batch)
        if args.limit and len(targets) >= args.limit:
            break
    return targets[: args.limit] if args.limit else targets


def main() -> int:
    ap = argparse.ArgumentParser(description="Seed State DB (discovery → pending)")
    ap.add_argument("bce_numbers", nargs="*", help="Numéros BCE cibles (sinon lecture catalogue)")
    ap.add_argument("--prefix", default="", help="Filtre préfixe BCE (ex. 0878)")
    ap.add_argument("--limit", type=int, default=None, help="Nombre max d'entreprises")
    ap.add_argument("--tor", action="store_true", help="Passer par Tor")
    args = ap.parse_args()

    pipe = BronzePipeline(use_tor=args.tor)
    try:
        targets = _targets(pipe, args)
        if not targets:
            print("Aucune entreprise cible.")
            return 1
        print(f"Discovery sur {len(targets)} entreprise(s), sources={SOURCES}\n")
        totals = {s: 0 for s in SOURCES}
        for i, bce in enumerate(targets, 1):
            try:
                counts = pipe.discover(bce, sources=SOURCES)
            except Exception as exc:
                print(f"[{i}/{len(targets)}] {bce}  ERREUR discovery: {exc}")
                continue
            for s, n in counts.items():
                totals[s] += n
            print(f"[{i}/{len(targets)}] {bce}  " + "  ".join(f"{s}={counts.get(s,0)}" for s in SOURCES))

        st = pipe.state._col
        print("\n=== State DB après seed ===")
        for s in SOURCES:
            pend = st.count_documents({"source": s, "status": "pending"})
            done = st.count_documents({"source": s, "status": "done"})
            print(f"  {s:9} pending={pend:5}  done={done:5}  (nouveaux ce run: {totals[s]})")
        return 0
    finally:
        pipe.close()


if __name__ == "__main__":
    raise SystemExit(main())
