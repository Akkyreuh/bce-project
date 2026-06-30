"""Vérification rapide du pipeline bronze — lancer depuis la racine du projet."""

from __future__ import annotations

import sys

from bce.catalog import CatalogStore
from bce.state import ArtifactStore


def main() -> int:
    print("=== BCE Pipeline — vérification ===\n")

    catalog = CatalogStore()
    state = ArtifactStore()
    try:
        total = catalog.count()
        active = catalog.count({"status": "AC"})
        morales = catalog.count({"type_of_enterprise": "2"})
        print(f"Catalogue  total : {total:,}  (actives AC : {active:,}, personnes morales : {morales:,})")

        for source in ("cbso", "stapor", "ejustice"):
            done = state._col.count_documents({"source": source, "status": "done"})
            pending = state._col.count_documents({"source": source, "status": "pending"})
            err = state._col.count_documents({"source": source, "status": "error"})
            print(f"Bronze {source:8} done={done:4}  pending={pending:4}  error={err:4}")

        sample = state._col.find_one({"status": "done"})
        if sample:
            print(f"\nExemple fichier : {sample.get('hdfs_path')}  ({sample.get('file_size')} bytes)")
        else:
            print("\nAucun artefact 'done' — relance cbso_bronze_daily avec limit=3, prefix=0878")

        print("\nMongo : mongosh mongodb://localhost:27017")
        print("  use bce_state")
        print('  db.artifacts.find({status:"done"}).limit(5)')
    finally:
        catalog.close()
        state.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
