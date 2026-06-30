#!/usr/bin/env bash
# Pilote local — 3 entreprises
set -euo pipefail
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
export MONGO_URI="${MONGO_URI:-mongodb://localhost:27017}"
export BCE_PILOT_PREFIX="${BCE_PILOT_PREFIX:-0878}"
export BRONZE_ROOT="${BRONZE_ROOT:-$(pwd)/bronze}"

python - <<'PY'
from bce.pipeline import BronzePipeline

pipe = BronzePipeline(use_tor=False)
try:
    for batch in pipe.iter_batches(limit=3):
        print(pipe.process_batch(batch, sources=["cbso"]))
        break
finally:
    pipe.close()
PY
