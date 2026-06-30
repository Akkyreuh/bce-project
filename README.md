# BCE Pipeline — ingestion bronze

Industrialisation du notebook `BCE_final.ipynb`.

## Structure (simplifiée)

```
src/bce/
  config.py      # variables d'env
  utils.py       # normalisation BCE / TVA
  catalog.py     # ingest KBO → Mongo
  state.py       # state DB + stockage bronze
  scrapers.py    # CBSO, Stapor, eJustice, KBO + Tor
  pipeline.py    # discovery + download + orchestration
dags/
  bce_dags.py    # tous les DAGs Airflow
tests/
  test_bce.py
```

## Setup

```powershell
cd "C:\Users\louis\Desktop\BCE-project"
copy .env.example .env
pip install -e ".[dev]"
python -m playwright install chromium
docker compose up -d mongo tor postgres
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow : **http://localhost:8082** — `admin` / `admin`

## DAGs

| DAG | Rôle |
|-----|------|
| `kbo_catalog_ingest` | ~1,19 M personnes morales → Mongo |
| `cbso_bronze_daily` | NBB PDF+CSV → bronze |
| `stapor_bronze_weekly` | Statuts → bronze |
| `ejustice_bronze_weekly` | Moniteur → bronze |
| `state_reconcile_hdfs` | Aligne state ↔ fichiers bronze |
| `cbso_bronze_partition` | Scale par préfixe BCE |

Pilote CBSO : params `{"limit": 3, "max_batches": 1, "prefix": "0878"}`

## Tests

```powershell
pytest
```

## Bronze layout

```
/bronze/nbb/pdfs/{bce}/{year}.pdf
/bronze/nbb/csvs/{bce}/{year}.csv
/bronze/stapor/{bce}/{document_id}.pdf
/bronze/ejustice/{bce}/{numac}.pdf
```
