"""Tous les DAGs BCE — catalogue + bronze."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator

from bce.catalog import ingest_kbo_catalog
from bce.pipeline import BronzePipeline
from bce.state import ArtifactStore, reconcile_local_bronze

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "bce",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
START = datetime(2026, 1, 1)


def _run_ingest(**context):
    stats = ingest_kbo_catalog()
    context["ti"].xcom_push(key="catalog_stats", value=stats)
    return stats


def _run_bronze(source: str, **context):
    params = context.get("params", {})
    max_batches = params.get("max_batches", 2)
    limit = params.get("limit")
    prefix = params.get("prefix")
    if prefix:
        os.environ["BCE_PILOT_PREFIX"] = prefix

    pipe = BronzePipeline(use_tor=True)
    try:
        results = []
        for i, batch in enumerate(pipe.iter_batches(limit=limit)):
            if i >= max_batches:
                break
            logger.info("%s batch %s: %s enterprises", source, i + 1, len(batch))
            results.append(pipe.process_batch(batch, sources=[source]))
        return {"batches": len(results), "results": results}
    finally:
        pipe.close()


def _run_reconcile(**_context):
    store = ArtifactStore()
    try:
        return {src: reconcile_local_bronze(store, source=src) for src in ("cbso", "stapor", "ejustice")}
    finally:
        store.close()


def _run_partition(**context):
    prefix = context["params"]["prefix"]
    max_batches = context["params"].get("max_batches", 10)
    pipe = BronzePipeline(use_tor=True)
    try:
        results = []
        for i, batch in enumerate(pipe.iter_batches(prefix=prefix)):
            if i >= max_batches:
                break
            results.append(pipe.process_batch(batch, sources=["cbso"]))
        return {"prefix": prefix, "batches": len(results)}
    finally:
        pipe.close()


with DAG(
    dag_id="kbo_catalog_ingest",
    default_args=DEFAULT_ARGS,
    description="Charge enterprise.csv (personnes morales) dans MongoDB",
    schedule="@monthly",
    start_date=START,
    catchup=False,
    tags=["bce", "catalog"],
) as dag_catalog:
    PythonOperator(task_id="ingest_kbo_catalog", python_callable=_run_ingest)

with DAG(
    dag_id="cbso_bronze_daily",
    default_args=DEFAULT_ARGS,
    description="CBSO/NBB PDF+CSV → bronze",
    schedule="0 2 * * *",
    start_date=START,
    catchup=False,
    params={"max_batches": 2, "limit": 100, "prefix": ""},
    tags=["bce", "bronze", "cbso"],
) as dag_cbso:
    PythonOperator(task_id="cbso_bronze_pipeline", python_callable=_run_bronze, op_kwargs={"source": "cbso"})

with DAG(
    dag_id="stapor_bronze_weekly",
    default_args=DEFAULT_ARGS,
    description="Statuts Stapor → bronze",
    schedule="0 3 * * 0",
    start_date=START,
    catchup=False,
    params={"max_batches": 2, "limit": 50},
    tags=["bce", "bronze", "stapor"],
) as dag_stapor:
    PythonOperator(task_id="stapor_bronze_pipeline", python_callable=_run_bronze, op_kwargs={"source": "stapor"})

with DAG(
    dag_id="ejustice_bronze_weekly",
    default_args=DEFAULT_ARGS,
    description="Publications eJustice → bronze",
    schedule="0 4 * * 0",
    start_date=START,
    catchup=False,
    params={"max_batches": 2, "limit": 50},
    tags=["bce", "bronze", "ejustice"],
) as dag_ej:
    PythonOperator(task_id="ejustice_bronze_pipeline", python_callable=_run_bronze, op_kwargs={"source": "ejustice"})

with DAG(
    dag_id="state_reconcile_hdfs",
    default_args=DEFAULT_ARGS,
    description="Aligne state DB avec fichiers bronze existants",
    schedule=None,
    start_date=START,
    catchup=False,
    tags=["bce", "state"],
) as dag_reconcile:
    PythonOperator(task_id="reconcile_bronze", python_callable=_run_reconcile)

with DAG(
    dag_id="cbso_bronze_partition",
    default_args=DEFAULT_ARGS,
    description="CBSO bronze par préfixe BCE (scale)",
    schedule=None,
    start_date=START,
    catchup=False,
    params={"prefix": "08", "max_batches": 10},
    tags=["bce", "bronze", "cbso", "scale"],
) as dag_partition:
    PythonOperator(task_id="cbso_partition_pipeline", python_callable=_run_partition)
