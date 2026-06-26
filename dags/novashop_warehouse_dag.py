"""
DAG B — novashop_warehouse (Séance 4 : chargement, automatisation, supervision)
Déclenché par le Dataset publié par le DAG A (orchestration data-aware).

Pour la date logique du run ({{ ds }}) :
  check_new_data (branche) -> load_fact (incrémental, idempotent) | no_new_data
  load_fact -> refresh_aggregate_mart -> quality_gate -> report_metrics
  no_new_data -> report_metrics

Incrémental + idempotent : chaque run traite la partition d'un jour et purge
la partition avant réinsertion. Le backfill se fait via `airflow dags backfill`.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

import novashop_lib as L

CURATED = Dataset("novashop://curated")

default_args = {
    "owner": "data_instructor",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(hours=1),
    "on_failure_callback": lambda ctx: print(
        f"[ALERTE] échec {ctx['task_instance'].task_id} ({ctx['ds']})"
    ),
}

def _check_new_data(ds=None, **_):
    assert ds is not None, "ds ne peut pas être None"
    return "load_fact" if L.has_new_data(ds) else "no_new_data"

def _load_fact(ds=None, **_):
    assert ds is not None, "ds ne peut pas être None"
    L.load_fact_incremental(ds)

with DAG(
    dag_id="novashop_warehouse",
    description="Chargement incrémental + supervision (séance 4)",
    default_args=default_args,
    start_date=datetime(2024, 7, 1),
    schedule=[CURATED],
    catchup=False,
    tags=["formation", "etl", "b3", "novashop", "seance4"],
) as dag:

    check = BranchPythonOperator(task_id="check_new_data", python_callable=_check_new_data)
    load_fact = PythonOperator(task_id="load_fact", python_callable=_load_fact)
    no_new = EmptyOperator(task_id="no_new_data")

    mart = PythonOperator(
        task_id="refresh_aggregate_mart",
        python_callable=L.refresh_aggregate_mart,
    )

    quality = PythonOperator(
        task_id="quality_gate",
        python_callable=L.quality_gate,
    )

    report = PythonOperator(
        task_id="report_metrics",
        python_callable=L.report_metrics,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check >> [load_fact, no_new]
    load_fact >> mart >> quality >> report
    no_new >> report