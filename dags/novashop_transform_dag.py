"""
DAG A — novashop_transform  (Séance 3 : transformation & nettoyage)
Ingestion multi-sources -> nettoyage avec quarantaine -> SCD Type 2 -> dimensions.
Produit les tables de référence dans l'entrepôt et émet un Dataset qui
déclenchera automatiquement le DAG B (orchestration data-aware, séance 4).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

import novashop_lib as L

CURATED = Dataset("novashop://curated")   # contrat de données entre les deux DAGs

default_args = {
    "owner": "data_instructor",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": lambda ctx: print(f"[ALERTE] échec {ctx['task_instance'].task_id}"),
}

with DAG(
    dag_id="novashop_transform",
    description="Ingestion + nettoyage + SCD2 (séance 3)",
    default_args=default_args,
    start_date=datetime(2024, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["formation", "etl", "b3", "novashop", "seance3"],
) as dag:

    extract = PythonOperator(task_id="extract", python_callable=L.extract_all)

    with TaskGroup("clean") as clean:
        c_cust = PythonOperator(task_id="customers", python_callable=L.clean_customers)
        c_chg = PythonOperator(task_id="customer_changes", python_callable=L.clean_customer_changes)
        c_prod = PythonOperator(task_id="products", python_callable=L.clean_products)
        c_ord = PythonOperator(task_id="orders", python_callable=L.clean_orders)
        c_item = PythonOperator(task_id="order_items", python_callable=L.clean_order_items)
        c_ret = PythonOperator(task_id="returns", python_callable=L.clean_returns)
        # order_items enrichi depuis products ; returns dépend de order_items
        c_prod >> c_item >> c_ret

    scd2 = PythonOperator(task_id="build_dim_customer_scd2", python_callable=L.build_dim_customer_scd2)
    dims = PythonOperator(task_id="build_dimensions", python_callable=L.build_dimensions)

    # Le chargement des tables de référence "publie" le Dataset -> déclenche le DAG B
    load_ref = PythonOperator(
        task_id="load_reference_tables",
        python_callable=L.load_reference_tables,
        outlets=[CURATED],
    )

    extract >> clean >> [scd2, dims] >> load_ref
