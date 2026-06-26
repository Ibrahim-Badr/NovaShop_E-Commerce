import sys
import os
from datetime import datetime, timedelta

# Ajouter le dossier des DAGs au PYTHONPATH pour pouvoir importer novashop_lib
sys.path.append('/opt/airflow/dags')
from novashop_lib import load_fact_incremental

print("Début du chargement manuel de fct_sales...")

start_date = datetime(2024, 7, 1)
end_date = datetime(2025, 6, 30)

current_date = start_date
loaded = 0
while current_date <= end_date:
    ds = current_date.strftime("%Y-%m-%d")
    load_fact_incremental(ds)
    current_date += timedelta(days=1)
    loaded += 1

print(f"\nChargement terminé pour {loaded} jours !")
