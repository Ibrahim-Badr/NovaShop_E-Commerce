> **Démarrage one-click :** `docker compose up -d` puis http://localhost:8080 (airflow / airflow). Détails dans DEMARRAGE.md.

# Projet NovaShop — Énoncé étudiant (Séances 3 & 4)

Module B3 IA & Data · Outils ETL · **Projet de groupe** · 100 % Apache Airflow.

NovaShop est un e-commerce alimenté par **6 sources** brutes. Votre mission : construire
un pipeline ETL complet qui produit un **entrepôt en étoile** exploitable, puis calculer
3 KPI. Le squelette du code et les deux DAGs vous sont fournis ; vous implémentez la
logique de transformation, de modélisation et de chargement.

## 1. Ce qui vous est fourni
```
novashop_airflow/
├── dags/
│   ├── novashop_lib.py            # STARTER : fonctions à compléter (# TODO)
│   ├── novashop_transform_dag.py  # DAG A (séance 3) — fourni
│   └── novashop_warehouse_dag.py  # DAG B (séance 4) — fourni
├── data/                          # 6 sources brutes (CSV)
├── sql/kpi_a_completer.sql        # énoncé des 3 KPI
├── docs/TEMPLATE_documentation.md # documentation à rendre
└── output/                        # l'entrepôt warehouse.db sera créé ici
```

## 2. Les 6 sources
| Source | Système | Clé |
|---|---|---|
| customers_raw | CRM | customer_id |
| customer_changes_raw | CRM (changements de segment) | customer_id + change_date |
| products_raw | Catalogue | product_id |
| orders_raw | App e-commerce | order_id |
| order_items_raw | App e-commerce (lignes) | order_item_id |
| returns_raw | SAV / retours | return_id |

> Les sources contiennent **des anomalies réalistes** (doublons, valeurs manquantes,
> mauvais types, clés orphelines, dates au format FR, quantités invalides…).
> À vous de les détecter et de les traiter — **sans supprimer en silence** : routez
> les lignes invalides vers la table de quarantaine `rejects` (helper `_reject` fourni).

## 3. Travail à réaliser

### Séance 3 — Transformation & nettoyage (DAG A)
Compléter dans `novashop_lib.py` :
- `clean_customers`, `clean_customer_changes`, `clean_products`, `clean_orders`,
  `clean_order_items`, `clean_returns` (nettoyage + quarantaine + enrichissement).
- `build_dim_customer_scd2` : **dimension client en SCD Type 2** (historiser les
  segments : `valid_from` / `valid_to` / `is_current`, clé technique `customer_sk`).
- **Documenter** chaque transformation (template `docs/`).

### Séance 4 — Chargement, automatisation, supervision (DAG B)
Compléter dans `novashop_lib.py` :
- `build_dimensions` (dim_product, dim_date).
- `has_new_data` + `build_fact_for_day` : **table de faits** (grain = ligne, commandes
  `completed`), jointure **SCD2** pour `segment_at_order`, intégration des **retours**
  (`montant_net_de_retours`).
- `refresh_aggregate_mart` (`sales_monthly`) et `quality_gate` (contrôles bloquants).
- Comprendre et expliquer l'**orchestration** (le DAG A publie un Dataset qui déclenche
  le DAG B), le **chargement incrémental idempotent** par jour, les **retries/SLA/alerting**.

## 4. Mise en route
1. Copier `dags/*.py` dans le dossier `dags/` d'Airflow (gardez `novashop_lib.py` à côté).
2. Placer `data/` et `output/` sous `/opt/airflow`, ou définir `ETL_BASE_DIR`.
3. Implémenter les fonctions, activer `novashop_transform` puis observer le déclenchement
   automatique de `novashop_warehouse`. Backfill :
   `airflow dags backfill novashop_warehouse -s 2024-07-01 -e 2025-06-30`.
> Tant qu'une fonction n'est pas implémentée, elle lève `NotImplementedError` (normal).

## 5. Les 3 KPI à calculer (`sql/kpi_a_completer.sql`)
1. **CA net de retours mensuel** (+ variation % vs mois précédent).
2. **Taux de retour par catégorie** de produit.
3. **Panier moyen par segment au moment de la commande** (utiliser la version SCD2 valide
   à la date de commande, pas le segment courant).

## 6. Livrables (par groupe)
- Code complété (`novashop_lib.py`) + pipeline qui s'exécute.
- Entrepôt `warehouse.db` généré + requêtes des 3 KPI + résultats + analyse (3–4 phrases).
- **Documentation** complète selon `docs/TEMPLATE_documentation.md`.
- **Soutenance** courte (rôles répartis dans le groupe).
