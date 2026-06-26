# Projet NovaShop — ETL Apache Airflow

Projet de groupe du module B3 IA & Data / Outils ETL.  
Le but est de construire un pipeline ETL complet avec Apache Airflow à partir de 6 sources CSV, puis de produire un entrepôt en étoile exploitable et 3 KPI métier.

## Démarrage rapide

1. Lancer les services :
```bash
docker compose up -d
```

2. Ouvrir Airflow :
- http://localhost:8080
- identifiants par défaut : `airflow` / `airflow`

3. Vérifier l’état des DAGs :
- `novashop_transform`
- `novashop_warehouse`

## Structure du projet

```text
novashop_airflow/
├── dags/
│   ├── novashop_lib.py
│   ├── novashop_transform_dag.py
│   └── novashop_warehouse_dag.py
├── data/
├── sql/
│   └── kpi_a_completer.sql
├── docs/
│   └── TEMPLATE_documentation.md
└── output/
    └── warehouse.db
```

## Sources de données

Le projet repose sur 6 fichiers bruts :

- `customers_raw`
- `customer_changes_raw`
- `products_raw`
- `orders_raw`
- `order_items_raw`
- `returns_raw`

Les données contiennent volontairement des anomalies réalistes :
- doublons,
- valeurs manquantes,
- clés orphelines,
- types incohérents,
- dates au format français,
- quantités invalides.

Les lignes invalides sont envoyées vers la table de quarantaine `rejects`.

## Pipeline Airflow

### DAG A — `novashop_transform`
Ce DAG gère :
- l’extraction des fichiers bruts,
- le nettoyage et la normalisation,
- la création de la dimension client en SCD Type 2,
- le chargement des tables de référence.

### DAG B — `novashop_warehouse`
Ce DAG est déclenché par le Dataset publié par le DAG A.  
Il gère :
- la construction des dimensions,
- le chargement incrémental et idempotent de la table de faits,
- le calcul du mart agrégé mensuel,
- les contrôles qualité,
- la supervision.

## Modélisation de l’entrepôt

Tables finales dans `output/warehouse.db` :

- `dim_customer_scd`
- `dim_product`
- `dim_date`
- `fct_sales`
- `sales_monthly`
- `rejects`
- `audit_runs`

### Volumétrie validée

- `dim_customer_scd` : 265 lignes
- `dim_product` : 42 lignes
- `dim_date` : 365 lignes
- `fct_sales` : 4 078 lignes
- `sales_monthly` : 12 lignes
- `rejects` : 183 lignes
- `audit_runs` : 365 lignes

## KPI

Les 3 KPI demandés sont calculés sur l’entrepôt `warehouse.db` à partir du fichier `sql/kpi_a_completer.sql`.

### KPI 1 — CA net de retours mensuel
Ce KPI mesure le chiffre d’affaires net de retours par mois à partir de `sales_monthly`.  
Il inclut aussi la variation mensuelle et le taux de marge.

**Constats :**
- Le chiffre d’affaires reste globalement stable sur l’année.
- Un pic apparaît en août 2024.
- Le taux de marge reste cohérent, autour de 26 % à 32 %.

### KPI 2 — Taux de retour par catégorie
Ce KPI mesure la part de retours par catégorie produit à partir de `fct_sales` et `dim_product`.

**Constats :**
- La catégorie Jardin présente le taux de retour le plus élevé parmi les vraies catégories.
- Informatique affiche le taux de retour le plus faible.
- Les écarts restent modérés.

### KPI 3 — Panier moyen par segment
Ce KPI mesure le panier moyen par segment client, en utilisant le segment historisé au moment de la commande via le SCD Type 2.

**Constats :**
- Le segment Particulier génère le plus grand nombre de commandes.
- Le segment Particulier a aussi le panier moyen le plus élevé.
- L’usage du SCD2 garantit une analyse correcte dans le temps.

## Exécution des KPI

Le fichier `sql/kpi_a_completer.sql` contient les 3 requêtes SQL.  
Les résultats peuvent être exécutés via Docker ou dans un outil SQLite.

### Exemple d’exécution dans Docker
```bash
docker compose cp .\sql\kpi_a_completer.sql airflow-scheduler:/tmp/kpi.sql
docker compose exec -T airflow-scheduler python /tmp/kpi_runner.py
```

## Orchestration

- Le DAG A publie un Dataset.
- Le DAG B est déclenché automatiquement par ce Dataset.
- Le chargement est incrémental, idempotent et journalisé dans `audit_runs`.

## Documentation

La documentation complète du projet est disponible dans `docs/TEMPLATE_documentation.md`.  
Elle décrit :
- les sources,
- les traitements,
- la modélisation,
- l’orchestration,
- les KPI,
- les résultats de validation.

## Démarrage de secours

Si nécessaire :

```bash
docker compose down -v
docker compose up -d
```

## Remarques

- Le projet utilise Apache Airflow en local avec Docker Compose.
- Les tâches de transformation et de chargement ont été validées sur les données fournies.
- La table `rejects` permet de tracer les anomalies détectées pendant le traitement.