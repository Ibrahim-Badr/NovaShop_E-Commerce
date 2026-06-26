# Documentation du projet NovaShop
Module B3 IA & Data — Outils ETL  
Projet de groupe — Apache Airflow

## 1. Objectif du projet

L’objectif du projet est de construire un pipeline ETL complet avec Apache Airflow à partir de 6 sources CSV brutes, puis de modéliser un entrepôt de données en étoile exploitable pour l’analyse métier.

Le pipeline produit :
- des données nettoyées et normalisées,
- une dimension client historisée en SCD Type 2,
- une table de faits au grain ligne de commande,
- un mart agrégé mensuel,
- une table de quarantaine pour les anomalies,
- une table d’audit des chargements.

## 2. Architecture générale

Le projet est composé de deux DAGs :

### DAG A — `novashop_transform`
Ce DAG gère la transformation des sources brutes :
- extraction des 6 fichiers CSV,
- nettoyage et contrôle qualité,
- envoi des lignes invalides vers `rejects`,
- création de la dimension client SCD2,
- chargement des tables de référence.

### DAG B — `novashop_warehouse`
Ce DAG est déclenché par le Dataset publié par le DAG A.
Il gère :
- la création des dimensions de référence,
- le chargement incrémental de la table de faits,
- la construction du mart mensuel,
- les contrôles qualité,
- la supervision.

## 3. Sources de données

Le projet exploite 6 sources :

| Source | Description | Clé métier |
|---|---|---|
| `customers_raw` | Clients CRM | `customer_id` |
| `customer_changes_raw` | Historique des changements de segment | `customer_id + change_date` |
| `products_raw` | Catalogue produits | `product_id` |
| `orders_raw` | Commandes e-commerce | `order_id` |
| `order_items_raw` | Lignes de commande | `order_item_id` |
| `returns_raw` | Retours clients | `return_id` |

Les données contiennent volontairement des anomalies :
- doublons,
- valeurs manquantes,
- clés orphelines,
- dates au format français,
- types incohérents,
- quantités invalides.

Les lignes rejetées sont tracées dans `rejects`.

## 4. Nettoyage et transformation

### `clean_customers`
- suppression des doublons sur `customer_id`,
- normalisation de la casse,
- traitement de l’email manquant,
- conversion de `signup_date` au format ISO,
- rejet des lignes invalides.

### `clean_customer_changes`
- conversion de `change_date`,
- normalisation du nouveau segment,
- rejet des dates invalides.

### `clean_products`
- conversion des prix en numérique,
- rejet des produits sans prix,
- nettoyage de la catégorie.

### `clean_orders`
- suppression des commandes sans identifiant,
- conversion de `order_date`,
- normalisation de `status` et `channel`.

### `clean_order_items`
- suppression des doublons,
- conversion des quantités et remises,
- rejet des quantités invalides,
- suppression des produits orphelins,
- enrichissement du prix si absent.

### `clean_returns`
- suppression des doublons,
- conversion de `qty_returned`,
- rejet des quantités non numériques,
- suppression des retours orphelins,
- conversion de `return_date`.

## 5. Dimension client SCD2

La fonction `build_dim_customer_scd2` construit la dimension historisée des clients avec :
- une clé technique `customer_sk`,
- `valid_from`,
- `valid_to`,
- `is_current`.

Cette logique permet d’analyser les commandes selon le segment valide au moment de l’achat, et non le segment actuel du client.

## 6. Dimensions de référence

La fonction `build_dimensions` construit :
- `dim_product`,
- `dim_date`.

### `dim_product`
Copie la table produit nettoyée dans l’entrepôt.

### `dim_date`
Construit une dimension calendrier à partir des dates de commande, avec :
- année,
- mois,
- `year_month`,
- trimestre.

## 7. Table de faits

La fonction `load_fact_incremental(ds)` alimente `fct_sales`.

### Grain
Le grain est la **ligne de commande**.

### Filtre métier
Seules les commandes :
- du jour `ds`,
- avec statut `completed`,
sont chargées.

### Enrichissements
La table de faits intègre :
- le client historisé via SCD2,
- le segment au moment de la commande (`segment_at_order`),
- les retours agrégés,
- les métriques financières :
  - `montant_brut`,
  - `montant_net`,
  - `montant_retourne`,
  - `montant_net_de_retours`,
  - `cout_total`,
  - `marge`.

### Idempotence
Le chargement supprime d’abord la partition du jour dans `fct_sales`, puis réinsère les lignes calculées.

## 8. Mart mensuel

La fonction `refresh_aggregate_mart` construit `sales_monthly`.

Ce mart agrège la table de faits par mois :
- chiffre d’affaires net de retours,
- marge,
- quantité vendue,
- quantité retournée.

Ce mart sert directement au KPI mensuel.

## 9. Contrôles qualité

La fonction `quality_gate` vérifie :
- l’existence de `fct_sales`,
- l’absence de `customer_sk` nul,
- l’absence de produits orphelins,
- l’absence de montants nets de retours négatifs.

En cas d’erreur, la tâche échoue pour bloquer la chaîne de traitement.

## 10. Quarantaine

La table `rejects` conserve les anomalies détectées pendant le nettoyage :
- source,
- clé métier,
- motif du rejet,
- horodatage.

Elle permet de tracer les problèmes de qualité sans supprimer les lignes en silence.

## 11. Orchestration Airflow

Le flux fonctionne ainsi :

1. `novashop_transform` termine le nettoyage.
2. Le Dataset `novashop://curated` est publié.
3. `novashop_warehouse` est déclenché automatiquement.
4. Le DAG B teste la présence de nouvelles données.
5. S’il existe des commandes complétées pour la date logique, il charge la table de faits.
6. Il rafraîchit ensuite le mart.
7. Il exécute les contrôles qualité.
8. Il termine par la supervision.

Le chargement est donc :
- **incrémental**,
- **idempotent**,
- **orchestré par Dataset**.

## 12. Supervision

La fonction `report_metrics` affiche les volumes des tables principales :
- dimensions,
- faits,
- mart,
- rejetés.

Elle permet de vérifier rapidement l’état de l’entrepôt.

## 13. Résultats validés

Volumétrie finale constatée :

| Table | Lignes |
|---|---:|
| `dim_customer_scd` | 265 |
| `dim_product` | 42 |
| `dim_date` | 365 |
| `fct_sales` | 4 078 |
| `sales_monthly` | 12 |
| `rejects` | 183 |
| `audit_runs` | 365 |

Ces chiffres montrent que le pipeline fonctionne correctement de bout en bout.

## 14. KPI calculés

### KPI 1 — CA net de retours mensuel
Mesure le chiffre d’affaires net de retours par mois à partir de `sales_monthly`.  
Le résultat montre une activité relativement stable avec une saisonnalité modérée.

### KPI 2 — Taux de retour par catégorie
Mesure la part de retours par catégorie produit à partir de `fct_sales` et `dim_product`.  
La catégorie Jardin est la plus exposée parmi les principales catégories.

### KPI 3 — Panier moyen par segment
Mesure le panier moyen par segment client à partir du segment historisé au moment de la commande.  
Le segment Particulier génère le plus grand volume et le panier moyen le plus élevé.

## 15. Exécution des KPI

Le fichier `sql/kpi_a_completer.sql` contient les 3 requêtes SQL demandées.

Exécution possible dans Docker :
```bash
docker compose cp .\sql\kpi_a_completer.sql airflow-scheduler:/tmp/kpi.sql
```

Puis exécution dans le conteneur via Python ou DB Browser for SQLite.

## 16. Limites et remarques

- Le backfill natif d’Airflow n’est pas adapté tel quel à un DAG strictement dataset-triggered.
- Pour historiser toutes les dates, un script manuel de chargement des faits a été utilisé.
- Le modèle reste cependant conforme à l’architecture demandée pour le projet.

## 17. Conclusion

Le projet NovaShop met en œuvre un pipeline ETL complet sous Airflow, depuis les sources brutes jusqu’à l’entrepôt analytique final.  
Les transformations, la modélisation SCD2, le chargement incrémental, la supervision et les KPI sont tous opérationnels.