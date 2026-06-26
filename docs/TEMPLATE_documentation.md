# Documentation technique du pipeline — *(à compléter par le groupe)*

> Template imposé (cf. séance 4). Une section = un point évalué (qualité & traçabilité, C2.3.2).

## 1. Présentation
Objectif métier du pipeline, périmètre, parties prenantes.

## 2. Architecture
Schéma du flux : sources → extraction → transformation → modèle → entrepôt → supervision.
Préciser l'orchestrateur (Airflow), la fréquence, l'entrepôt cible.

## 3. Sources de données
| Source | Système | Format | Clé primaire | Volumétrie |
|---|---|---|---|---|
| customers_raw | CRM | CSV | customer_id | ~200 |
| customer_changes_raw | CRM | CSV | (customer_id, change_date) | ~65 |
| products_raw | Catalogue/PIM | CSV | product_id | ~43 |
| orders_raw | App e-commerce | CSV | order_id | ~2 500 |
| order_items_raw | App e-commerce | CSV | order_item_id | ~6 200 |
| returns_raw | SAV / retours | CSV | return_id | ~330 |

## 4. Extraction
Tâche `extract` du DAG : lecture des sources, dépôt en staging. Mode d'incrémentalité (le cas échéant).

## 5. Transformations *(séance 3)*
Pour chaque source, **documenter la règle appliquée** :
- Doublons : …
- Valeurs manquantes : …
- Typage / formats (dates ISO, numériques) : …
- Normalisation (casse, référentiel) : …
- Enrichissement (jointures) : …
- **Quarantaine** : politique de rejet (table `rejects`, motifs documentés) : …
- **SCD Type 2** : règle d'historisation des segments (valid_from / valid_to / is_current) : …

## 6. Modèle cible *(séance 4)*
Schéma en étoile : table de faits `fct_sales` (grain = ligne de commande) +
dimensions `dim_customer`, `dim_product`, `dim_date`. Joindre un diagramme.

## 7. Chargement
Chargement **incrémental** des faits par jour (`ds`), **idempotent** (delete-insert par partition) ; dimensions/rejets en full refresh. **Orchestration** : Dataset A→B.

## 8. Automatisation
Planification (`schedule`), `retries`, `retry_delay`, SLA. Comment relancer / rattraper un run.

## 9. Supervision
Contrôles qualité (`quality_gate`), métriques suivies (volumétrie, durée), alerting
(`on_failure_callback`). Que se passe-t-il en cas d'échec ?

## 10. Qualité & traçabilité
Tests de qualité, anomalies connues et leur traitement, journal des décisions.

## 11. Exploitation
Comment consommer l'entrepôt (les 3 KPI), prérequis, limitations.

## 12. Annexes
Captures du graphe Airflow, logs significatifs, requêtes KPI, dictionnaire de données.
