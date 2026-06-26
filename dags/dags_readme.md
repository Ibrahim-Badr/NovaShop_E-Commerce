# Projet NovaShop — DAGs Apache Airflow

Projet de groupe du module B3 IA & Data / Outils ETL.  
L’objectif est de transformer 6 fichiers CSV bruts en un entrepôt de données exploitable avec Apache Airflow.

---

## 1. Nature des données

NovaShop est un site e-commerce.  
Les données représentent :

- des clients CRM,
- des changements de segment client,
- des produits du catalogue,
- des commandes,
- des lignes de commande,
- des retours clients.

Ces données sont volontairement imparfaites.  
Elles contiennent des doublons, des valeurs manquantes, des clés orphelines, des dates au format français et des erreurs de typage.

---

## 2. Structure du pipeline

Le projet est composé de 2 DAGs :

- **DAG A — `novashop_transform`**
  - extraction des CSV,
  - nettoyage,
  - normalisation,
  - historisation client en SCD Type 2,
  - chargement des tables de référence.

- **DAG B — `novashop_warehouse`**
  - création des dimensions,
  - chargement incrémental de la table de faits,
  - calcul d’un agrégat mensuel,
  - contrôles qualité,
  - supervision.

Le DAG A publie un Dataset qui déclenche automatiquement le DAG B [web:24][web:212].

---

## 3. Étapes de nettoyage

Le nettoyage sert à corriger les anomalies présentes dans les sources.

### a) Nettoyage des clients
- suppression des doublons sur `customer_id`,
- normalisation des noms, pays et segments,
- gestion des emails manquants,
- conversion des dates au format ISO,
- rejet des lignes invalides dans `rejects`.

### b) Nettoyage des changements clients
- conversion de la date de changement,
- normalisation du nouveau segment,
- rejet des dates invalides.

### c) Nettoyage des produits
- conversion des prix en numérique,
- rejet des produits sans prix,
- normalisation de la catégorie.

### d) Nettoyage des commandes
- suppression des commandes sans identifiant,
- conversion de la date de commande,
- normalisation du statut et du canal.

### e) Nettoyage des lignes de commande
- suppression des doublons,
- conversion des quantités et remises,
- rejet des quantités invalides,
- suppression des produits orphelins,
- récupération du prix si absent.

### f) Nettoyage des retours
- suppression des doublons,
- conversion des quantités retournées,
- rejet des valeurs non numériques,
- suppression des retours orphelins,
- conversion de la date de retour.

---

## 4. Normalisation des données

La normalisation vise à harmoniser les valeurs pour faciliter les jointures et les analyses.

Elle comprend :
- l’uniformisation des majuscules/minuscules,
- la conversion des dates au format standard `YYYY-MM-DD`,
- la conversion des types en numérique,
- la suppression des espaces inutiles,
- le traitement des valeurs manquantes.

Cette étape permet d’avoir des données propres et cohérentes entre les différentes tables.

---

## 5. Transformation des données

Après nettoyage, les données sont transformées pour être chargées dans l’entrepôt.

### a) Dimension client SCD Type 2
La dimension client conserve l’historique des segments.  
Chaque changement de segment crée une nouvelle version du client avec :

- `customer_sk` : clé technique,
- `valid_from` : début de validité,
- `valid_to` : fin de validité,
- `is_current` : indicateur de version courante.

Cette approche permet d’analyser les ventes selon le segment exact du client au moment de la commande.

### b) Dimensions simples
Le pipeline crée aussi :

- `dim_product`,
- `dim_date`.

### c) Table de faits
La table `fct_sales` est construite au grain **ligne de commande**.  
Elle ne charge que les commandes avec statut `completed`.

Elle contient :
- l’identifiant de commande,
- le client,
- le segment au moment de l’achat,
- le produit,
- la date,
- le canal,
- les quantités,
- les retours,
- les montants financiers,
- la marge.

### d) Agrégat mensuel
La table `sales_monthly` résume les ventes par mois avec :
- le chiffre d’affaires net de retours,
- la marge,
- les quantités vendues,
- les quantités retournées.

---

## 6. Quarantaine des anomalies

Les lignes invalides ne sont pas supprimées silencieusement.  
Elles sont enregistrées dans la table `rejects` avec :

- la source,
- la clé métier,
- la raison du rejet,
- la date du rejet.

Cela permet de garder une traçabilité complète des problèmes qualité.

---

## 7. Orchestration Airflow

Le fonctionnement du pipeline est le suivant :

1. `novashop_transform` traite les données brutes.
2. Il publie le Dataset `novashop://curated`.
3. Ce Dataset déclenche `novashop_warehouse`.
4. `novashop_warehouse` vérifie s’il existe de nouvelles données.
5. Si oui, il charge la table de faits.
6. Il reconstruit l’agrégat mensuel.
7. Il exécute les contrôles qualité.
8. Il affiche les métriques de supervision.

Le chargement est :
- incrémental,
- idempotent,
- automatisé par Airflow.

---

## 8. Tables finales

L’entrepôt final se trouve dans `output/warehouse.db`.

Tables principales :
- `dim_customer_scd`
- `dim_product`
- `dim_date`
- `fct_sales`
- `sales_monthly`
- `rejects`
- `audit_runs`

---

## 9. KPI calculés

Les 3 KPI du projet sont calculés à partir de cet entrepôt :

### KPI 1 — CA net de retours mensuel
Mesure le chiffre d’affaires net de retours par mois, avec la variation par rapport au mois précédent.

### KPI 2 — Taux de retour par catégorie
Mesure le pourcentage de retours par catégorie produit.

### KPI 3 — Panier moyen par segment
Mesure le panier moyen par segment client au moment de la commande.

---

## 10. Résultats validés

Volumétrie observée :
- `dim_customer_scd` : 265 lignes
- `dim_product` : 42 lignes
- `dim_date` : 365 lignes
- `fct_sales` : 4 078 lignes
- `sales_monthly` : 12 lignes
- `rejects` : 183 lignes
- `audit_runs` : 365 lignes

Ces résultats confirment que le pipeline fonctionne correctement de bout en bout.

---

## 11. Lancement du projet

### Démarrage
```bash
docker compose up -d
```

### Accès Airflow
- http://localhost:8080
- identifiants : `airflow / airflow`

### Vérification des DAGs
- `novashop_transform`
- `novashop_warehouse`

---

## 12. Conclusion

Le projet NovaShop transforme des données e-commerce brutes en un entrepôt de données exploitable.  
Le pipeline nettoie, normalise, historise et charge les données, puis calcule des KPI métier utiles pour l’analyse.