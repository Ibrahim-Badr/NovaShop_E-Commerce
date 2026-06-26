# Démarrage one-click (Docker)

## Prérequis
Docker Desktop (Mac/Windows) ou Docker Engine + plugin compose (Linux).

## Lancer
Depuis ce dossier :
```bash
docker compose up -d
```
(ou double-cliquez `start.sh` sous Mac/Linux, `start.bat` sous Windows)

Première exécution : ~2 à 4 min (téléchargement des images + installation de pandas).

Puis ouvrez **http://localhost:8080** — identifiants **airflow / airflow**.

> Linux uniquement : si l'écriture dans `output/` pose souci, lancez une fois
> `echo "AIRFLOW_UID=$(id -u)" > .env` avant `docker compose up -d`.

## Utiliser
Les DAGs `novashop_transform` et `novashop_warehouse` apparaissent dans l'UI (en pause).
1. Activez et déclenchez `novashop_transform` → il publie un Dataset qui déclenche
   automatiquement `novashop_warehouse`.
2. Pour charger tout l'historique (chargement incrémental par jour) :
   ```bash
   docker compose exec airflow-scheduler \
     airflow dags backfill novashop_warehouse -s 2024-07-01 -e 2025-06-30
   ```
3. L'entrepôt apparaît sur votre disque dans `output/warehouse.db`.

> Version étudiant : tant que les fonctions `# TODO` de `dags/novashop_lib.py`
> ne sont pas complétées, les tâches échouent volontairement (NotImplementedError).
> C'est le point de départ du travail.

## Commandes utiles
```bash
docker compose logs -f airflow-scheduler   # suivre l'exécution
docker compose down                         # arrêter
docker compose down -v                      # arrêter + effacer la base de métadonnées
```
