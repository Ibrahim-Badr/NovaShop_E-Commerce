#!/usr/bin/env bash
# One-click : démarre la stack Airflow et ouvre l'interface.
set -e
cd "$(dirname "$0")"
echo "Démarrage d'Airflow (première fois : téléchargement des images + install pandas, ~2-4 min)..."
docker compose up -d
echo
echo "Airflow démarre. Interface : http://localhost:8080   (identifiants : airflow / airflow)"
echo "Suivre les logs :   docker compose logs -f airflow-scheduler"
echo "Arrêter :           docker compose down      (tout effacer : docker compose down -v)"
