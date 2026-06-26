@echo off
REM One-click Windows : demarre la stack Airflow
cd /d "%~dp0"
echo Demarrage d'Airflow (premiere fois : ~2-4 min)...
docker compose up -d
echo.
echo Interface : http://localhost:8080   (identifiants : airflow / airflow)
echo Logs : docker compose logs -f airflow-scheduler
echo Arret : docker compose down
pause
