from datetime import datetime, timedelta
import sys, os
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

# Asegura que Python encuentre tu repo montado:
REPO = "C:\airflow\repo\"
if REPO not in sys.path:
    sys.path.append(REPO)

# Importa tus funciones
from scripts.pipeline_api import (
    run_scraping_pipeline,
    spark_merge_and_convert,
    spark_compute_scores,
    append_to_history,
)

DEFAULT_ARGS = {
    "owner": "tfm",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

from pathlib import Path
BASE = Path(__file__).resolve().parents[1]
REPO = str(BASE)
WORK = BASE / "work" 
with DAG(
    dag_id="tfm_weekly_pipeline",
    description="Scrape → merge → score → append histórico",
    start_date=pendulum.datetime(2025, 8, 1, 3, 0, tz="Europe/Madrid"),
    schedule_interval="0 3 * * 2",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    timezone=pendulum.timezone("Europe/Madrid"),  # asegura horario local
    tags=["tfm", "bicis", "spark"],
) as dag:

    def _scrape(**context):
        season = context["dag_run"].conf.get("season", "2025-2026")
        ejecutar_r = bool(context["dag_run"].conf.get("ejecutar_r", True))
        run_scraping_pipeline(season=season, ejecutar_r=ejecutar_r)

    def _merge(**_):
        # Devuelve la ruta del clean parquet
        return spark_merge_and_convert()

    def _score(ti, **_):
        clean_path = ti.xcom_pull(task_ids="merge")
        return spark_compute_scores(input_parquet=clean_path, output_parquet= str(WORK / "players_clean.parquet"))

    def _append(ti, **_):
        scored_path = ti.xcom_pull(task_ids="score")
        return append_to_history(scored_path)

    scrape = PythonOperator(task_id="scrape", python_callable=_scrape, provide_context=True)
    merge  = PythonOperator(task_id="merge",  python_callable=_merge,  provide_context=True)
    score  = PythonOperator(task_id="score",  python_callable=_score,  provide_context=True)
    append = PythonOperator(task_id="append", python_callable=_append, provide_context=True)

    scrape >> merge >> score >> append
