from datetime import timedelta
import sys
from pathlib import Path
import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param

# --- Rutas del repo (montaje local), NO uses una URL ---

REPO_MOUNT = "/opt/airflow/repo"
if REPO_MOUNT not in sys.path:
    sys.path.append(REPO_MOUNT)

BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.append(str(BASE))

WORK = BASE / "work"

# Importa tus funciones del repo
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

def _to_bool(x):
    if isinstance(x, str):
        return x.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return bool(x)

@dag(
    dag_id="tfm_weekly_pipeline",
    description="Scrape → merge → score → append histórico",
    start_date=pendulum.datetime(2024, 8, 1, 3, 0, tz="Europe/Madrid"),
    schedule="0 3 * * 2",  # Martes 03:00 hora de Madrid
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["tfm", "ball", "spark"],
    params={
        "season": Param("2024-2025", type="string"),
        "ejecutar_r": Param(True, type="boolean"),
    },
)
def tfm_weekly_pipeline():

    @task
    def scrape(season: str, ejecutar_r) -> None:
        run_scraping_pipeline(season=season, ejecutar_r= False)

    @task
    def merge() -> str:
        return spark_merge_and_convert()

    @task
    def score(clean_path: str) -> str:
        outo = "/opt/airflow/v1/players_clean.parquet"
        out = WORK / "players_clean.parquet"
        return spark_compute_scores(
            input_parquet=str(outo),
            output_parquet=str(out),
        )

    @task
    def append(scored_path: str) -> str:
        return append_to_history(scored_path)

    # Encadenado con XCom implícito y params templated
    scrape_out = scrape(
        season="{{ params.season }}",
        ejecutar_r="{{ params.ejecutar_r }}",
    )

    merge_task = merge()
    scrape_out >> merge_task      # ← dependencia correcta

    scored = score(merge_task)    # pasa el XCom (ruta) a la siguiente
    append(scored)


dag = tfm_weekly_pipeline()
