from datetime import timedelta
import sys
from pathlib import Path
import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param

import os, requests, msal

AUTHORITY = f"https://login.microsoftonline.com/{os.environ['PBI_TENANT_ID']}"
SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

PBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
# --- Rutas del repo (montaje local), NO uses una URL ---

REPO_MOUNT = "/opt/airflow/repo"
if REPO_MOUNT not in sys.path:
    sys.path.append(REPO_MOUNT)

BASE = Path(__file__).resolve().parents[1]
if str(BASE) not in sys.path:
    sys.path.append(str(BASE))

WORK = BASE / "work"

# Importa tus funciones del repo
from repo.scripts.pipeline_api import (
    run_scraping_pipeline,
    spark_merge_and_convert,
    spark_compute_scores,
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

def get_pbi_token():
        app = msal.ConfidentialClientApplication(
            client_id=os.environ["PBI_CLIENT_ID"],
            authority=AUTHORITY,
            client_credential=os.environ["PBI_CLIENT_SECRET"],
        )
        result = app.acquire_token_silent(scopes=SCOPE, account=None)
        if not result:
            result = app.acquire_token_for_client(scopes=SCOPE)
        if "access_token" not in result:
            raise RuntimeError(f"Error obteniendo token: {result}")
        return result["access_token"]

@dag(
    dag_id="tfm_weekly_pipeline",
    description=" merge → score ",
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
    season_t = "{{ dag_run.conf.get('season', params.season) }}"
    ejecutar_r_t = False
    @task(task_id="merge")
    def merge() -> str:
        return spark_merge_and_convert()
    @task(task_id="score")
    def score(clean_path: str) -> str:
        outo = "/opt/airflow/v1/players_clean.parquet"
        out = WORK / "players_clean.parquet"
        return spark_compute_scores(input_parquet=clean_path, output_parquet=outo)
    p_clean = merge()
    p_scored = score(p_clean)
    @task(task_id="trigger_powerbi_refresh")
    def trigger_powerbi_refresh():
        required = ["PBI_TENANT_ID","PBI_CLIENT_ID","PBI_CLIENT_SECRET","PBI_GROUP_ID","PBI_DATASET_ID"]
        missing = [k for k in required if not os.environ.get(k)]
        if missing:
            raise RuntimeError(f"Faltan variables de entorno: {missing}")
        token = get_pbi_token()
        group_id = os.environ["PBI_GROUP_ID"]
        dataset_id = os.environ["PBI_DATASET_ID"]
        url = f"{PBI_API_BASE}/groups/{group_id}/datasets/{dataset_id}/refreshes"
        payload = { "type": "Full", "notifyOption": "MailOnFailure" }  # "Incremental" si procede
        r = requests.post(url, json=payload, headers={"Authorization": f"Bearer {token}"})
        r.raise_for_status()
# === Instanciación y dependencias ===
    m = merge()
    sc = score(m)
    m >> sc >> trigger_powerbi_refresh()
dag = tfm_weekly_pipeline()


