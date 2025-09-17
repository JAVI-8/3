# TFM/scripts/pipeline_api.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

# Que Python encuentre tus módulos
'''if REPO not in sys.path:
    sys.path.append(REPO)
    '''
# 1) Tu controlador (scrapea+limpia y guarda CSVs)
from scripts import controlador_scrpapping as ctrl  # ctrl.run(season, out_dir, ejecutar_r)

# 2) Spark: unión y conversiones → Parquet
from spark_pipeline.procesamiento_spark import procesar   # procesar(input_parquet, output_parquet) o similar
# 3) Spark: scores → Parquet
from spark_pipeline.scoring_utils import calcular         # calcular(input_parquet, output_parquet)

# ===== 2) Spark: unir CSVs ya limpios y dejar Parquet 'clean' =====

WORK_DIR = Path("WORK_DIR", "/opt/airflow/work/parquets/latest")

WORK_DIR.mkdir(parents=True, exist_ok=True)

def spark_merge_and_convert() -> str:
    """
    Llama a tu script Spark que UNE TODOS los CSV (ya limpios) y guarda un Parquet "clean".
    No le pasamos rutas de entrada porque tu script ya sabe leer de data/.
    """
    return procesar("", WORK_DIR)
# ===== 3) Spark: calcular scores sobre el Parquet 'clean' =====
def spark_compute_scores(input_parquet,output_parquet) -> str:
    """
    Llama a tu script Spark que calcula performance_score y penalty_score y guarda Parquet final.
    """
    calcular(input=input_parquet, output=output_parquet)
    return output_parquet


