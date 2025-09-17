
from __future__ import annotations
from pathlib import Path
import pandas as pd

#spark: unión y conversiones 
from spark_pipeline.procesamiento_spark import procesar
#spark: scores 
from spark_pipeline.scoring_utils import calcular     

WORK_DIR = Path("WORK_DIR", "/opt/airflow/work/parquets/latest") #ruta donde se guardan los parquets
WORK_DIR.mkdir(parents=True, exist_ok=True)

#unir csvs
def spark_merge_and_convert() -> str:
    """
    Llama a tu script Spark que UNE TODOS los CSV (ya limpios) y guarda un Parquet "clean".
    No le pasamos rutas de entrada porque tu script ya sabe leer de data/.
    """
    return procesar("", WORK_DIR)
#calcular scores de los jugadores
def spark_compute_scores(input_parquet,output_parquet) -> str:
    """
    Llama a tu script Spark que calcula performance_score y penalty_score y guarda Parquet final.
    """
    calcular(input=input_parquet, output=output_parquet)
    return output_parquet


