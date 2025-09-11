# TFM/scripts/pipeline_api.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

WORK = Path("/opt/airflow/work")
WORK.mkdir(parents=True, exist_ok=True)

HIST = WORK / "historico_players.parquet"
input = "ruta"
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

# ===== 1) Ingesta semanal: solo dispara tu controlador =====
def run_scraping_pipeline(season: str = "2025-2026", ejecutar_r: bool = False) -> None:
    """
    El controlador: scrapea -> limpia -> guarda CSVs por liga/tipo (y mercado en R si activar).
    """
    
    return ctrl.run(season=season, ejecutar_r=ejecutar_r)

# ===== 2) Spark: unir CSVs ya limpios y dejar Parquet 'clean' =====
def spark_merge_and_convert() -> str:
    """
    Llama a tu script Spark que UNE TODOS los CSV (ya limpios) y guarda un Parquet "clean".
    No le pasamos rutas de entrada porque tu script ya sabe leer de data/.
    """
    return procesar()

# ===== 3) Spark: calcular scores sobre el Parquet 'clean' =====
def spark_compute_scores(input_parquet,output_parquet) -> str:
    """
    Llama a tu script Spark que calcula performance_score y penalty_score y guarda Parquet final.
    """
    calcular(input=input_parquet, output=output_parquet)
    return output_parquet

# ===== 4) (Opcional) Append + dedupe al histórico =====
def append_to_history(new_parquet: str) -> str:
    """
    Añade el Parquet final al histórico con deduplicación por clave compuesta.
    """
    df_new = pd.read_parquet(new_parquet)

    if HIST.exists():
        df_old = pd.read_parquet(HIST)
        cols = sorted(set(df_old.columns).union(df_new.columns))
        df_old = df_old.reindex(columns=cols)
        df_new = df_new.reindex(columns=cols)
        df = pd.concat([df_old, df_new], ignore_index=True)

        # Clave compuesta (ajústala si tienes IDs mejores)
        keys_pref = [
            ["Player", "Season", "Squad", "Liga"]
        ]
        keys = next((ks for ks in keys_pref if all(k in df.columns for k in ks)), None)
        df = df.drop_duplicates(subset=keys, keep="last") if keys else df.drop_duplicates(keep="last")
    else:
        df = df_new

    HIST.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(HIST, index=False)
    return str(HIST)

# ===== 5) (Conveniencia) Todo de una: scrape -> merge -> scores -> append =====
def weekly_update(season: str = "2025-2026", ejecutar_r: bool = True) -> str:
    """
    Flujo completo: controlador (CSV) -> Spark merge (Parquet clean) -> Spark scores (Parquet final) -> Append histórico.
    Devuelve la ruta del histórico actualizado.
    """
    run_scraping_pipeline(season=season, ejecutar_r=ejecutar_r)
    clean_path = spark_merge_and_convert()
    scored_path = spark_compute_scores(input_parquet=clean_path)
    return append_to_history(scored_path)
