
import subprocess, sys, os
import pandas as pd
import shlex
import time
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
    
from scripts.scrape_fbref import scrape_fbref
from scripts.limpiar_datos import limpiar

import subprocess
import os
from pathlib import Path
# Ruta al script R

# Definir ligas y temporadas
ids_types = [("stats_standard", "stats"), ("stats_possession", "possession"), ("stats_defense", "defense"), ("stats_misc", "misc"), ("stats_passing", "passing"), ("stats_shooting", "shooting"), ("stats_keeper", "keepers"), ("stats_keeper_adv", "keepersadv")]
temporadas = ["2024-2025"]



def run_r(temporada: str = "2024-2025"):
    print("ejecutando scrape transfermarkt")
    RSCRIPT = r"C:\Program Files\R\R-4.5.1\bin\x64\Rscript.exe"

    # Script R en la misma carpeta que este archivo
    SCRIPT = Path(__file__).resolve().parent / "get_market_values.R"
    WORKDIR = SCRIPT.parent

    if not SCRIPT.exists():
        raise FileNotFoundError(f"No se encontró el script R: {SCRIPT}")

    env = os.environ.copy()
    env["R_LIBS_USER"] = r"C:\Users\jahoy\Rlibs"

    cmd = [RSCRIPT, "--vanilla", str(SCRIPT), f"--temporada={temporada}"]

    result = subprocess.run(
        cmd,
        cwd=str(WORKDIR),          # Carpeta donde está el .R
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env
    )

    print("=== STDOUT ===")
    print(result.stdout)
    print("=== STDERR ===")
    print(result.stderr)
    print("Exit code:", result.returncode)

    if result.returncode != 0:
        raise RuntimeError(
            f"Rscript falló con código {result.returncode}\n\n"
            f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
        )

    return result.stdout
def limpiar_mercado():
    print("leyendo datos en bruto de transfermrkt")
    df_mercado = pd.read_csv("data/v3/valores_mercado3_2024-2025.csv")
    limpiar(df_mercado, "mercado")
    
def scrap_fbref_and_clean(temporada):
    
    for id, type in ids_types:
        print(f"\nProcesando archivo: {type}")
        try:
            df = scrape_fbref(temporada, id, type)
            print("Limpiando...")
            out_dir = limpiar(df, type)
           # return out_dir
        except Exception as e:
            print(f"FBref error en {temporada} [{type}]: {e}")
    return out_dir
        
def run(season, ejecutar_r):
    #fbref + limpiar
    scrap_fbref_and_clean(season)
    
    if ejecutar_r:
        #mercado en R + limpiar mercado
        print(f"\nProcesando transfermarkt")
        run_r(season)
        limpiar_mercado()

    
if __name__ == "__main__":
    run_r()

