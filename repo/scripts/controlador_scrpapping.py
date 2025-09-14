
import subprocess, sys, os
import pandas as pd
import shlex
import time
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
    
from scripts.scrape_fbref import scrape_fbref
from scripts.limpiar_datos import limpiar


# Definir ligas y temporadas
ids_types = [("stats_standard", "stats"), ("stats_possession", "possession"), ("stats_defense", "defense"), ("stats_misc", "misc"), ("stats_passing", "passing"), ("stats_shooting", "shooting"), ("stats_keeper", "keepers"), ("stats_keeper_adv", "keepersadv")]
temporadas = ["2024-2025"]

R_FILE   = r"C:\Universidad\Master BDDE UCM\TFM\TFM\scripts\controlador_scrapping.R"
RSCRIPT  = os.environ.get("RSCRIPT", r"C:\Program Files\R\R-4.5.1\bin\x64\Rscript.exe")

def run_r(temporada: str):
    cmd = [RSCRIPT, "--vanilla", R_FILE, f"--temporada={temporada}"]
    print("Ejecutando:", " ".join(shlex.quote(c) for c in cmd))
    subprocess.run(cmd, check=True)
    
def limpiar_mercado():
    print("leyendo datos en bruto de transfermrkt")
    df_mercado = pd.read_csv("data/v2/valores_mercado_2024-2025.csv")
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
    run(season="2024-2025", ejecutar_r=False)

