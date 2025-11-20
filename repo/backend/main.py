# repo/backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd

# RUTA A TU GOLD
GOLD_PATH = Path(r"C:/Users/jahoy/Documents/scouting/lake/gold/players_stats_market")

app = FastAPI(title="Scouting API")

# CORS para permitir peticiones desde Vite (5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# cargamos el parquet al arrancar el servidor
# (para probar, luego ya pensamos en algo más fino)
print("📥 Leyendo parquet GOLD...")
df_gold = pd.read_parquet(GOLD_PATH)
print(f"✅ GOLD cargado con {len(df_gold)} filas y {len(df_gold.columns)} columnas")

@app.get("/players")
def get_players(limit: int = 100):
    """
    Devuelve las primeras `limit` filas del GOLD para ver la tabla.
    """
    # nos quedamos con algunas columnas típicas (ajusta a tu esquema real)
    columnas = [
        "player_name",
        "team_name",
        "player_position",
        "minutesPlayed",
        "goals",
        "assists",
        "player_market_value_euro",
        "Season",
        "Liga",
    ]
    cols_existentes = [c for c in columnas if c in df_gold.columns]

    datos = df_gold[cols_existentes].head(limit).to_dict(orient="records")
    return datos
