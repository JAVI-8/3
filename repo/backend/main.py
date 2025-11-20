# repo/backend/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd
import numpy as np
import math  # 👈 NUEVO

# Carpeta del dataset particionado (tipo hive):
# players_stats_market/
#   Liga=La-Liga/Season=2022-2023/part-...parquet
#   Liga=Premier-League/Season=2023-2024/part-...parquet
GOLD_PATH = Path(r"C:/Users/jahoy/Documents/scouting/lake/gold/players_stats_market")

app = FastAPI(title="Scouting API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("📥 Leyendo esquema GOLD (parquet particionado)...")
df_sample = pd.read_parquet(GOLD_PATH, engine="pyarrow", columns=None)
print(f"✅ GOLD ejemplo con {len(df_sample)} filas y {len(df_sample.columns)} columnas")


def sanitize_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    - inf / -inf -> NaN
    - NaN -> None (JSON-friendly)
    """
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df


def clean_records_for_json(records: list[dict]) -> list[dict]:
    """
    Recorre todos los registros y:
    - NaN / inf / -inf -> None
    - numpy types -> tipos nativos de Python
    """
    cleaned = []
    for rec in records:
        new_rec = {}
        for k, v in rec.items():
            # Floats (Python y numpy)
            if isinstance(v, (float, np.floating)):
                if math.isnan(v) or math.isinf(v):
                    new_rec[k] = None
                else:
                    new_rec[k] = float(v)
            # Enteros numpy -> int nativo
            elif isinstance(v, (np.integer,)):
                new_rec[k] = int(v)
            else:
                new_rec[k] = v
        cleaned.append(new_rec)
    return cleaned


def load_gold(
    filters: list | None = None,
    columns: list | None = None,
) -> pd.DataFrame:
    """
    Carga el parquet particionado aplicando filtros a nivel de partición.
    - filters: lista de tuplas (col, op, value), ej.
        [("Liga", "=", "La-Liga"), ("Season", "=", "2023-2024")]
    - columns: lista de columnas a leer (para no traer todas)
    """
    df = pd.read_parquet(
        GOLD_PATH,
        engine="pyarrow",
        filters=filters,
        columns=columns,
    )
    return df


# ------------------ ENDPOINT BASE: lista de jugadores ------------------ #
@app.get("/players")
def get_players(
    limit: int = 300,
    league: str | None = None,
    season: str | None = None,
):
    """
    Devuelve jugadores, opcionalmente filtrados por liga y temporada.
    Aprovecha el particionado del parquet.
    """
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

    # Filtros para el parquet particionado
    filters = []
    if league is not None:
        filters.append(("Liga", "=", league))
    if season is not None:
        filters.append(("Season", "=", season))

    if not filters:
        filters = None  # sin filtros => todas las particiones

    df = load_gold(filters=filters, columns=columnas)

    cols_existentes = [c for c in columnas if c in df.columns]
    df = df[cols_existentes].head(limit).copy()

    df = sanitize_for_json(df)
    records = df.to_dict(orient="records")
    records = clean_records_for_json(records)  # 👈 limpieza final

    return records


# ------------------ ENDPOINT: lista de ligas ------------------ #
@app.get("/leagues")
def get_leagues():
    """
    Devuelve la lista de ligas disponibles.
    Se lee solo la columna 'Liga' del parquet particionado.
    """
    if "Liga" not in df_sample.columns:
        return []

    df = load_gold(columns=["Liga"])
    ligas = (
        df["Liga"]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )
    return ligas


# ------------------ ENDPOINT: jugadores por liga ------------------ #
@app.get("/players/league/{league}")
def get_players_by_league(
    league: str,
    limit: int = 300,
    season: str | None = None,
):
    """
    Devuelve jugadores filtrados por liga (y opcionalmente por temporada).
    Internamente es un wrapper de /players usando filters en el parquet particionado.
    """
    columnas = [
        "player_name",
        "team_name",
        "player_position",
        "minutesPlayed",
        "goals",
        "assists",
        "Season",
        "Liga",
    ]

    filters = [("Liga", "=", league)]
    if season is not None:
        filters.append(("Season", "=", season))

    df = load_gold(filters=filters, columns=columnas)

    if df.empty:
        return []

    cols_existentes = [c for c in columnas if c in df.columns]
    df = df[cols_existentes].head(limit).copy()

    df = sanitize_for_json(df)
    records = df.to_dict(orient="records")
    records = clean_records_for_json(records)  # 👈 limpieza final

    return records


# ------------------ ENDPOINT: tabla por temporada ------------------ #
@app.get("/league/{league}/season_summary")
def get_league_season_summary(league: str):
    """
    Devuelve una tabla (una fila por temporada) con métricas agregadas de esa liga.
    Se lee solo la partición de esa liga y se agrupa por Season.
    """
    required_cols = [
        "Liga",
        "Season",
        "player_name",
        "minutesPlayed",
        "goals",
        "assists",
    ]

    df = load_gold(
        filters=[("Liga", "=", league)],
        columns=required_cols,
    )

    if df.empty:
        return []

    agg_dict = {}

    if "player_name" in df.columns:
        agg_dict["num_players"] = ("player_name", "nunique")
    if "minutesPlayed" in df.columns:
        agg_dict["total_minutes"] = ("minutesPlayed", "sum")
    if "goals" in df.columns:
        agg_dict["total_goals"] = ("goals", "sum")
    if "assists" in df.columns:
        agg_dict["total_assists"] = ("assists", "sum")

    summary = (
        df.groupby("Season")
        .agg(**agg_dict)
        .reset_index()
        .sort_values("Season")
    )

    summary = sanitize_for_json(summary)
    records = summary.to_dict(orient="records")
    records = clean_records_for_json(records)  # 👈 limpieza final

    return records


# ------------------ ENDPOINT DEBUG (opcional) ------------------ #
@app.get("/debug/nans")
def debug_nans(limit: int = 1000):
    """
    Diagnóstico rápido sobre NaN/inf en algunas filas.
    Usa df_sample o vuelve a leer una muestra si quieres.
    """
    num_df = df_sample.select_dtypes(include=[float, int]).head(limit)
    mask_inf = ~np.isfinite(num_df)
    mask_nan = num_df.isna()

    cols_with_inf = [c for c in num_df.columns if mask_inf[c].any()]
    cols_with_nan = [c for c in num_df.columns if mask_nan[c].any()]

    return {
        "columns_with_inf": cols_with_inf,
        "columns_with_nan": cols_with_nan,
    }
