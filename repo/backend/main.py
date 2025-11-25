# repo/backend/main.py
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import pandas as pd
import numpy as np
import math
import pyarrow.dataset as ds  # para leer esquema sin cargar todo

# ============================================================
# CONFIG
# ============================================================
# ⚠️ Ajusta esta ruta si tus scores están en otra carpeta
GOLD_PATH = Path(r"C:/Users/jahoy/Documents/scouting/lake/gold/players_score")

app = FastAPI(title="Scouting API")

# CORS (front en Vite 5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("📥 Inicializando esquema (parquet particionado)...")
_DATASET = ds.dataset(GOLD_PATH)      # metadata del parquet, no carga datos
_SCHEMA_COLS = set(_DATASET.schema.names)
print(f"✅ Columnas detectadas: {_DATASET.schema.names}")

# Sample para /debug/nans
try:
    df_sample = pd.read_parquet(GOLD_PATH, engine="pyarrow", columns=None)
    print(f"🧪 Muestra inicial: {len(df_sample)} filas, {len(df_sample.columns)} columnas")
except Exception as e:
    print("⚠️ No se pudo leer muestra inicial:", e)
    df_sample = pd.DataFrame()

# ============================================================
# UTILS
# ============================================================
def sanitize_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """inf/-inf -> NaN, NaN -> None (JSON-friendly)."""
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.where(pd.notnull(df), None)
    return df

def clean_records_for_json(records: list[dict]) -> list[dict]:
    """Convierte tipos numpy a nativos y limpia NaN/inf."""
    cleaned = []
    for rec in records:
        new_rec = {}
        for k, v in rec.items():
            if isinstance(v, (float, np.floating)):
                try:
                    if math.isnan(v) or math.isinf(v):
                        new_rec[k] = None
                    else:
                        new_rec[k] = float(v)
                except Exception:
                    new_rec[k] = None
            elif isinstance(v, (np.integer,)):
                new_rec[k] = int(v)
            else:
                new_rec[k] = v
        cleaned.append(new_rec)
    return cleaned

def _safe_cols(requested: list[str] | None) -> list[str] | None:
    """Devuelve solo las columnas que existen realmente en el esquema."""
    if requested is None:
        return None
    keep = [c for c in requested if c in _SCHEMA_COLS]
    return keep or None

def load_gold_safe(filters: list | None = None, columns: list[str] | None = None) -> pd.DataFrame:
    """Lectura segura con pyarrow respetando filtros de partición y columnas existentes."""
    try:
        cols = _safe_cols(columns)
        df = pd.read_parquet(
            GOLD_PATH,
            engine="pyarrow",
            filters=filters,
            columns=cols,
        )
        return df
    except Exception as e:
        print("❌ ERROR load_gold_safe:", e)
        raise HTTPException(status_code=500, detail=f"read_parquet failed: {e}")

# Normalización de nombres de liga desde el front
LEAGUE_ALIASES = {
    "la-liga": "La-liga",
    "laliga": "La-liga",
    "premier-league": "Premier-League",
    "bundesliga": "Bundesliga",
    "ligue-1": "Ligue-1",
    "serie-a": "Serie-A",
}
def norm_league_in(lg: str | None) -> str | None:
    if lg is None:
        return None
    return LEAGUE_ALIASES.get(lg.strip().lower(), lg)

def _partcol(name: str) -> str | None:
    """Devuelve 'name' o 'name_part' si existe en el esquema."""
    if name in _SCHEMA_COLS:
        return name
    cand = f"{name}_part"
    if cand in _SCHEMA_COLS:
        return cand
    return None

# ============================================================
# ENDPOINTS
# ============================================================

# ------------------ /players ------------------ #
@app.get("/players")
def get_players(
    limit: int = 300,
    league: str | None = None,
    season: str | None = None,
    team: str | None = None,
):
    """
    Devuelve jugadores (puede incluir columnas de score si existen en el parquet actual).
    """
    columnas = [
        "player_name", "team_name",
        "liga", "season", "position",
        "age", "height_cm", "nationality",
        "games", "time",
        "goals", "assists",
        "market_value"
    ]

    filters = []
    if league is not None:
        league = norm_league_in(league)
        col = _partcol("liga")
        if col: filters.append((col, "=", league))
    if season is not None:
        col = _partcol("season")
        if col: filters.append((col, "=", season))
    if team is not None and "team_name" in _SCHEMA_COLS:
        filters.append(("team_name", "=", team))

    df = load_gold_safe(filters=filters or None, columns=columnas)
    if df.empty:
        return []

    # Alias de particiones
    if "liga" not in df.columns and "liga_part" in df.columns:
        df = df.rename(columns={"liga_part": "liga"})
    if "season" not in df.columns and "season_part" in df.columns:
        df = df.rename(columns={"season_part": "season"})

    prefer_order = [
        "player_name", "team_name", "position", "age", "height_cm", "nationality",
        "liga", "season", "games", "time", "goals", "assists", "market_value",
    ]
    cols_existentes = [c for c in prefer_order if c in df.columns]
    df = df[cols_existentes].head(limit).copy()

    df = sanitize_for_json(df)
    return clean_records_for_json(df.to_dict(orient="records"))
#-----------------------------score------------------------------------------------------
@app.get("/players_scores")
def get_players_score(
    limit: int = 300,
    league: str | None = None,
    season: str | None = None,
    team: str | None = None,
    sort_by: str = "-adjusted_score",
):
    """
    Devuelve jugadores con sus scores: performance_score, penalty_score, adjusted_score
    y auxiliares *_per90. Filtra por liga/season/team y ordena por sort_by.
    """
    columnas = [
        "player_name", "team_name", "position", "age", "nationality",
        "liga", "season",
        "games", "time",
        "market_value",
        "performance_score", "penalty_score", "adjusted_score",
    ]

    filters = []
    if league is not None:
        league_norm = norm_league_in(league)
        col = _partcol("liga")
        if col: filters.append((col, "=", league_norm))
    if season is not None:
        col = _partcol("season")
        if col: filters.append((col, "=", season))
    if team is not None and "team_name" in _SCHEMA_COLS:
        filters.append(("team_name", "=", team))

    df = load_gold_safe(filters=filters or None, columns=columnas)
    if df.empty:
        return []

    # Alias particiones
    if "liga" not in df.columns and "liga_part" in df.columns:
        df = df.rename(columns={"liga_part": "liga"})
    if "season" not in df.columns and "season_part" in df.columns:
        df = df.rename(columns={"season_part": "season"})

    # Ordenación
    if sort_by:
        asc = not sort_by.startswith("-")
        key = sort_by.lstrip("-")
        if key in df.columns:
            df = df.sort_values(key, ascending=asc)

    df = df.head(limit).copy()
    df = sanitize_for_json(df)
    return clean_records_for_json(df.to_dict(orient="records"))


# ------------------ /teams ------------------ #
@app.get("/teams")
def get_teams(league: str, season: str | None = None):
    """Devuelve equipos únicos de una liga (y temporada)."""
    league = norm_league_in(league)
    filters = []
    col_liga = _partcol("liga")
    if col_liga: filters.append((col_liga, "=", league))
    if season:
        col_season = _partcol("season")
        if col_season: filters.append((col_season, "=", season))

    df = load_gold_safe(filters=filters or None, columns=["team_name"])
    if df.empty:
        return []

    teams = (
        df["team_name"]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )
    return teams

# ------------------ /leagues ------------------ #
@app.get("/leagues")
def get_leagues():
    """Devuelve ligas disponibles (columna 'liga' o partición)."""
    # Intenta primero como data column
    cols_try = ["liga", "liga_part"]
    col = next((c for c in cols_try if c in _SCHEMA_COLS), None)
    df = load_gold_safe(columns=[col] if col else None)
    if df.empty or col is None:
        return []
    ligas = df[col].dropna().astype(str).sort_values().unique().tolist()
    # si viene como liga_part, renombra en salida
    if col == "liga_part":
        ligas = [x for x in ligas]
    return ligas

# ------------------ /players/league/{league} ------------------ #
@app.get("/players/league/{league}")
def get_players_by_league(
    league: str,
    limit: int = 300,
    season: str | None = None,
):
    """Wrapper de /players filtrando por liga/season."""
    return get_players(limit=limit, league=league, season=season)

# ------------------ /league/{league}/season_summary ------------------ #
@app.get("/league/{league}/season_summary")
def get_league_season_summary(league: str):
    """Agregados por temporada dentro de una liga."""
    league = norm_league_in(league)
    required = [
        "player_name", "liga", "season",
        "games", "time", "minutesPlayed",
        "goals", "assists",
    ]

    col_liga = _partcol("liga")
    filters = [(col_liga, "=", league)] if col_liga else None
    df = load_gold_safe(filters=filters, columns=required)
    if df.empty:
        return []

    # Alias particiones
    if "liga" not in df.columns and "liga_part" in df.columns:
        df = df.rename(columns={"liga_part": "liga"})
    if "season" not in df.columns and "season_part" in df.columns:
        df = df.rename(columns={"season_part": "season"})

    # Minutos
    if "time" in df.columns and "minutesPlayed" in df.columns:
        df["time"] = df["time"].fillna(df["minutesPlayed"])
        df = df.drop(columns=["minutesPlayed"])
    elif "minutesPlayed" in df.columns and "time" not in df.columns:
        df = df.rename(columns={"minutesPlayed": "time"})

    agg = {}
    if "player_name" in df.columns: agg["num_players"] = ("player_name", "nunique")
    if "time" in df.columns:         agg["total_minutes"] = ("time", "sum")
    if "goals" in df.columns:        agg["total_goals"]   = ("goals", "sum")
    if "assists" in df.columns:      agg["total_assists"] = ("assists", "sum")
    if not agg:
        return []

    summary = (
        df.groupby("season")
          .agg(**agg)
          .reset_index()
          .sort_values("season")
    )
    summary = sanitize_for_json(summary)
    return clean_records_for_json(summary.to_dict(orient="records"))

# ------------------ NUEVO: /players_scores ------------------ #
@app.get("/players_scores/league/{league}")
def get_players_scores(
    league: str,
    limit: int = 300,
    season: str | None = None
):
    return get_players_score(limit=limit, league=league, season=season)
# ------------------ /debug/nans ------------------ #
@app.get("/debug/nans")
def debug_nans(limit: int = 1000):
    """Columnas con NaN/inf en una muestra."""
    if df_sample.empty:
        return {"columns_with_inf": [], "columns_with_nan": []}
    num_df = df_sample.select_dtypes(include=[float, int]).head(limit)
    mask_inf = ~np.isfinite(num_df)
    mask_nan = num_df.isna()
    cols_with_inf = [c for c in num_df.columns if mask_inf[c].any()]
    cols_with_nan = [c for c in num_df.columns if mask_nan[c].any()]
    return {"columns_with_inf": cols_with_inf, "columns_with_nan": cols_with_nan}
