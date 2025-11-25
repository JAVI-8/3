import os
import glob
import time
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(x, **kwargs): 
        return x

import ScraperFC as sfc


# ========== CONFIG ==========
INPUT_DIR = r"C:/Users/jahoy/Documents/scouting/lake/bronze/mercado"     # carpeta con CSV de entrada
OUTPUT_DIR = r"C:/Users/jahoy/Documents/scouting/lake/bronze/info"       # carpeta donde guardar los CSV con info
URL_COLUMN_NAME = None  # si sabes el nombre exacto (p.ej. "transfermarkt_url"), ponlo aquí; si no, deja None
SLEEP_BETWEEN = 1.0     # segundos entre requests
RETRIES = 3             # reintentos por URL
# ===========================


# Mapeo de nombres (lo que devuelve tu entorno -> nombres estándar)
RENAME_MAP = {
    "Name": "player_name",
    "ID": "player_id",
    "DOB": "date_of_birth",
    "Age": "age",
    "Height (m)": "height_m",
    "Nationality": "nationality",
    "Position": "position",
    "Other positions": "other_positions",
    "Team": "team_name",
    "Joined": "joined",
    "Contract expiration": "contract_expires",
    "Value": "market_value",
}

URL_COL_CANDIDATES = ["url", "link", "transfermarkt", "transfermarkt_url", "player_url"]

def detect_url_column(df: pd.DataFrame) -> str:
    """Detecta la columna con URLs de Transfermarkt si no se especifica."""
    if URL_COLUMN_NAME and URL_COLUMN_NAME in df.columns:
        return URL_COLUMN_NAME

    # Por nombre de columna (case-insensitive)
    lower_map = {c.lower(): c for c in df.columns}
    for cand in URL_COL_CANDIDATES:
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]

    # Por contenido
    for c in df.columns:
        if df[c].astype(str).str.contains("transfermarkt.com", na=False).any():
            return c

    raise ValueError("No se encontró columna con URLs de Transfermarkt en este CSV.")

def scrape_one(tm: sfc.Transfermarkt, url: str) -> pd.DataFrame | None:
    """Scrapea un jugador (1 fila) con reintentos."""
    last_err = None
    for _ in range(RETRIES):
        try:
            df = tm.scrape_player(url)
            if "Player URL" not in df.columns and "player_url" not in df.columns:
                df["Player URL"] = url  # trazabilidad
            return df
        except Exception as e:
            last_err = e
            time.sleep(2.5)
    print(f"[WARN] Falló: {url} -> {last_err}")
    return None

def normalize_and_order(df: pd.DataFrame) -> pd.DataFrame:
    """Renombra columnas conocidas, crea height_cm y ordena columnas."""
    rename_map = {k: v for k, v in RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    if "height_m" in df.columns:
        df["height_m"] = pd.to_numeric(df["height_m"], errors="coerce")
        df["height_cm"] = (df["height_m"] * 100).round(0)

    preferred_order = [
        "player_name", "player_id", "team_name", "position", "other_positions",
        "date_of_birth", "age", "height_m", 
        "nationality",
        "market_value", "joined", "contract_expires",
    ]
    cols = [c for c in preferred_order if c in df.columns] + \
           [c for c in df.columns if c not in preferred_order]
    return df[cols]

def process_one_csv(tm: sfc.Transfermarkt, in_path: str, out_dir: str):
    """Lee un CSV con URLs, scrapea cada jugador y guarda un CSV de salida."""
    # Lee entrada con fallback de encoding
    try:
        in_df = pd.read_csv(in_path)
    except UnicodeDecodeError:
        in_df = pd.read_csv(in_path, encoding="latin-1")

    url_col = detect_url_column(in_df)
    urls = (
        in_df[url_col]
        .dropna()
        .astype(str)
        .str.strip()
    )
    urls = urls[urls.str.contains("transfermarkt.com", na=False)].drop_duplicates()

    print(f"[{os.path.basename(in_path)}] URLs detectadas: {len(urls)} (columna: {url_col})")

    rows = []
    for url in tqdm(urls, desc=f"Scrapeando {os.path.basename(in_path)}"):
        res = scrape_one(tm, url)
        if res is not None and not res.empty:
            rows.append(res)
        time.sleep(SLEEP_BETWEEN)

    if not rows:
        print(f"[{os.path.basename(in_path)}] No se obtuvo info de jugadores.")
        return

    out_df = pd.concat(rows, ignore_index=True)
    out_df = normalize_and_order(out_df)

    os.makedirs(out_dir, exist_ok=True)
    out_name = os.path.splitext(os.path.basename(in_path))[0] + "_tm_info.csv"
    out_path = os.path.join(out_dir, out_name)
    out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[OK] Guardado: {out_path} ({len(out_df)} jugadores)")

def main():
    tm = sfc.Transfermarkt()
    csv_files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"No hay CSV en {INPUT_DIR}")

    for csv_path in csv_files:
        process_one_csv(tm, csv_path, OUTPUT_DIR)

if __name__ == "__main__":
    main()
