# limpiar_datos.py
import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
#culumnas que son utilea para cada df
COLUMNAS_UTILES = {
    
}
FOLDERIN = "lake/bronze"
FOLDEROUT = "lake/silver"

def eliminarguiones(df):
    for col in df.columns:
        df[col]=df[col].replace("-", pd.NA)
    return df
def eliminar_col_mercado(df):
    cols = ["club", "player", "market_value_eur", "season", "competition_code"]  # columnas deseadas
    df = df[cols]
    return df

def cambiar_tipos_mercado(df):
    str_cols = [
        "region",
        "country",
        "squad",
        "player_name",
        "player_position",
        "player_nationality",
        "current_club",
        "player_foot",
        "season",
        "liga",
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # ---- columnas float ----
    float_cols = [
        "player_height_mtrs",
        "player_market_value_euro",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    # ---- columnas int (permitiendo NaN) ----
    int_cols = [
        "player_num",
        "player_age",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df



def limpiar_valores_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reemplaza NaN, inf y -inf por None (o por NaN si se prefiere).
    - No elimina filas.
    - Deja el df preparado para JSON (FastAPI), CSV o Parquet.
    """
    # Reemplazar inf y -inf por NaN
    df = df.replace([np.inf, -np.inf], np.nan)

    # Opcional: si usas Pandas + FastAPI → convertir NaN a None (JSON-safe)
    df = df.where(pd.notnull(df), None)

    return df



def save_parquet_string_safe(df, path):
    fields = []
    for col in df.columns:
        if df[col].dtype == object:
            fields.append(pa.field(col, pa.string()))
        elif df[col].dtype in ["float64", "float32"]:
            fields.append(pa.field(col, pa.float64()))
        elif df[col].dtype in ["int64", "int32"]:
            fields.append(pa.field(col, pa.int64()))
        else:
            fields.append(pa.field(col, pa.string()))
    
    schema = pa.schema(fields)
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, path)

def aniadir_nombre_y_temporada(file, df):
    fname = file.lower()

    if "bundesliga" in fname:
        liga = "Bundesliga"
    elif "epl" in fname:
        liga = "Premier-League"
    elif "ligue_1" in fname:
        liga = "Ligue-1"
    elif "serie_a" in fname:
        liga = "Serie-A"
    elif "la_liga":
        liga = "La-liga"

    
    df["liga"] = liga

    # --- season desde nombre de archivo ---
    season = None
    if ("20_21" in fname) or ("2021" in fname):
        season = "2020-2021"
    elif ("21_22" in fname) or ("2022" in fname):
        season = "2021-2022"
    elif ("22_23" in fname) or ("2023" in fname):
        season = "2022-2023"
    elif ("23_24" in fname) or ("2024" in fname):
        season = "2023-2024"
    elif ("24_25" in fname) or ("2025" in fname):
        season = "2024-2025"
    elif ("25_26" in fname) or ("2026" in fname):
        season = "2025-2026"

    df["season"] = season
    
    return df

def aniadir_nombre_y_temporada_mercado(file, df):
    fname = file.lower()
    if "l1" in fname:
        liga = "Bundesliga"
    elif "gb1" in fname:
        liga = "Premier-League"
    elif "fr1" in fname:
        liga = "Ligue-1"
    elif "it1" in fname:
        liga = "Serie-A"
    elif "es1":
        liga = "La-liga"

    
    df["liga"] = liga

    # --- season desde nombre de archivo ---
    season = None
    if ("2021" in fname) or ("2021" in fname):
        season = "2020-2021"
    elif ("2022" in fname) or ("2022" in fname):
        season = "2021-2022"
    elif ("2023" in fname) or ("2023" in fname):
        season = "2022-2023"
    elif ("2024" in fname) or ("2024" in fname):
        season = "2023-2024"
    elif ("2025" in fname) or ("2025" in fname):
        season = "2024-2025"
    elif ("2026" in fname) or ("2026" in fname):
        season = "2025-2026"
    
    df["season"] = season
    '''
    df["season"] = df["season"].astype(str).replace({
    "2021": "2020-2021",
    "2022": "2021-2022",
    "2023": "2022-2023",
    "2024": "2023-2024",
    "2025": "2024-2025",
    "2026": "2025-2026",

    })

    df["competition_code"] = df["competition_code"].astype(str)

    df.loc[df["competition_code"].str.contains("ES", na=False), "competition_code"] = "La-liga"
    df.loc[df["competition_code"].str.contains("GB", na=False), "competition_code"] = "Premier-League"
    df.loc[df["competition_code"].str.contains("FR", na=False), "competition_code"] = "Ligue-1"
    df.loc[df["competition_code"].str.contains("IT", na=False), "competition_code"] = "Serie-A"
    df.loc[df["competition_code"].str.contains("L1",  na=False), "competition_code"] = "Bundesliga"'''
    return df

def eliminar_col_info(df):
    cols = ["player_name", "team_name", "position", "age", "height_cm", "nationality", "market_value", "contract_expires", ]  # columnas deseadas
    df = df[cols]
    return df

def limpiar_info():
    rutaIn = os.path.join(FOLDERIN, "info")
    for file in os.listdir(rutaIn):
        if file.endswith(".csv"):
            pathIn = os.path.join(rutaIn, file)
            print(f"Procesando: {file}")
            # Cargar CSV
            df = pd.read_csv(pathIn)
            #elimnar columas
            df = eliminar_col_info(df)
            df = eliminarguiones(df)
            # Eliminar duplicados
            df = df.drop_duplicates()
            # Eliminar filas totalmente nulas o con un mínimo de columnas nulas
            df = aniadir_nombre_y_temporada_mercado(file, df)

            df = limpiar_valores_invalidos(df)
            #cambiar los tipos de cada columna
            df = cambiar_tipos_mercado(df)
            df["source"]="info_transfermarkt"
            df = df.rename(columns={

                "season": "season",
                "competition_code": "liga"
            })
            df["player_name"] = df["player_name"].astype(str)
            df["team_name"]   = df["team_name"].astype(str)
            df["liga"]        = df["liga"].astype(str)
            df["season"]      = df["season"].astype(str)
            df["source"]      = df["source"].astype(str)
            nuevo_file = file.replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "info", nuevo_file)
            #guardar en parquet en formato limpio limpio
            df.to_parquet(pathOut, index=False)
            print(f"Archivo: {file} limpio y guardado en: {pathOut}")


def limpiar_sofascore():
    rutaIn = os.path.join(FOLDERIN, "sofascore")
    for file in os.listdir(rutaIn):
        if file.endswith(".csv"):
            pathIn = os.path.join(rutaIn, file)
            print(f"Procesando: {file}")
            # Cargar CSV
            df = pd.read_csv(pathIn)
            df = df.drop(columns=["player id", "team id", "hitWoodwork", "minutesPlayed", "appearances", "expectedGoals"])
            # Eliminar duplicados
            df = df.drop_duplicates()
            # Eliminar filas totalmente nulas o con un mínimo de columnas nulas
            #df = df.dropna(how="any")
            df = aniadir_nombre_y_temporada(file, df)
            df["source"]="Sofascore"
            df = limpiar_valores_invalidos(df)
                        #cambiar los tipos de cada columna
            df = df.rename(columns={
                "player": "player_name",
                "team": "team_name",
            })
            df["player_name"] = df["player_name"].astype(str)
            df["team_name"]   = df["team_name"].astype(str)
            df["liga"]        = df["liga"].astype(str)
            df["season"]      = df["season"].astype(str)
            df["source"]      = df["source"].astype(str)
    
            #cambiar nombres con espacios quitar espacios al inicio/fin y formatto consistente
            nuevo_file = file.replace("_raw", "").replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "sofascore", nuevo_file)
            #guardar en parquet en formato limpio limpio
            save_parquet_string_safe(df, pathOut)

def eliminar_columnas_understat(df):
    cols = ["player_name", "games", "time", "xG", "xA", "team_title", "xGChain","xGBuildup"]  # columnas deseadas
    df = df[cols]
    return df

def limpiar_understat():
    rutaIn = os.path.join(FOLDERIN, "understat")
    for file in os.listdir(rutaIn):
        if file.endswith(".csv"):
            pathIn = os.path.join(rutaIn, file)
            print(f"Procesando: {file}")
            # Cargar CSV
            df = pd.read_csv(pathIn)
            df = eliminar_columnas_understat(df)
            # Eliminar duplicados
            df = df.drop_duplicates()
            # Eliminar filas totalmente nulas o con un mínimo de columnas nulas
            #df = df.dropna(how="any")
            df = aniadir_nombre_y_temporada(file, df)
            df["source"]="understat"
            df = limpiar_valores_invalidos(df)
                        #cambiar los tipos de cada columna
            df = df.rename(columns={
                "team_title": "team_name",
            })
            df["player_name"] = df["player_name"].astype(str)
            df["team_name"]   = df["team_name"].astype(str)
            df["liga"]        = df["liga"].astype(str)
            df["season"]      = df["season"].astype(str)
            df["source"]      = df["source"].astype(str)
    
            #cambiar nombres con espacios quitar espacios al inicio/fin y formatto consistente
            nuevo_file = file.replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "understat", nuevo_file)
            #guardar en parquet en formato limpio limpio
            save_parquet_string_safe(df, pathOut)

if __name__ == "__main__":
    limpiar_info()
    limpiar_sofascore()
    limpiar_understat()

'''def limpiar_mercado():
    rutaIn = os.path.join(FOLDERIN, "mercado")
    for file in os.listdir(rutaIn):
        if file.endswith(".csv"):
            pathIn = os.path.join(rutaIn, file)
            print(f"Procesando: {file}")
            # Cargar CSV
            df = pd.read_csv(pathIn)
            #elimnar columas
            df = eliminar_col_mercado(df)
            df = eliminarguiones(df)
            # Eliminar duplicados
            df = df.drop_duplicates()
            # Eliminar filas totalmente nulas o con un mínimo de columnas nulas
            df = aniadir_nombre_y_temporada_mercado(df)

            df = limpiar_valores_invalidos(df)
            #cambiar los tipos de cada columna
            df = cambiar_tipos_mercado(df)
            df["source"]="transfermarkt"
            df = df.rename(columns={

                "club": "team_name",
                "player": "player_name",
                "season": "season",
                "competition_code": "liga"
            })
            df["player_name"] = df["player_name"].astype(str)
            df["team_name"]   = df["team_name"].astype(str)
            df["liga"]        = df["liga"].astype(str)
            df["season"]      = df["season"].astype(str)
            df["source"]      = df["source"].astype(str)
            nuevo_file = file.replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "mercado", nuevo_file)
            #guardar en parquet en formato limpio limpio
            df.to_parquet(pathOut, index=False)
            print(f"Archivo: {file} limpio y guardado en: {pathOut}")
def eliminar_columnas(df):
    cols = ["minutesPlayed", "appearances", "team", "player"]  # columnas deseadas
    df = df[cols]
    return df

def limpiar_sofascore_minutes():
    rutaIn = os.path.join(FOLDERIN, "minutes")
    for file in os.listdir(rutaIn):
        if file.endswith(".csv"):
            pathIn = os.path.join(rutaIn, file)
            print(f"Procesando: {file}")
            # Cargar CSV
            df = pd.read_csv(pathIn)
            df = eliminar_columnas(df)
            # Eliminar duplicados
            df = df.drop_duplicates()
            # Eliminar filas totalmente nulas o con un mínimo de columnas nulas
            #df = df.dropna(how="any")
            df = aniadir_nombre_y_temporada(file, df)
            df["source"]="Sofascore_minutes"
            df = limpiar_valores_invalidos(df)
                        #cambiar los tipos de cada columna
            df = df.rename(columns={
                "player": "player_name",
                "team": "team_name",
            })
            df["player_name"] = df["player_name"].astype(str)
            df["team_name"]   = df["team_name"].astype(str)
            df["liga"]        = df["liga"].astype(str)
            df["season"]      = df["season"].astype(str)
            df["source"]      = df["source"].astype(str)
    
            #cambiar nombres con espacios quitar espacios al inicio/fin y formatto consistente
            nuevo_file = file.replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "minutes", nuevo_file)
            #guardar en parquet en formato limpio limpio
            save_parquet_string_safe(df, pathOut)

'''
