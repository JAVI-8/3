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
    df = df.drop(columns=["comp_name", "season_start_year", "player_dob", "date_joined", "joined_from", "contract_expiry", "player_url"])
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
        "Season",
        "Liga",
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

def aniadir_nombre_y_temporada(file, df):
    if "Bundesliga" in file:
        df["Liga"] = "Bundesliga"
    if "EPL" in file:
        df["Liga"] = "Premier-League"
    if "Ligue_1" in file:
        df["Liga"] = "Ligue-1"
    if "Serie_A" in file:
        df["Liga"] = "Serie-A"
    if "La_Liga" in file:
        df["Liga"] = "La-Liga"

    if "20_21" in file:
        df["Season"] = "2020-2021"
    if "21_22" in file:
        df["Season"] = "2021-2022"
    if "22_23" in file:
        df["Season"] = "2022-2023"
    if "23_24" in file:
        df["Season"] = "2023-2024"
    if "24_25" in file:
        df["Season"] = "2024-2025"
    if "25_26" in file:
        df["Season"] = "2025-2026"
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

def limpiar_mercado():
    rutaIn = os.path.join(FOLDERIN, "mercado")
    for file in os.listdir(rutaIn):
        if file.endswith(".csv"):
            pathIn = os.path.join(rutaIn, file)
            print(f"Procesando: {file}")
            # Cargar CSV
            df = pd.read_csv(pathIn)
            #elimnar columnas
            df = eliminar_col_mercado(df)
            df = eliminarguiones(df)
            # Eliminar duplicados
            df = df.drop_duplicates()
            # Eliminar filas totalmente nulas o con un mínimo de columnas nulas
            df = limpiar_valores_invalidos(df)
            #cambiar los tipos de cada columna
            df = cambiar_tipos_mercado(df)
            df["source"]="transfermarkt"
            df = df.rename(columns={

                "squad": "team_name",
            })
            nuevo_file = file.replace("_raw", "").replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "mercado", nuevo_file)
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
            df = df.drop(columns=["player id", "team id", "hitWoodwork"])
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
            df["Liga"]        = df["Liga"].astype(str)
            df["Season"]      = df["Season"].astype(str)
            df["source"]      = df["source"].astype(str)
    
            #cambiar nombres con espacios quitar espacios al inicio/fin y formatto consistente
            nuevo_file = file.replace("_raw", "").replace(".csv", ".parquet")
            pathOut = os.path.join(FOLDEROUT, "sofascore", nuevo_file)
            #guardar en parquet en formato limpio limpio
            save_parquet_string_safe(df, pathOut)


if __name__ == "__main__":
    limpiar_mercado()
    limpiar_sofascore()
