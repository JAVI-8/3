import os
import pandas as pd
from pathlib import Path
import unicodedata
import re
from rapidfuzz import process
# Carpeta de origen y destino
FOLDER_ORIGEN = Path("data")
FOLDER_DESTINO = Path("data/limpios")
FOLDER_DESTINO.mkdir(parents=True, exist_ok=True)

SUBCARPETAS = ["stats", "defense", "misc", "passing", "possession", "shooting", "mercado", "keepers", "keepersadv"]

# Columnas clave a conservar por carpeta
COLUMNAS_UTILES = {
    "stats": [
        "Player", "Squad", "Born", "MP", "Starts", "Min", "Gls", "Ast", "CrdY", "CrdR", "PrgC","PrgP","PrgR"
    ],
    "defense": [
       "Player", "Squad", "Def 3rd", "Mid 3rd","Att 3rd", "Tkl%", "Tkl.1", "TklW", "Int", "Blocks", "Sh", "Pass", "Clr", "Err"
    ],
    "misc": [
       "Player", "Squad", "Fls", "Off","Crs", "Recov", "Won%"
    ],
    "passing": [
       "Player", "Squad", "Cmp", "Cmp%", "Att.1", "Cmp%.1", "Att.2", "Cmp%.2", "Att.3", "Cmp%.3", "xAG", "xA", "A-xAG", "Kp", "1/3", "PPA", "CrsPA"
    ],
    "possession": [
       "Player", "Squad", "Def 3rd", "Mid 3rd","Att 3rd", "Att Pen", "Live", "Att", "Succ%", "Tkld", "Tkld%", "PrgDist", "1/3", "CPA", "Mis", "Dis"
    ],
    "shooting": [
       "Player", "Squad", "Sh", "SoT","G/Sh", "xG", "npxG", "npxG/Sh", "G-xG"
    ],
    "mercado": [
        "player_name", "player_position", "squad", "player_height_mtrs", "player_market_value_euro"
    ],
    "keepers": [
        "Player", "Squad", "GA", "SoTA", "Save%", "CS%", "PKatt", "Save%.1"
    ],
    "keepersadv": [
        "Player", "Squad", "PSxG", "PSxG/SoT", "PSxG+/-", "Launch%", "Opp", "Stp%", "#OPA" 
    ]
}

# Estandarizar nombres y columnas comunes
def estandarizar_columnas(df, carpeta):
    if carpeta == "stats":
        df = df.rename(columns={"CrdY": "YellowC", "CrdR": "RedC"})
    elif carpeta == "defense":
        df = df.rename(columns={"Def 3rd": "Tack_Def_3rd", "Mid 3rd": "Tack_Mid_3rd", "Att 3rd": "Tack_Att_3rd", "Tkl.1": "Tkl", "Sh": "Block_Shots", "Pass": "Bolck_Pass", "Clr": "Clearences"})
    elif carpeta == "misc":
        df = df.rename(columns={"Crs": "Crosses", "Won%": "Aerialwon%"})
    elif carpeta == "passing":
        df = df.rename(columns={"Cmp": "Pass_cmp", "Cmp%": "Pass_cmp%", "Cmp%.1": "Pass_cmp_Short%", "Att.1": "Pass_Short", "Cmp%.2": "Pass_cmp_Medium%", "Att.2": "Pass_Medium", "Cmp%.3": "Pass_cmp_Long%", "Att.3": "pass_Long", "Kp": "Key_pases", "1/3": "Pass_cmp_Att_3rd"})
    elif carpeta == "possession":
        df = df.rename(columns={"Def 3rd": "Touch_Def_3rd", "Mid 3rd": "Touch_Mid_3rd", "Att 3rd": "Touch_Att_3rd", "Att Pen": "Touch_Att_Pen", "Live": "touch_Live", "Att": "drib_Att", "Succ%": "drib_Succ%", "Tkld": "Tckl_Drib" ,"Tkld%": "Tckl_Drib%", "1/3": "Carries_Att_3rd", "CPA": "Carries_Att_Pen", "Mis": "fail_To_Gain_Control", "Dis": "Loss_Control_Tackle"})
    elif carpeta == "shooting":
        df = df.rename(columns={"Crs": "crosses", "Won%": "Aerialwon%"})
    elif carpeta == "mercado":
        df = df.rename(columns={ "player_name": "Player", "squad": "Squad", "player_position": "Pos", "player_height_mtrs": "Height", "player_market_value_euro": "Value"})
    elif carpeta == "keepers":
        df = df.rename(columns={"Save%.1": "P_Save%"})
    elif carpeta == "keepersadv":
        df = df.rename(columns = {"Opp": "Crosses_Opp", "Stp%": "Crosses_stp%"})
        
    if "Player" in df.columns:
       df["Player"] = df["Player"].astype(str).apply(normalizar)
    if "club_actual" in df.columns:
        df["club_actual"] = df["club_actual"].astype(str).apply(normalizar)
    if "liga" in df.columns:
        df["Competition"] = df["Competition"].astype(str).apply(normalizar)
    return df

def normalizar(var):
    if pd.isna(var):
        return None
    var = var.lower().strip()
    var = unicodedata.normalize('NFKD', var).encode('ASCII', 'ignore').decode('utf-8')
    var = re.sub(r'[^a-z\s]', '', var)  # Solo letras y espacios
    return ' '.join(var.split())  # Espacios únicos

def mapear( variable, lista_fuente, lista_objetivo, umbral=85):
    mapping = {}
    for variable in lista_fuente:
        mejor, score, _ = process.extractOne(variable, lista_objetivo)
        if score >= umbral:
            mapping[variable] = mejor
    return mapping


def aniadir_col(carpeta, archivo, df):
    nombre = archivo.stem
    partes = nombre.split("_")
    
    if carpeta == "mercado":
        df["Season"] = partes[3]
        df["Competition"] = partes[2].lower().strip()
    else:
        temporada = f"{partes[1]}-{partes[2]}"
        df["Season"] = temporada
        df["Competition"] = partes[0].lower().strip()
    return df

def guardar_csv(dataframes, rutas):
    for nombre, df in dataframes.items():
        salida = rutas[nombre]
        df.to_csv(salida, index=False)

def limpiar_ligas_transfermarkt(df):
    ligas_principales = ["Serie A", "LaLiga", "Premier League", "Bundesliga"]
    return df[df["comp_name"].isin(ligas_principales)]

def igualar_nombres(dataframes):
    nombres_fbref = dataframes["stats"]["Player"].unique().tolist()
    nombres_mercado = dataframes["mercado"]["Player"].unique().tolist()

    club_actual_fbref = dataframes["stats"]["Squad"].unique().tolist()
    club_actual_mercado = dataframes["mercado"]["Squad"].unique().tolist()
    
    mapping_m = mapear("Player", nombres_mercado, nombres_fbref)
    dataframes["mercado"]["Player"] = dataframes["mercado"]["Player"].replace(mapping_m)
    
    mapping_m = mapear("Squad", club_actual_mercado, club_actual_fbref)
    dataframes["mercado"]["Squad"] = dataframes["mercado"]["Squad"].replace(mapping_m)
    
def limpiar():  
    dataframes = {}
    ruta_destino = {}
    for carpeta in SUBCARPETAS:
        dfs=[]
        carpeta_origen = FOLDER_ORIGEN / carpeta
        carpeta_destino = FOLDER_DESTINO / carpeta
        carpeta_destino.mkdir(parents=True, exist_ok=True)
        for archivo in carpeta_origen.glob("*.csv"):
            try:
                print(f"Procesando {archivo}")
                try:
                    df = pd.read_csv(archivo)
                except Exception:
                    df = pd.read_excel(archivo, engine="openpyxl")
                    
                if carpeta == "mercado":
                  df = limpiar_ligas_transfermarkt(df)
                
                df.columns = df.columns.str.strip()
                df = df.loc[:, ~df.columns.duplicated()]
                
                #me dquedo con las columnas interesantes
                columnas_utiles = COLUMNAS_UTILES.get(carpeta, df.columns.tolist())
                columnas_disponibles = [col for col in columnas_utiles if col in df.columns]
                df = df[columnas_disponibles]
                
                #cambio de nombre de las columnas y estandarización
                df = estandarizar_columnas(df, carpeta)
                
                #se añade la liga y la temporada al dataset
                df = aniadir_col(carpeta, archivo, df)
                
                #se apilan los dataset de cada categoria
                dfs.append(df)
                
                #guardar los dataset limpios
                salida = carpeta_destino / archivo.name
                df.to_csv(salida, index=False)
                print(f"Guardado en {salida}")
                
            except Exception as e:
                print(f"Error con {archivo}: {e}")
        if dfs:
            #se unen en un solo dataset por cada categoria
            unidos = pd.concat(dfs, ignore_index=True)
            archivo_salida = carpeta_destino / f"{carpeta}.csv"
            #rutas para guardar los dataframes
            ruta_destino[carpeta] = archivo_salida
            #dataframes por categoria
            dataframes[carpeta] = unidos
    #igualar nombres de transfermarkt con los demas
    igualar_nombres(dataframes)
    
    #guardar dataframes en csvs
    guardar_csv(dataframes, ruta_destino)

            
limpiar()