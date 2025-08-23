import os
import pandas as pd
from pathlib import Path
import unicodedata
import re

# Carpeta de origen y destino
FOLDER_ORIGEN = Path("data")
FOLDER_DESTINO = Path("data/limpios")
FOLDER_DESTINO.mkdir(parents=True, exist_ok=True)

SUBCARPETAS = ["stats", "defense", "misc", "passing", "possession", "shooting", "mercado", "keepers", "keepersadv"]

# Columnas clave a conservar por carpeta
COLUMNAS_UTILES = {
    "stats": [
        "Player", "Squad", "Liga", "Season", "Born", "MP", "Starts", "Min", "Gls", "Ast", "CrdY", "CrdR", "PrgC","PrgP","PrgR"
    ],
    "defense": [
       "Player", "Squad", "Liga", "Season","Def 3rd", "Mid 3rd","Att 3rd", "Tkl%", "Tkl.1", "TklW", "Int", "Blocks", "Sh", "Pass", "Clr", "Err"
    ],
    "misc": [
       "Player", "Squad", "Liga", "Season","Fls", "Off","Crs", "Recov", "Won%"
    ],
    "passing": [
       "Player", "Squad", "Liga", "Season","Cmp", "Cmp%", "Att.1", "Cmp%.1", "Att.2", "Cmp%.2", "Att.3", "Cmp%.3", "xAG", "xA", "A-xAG", "Kp", "1/3", "PPA", "CrsPA"
    ],
    "possession": [
       "Player", "Squad", "Liga", "Season","Def 3rd", "Mid 3rd","Att 3rd", "Att Pen", "Live", "Att", "Succ%", "Tkld", "Tkld%", "PrgDist", "1/3", "CPA", "Mis", "Dis"
    ],
    "shooting": [
       "Player", "Squad", "Liga", "Season","Sh", "SoT","G/Sh", "xG", "npxG", "npxG/Sh", "G-xG"
    ],
    "mercado": [
        "player_name", "Squad", "comp_name", "Season" ,"player_position", "squad", "player_height_mtrs", "player_market_value_euro"
    ],
    "keepers": [
        "Player", "squad", "Liga", "Season","GA", "SoTA", "Save%", "CS%", "PKatt", "Save%.1"
    ],
    "keepersadv": [
        "Player", "Squad", "Liga", "Season","PSxG", "PSxG/SoT", "PSxG+/-", "Launch%", "Opp", "Stp%", "#OPA" 
    ]
}
float_columns = [
        "Born", "MP", "Starts", "Min", "Gls", "Ast", "YellowC", "RedC", "PrgC", "PrgP", "PrgR",
        "Fls", "Off", "Crosses", "Recov", "Aerialwon%", "Tack_Def_3rd", "Tack_Mid_3rd",
        "Tack_Att_3rd", "Tkl%", "Tkl", "TklW", "Int", "Blocks", "Block_Shots", "Block_Pass",
        "Clearences", "Err", "Pass_cmp", "Pass_cmp%", "Pass_cmp_Short%", "Pass_cmp_Medium%",
        "Pass_cmp_Long%", "xAG", "xA", "A-xAG", "Pass_cmp_Att_3rd", "PPA", "CrsPA",
        "Touch_Def_3rd", "Touch_Mid_3rd", "Touch_Att_3rd", "Touch_Att_Pen", "drib_Att",
        "drib_Succ%", "PrgDist", "Carries_Att_3rd", "Carries_Att_Pen", "fail_To_Gain_Control",
        "Loss_Control_Tackle", "Sh", "SoT", "G/Sh", "xG", "npxG", "npxG/Sh", "G-xG", "Height", "Value",
        "Pass_Medium", "Pass_Long", "Pass_Short", "Touch_Live", "Tckl_Drib%", "Tckl_Drib", "GA", 
        "SoTA", "Save%", "CS%", "PKatt", "P_Save%", "PSxG","PSxG/SoT", "PSxG+/-", "Launch%", 
        "Crosses_Opp", "Crosses_stp%", "#OPA"
]

# Estandarizar nombres y columnas comunes
def estandarizar_columnas(df, carpeta):
    if carpeta == "stats":
        df = df.rename(columns={"CrdY": "YellowC", "CrdR": "RedC"})
    elif carpeta == "defense":
        df = df.rename(columns={"Def 3rd": "Tack_Def_3rd", "Mid 3rd": "Tack_Mid_3rd", "Att 3rd": "Tack_Att_3rd", "Tkl.1": "Tkl", "Sh": "Block_Shots", "Pass": "Block_Pass", "Clr": "Clearences"})
    elif carpeta == "misc":
        df = df.rename(columns={"Crs": "Crosses", "Won%": "Aerialwon%"})
    elif carpeta == "passing":
        df = df.rename(columns={"Cmp": "Pass_cmp", "Cmp%": "Pass_cmp%", "Cmp%.1": "Pass_cmp_Short%", "Att.1": "Pass_Short", "Cmp%.2": "Pass_cmp_Medium%", "Att.2": "Pass_Medium", "Cmp%.3": "Pass_cmp_Long%", "Att.3": "pass_Long", "Kp": "Key_pases", "1/3": "Pass_cmp_Att_3rd"})
    elif carpeta == "possession":
        df = df.rename(columns={"Def 3rd": "Touch_Def_3rd", "Mid 3rd": "Touch_Mid_3rd", "Att 3rd": "Touch_Att_3rd", "Att Pen": "Touch_Att_Pen", "Live": "touch_Live", "Att": "drib_Att", "Succ%": "drib_Succ%", "Tkld": "Tckl_Drib" ,"Tkld%": "Tckl_Drib%", "1/3": "Carries_Att_3rd", "CPA": "Carries_Att_Pen", "Mis": "fail_To_Gain_Control", "Dis": "Loss_Control_Tackle"})
    elif carpeta == "shooting":
        df = df.rename(columns={"Crs": "crosses", "Won%": "Aerialwon%"})
    elif carpeta == "mercado":
        df = df.rename(columns={ "player_name": "Player", "squad": "Squad", "comp_name": "Liga", "player_position": "Pos", "player_height_mtrs": "Height", "player_market_value_euro": "Value"})
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

def normalizar_porcentajes(df):
    percent_cols = [c for c in df.columns if str(c).endswith("%")]
    df[percent_cols] = df[percent_cols].apply(pd.to_numeric, errors="coerce") / 100
    return df

def pasar_a_numeric(df):
    existentes = [c for c in float_columns if c in df.columns]
    if existentes:
        df.loc[:, existentes] = df.loc[:, existentes].apply(pd.to_numeric, errors="coerce")
    return df
    
def normalizar(var):
    if pd.isna(var):
        return None
    var = var.lower().strip()
    var = unicodedata.normalize('NFKD', var).encode('ASCII', 'ignore').decode('utf-8')
    var = re.sub(r'[^a-z\s]', '', var)  # Solo letras y espacios
    return ' '.join(var.split())  # Espacios únicos

def limpiar_ligas_transfermarkt(df):
    ligas_principales = ["Serie A", "LaLiga", "Premier League", "Bundesliga", "Ligue 1"]
    return df[df["Liga"].isin(ligas_principales)]
    
def limpiar(df, type):  
        print(f"limpiando {type}")
        #me dquedo con las columnas interesantes
        columnas_utiles = COLUMNAS_UTILES.get(type, df.columns.tolist())
        columnas_disponibles = [col for col in columnas_utiles if col in df.columns]
        df = df[columnas_disponibles]
        df = estandarizar_columnas(df, type)
        df = normalizar_porcentajes(df)
        df = pasar_a_numeric(df)
        if(type == "mercado"):
            df = limpiar_ligas_transfermarkt(df)
        outdir = Path("data/v1")
        outdir.mkdir(parents=True, exist_ok=True)
        ruta = outdir / f"{type}.csv"
        df.to_csv(ruta, index=False)     
    
def main():
    df = pd.read_csv("valores_mercado_2024-2025.csv")
    df = limpiar(df, "mercado")
    
main()
            

        