# limpiar_datos.py
from pathlib import Path
import pandas as pd
import unicodedata
import re

#culumnas que son utilea para cada df
COLUMNAS_UTILES = {
    "stats": [
        "Player", "Squad", "Liga", "Season", "Born", "MP", "Starts", "Min",
        "Gls", "Ast", "CrdY", "CrdR", "PrgC", "PrgP", "PrgR"
    ],
    "defense": [
        "Player", "Squad", "Liga", "Season", "Def 3rd", "Mid 3rd", "Att 3rd",
        "Tkl%", "Tkl.1", "TklW", "Int", "Blocks", "Sh", "Pass", "Clr", "Err"
    ],
    "misc": [
        "Player", "Squad", "Liga", "Season", "Fls", "Off", "Crs", "Recov", "Won%"
    ],
    "passing": [
        "Player", "Squad", "Liga", "Season", "Cmp", "Cmp%", "Att.1", "Cmp%.1",
        "Att.2", "Cmp%.2", "Att.3", "Cmp%.3", "xAG", "xA", "A-xAG", "Kp", "1/3",
        "PPA", "CrsPA"
    ],
    "possession": [
        "Player", "Squad", "Liga", "Season", "Def 3rd", "Mid 3rd", "Att 3rd",
        "Att Pen", "Live", "Att", "Succ%", "Tkld", "Tkld%", "PrgDist", "1/3",
        "CPA", "Mis", "Dis"
    ],
    "shooting": [
        "Player", "Squad", "Liga", "Season", "Sh", "SoT", "G/Sh", "xG", "npxG",
        "npxG/Sh", "G-xG"
    ],
    "mercado": [
        "player_name", "Liga", "Season", "player_position",
        "squad", "player_height_mtrs", "player_market_value_euro"
    ],
    "keepers": [
        "Player", "Squad", "Liga", "Season", "GA", "SoTA", "Save%", "CS%",
        "PKatt", "Save%.1"
    ],
    "keepersadv": [
        "Player", "Squad", "Liga", "Season", "PSxG", "PSxG/SoT", "PSxG+/-",
        "Launch%", "Opp", "Stp%", "#OPA"
    ],
}

#normalizar las variables
def normalizar(texto):
    if pd.isna(texto):
        return None
    s = str(texto).lower().strip()
    s = unicodedata.normalize('NFKD', s).encode('ASCII', 'ignore').decode('utf-8')
    s = re.sub(r'[^a-z\s]', '', s)
    return ' '.join(s.split())


#renombrar columnas y normalizar variables de unión
def estandarizar_columnas(df: pd.DataFrame, carpeta: str) -> pd.DataFrame:
    if carpeta == "stats":
        df = df.rename(columns={"CrdY": "YellowC", "CrdR": "RedC"})
    elif carpeta == "defense":
        df = df.rename(columns={
            "Def 3rd": "Tack_Def_3rd", "Mid 3rd": "Tack_Mid_3rd",
            "Att 3rd": "Tack_Att_3rd", "Tkl.1": "Tkl",
            "Sh": "Block_Shots", "Pass": "Block_Pass", "Clr": "Clearences",
        })
    elif carpeta == "misc":
        df = df.rename(columns={"Crs": "Crosses", "Won%": "Aerialwon%"})
    elif carpeta == "passing":
        df = df.rename(columns={
            "Cmp": "Pass_cmp", "Cmp%": "Pass_cmp%", "Cmp%.1": "Pass_cmp_Short%",
            "Att.1": "Pass_Short", "Cmp%.2": "Pass_cmp_Medium%",
            "Att.2": "Pass_Medium", "Cmp%.3": "Pass_cmp_Long%",
            "Att.3": "Pass_Long", "Kp": "Key_pases", "1/3": "Pass_cmp_Att_3rd",
        })
    elif carpeta == "possession":
        df = df.rename(columns={
            "Def 3rd": "Touch_Def_3rd", "Mid 3rd": "Touch_Mid_3rd",
            "Att 3rd": "Touch_Att_3rd", "Att Pen": "Touch_Att_Pen",
            "Live": "Touch_Live", "Att": "drib_Att", "Succ%": "drib_Succ%",
            "Tkld": "Tckl_Drib", "Tkld%": "Tckl_Drib%",
            "1/3": "Carries_Att_3rd", "CPA": "Carries_Att_Pen",
            "Mis": "fail_To_Gain_Control", "Dis": "Loss_Control_Tackle",
        })
    elif carpeta == "shooting":
        pass
    elif carpeta == "mercado":
        df = df.rename(columns={
            "player_name": "Player", "squad": "Squad",
            "comp_name": "Liga", "player_position": "Pos",
            "player_height_mtrs": "Height",
            "player_market_value_euro": "Value",
        })
    elif carpeta == "keepers":
        df = df.rename(columns={"Save%.1": "P_Save%"})
    elif carpeta == "keepersadv":
        df = df.rename(columns={"Opp": "Crosses_Opp", "Stp%": "Crosses_stp%"})
        
    #normalizar columnsas de union player y squad
    if "Player" in df.columns:
        df["Player"] = df["Player"].astype(str).apply(normalizar)
    if "Squad" in df.columns:
        df["Squad"] = df["Squad"].astype(str).apply(normalizar)
    return df

#que los valores sean numericos
def normalizar_porcentajes(df: pd.DataFrame) -> pd.DataFrame:
    percent_cols = [c for c in df.columns if str(c).endswith("%")]
    if percent_cols:
        df[percent_cols] = df[percent_cols].apply(pd.to_numeric, errors="coerce") / 100
    return df

def limpiar_ligas_transfermarkt(df: pd.DataFrame) -> pd.DataFrame:
    ligas_principales = ["Serie A", "La-Liga", "Premier-League", "Bundesliga", "Ligue-1"]
    if "Liga" in df.columns:
        return df[df["Liga"].isin(ligas_principales)]
    return df

def limpiar(df: pd.DataFrame, carpeta: str) -> Path:
    print(f"[limpiar_datos] limpiando {carpeta}")
    columnas_utiles = COLUMNAS_UTILES.get(carpeta, df.columns.tolist())
    columnas_disponibles = [c for c in columnas_utiles if c in df.columns]
    df = df[columnas_disponibles]
    df = estandarizar_columnas(df, carpeta)
    df = normalizar_porcentajes(df)
    if carpeta == "mercado":
        df = limpiar_ligas_transfermarkt(df)
    out_path = f"repo/data/{carpeta}.csv"
    df.to_csv(out_path, index=False)
    return out_path

if __name__ == "__main__":
    print(f"[limpiar_datos] generado")
