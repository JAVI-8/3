import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import os
from io import StringIO
import time
ids_types = [("stats_standard", "stats"), ("stats_possession", "possession"), ("stats_defense", "defense"), ("stats_misc", "misc"), ("stats_passing", "passing"), ("stats_shooting", "shooting"), ("stats_keeper", "keepers"), ("stats_keeper_adv", "keepersadv")]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}
def extraer_tabla_jugadores(url, table_id="stats_possession"):
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        raise ConnectionError(f"Error al acceder a {url}: código {response.status_code}")
    
    soup = BeautifulSoup(response.content, "lxml")
    comentarios = soup.find_all(string=lambda text: isinstance(text, Comment))

    for c in comentarios:
        if table_id in c:
            soup_comentario = BeautifulSoup(c, "lxml")
            tabla = soup_comentario.find("table", {"id": table_id})
            if tabla:
                df = pd.read_html(StringIO(str(tabla)), header=1)[0]
                if "Rk" in df.columns:
                    df = df[df["Rk"].apply(lambda x: str(x).isdigit())]
                return df

    raise ValueError(f"No se encontró la tabla {table_id} en {url}")

def build_defense_url(competition_id: int, season_label: str, type:str, slug:str) -> str:
    return f"https://fbref.com/en/comps/{competition_id}/{season_label}/{type}/{season_label}-{slug}-Stats"

def crear_tablas(competition_id: int, season_label: str, league_name: str, slug:str):
    for id, type in ids_types:
        fbref_D_url = build_defense_url(competition_id, season_label, type, slug)
        df = extraer_tabla_jugadores(fbref_D_url, id)
        time.sleep(10)
        nombre_archivo = f"prueba_data/{type}/{slug}_{season_label.replace('-', '_')}_{type}.csv"
        df.to_csv(nombre_archivo, index=False)
        print(f"✅ Guardado: {nombre_archivo}")