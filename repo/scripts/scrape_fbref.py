import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
from io import StringIO
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}
#obtener la info de cada jugador
def extraer_tabla_jugadores(url, table_id):
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

#construir url
def build_url(competition_id: int, type:str, slug:str, season) -> str:
    
    return f"https://fbref.com/en/comps/{competition_id}/{season}/{type}/{season}-{slug}-Stats"
#crea el df de una caracteristica
def crear_tablas(competition_id: int, name: str, slug:str, season, id, type):
    time.sleep(10)
    fbref_D_url = build_url(competition_id, type, slug, season)
    print(fbref_D_url)
    df = extraer_tabla_jugadores(fbref_D_url, id)
    df["Liga"] = slug
    df["Season"] = season
    return df
    
def scrape_fbref(season, id, type):
    #info de fbef
    ligas = [
    {"name": "La Liga", "slug": "La-Liga", "code": 12},
    {"name": "Premier League", "slug": "Premier-League", "code": 9},
    {"name": "Serie A", "slug": "Serie-A", "code": 11},
    {"name": "Bundesliga", "slug": "Bundesliga", "code": 20},
    {"name": "Ligue-1", "slug": "Ligue-1", "code": 13}
]
    unidos = []
    temporada=[]
    #recorre el array de info de fbref para recorrere las ligas
    for liga in ligas:
        print(f"\nProcesando: {liga['name']}")
        try:
            df = crear_tablas(liga["code"], liga["name"], liga["slug"], season, id, type)
            unidos.append(df)#une del mimo año en un solo df
            temporada = pd.concat(unidos, ignore_index=True)
            
        except Exception as e:
            print(f"FBref error en {liga['name']}: {e}")
            
    return temporada