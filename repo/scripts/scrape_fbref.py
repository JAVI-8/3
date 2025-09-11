import requests
import time
import random
from bs4 import BeautifulSoup, Comment
import pandas as pd
from io import StringIO
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Referer": "https://fbref.com/",
}

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=5, backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

def extraer_tabla_desde_html(html: str, table_id: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    # FBref suele meter las tablas dentro de comentarios
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        if table_id in c:
            tabla = BeautifulSoup(c, "lxml").find("table", {"id": table_id})
            if tabla is not None:
                df = pd.read_html(StringIO(str(tabla)), header=1)[0]
                if "Rk" in df.columns:
                    df = df[df["Rk"].astype(str).str.isdigit()]
                return df.reset_index(drop=True)
    raise ValueError(f"No se encontró la tabla '{table_id}' en el HTML")

def build_url(competition_id: int, path_slug: str, liga_slug: str, season: str) -> str:
    # p.ej. /en/comps/20/2024-2025/possession/2024-2025-Bundesliga-Stats
    return f"https://fbref.com/en/comps/{competition_id}/{season}/{path_slug}/{season}-{liga_slug}-Stats"

def crear_tabla_liga(competition_id: int, liga_name: str, liga_slug: str,
                     season: str, table_id: str, path_slug: str,
                     session: requests.Session) -> pd.DataFrame:
    # warmup: visita el índice para cookies/referrer
    idx = f"https://fbref.com/en/comps/{competition_id}/{season}/"
    r1 = session.get(idx, timeout=20)
    if r1.status_code != 200:
        raise ConnectionError(f"Índice {idx} devolvió {r1.status_code}")

    url = build_url(competition_id, path_slug, liga_slug, season)
    print(url)
    r2 = session.get(url, timeout=30)
    if r2.status_code == 403:
        # backoff + retry
        time.sleep(2.0 + random.random())
        r2 = session.get(url, timeout=30)
    if r2.status_code != 200:
        raise ConnectionError(f"Error al acceder a {url}: código {r2.status_code}")

    df = extraer_tabla_desde_html(r2.text, table_id)
    df["Liga"] = liga_slug
    df["Season"] = season
    return df

def scrape_fbref(season: str, table_id: str, path_slug: str) -> pd.DataFrame:
    ligas = [
        {"name": "La Liga",        "slug": "La-Liga",        "code": 12},
        {"name": "Premier League", "slug": "Premier-League", "code": 9},
        {"name": "Serie A",        "slug": "Serie-A",        "code": 11},
        {"name": "Bundesliga",     "slug": "Bundesliga",     "code": 20},
        {"name": "Ligue-1",        "slug": "Ligue-1",        "code": 13},
    ]
    session = make_session()
    dfs = []

    for liga in ligas:
        print(f"\nProcesando: {liga['name']}")
        try:
            df = crear_tabla_liga(
                competition_id=liga["code"],
                liga_name=liga["name"],
                liga_slug=liga["slug"],
                season=season,
                table_id=table_id,
                path_slug=path_slug,
                session=session,
            )
            dfs.append(df)
        except Exception as e:
            print(f"FBref error en {liga['name']}: {e}")
        finally:
            # Pausa “cortés” entre peticiones para evitar bloqueos
            time.sleep(1.0 + random.random())

    if not dfs:
        # Evita variable no definida si todas fallan
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

if __name__ == "__main__":
    # ejemplo
    df = scrape_fbref("2025-2026", "stats_standard", "stats")
    print(df.shape)
