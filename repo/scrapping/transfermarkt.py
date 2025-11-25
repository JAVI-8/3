# analisis.py
# -*- coding: utf-8 -*-
"""
Scraper de valores de mercado y biografía básica de Transfermarkt por competición y temporada.
Guarda CSVs en: C:/Users/jahoy/Documents/scouting/lake/bronze/mercado/

Extrae por jugador:
- market_value_eur, market_value_str
- shirt_number (dorsal)
- position
- height_cm
- birth_date, age
- nationality
- foot (si está disponible)

Nota: requiere respeto de rate-limits => usa 'pause_sec' y evita bajar demasiado.
"""

import os
import re
import time
import math
import unicodedata
from typing import Optional, List, Dict, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from datetime import date, datetime

BASE = "https://www.transfermarkt.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9,es-ES;q=0.8,es;q=0.7",
}

# código competición -> slug de la URL
COMP_SLUGS: Dict[str, str] = {
    "ES1": "laliga",           # LaLiga
    "GB1": "premier-league",   # Premier League
    "IT1": "serie-a",          # Serie A
    "FR1": "ligue-1",
    "L1": "bundesliga",        # corrige a minúsculas para consistencia
}

# temporadas (año de inicio): 2024 => 2024/25
SEASONS: List[int] = [2021, 2022, 2023, 2024]  # añade más si quieres

# =======================================================================================
# Utils
# =======================================================================================

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=4, backoff_factor=0.7, status_forcelist=(429, 500, 502, 503, 504))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

def sleep_safely(sec: float = 1.8):
    time.sleep(sec)

def normalize_space(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def normalize_key(s: Optional[str]) -> str:
    return strip_accents(normalize_space(s or "")).lower()

def parse_money_to_eur(value_str: Optional[str]) -> Optional[float]:
    if not value_str:
        return None
    s = normalize_space(value_str).lower().replace("€", "").replace(",", "")
    if s in {"-", "?", "free transfer"}:
        return None
    mult = 1.0
    if s.endswith("m"):
        mult = 1_000_000.0
        s = s[:-1]
    elif s.endswith("k"):
        mult = 1_000.0
        s = s[:-1]
    try:
        return float(s) * mult
    except ValueError:
        return None

def safe_int(x) -> Optional[int]:
    try:
        xi = int(str(x).strip())
        return xi
    except:
        return None

def compute_age_from_iso(dob: Optional[str]) -> Optional[int]:
    # dob formato libre; intentamos YYYY-MM-DD primero
    if not dob:
        return None
    try:
        y, m, d = map(int, re.findall(r"\d+", dob)[:3])
        b = date(y, m, d)
        t = date.today()
        return t.year - b.year - ((t.month, t.day) < (b.month, b.day))
    except Exception:
        return None

def to_cm(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    m = re.search(r"(\d{3})\s*cm", text)
    if m:
        return int(m.group(1))
    # a veces viene en metros "1,87 m" / "1.87 m"
    m2 = re.search(r"(\d)[\.,](\d{2})\s*m", text)
    if m2:
        return int(m2.group(1)) * 100 + int(m2.group(2))
    return None

# =======================================================================================
# Scraper
# =======================================================================================

def get_competition_url(comp_code: str, season: int, comp_slug: Optional[str] = None) -> str:
    slug = comp_slug or COMP_SLUGS.get(comp_code)
    if not slug:
        raise ValueError(f"No se conoce el slug para {comp_code}. Añádelo a COMP_SLUGS o pásalo manualmente.")
    # patrón: /{slug}/startseite/wettbewerb/{comp_code}/saison_id/{season}
    return f"{BASE}/{slug}/startseite/wettbewerb/{comp_code}/saison_id/{int(season)}"

def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    r = session.get(url, timeout=30)
    if r.status_code == 404:
        raise requests.HTTPError(f"404 Not Found en {url}")
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def extract_teams_from_comp(session, comp_code, season, comp_slug=None) -> List[Dict]:
    url = get_competition_url(comp_code, season, comp_slug)
    soup = fetch_soup(session, url)

    teams = []
    for a in soup.select("table.items td.hauptlink a[href*='/startseite/verein/']"):
        name = normalize_space(a.text)
        link = urljoin(BASE, a["href"])
        m = re.search(r"/verein/(\d+)", link)
        if m:
            teams.append({"team_name": name, "team_url": link, "team_id": int(m.group(1))})

    if not teams:
        raise RuntimeError(f"No se encontraron equipos en {url}")

    uniq = {(t["team_id"], t["team_name"]): t for t in teams}
    return list(uniq.values())

def squad_url_from_team(team_url: str, season: int) -> List[str]:
    parsed = urlparse(team_url)
    path = parsed.path
    m = re.search(r"/verein/(\d+)", path)
    team_id = m.group(1) if m else None

    parts = [p for p in path.split("/") if p]
    team_slug = parts[0] if parts else None
    if team_slug in {"startseite", "kader", "verein", "profil", "spieler"}:
        team_slug = None

    candidates: List[str] = []
    if team_id:
        if team_slug:
            candidates += [
                f"{BASE}/{team_slug}/kader/verein/{team_id}/saison_id/{season}/plus/1",
                f"{BASE}/{team_slug}/kader/verein/{team_id}/saison_id/{season}",
                f"{BASE}/{team_slug}/startseite/verein/{team_id}/saison_id/{season}",
            ]
        candidates += [
            f"{BASE}/kader/verein/{team_id}/saison_id/{season}/plus/1",
            f"{BASE}/kader/verein/{team_id}/saison_id/{season}",
            f"{BASE}/startseite/verein/{team_id}/saison_id/{season}",
            f"{BASE}/kader/verein/{team_id}?saison_id={season}",
        ]
    else:
        candidates.append(team_url.replace("/startseite/", "/kader/"))

    seen = set()
    uniq = []
    for u in candidates:
        if u and u not in seen:
            uniq.append(u); seen.add(u)
    return uniq

# --------------------- Perfil de jugador (datos bio) ---------------------

def parse_profile_bio(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[int], Optional[str], Optional[str], Optional[str]]:
    """
    Devuelve (birth_date_iso, height_cm, foot, nationality, position_text)
    Busca en diferentes bloques del perfil por robustez.
    """
    # 1) Fecha de nacimiento (DOB)
    birth_date = None
    # Indicadores típicos: "Date of birth:", "Born:"
    lab = soup.find(string=re.compile(r"(Date of birth|Born)", re.I))
    if lab:
        val = lab.find_next("span")
        if val:
            txt = normalize_space(val.get_text(" ", strip=True))
            # intenta extraer YYYY-MM-DD
            m = re.search(r"(\d{4})[-/\.](\d{1,2})[-/\.](\d{1,2})", txt)
            if m:
                y, mo, d = m.groups()
                birth_date = f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
            else:
                # formatos tipo "Jan 1, 2000"
                try:
                    dt = datetime.strptime(txt, "%b %d, %Y")
                    birth_date = dt.strftime("%Y-%m-%d")
                except:
                    pass

    # 2) Altura
    height_cm = None
    ht_strong = soup.find("strong", string=re.compile(r"Height", re.I))
    if ht_strong and ht_strong.parent:
        txt = ht_strong.parent.get_text(" ", strip=True)
        height_cm = to_cm(txt)
    if not height_cm:
        # buscar en la cabecera (algunas plantillas ponen "1,87 m" sin etiqueta)
        header = soup.select_one("div.dataHeader, div.dataDaten")
        if header:
            height_cm = to_cm(header.get_text(" ", strip=True))

    # 3) Pie
    foot = None
    ft_label = soup.find(string=re.compile(r"Foot", re.I))
    if ft_label:
        ft_val = ft_label.find_next("span")
        foot = normalize_space(ft_val.get_text(strip=True)) if ft_val else None

    # 4) Nacionalidad (primera)
    nationality = None
    nat_spans = soup.select("span[itemprop='nationality'], span.nationality")
    if nat_spans:
        nationality = normalize_space(nat_spans[0].get_text(strip=True))

    # 5) Posición (bloque "Main position" o "Position")
    position_text = None
    pos_lab = soup.find(string=re.compile(r"(Main position|Position)", re.I))
    if pos_lab:
        pos_val = pos_lab.find_next("span")
        if pos_val:
            position_text = normalize_space(pos_val.get_text(" / ", strip=True))
    # fallback: a veces está en un <div> con "position" en clases
    if not position_text:
        pos_div = soup.find("div", class_=re.compile("position", re.I))
        if pos_div:
            position_text = normalize_space(pos_div.get_text(" / ", strip=True))

    return birth_date, height_cm, foot, nationality, position_text

# Cache simple en memoria de perfiles ya visitados
_PROFILE_CACHE: Dict[str, Dict] = {}

def scrape_player_profile(session: requests.Session, profile_url: str, pause_sec: float = 0.6) -> Dict:
    if profile_url in _PROFILE_CACHE:
        return _PROFILE_CACHE[profile_url]
    r = session.get(profile_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    birth_date, height_cm, foot, nationality, position_text = parse_profile_bio(soup)
    out = {
        "birth_date": birth_date,
        "height_cm": height_cm,
        "foot": foot,
        "nationality": nationality,
        "position_profile": position_text,
    }
    _PROFILE_CACHE[profile_url] = out
    sleep_safely(pause_sec)  # respeta servidor
    return out

# --------------------- Lectura de plantilla (squad) ---------------------

def extract_players_from_team(session, team_name, team_id, squad_url_or_list, pause_profile: float = 0.6):
    if isinstance(squad_url_or_list, str):
        candidates = [squad_url_or_list]
    else:
        candidates = list(squad_url_or_list)

    soup = None
    last_err = None
    for url in candidates:
        try:
            r = session.get(url, timeout=30)
            if r.status_code == 404:
                last_err = f"404 en {url}"
                continue
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            break
        except Exception as e:
            last_err = repr(e)
            continue

    if soup is None:
        raise requests.HTTPError(f"No se pudo abrir la página de plantilla para {team_name}. Último error: {last_err}")

    players = []
    rows = soup.select("table.items > tbody > tr")

    for row in rows:
        name_tag = row.select_one("td.posrela td.hauptlink a[href*='/profil/spieler/']") or \
                   row.select_one("td.hauptlink a[href*='/profil/spieler/']")
        if not name_tag:
            continue

        player_name = normalize_space(name_tag.get_text())
        player_url = urljoin(BASE, name_tag.get("href"))
        m = re.search(r"/spieler/(\d+)", player_url)
        player_id = int(m.group(1)) if m else None

        # Dorsal: suele estar en el primer td centrado (pero varía)
        shirt_td = row.select_one("td.zentriert")
        shirt_number = safe_int(shirt_td.get_text()) if shirt_td else None

        # Posición "de fila" (puede ser menos precisa que el perfil)
        pos_td = row.select_one("td[class*='positional']") or row.select_one("td:nth-of-type(5)")
        position_row = normalize_space(pos_td.get_text()) if pos_td else None

        # Market value
        mv_cell = row.select_one("td.rechts.hauptlink")
        mv_text = normalize_space(mv_cell.get_text()) if mv_cell else None
        market_value_eur = parse_money_to_eur(mv_text)

        # Enriquecer con el perfil:
        bio = {}
        try:
            bio = scrape_player_profile(session, player_url, pause_sec=pause_profile)
        except Exception as e:
            # En caso de fallo de perfil, continuamos con lo disponible
            bio = {"birth_date": None, "height_cm": None, "foot": None, "nationality": None, "position_profile": None}
            print(f"[WARN] Perfil falló {player_url}: {e}")

        age = compute_age_from_iso(bio.get("birth_date"))

        players.append({
            "club": team_name,
            "club_id": team_id,
            "player": player_name,
            "player_id": player_id,
            "player_url": player_url,
            "market_value_str": mv_text,
            "market_value_eur": market_value_eur,
            "shirt_number": shirt_number,
            "position_row": position_row,
            "position": bio.get("position_profile") or position_row,
            "height_cm": bio.get("height_cm"),
            "birth_date": bio.get("birth_date"),
            "age": age,
            "nationality": bio.get("nationality"),
            "foot": bio.get("foot"),
        })
    return players

def scrape_transfermarkt_competition(comp_code, season, pause_sec: float = 1.8, comp_slug=None, pause_profile: float = 0.6) -> pd.DataFrame:
    session = make_session()
    teams = extract_teams_from_comp(session, comp_code, season, comp_slug)

    all_rows = []
    for idx, t in enumerate(teams, 1):
        print(f"[{idx}/{len(teams)}] {t['team_name']} — leyendo plantilla…")
        squad_urls = squad_url_from_team(t["team_url"], season)
        sleep_safely(pause_sec)
        all_rows.extend(extract_players_from_team(session, t["team_name"], t["team_id"], squad_urls, pause_profile=pause_profile))
        sleep_safely(pause_sec)

    df = pd.DataFrame(all_rows)
    if df.empty:
        raise RuntimeError("No se extrajo ningún jugador.")

    # Normalizaciones y metadatos
    df["player_norm"] = df["player"].apply(normalize_key)
    df["club_norm"] = df["club"].apply(normalize_key)
    df["season"] = int(season)
    df["competition_code"] = comp_code

    # Orden columnas
    col_order = [
        "competition_code", "season",
        "club", "club_id",
        "player", "player_id", "player_url",
        "shirt_number", "position", "position_row",
        "height_cm", "birth_date", "age", "nationality", "foot",
        "market_value_str", "market_value_eur",
        "player_norm", "club_norm"
    ]
    df = df[[c for c in col_order if c in df.columns]]
    return df

# =======================================================================================
# Lote: ligas x temporadas
# =======================================================================================

if __name__ == "__main__":
    pause_sec = 1.8          # pausa entre peticiones "grandes"
    pause_profile = 0.6      # pausa entre perfiles (cada jugador)
    out_dir = r"C:/Users/jahoy/Documents/scouting/lake/bronze/mercado"
    os.makedirs(out_dir, exist_ok=True)

    for season in SEASONS:
        for comp_code in COMP_SLUGS.keys():
            print("=" * 88)
            print(f"Iniciando scrapeo: {comp_code} {season}")
            try:
                df = scrape_transfermarkt_competition(
                    comp_code, int(season),
                    pause_sec=pause_sec,
                    comp_slug=COMP_SLUGS.get(comp_code),
                    pause_profile=pause_profile
                )
                out_path = os.path.join(out_dir, f"transfermarkt_{comp_code}_{season}.csv")
                df.to_csv(out_path, index=False, encoding="utf-8-sig")
                print(f"✅ Guardado: {out_path} ({len(df)} filas)")
            except Exception as e:
                print(f"❌ Error en {comp_code} {season}: {e}")
