
from scrape_fbref import crear_tablas
#from limpiar_datos import limpiar
# Definir ligas y temporadas
ligas = [
    {"name": "La Liga", "slug": "laliga", "code": 12},
]

temporadas = ["2022-2023", "2023-2024", "2024-2025"]

def scrapping():
    for liga in ligas:
        for temporada in temporadas:
            print(f"\nProcesando: {liga['name']} - {temporada}")
            # --- FBref ---
            try:
                crear_tablas(liga["code"], temporada, liga["name"])
            except Exception as e:
                print(f"FBref error en {liga['name']} {temporada}: {e}")
                
scrapping()
#limpiar()