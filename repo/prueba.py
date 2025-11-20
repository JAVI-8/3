import os
import pandas as pd

MERC_SILVER_PATH = r"C:/Users/jahoy/Documents/scouting/lake/gold/players_stats_market"

print(os.listdir(MERC_SILVER_PATH))

# Si hay parquets, prueba a leer uno:
for f in os.listdir(MERC_SILVER_PATH):
    if f.endswith(".parquet"):
        p = os.path.join(MERC_SILVER_PATH, f)
        print("Leyendo:", p)
        df = pd.read_parquet(p)
        print(df.shape)
        print(df.head())
        break
