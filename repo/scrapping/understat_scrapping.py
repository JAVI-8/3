import asyncio
from understat import Understat
import aiohttp
import pandas as pd
import os

LEAGUES = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1"]
YEARS = ["2025", "2026"]

OUTPUT_DIR = r"C:/Users/jahoy/Documents/scouting/lake/bronze/understat"
os.makedirs(OUTPUT_DIR, exist_ok=True)

async def get_league_players_stats(league_name: str, season: str) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        u = Understat(session)
        players = await u.get_league_players(
            league_name=league_name,
            season=season
        )
    return pd.DataFrame(players)

async def main():
    for season in YEARS:
        for league in LEAGUES:
            try:
                print(f"Descargando datos Understat: {league} {season}…")

                df = await get_league_players_stats(league, season)

                if df.empty:
                    print(f"⚠️ Sin datos para {league} {season}")
                    continue

                filename = f"{league}_{season}_understat.csv"
                outpath = os.path.join(OUTPUT_DIR, filename)
                df.to_csv(outpath, index=False)

                print(f"Guardado en: {outpath}\n")

            except Exception as e:
                print(f"❌ Error con {league} {season}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
