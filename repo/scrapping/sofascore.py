import ScraperFC as sfc

ss = sfc.Sofascore()

ligas = ["Bundesliga", "EPL", "La Liga", "Ligue 1", "Serie A"]
years = ["24/25", "25/26"] # Formato correcto para SofaScore
for year in years:
    for liga in ligas:
        df = ss.scrape_player_league_stats(
            year=year,
            league= liga,       # <<--- CORRECTO
            accumulation="per90",   # "total" | "perMatch" | "per90"
            selected_positions=["Goalkeepers","Defenders","Midfielders","Forwards"]
        )
        safe_year = year.replace("/", "_")
        file = f"C:/Users/jahoy/Documents/scouting/lake/bronze/sofascore/{liga.replace(' ', '_')}_{safe_year}_raw.csv"
        df.to_csv(file, index=False)
        print(f"C:/Users/jahoy/Documents/scouting/lake/bronze/sofascore/{liga.replace(' ', '_')}_{safe_year}_raw.csv")
