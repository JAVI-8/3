import ScraperFC as sfc

ss = sfc.Sofascore()
ligas = ["Bundesliga", "EPL", "La Liga", "Ligue 1", "Serie A"]
years = ["20/21", "21/22", "22/23", "23/24", "24/25", "25/26"] # Formato correcto para SofaScore
def stats_x90min():
   
    for year in years:
        for liga in ligas:
            df = ss.scrape_player_league_stats(
                year=year,
                league= liga,   
                accumulation="per90",   
                selected_positions=["Goalkeepers","Defenders","Midfielders","Forwards"]
            )
            safe_year = year.replace("/", "_")
            file = f"C:/Users/jahoy/Documents/scouting/lake/bronze/sofascore/{liga.replace(' ', '_')}_{safe_year}_raw.csv"
            df.to_csv(file, index=False)
            print(f"C:/Users/jahoy/Documents/scouting/lake/bronze/sofascore/{liga.replace(' ', '_')}_{safe_year}_raw.csv")

def stats():
     for year in years:
        for liga in ligas:
            df = ss.scrape_player_league_stats(
                        year=year,
                        league= liga,       
                        accumulation="total",   
                        selected_positions=["Goalkeepers","Defenders","Midfielders","Forwards"]
                    )
            safe_year = year.replace("/", "_")
            file = f"C:/Users/jahoy/Documents/scouting/lake/bronze/minutes/{liga.replace(' ', '_')}_{safe_year}_minutes.csv"
            df.to_csv(file, index=False)
            print(f"C:/Users/jahoy/Documents/scouting/lake/bronze/minutes/{liga.replace(' ', '_')}_{safe_year}_minutes.csv")

if __name__ == "__main__":
    stats()