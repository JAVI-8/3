# 1) Ejecuta esto UNA VEZ para guardar cookies.json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import json, time

BASE = "https://www.transfermarkt.us"
LEAGUE_URL = f"{BASE}/laliga/teilnehmer/wettbewerb/ES1/saison_id/2024"

opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--window-size=1200,900")
driver = webdriver.Chrome(options=opts)
driver.get(LEAGUE_URL)
time.sleep(3)
# si aparece banner, intenta click en botón
try:
    btn = driver.find_element("css selector", "button[aria-label*='Accept'],button#onetrust-accept-btn-handler")
    btn.click()
    time.sleep(2)
except Exception:
    pass

cookies = driver.get_cookies()
with open("cookies.json", "w", encoding="utf-8") as f:
    json.dump(cookies, f, ensure_ascii=False, indent=2)
driver.quit()
