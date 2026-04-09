import pandas as pd
import time
import io
import re
import gc
from datetime import date
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def exportFLScratcherRecs():
    print("Initializing Florida Scraper (Fixed X-of-Y Parsing)...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("permissions.default.image", 2)
    
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)

    try:
        # 1. Harvest Master List
        driver.get("https://floridalottery.com/games/scratch-offs/top-remaining-prizes")
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        game_links = [urljoin("https://floridalottery.com", a['href']) for a in soup.find_all('a', href=True) 
                      if "/games/scratch-offs/view" in a['href']]
        game_links = list(set(game_links))

        all_prize_tables = []
        summary_data = []

        for i, link in enumerate(game_links):
            # Restart browser every 10 games to prevent memory hangs
            if i > 0 and i % 10 == 0:
                driver.quit()
                driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)

            try:
                driver.get(link)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # --- TABLE PARSING ---
                tables = pd.read_html(io.StringIO(str(detail_soup.find('table'))))
                if tables:
                    df = tables[0].copy()
                    df.columns = [str(c).lower().strip() for c in df.columns]
                    
                    # Target the actual FL header "prizes remaining"
                    target_col = next((c for c in df.columns if 'remaining' in c), None)
                    prize_col = next((c for c in df.columns if 'prize amount' in c or 'prizes' in c), None)

                    if target_col and prize_col:
                        # Rename for standardization
                        df = df.rename(columns={prize_col: 'prizeamount'})

                        # --- SPLIT "X of Y" LOGIC ---
                        # Extracts first number as Unclaimed, second as At Start
                        def extract_unclaimed(val):
                            match = re.search(r'(\d[\d,]*)\s+of\s+(\d[\d,]*)', str(val))
                            return float(match.group(1).replace(',', '')) if match else 0

                        def extract_start(val):
                            match = re.search(r'(\d[\d,]*)\s+of\s+(\d[\d,]*)', str(val))
                            return float(match.group(2).replace(',', '')) if match else 0

                        df['Winning Tickets Unclaimed'] = df[target_col].apply(extract_unclaimed)
                        df['Winning Tickets At Start'] = df[target_col].apply(extract_start)

                        # Clean Prize Amount
                        df['prizeamount'] = df['prizeamount'].astype(str).str.replace(r'[^\d\.]', '', regex=True)
                        df['prizeamount'] = pd.to_numeric(df['prizeamount'], errors='coerce').fillna(0)

                        game_num = link.split('=')[-1]
                        game_name = detail_soup.find('h1').get_text(strip=True) if detail_soup.find('h1') else "FL Game"

                        df['gameNumber'], df['gameName'], df['dateexported'] = game_num, game_name, date.today()
                        all_prize_tables.append(df)
                        
                        # Summary Metadata
                        top_row = df.sort_values('prizeamount', ascending=False).iloc[0]
                        summary_data.append({
                            'gameNumber': game_num, 'gameName': game_name, 
                            'topprize': top_row['prizeamount'],
                            'topprizestarting': top_row['Winning Tickets At Start'],
                            'topprizeremain': top_row['Winning Tickets Unclaimed'],
                            'gameURL': link, 'dateexported': date.today()
                        })
                        print(f"  > Processed: {game_name}")

                gc.collect()

            except Exception as e:
                print(f"    - Error on {link}: {e}")

        # 3. Final Export
        if all_prize_tables:
            pd.concat(all_prize_tables, ignore_index=True).to_csv("./FLscratchertables.csv", index=False)
            pd.DataFrame(summary_data).to_csv("./FLscratcherslist.csv", index=False)
            print("✅ Successfully compiled FL data.")

    finally:
        driver.quit()

if __name__ == "__main__":
    exportFLScratcherRecs()