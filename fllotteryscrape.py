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
from urllib.parse import urljoin, urlparse, parse_qs

# Install driver once to avoid API limits
GECKO_PATH = GeckoDriverManager().install()

def get_driver():
    options = Options()
    options.add_argument("--headless")
    # 'Normal' strategy ensures we wait for basic script execution
    options.page_load_strategy = 'normal' 
    options.set_preference("permissions.default.image", 2)
    service = Service(GECKO_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    driver.set_window_size(1920, 1080)
    return driver

def exportFLScratcherRecs():
    print("Initializing Florida Scraper (Deep-Element Targeting)...")
    driver = get_driver()
    base_url = "https://floridalottery.com"
    
    try:
        driver.get(f"{base_url}/games/scratch-offs/top-remaining-prizes")
        wait = WebDriverWait(driver, 30)
        # Wait for the main catalog table
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        game_links = []
        for a in soup.find_all('a', href=True):
            if "/games/scratch-offs/" in a['href'] and "id=" in a['href']:
                full_url = urljoin(base_url, a['href'])
                if full_url not in game_links:
                    game_links.append(full_url)

        print(f"✅ Found {len(game_links)} FL games. Starting targeted crawl...")

        all_prize_tables = []
        summary_data = []

        for i, link in enumerate(game_links):
            if i > 0 and i % 10 == 0:
                driver.quit()
                driver = get_driver()
                print(f"--- Session Reset (Game {i+1}) ---")

            try:
                driver.get(link)
                
                # CRITICAL: Wait for the specific section where the table lives
                # FL detail pages use an 'oddsPrizes' section
                wait = WebDriverWait(driver, 20)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.table-responsive, table")))
                
                # Small sleep to let JavaScript finish populating rows
                time.sleep(1) 
                
                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # --- TARGET THE CORRECT TITLE ---
                # Avoid 'Search Our Site' by targeting the breadcrumb or specific header class
                game_name_tag = detail_soup.find('h1', class_='page-title') or detail_soup.find('h1')
                game_name = game_name_tag.get_text(strip=True) if game_name_tag else "FL Scratcher"
                if "Search Our Site" in game_name:
                    # Fallback: Extract from the link ID if name is blocked
                    game_name = f"Game {parse_qs(urlparse(link).query).get('id',[''])[0]}"

                # --- TARGET THE PRIZE TABLE ---
                # We specifically look for tables within the content area
                tables = pd.read_html(io.StringIO(str(detail_soup)))
                df = None
                for t in tables:
                    cols = [str(c).lower() for c in t.columns]
                    # Prize tables have specific headers like 'odds' or 'remaining'
                    if any('remaining' in c or 'odds' in c for c in cols):
                        df = t.copy()
                        break
                
                if df is not None:
                    df.columns = [str(c).lower().strip() for c in df.columns]
                    
                    # Column Mapping
                    rem_col = next((c for c in df.columns if 'remaining' in c), None)
                    prz_col = next((c for c in df.columns if 'amount' in c or 'prize' in c and 'remaining' not in c), None)

                    if rem_col and prz_col:
                        df = df.rename(columns={prz_col: 'prizeamount'})

                        def split_fl_qty(val, part):
                            match = re.search(r'(\d[\d,]*)\s+of\s+(\d[\d,]*)', str(val))
                            if match:
                                num = match.group(part).replace(',', '')
                                return float(num)
                            # Fallback if only one number is present
                            num_match = re.findall(r'[\d,]+', str(val))
                            if num_match:
                                return float(num_match[0].replace(',', ''))
                            return 0

                        df['Winning Tickets Unclaimed'] = df[rem_col].apply(lambda x: split_fl_qty(x, 1))
                        df['Winning Tickets At Start'] = df[rem_col].apply(lambda x: split_fl_qty(x, 2))
                        df['prizeamount'] = pd.to_numeric(df['prizeamount'].astype(str).str.replace(r'[^\d\.]', '', regex=True), errors='coerce').fillna(0)

                        game_num = parse_qs(urlparse(link).query).get('id', ['unknown'])[0]
                        df['gameNumber'], df['gameName'], df['dateexported'] = game_num, game_name, date.today()
                        all_prize_tables.append(df)
                        
                        top_row = df.sort_values('prizeamount', ascending=False).iloc[0]
                        summary_data.append({
                            'gameNumber': game_num, 'gameName': game_name, 'price': 0,
                            'topprize': top_row['prizeamount'],
                            'topprizestarting': top_row['Winning Tickets At Start'],
                            'topprizeremain': top_row['Winning Tickets Unclaimed'],
                            'gameURL': link, 'dateexported': date.today()
                        })
                        print(f"  > Scraped: {game_name}")
                else:
                    print(f"    ⚠️ No valid data table found on {link}")

                gc.collect()

            except Exception as e:
                print(f"    - Error on {link}: {e}")

        if all_prize_tables:
            pd.concat(all_prize_tables, ignore_index=True).to_csv("./FLscratchertables.csv", index=False)
            pd.DataFrame(summary_data).to_csv("./FLscratcherslist.csv", index=False)
            print("✅ Process Complete.")

    finally:
        driver.quit()

if __name__ == "__main__":
    exportFLScratcherRecs()