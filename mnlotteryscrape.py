import pandas as pd
import time
import random
import re
import io
from datetime import date
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup

def exportScratcherRecs():
    print("Initializing Click-and-Scrape Firefox...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)
    driver.set_window_size(1920, 1080)

    try:
        url = "https://playonkansas.com/games/scratch-and-pull-tabs?filters=scratch"
        driver.get(url)
        print("Waiting for game grid to render...")
        
        # Wait for the actual game card links to appear in the DOM
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'scratch-and-pull-tabs/')]")))
        
        # 1. Harvest URLs using a reliable selector
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        game_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            if "/games/scratch-and-pull-tabs/" in href.lower() and len(href) > 40:
                full_url = "https://playonkansas.com" + href if not href.startswith('http') else href
                if full_url not in game_links:
                    game_links.append(full_url)

        print(f"✅ Found {len(game_links)} scratcher games. Starting detail scrape...")

        all_prize_tables = []
        # 2. Iterate through individual game pages
        for link in game_links[:15]: # Process 15 for a full run
            try:
                driver.get(link)
                # Wait for the specific "Unclaimed" table to be rendered by the Web Worker
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                
                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                game_name = detail_soup.find('h1').text.strip() if detail_soup.find('h1') else "Unknown"
                
                # Use Pandas to read the table directly from the rendered HTML
                tables = pd.read_html(io.StringIO(str(detail_soup.find('table'))))
                if tables:
                    df = tables[0]
                    df.columns = [str(c).lower() for c in df.columns]
                    # Identify and standardize columns for your stats engine
                    if any('unclaimed' in c or 'remain' in c for c in df.columns):
                        df.rename(columns={'amount':'prizeamount', 'unclaimed':'Winning Tickets Unclaimed', 'total':'Winning Tickets At Start'}, inplace=True, errors='ignore')
                        df['gameName'], df['gameNumber'], df['dateexported'] = game_name, link.split('-')[-1], date.today()
                        all_prize_tables.append(df)
                        print(f"  > Processed: {game_name}")

            except Exception as e:
                print(f"    - Error on detail page: {e}")

        # 3. Final Consolidation
        if all_prize_tables:
            final_df = pd.concat(all_prize_tables, ignore_index=True)
            final_df.to_csv("./KSscratchertables.csv", index=False)
            print("✅ Successfully compiled data into KSscratchertables.csv")

    finally:
        driver.quit()

if __name__ == "__main__":
    exportScratcherRecs()