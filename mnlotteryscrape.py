#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 17:08:05 2026

@author: michaeljames
"""

import pandas as pd
import io
import gc
import os
import re
from datetime import date
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
import time

def get_driver():
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    # Performance preference: disable images to save RAM
    firefox_options.set_preference("permissions.default.image", 2)
    service = Service(GeckoDriverManager().install())
    return webdriver.Firefox(service=service, options=firefox_options)

def exportScratcherRecs():
    print("Initializing Clean MN Scraper...")
    driver = get_driver()
    all_prize_tables = []

    try:
        driver.get("https://www.mnlottery.com/games/scratch")
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/games/scratch/']")))
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # --- ROBUST URL CLEANING ---
        raw_links = [a['href'] for a in soup.find_all('a', href=True) if "/games/scratch/" in a['href']]
        game_links = []
        for link in raw_links:
            # Strip domain if it exists, then re-add it once to ensure consistency
            clean_path = re.sub(r'https?://(www\.)?mnlottery\.com', '', link)
            full_url = f"https://www.mnlottery.com{clean_path}"
            if full_url not in game_links and len(clean_path.split('/')) > 3:
                game_links.append(full_url)

        print(f"✅ Found {len(game_links)} valid MN scratcher links.")

        for i, link in enumerate(game_links):
            # Refresh driver every 10 games to prevent memory bloat/Killed: 9
            if i > 0 and i % 10 == 0:
                driver.quit()
                driver = get_driver()

            try:
                driver.get(link)
                # Wait for the specific prize table container
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                
                tables = pd.read_html(io.StringIO(driver.page_source))
                if tables:
                    df = tables[0].copy()
                    df.columns = [str(c).lower().strip() for c in df.columns]
                    
                    # MN-Specific Header Mapping
                    mapping = {
                        'to win': 'prizeamount',
                        'number of winners**': 'Winning Tickets Unclaimed',
                        'number of winners': 'Winning Tickets Unclaimed',
                        'odds*': 'odds'
                    }
                    df = df.rename(columns=mapping)

                    if 'Winning Tickets Unclaimed' in df.columns:
                        # Clean numeric formatting
                        df['prizeamount'] = df['prizeamount'].replace(r'[\$,]', '', regex=True)
                        df['prizeamount'] = pd.to_numeric(df['prizeamount'], errors='coerce')
                        df['Winning Tickets Unclaimed'] = df['Winning Tickets Unclaimed'].replace(r'[,]', '', regex=True)
                        df['Winning Tickets Unclaimed'] = pd.to_numeric(df['Winning Tickets Unclaimed'], errors='coerce')
                        
                        game_slug = link.split('/')[-1]
                        df['gameName'] = game_slug.replace('-', ' ').title()
                        df['gameNumber'] = game_slug
                        df['dateexported'] = date.today()
                        
                        all_prize_tables.append(df)
                        print(f"  > Scraped: {df['gameName'].iloc[0]}")
                
                gc.collect()

            except Exception as e:
                print(f"    - Error on {link}: {e}")

        # --- SAFE SAVE BLOCK ---
        if all_prize_tables:
            final_df = pd.concat(all_prize_tables, ignore_index=True)
            
            # Use an absolute path to the current directory to avoid MacOS Permission/Timeout errors
            current_dir = os.path.dirname(os.path.abspath(__file__))
            output_file = os.path.join(current_dir, "MNscratchertables.csv")
            
            # Check if file is open/locked
            try:
                final_df.to_csv(output_file, index=False)
                print(f"✅ Final CSV saved successfully at: {output_file}")
            except Exception as save_err:
                # Fallback save with timestamp if original is locked
                alt_file = f"MN_data_{int(time.time())}.csv"
                final_df.to_csv(alt_file, index=False)
                print(f"⚠️ Primary file locked. Saved to: {alt_file}")

    finally:
        driver.quit()

if __name__ == "__main__":
    exportScratcherRecs()