#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updated CT Lottery Scraper
1. Switches source to 'ScratchGamesTable' for a reliable list of active games.
2. Navigates directly to detail pages (e.g., /ScratchGames/1234/) to avoid modal/click errors.
3. Extracts full prize tables including "Total Prizes" (Start) and "Unclaimed Prizes" (Remaining).
"""

import pandas as pd
import time
from datetime import date, datetime
from dateutil.tz import tzlocal
import re
import io
import numpy as np
import gc

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup

def exportCTScratcherRecs():
    # --- SELENIUM SETUP ---
    print("Initializing Firefox...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    
    # Low Memory Settings
    firefox_options.set_preference("permissions.default.image", 2)
    firefox_options.set_preference("plugin.state.flash", 0)
    firefox_options.set_preference("browser.cache.disk.enable", False)
    firefox_options.set_preference("browser.cache.memory.enable", False)
    firefox_options.set_preference("browser.cache.offline.enable", False)
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0")

    driver = None
    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
    except Exception as e:
        print(f"Error launching Firefox: {e}")
        return None, None

    try:
        # 1. GET GAME LIST FROM TABLE PAGE
        # This page is much lighter and easier to scrape than the main visual grid
        url_list = "https://ctlottery.org/ScratchGamesTable"
        print(f"Fetching game list from {url_list}...")
        
        driver.get(url_list)
        
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        except:
            print("  > Warning: Timeout waiting for table.")
            return None, None

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        main_table = soup.find('table')
        
        if not main_table:
            print("  > Error: Could not find the main games table.")
            return None, None

        # Parse the master list
        # The table usually has headers like: Game # | Game Name | Price | Top Prize | ...
        # We read it into a DF to easily grab IDs
        try:
            df_master = pd.read_html(io.StringIO(str(main_table)))[0]
        except ValueError:
            print("  > Error parsing master table HTML.")
            return None, None

        # Normalize headers
        df_master.columns = [str(c).strip().lower() for c in df_master.columns]
        
        # --- FIX: ROBUST COLUMN FINDER ---
        # Identify key columns by looking for keywords, otherwise fallback to index
        
        col_id = next((c for c in df_master.columns if '#' in c or 'game' in c), None)
        col_name = next((c for c in df_master.columns if 'name' in c), None)
        col_price = next((c for c in df_master.columns if 'price' in c), None)
        
        # Fallback to indices if names are weird (e.g. empty or just numbers)
        if not col_id: 
            col_id = df_master.columns[0] # Assume 1st col is ID
        if not col_name and len(df_master.columns) > 1:
            col_name = df_master.columns[1] # Assume 2nd col is Name
        if not col_price and len(df_master.columns) > 2:
            col_price = df_master.columns[2] # Assume 3rd col is Price

        print(f"  Using columns: ID='{col_id}', Name='{col_name}', Price='{col_price}'")
        print(f"Found {len(df_master)} active games. Starting detail scrape...")

        all_game_rows = []
        all_prize_tables = []

        # 2. ITERATE AND SCRAPE DETAILS
        for index, row in df_master.iterrows():
            try:
                # Clean Game Number (remove # and spaces)
                raw_id = str(row[col_id])
                gameNumber = re.sub(r'[^\d]', '', raw_id)
                
                if not gameNumber: continue # Skip empty rows
                
                gameName = str(row[col_name]).strip() if col_name else "Unknown"
                
                # Clean Price
                raw_price = str(row[col_price]) if col_price else "0"
                try:
                    price = float(raw_price.replace('$','').strip())
                except: price = 0

                # Construct Direct URL
                detail_url = f"https://www.ctlottery.org/ScratchGames/{gameNumber}/"
                print(f"Processing #{gameNumber}: {gameName}")

                # Visit Detail Page
                driver.get(detail_url)
                
                # Check if page exists/loaded (look for specific text or table)
                try:
                    WebDriverWait(driver, 5).until(
                        lambda d: d.find_elements(By.TAG_NAME, "table") or "Game Status: Ended" in d.page_source
                    )
                except:
                    # If timeout, page might be broken or empty, skip
                    # print("    > Timeout loading details.")
                    continue

                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                full_text = detail_soup.get_text(" ", strip=True)

                # Scrape Overall Odds
                overall_odds = 0
                odds_match = re.search(r'Overall Odds:?\s*1\s*[:\.]\s*([\d\.]+)', full_text, re.IGNORECASE)
                if odds_match:
                    overall_odds = float(odds_match.group(1))

                # Scrape Dates
                startDate = None
                start_match = re.search(r'Game Start:?\s*(\d{1,2}/\d{1,2}/\d{4})', full_text)
                if start_match: startDate = start_match.group(1)
                
                # Scrape Prize Table
                # Look for table with "Unclaimed" or "Total Prizes"
                tixdata = pd.DataFrame()
                tables = detail_soup.find_all('table')
                
                for tbl in tables:
                    headers = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
                    # CT tables usually: Prize Amount | Total Prizes | Unclaimed Prizes
                    if 'prize' in headers and ('unclaimed' in headers or 'total' in headers):
                        try:
                            tixdata = pd.read_html(io.StringIO(str(tbl)))[0]
                            break
                        except: pass
                
                topprize = 0
                topprizeremain = 0
                topprizestarting = 0

                if not tixdata.empty:
                    # Map Columns
                    cols_map = {}
                    for col in tixdata.columns:
                        c_low = str(col).lower()
                        if 'prize' in c_low and 'amount' in c_low: cols_map[col] = 'prizeamount'
                        elif 'total' in c_low: cols_map[col] = 'Winning Tickets At Start'
                        elif 'unclaimed' in c_low: cols_map[col] = 'Winning Tickets Unclaimed'
                    
                    tixdata.rename(columns=cols_map, inplace=True)
                    
                    # Clean Money
                    if 'prizeamount' in tixdata.columns:
                        def clean_money(val):
                            val = str(val).upper()
                            if 'ANNUITY' in val: val = val.replace('ANNUITY', '')
                            if 'WK' in val: return 52 * 20 * 1000 # Est
                            if 'YR' in val: return 20 * 100000 # Est
                            return float(re.sub(r'[^\d\.]', '', val))
                        
                        tixdata['prizeamount'] = tixdata['prizeamount'].apply(clean_money)

                    # Clean Counts
                    for c in ['Winning Tickets At Start', 'Winning Tickets Unclaimed']:
                        if c in tixdata.columns:
                            tixdata[c] = tixdata[c].astype(str).str.replace(r'[,]', '', regex=True)
                            tixdata[c] = pd.to_numeric(tixdata[c], errors='coerce').fillna(0)
                        else:
                            tixdata[c] = 0
                    
                    tixdata['gameNumber'] = gameNumber
                    tixdata['gameName'] = gameName
                    tixdata['price'] = price
                    tixdata['dateexported'] = date.today()
                    
                    all_prize_tables.append(tixdata)
                    
                    # Top Prize Stats
                    if not tixdata.empty:
                        # Sometimes top prize is at top, sometimes sorted differently. Use max().
                        top_row = tixdata.loc[tixdata['prizeamount'].idxmax()]
                        topprize = top_row['prizeamount']
                        topprizestarting = top_row['Winning Tickets At Start']
                        topprizeremain = top_row['Winning Tickets Unclaimed']

                # Append Summary
                all_game_rows.append({
                    'price': price, 'gameName': gameName, 'gameNumber': gameNumber,
                    'gameURL': detail_url, 'gamePhoto': None,
                    'topprize': topprize, 'overallodds': overall_odds,
                    'topprizestarting': topprizestarting, 'topprizeremain': topprizeremain,
                    'topprizeavail': "Available" if topprizeremain > 0 else "Claimed",
                    'startDate': startDate, 'endDate': None, 'lastdatetoclaim': None,
                    'extrachances': None, 'secondChance': None, 'dateexported': date.today()
                })
                
                # Cleanup
                if index % 20 == 0: gc.collect()

            except Exception as e:
                print(f"  > Error processing {gameNumber}: {e}")
                continue

    finally:
        driver.quit()

    # --- SAVE & CALCULATE ---
    if not all_game_rows:
        print("No data collected.")
        return None, None

    print("Compiling DataFrames...")
    tixlist = pd.DataFrame(all_game_rows)
    tixlist.to_csv("./CTtixlist.csv", index=False)
    
    if all_prize_tables:
        tixtables = pd.concat(all_prize_tables, ignore_index=True)
    else:
        tixtables = pd.DataFrame()
        
    tixtables.to_csv("./CTscratchertables.csv", index=False)
    
    scratchersall = tixlist.copy().drop_duplicates()
    scratchersall.to_csv("./CTscratcherslist.csv", index=False)

    # --- STATS CALCULATION ---
    print("Calculating Statistics...")
    
    # 1. Type Conversion
    scratchertables = tixtables.copy()
    for col in ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
        scratchertables[col] = pd.to_numeric(scratchertables[col], errors='coerce').fillna(0)
    
    scratchersall['price'] = pd.to_numeric(scratchersall['price'], errors='coerce')
    scratchersall['overallodds'] = pd.to_numeric(scratchersall['overallodds'], errors='coerce')

    # 2. Group & Merge
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index()
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    
    # 3. Calculate Totals (CT provides Start counts, so we can check if they sum up correctly)
    # Actually, CT provides "Total Prizes". Total TICKETS = Total Prizes * Overall Odds (roughly)
    # OR sometimes Total Tickets is listed on page. 
    # Let's use the Odds method for consistency.
    
    gamesgrouped['Total at start'] = gamesgrouped['Winning Tickets At Start'] * gamesgrouped['overallodds']
    gamesgrouped['Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * gamesgrouped['overallodds']
    
    # Top Prize Odds
    gamesgrouped['topprizeodds'] = gamesgrouped.apply(
        lambda x: x['Total at start'] / x['topprizestarting'] if x['topprizestarting'] > 0 else 0, axis=1
    )

    # 4. Detailed Metrics
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped[gamesgrouped['gameNumber'] == gameid].copy()
        if gamerow.empty: continue
        
        # Use calc'd totals
        start_total = gamerow['Total at start'].values[0]
        curr_total = gamerow['Total remaining'].values[0]
        price = gamerow['price'].values[0]
        
        prizes_df = scratchertables[scratchertables['gameNumber'] == gameid].copy()
        unclaimed_sum = prizes_df['Winning Tickets Unclaimed'].sum()
        
        # Avoid zero div
        if curr_total > 0:
            gamerow['Current Odds of Any Prize'] = curr_total / unclaimed_sum if unclaimed_sum > 0 else 0
            gamerow['Probability of Winning Any Prize'] = unclaimed_sum / curr_total
            
            prizes_df['EV'] = (prizes_df['prizeamount'] - price) * (prizes_df['Winning Tickets Unclaimed'] / curr_total)
            gamerow['Expected Value of Any Prize (as % of cost)'] = prizes_df['EV'].sum() / price if price > 0 else 0
        else:
            gamerow['Current Odds of Any Prize'] = 0
            gamerow['Probability of Winning Any Prize'] = 0
            gamerow['Expected Value of Any Prize (as % of cost)'] = 0

        # Profit Prizes (> price)
        prof_unclaimed = prizes_df.loc[prizes_df['prizeamount'] > price, 'Winning Tickets Unclaimed'].sum()
        if curr_total > 0 and prof_unclaimed > 0:
            gamerow['Odds of Profit Prize'] = curr_total / prof_unclaimed
            gamerow['Probability of Winning Profit Prize'] = prof_unclaimed / curr_total
        else:
            gamerow['Odds of Profit Prize'] = 0
            gamerow['Probability of Winning Profit Prize'] = 0

        # Defaults for schema match
        cols_needed = ['Change in Odds of Profit Prize', 'StdDev of All Prizes', 'Max Tickets to Buy', 
                       'Percent of Prizes Remaining', 'Rank by Cost', 'Overall Rank', 'Current Odds of Top Prize',
                       'Change in Current Odds of Top Prize', 'Change in Current Odds of Any Prize',
                       'Change in Probability of Any Prize', 'Change in Probability of Profit Prize',
                       'StdDev of Profit Prizes', 'Odds of Any Prize + 3 StdDevs', 'Odds of Profit Prize + 3 StdDevs',
                       'Expected Value of Profit Prize (as % of cost)', 'Change in Expected Value of Profit Prize',
                       'Percent of Profit Prizes Remaining', 'Ratio of Decline in Prizes to Decline in Losing Ticket',
                       'Rank by Best Probability of Winning Any Prize', 'Rank by Best Probability of Winning Profit Prize',
                       'Rank by Most Available Prizes', 'Rank by Best Change in Probabilities', 'Rank Average',
                       'Photo','FAQ', 'About', 'Directory']
        for c in cols_needed: gamerow[c] = 0
        
        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)

    # 5. Final Merge
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(columns=['gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x'], errors='ignore', inplace=True)
    ratingstable.rename(columns={'gameName_y':'gameName','dateexported_x':'dateexported','topprizeodds_x':'topprizeodds','overallodds_x':'overallodds'}, inplace=True)
    
    # Rankings
    ratingstable['Rank by Least Expected Losses'] = ratingstable['Expected Value of Any Prize (as % of cost)'].rank(ascending=False)
    ratingstable['Overall Rank'] = ratingstable['Rank by Least Expected Losses'].rank()
    
    ratingstable['Stats Page'] = "/connecticut-statistics-for-each-scratcher-game"
    
    ratingstable.to_csv("./CTratingstable.csv", index=False)
    print("Success! Saved CTratingstable.csv")
    
    return ratingstable, scratchertables

if __name__ == "__main__":
    exportCTScratcherRecs()