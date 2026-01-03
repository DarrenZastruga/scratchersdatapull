#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updated MI Lottery Scraper
1. Fixes Game Number/Price parsing to handle "Game Number: X" format.
2. Cleans up Game Name extraction from smashed text.
3. Broadens table column recognition to ensure data is captured.
"""

import pandas as pd
import time
from datetime import date, datetime
from dateutil.tz import tzlocal
import re
import io
import numpy as np

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportMIScratcherRecs():
    # --- SELENIUM SETUP ---
    print("Initializing Browser for Michigan...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0")

    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
    except Exception as e:
        print(f"Error launching Firefox: {e}")
        return None, None

    try:
        url = "https://www.michiganlottery.com/resources/instant-games-prizes-remaining"
        print(f"Fetching {url}...")
        
        driver.get(url)
        
        # Wait for content
        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            # Scroll to ensure all lazy elements load
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
        except:
            print("  > Warning: Timeout waiting for tables.")

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        tixtables = pd.DataFrame()
        tixlist = pd.DataFrame()
        
        tables = soup.find_all('table')
        print(f"Found {len(tables)} prize tables. Processing...")
        
        count_success = 0
        
        for i, tbl in enumerate(tables):
            try:
                # 1. FIND METADATA
                # The text usually looks like: "Lucky NumbersGame Number: 718Price: $10.00..."
                # We search the parent container for this text block.
                container = tbl.find_parent('div')
                header_text = ""
                
                # Look for the text block preceding the table
                if container:
                    # Collect text from parent container, ignoring the table itself
                    # (This helps capture the header which is usually a sibling of the table wrapper)
                    full_block_text = container.parent.get_text(" ", strip=True) if container.parent else ""
                    # If that's too much, try the direct previous sibling of the table container
                    prev = container.find_previous_sibling()
                    if prev: header_text = prev.get_text(" ", strip=True)
                    else: header_text = full_block_text
                
                # If finding strict siblings fails, rely on the text we saw in your logs
                # which suggests the header is tightly coupled.
                if "Game Number" not in header_text:
                    # Fallback: Search nearby headings
                    prev_header = tbl.find_previous(['h2','h3','h4','h5','h6','div'])
                    if prev_header: header_text = prev_header.get_text(" ", strip=True)

                # --- PARSING LOGIC ---
                gameName = "Unknown"
                gameNumber = "000"
                gamePrice = 0
                
                # Extract Game Number
                # Matches "Game Number: 718" or "Game #718"
                num_match = re.search(r'Game\s*(?:Number|#)\s*[:\.]?\s*(\d+)', header_text, re.IGNORECASE)
                if num_match:
                    gameNumber = num_match.group(1)
                    
                    # Name is usually everything BEFORE "Game Number"
                    split_name = header_text.split(num_match.group(0))[0]
                    gameName = split_name.strip()
                    # Clean up trailing garbage if any
                    if len(gameName) > 50: gameName = gameName[-50:] # Truncate if it grabbed too much prev text
                
                # Extract Price
                # Matches "Price: $10.00" or "$10"
                price_match = re.search(r'Price\s*[:\.]?\s*\$([\d\.]+)', header_text, re.IGNORECASE)
                if not price_match:
                    price_match = re.search(r'\$\s*(\d+\.\d{2})', header_text) # Look for currency format
                
                if price_match:
                    try: gamePrice = float(price_match.group(1))
                    except: pass

                # 2. PARSE TABLE
                tixdata = pd.read_html(io.StringIO(str(tbl)))[0]
                
                # Standardize Columns (Broad Matching)
                cols_map = {}
                for col in tixdata.columns:
                    c_low = str(col).lower()
                    if 'prize' in c_low: cols_map[col] = 'prizeamount'
                    elif 'start' in c_low or 'total' in c_low: cols_map[col] = 'Winning Tickets At Start'
                    elif 'remain' in c_low: cols_map[col] = 'Winning Tickets Unclaimed'
                
                tixdata.rename(columns=cols_map, inplace=True)
                
                # Validate
                if 'prizeamount' not in tixdata.columns:
                    # If scraping headers failed, sometimes data is in row 0
                    # But assuming standard MI format based on logs:
                    # Let's try mapping by index if names failed: 0=Prize, 1=Start, 2=Remaining
                    if len(tixdata.columns) >= 3:
                        tixdata.columns = ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']
                    else:
                        print(f"    > Skipping Table {i} (Cols: {list(tixdata.columns)}) - Unrecognized format")
                        continue

                # 3. CLEAN DATA
                tixdata['prizeamount'] = tixdata['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True)
                tixdata['prizeamount'] = pd.to_numeric(tixdata['prizeamount'], errors='coerce').fillna(0)
                
                for c in ['Winning Tickets At Start', 'Winning Tickets Unclaimed']:
                    tixdata[c] = tixdata[c].astype(str).str.replace(r'[,]', '', regex=True)
                    tixdata[c] = pd.to_numeric(tixdata[c], errors='coerce').fillna(0)
                
                # Fallback for MI: If Start is 0/Missing, assume Start = Remaining (prevents errors, though imperfect)
                if tixdata['Winning Tickets At Start'].sum() == 0:
                    tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets Unclaimed']

                tixdata['gameNumber'] = gameNumber
                tixdata['gameName'] = gameName
                tixdata['price'] = gamePrice
                tixdata['dateexported'] = date.today()
                
                tixtables = pd.concat([tixtables, tixdata], ignore_index=True)
                
                # 4. SUMMARY
                topprize = tixdata['prizeamount'].max()
                tp_row = tixdata[tixdata['prizeamount'] == topprize]
                
                topprizestart = tp_row['Winning Tickets At Start'].iloc[0] if not tp_row.empty else 0
                topprizeremain = tp_row['Winning Tickets Unclaimed'].iloc[0] if not tp_row.empty else 0
                
                new_row = {
                    'price': gamePrice,
                    'gameName': gameName,
                    'gameNumber': gameNumber,
                    'gameURL': url,
                    'gamePhoto': None,
                    'topprize': topprize,
                    'overallodds': 0, # MI grid often lacks this
                    'topprizestarting': topprizestart,
                    'topprizeremain': topprizeremain,
                    'topprizeavail': "Available" if topprizeremain > 0 else "Claimed",
                    'startDate': None,
                    'endDate': None,
                    'lastdatetoclaim': None,
                    'extrachances': None,
                    'secondChance': None,
                    'dateexported': date.today()
                }
                tixlist = pd.concat([tixlist, pd.DataFrame([new_row])], ignore_index=True)
                count_success += 1
                print(f"  Processed #{gameNumber} {gameName}")

            except Exception as e:
                print(f"    Error table {i}: {e}")
                continue

    finally:
        driver.quit()

    # --- STATS ---
    if tixlist.empty:
        print("No data collected.")
        return None, None

    print(f"Successfully scraped {count_success} games.")
    tixlist.to_csv("./MItixlist.csv", index=False)
    
    scratchersall = tixlist.copy().drop_duplicates()
    scratchersall.to_csv("./MIscratcherslist.csv", index=False)
    
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./MIscratchertables.csv", index=False)
    
    # Simple Stats (MI lacks Total Ticket Counts usually, so we do basic % calc)
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index()
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price']], how='left', on=['gameNumber'])
    
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped[gamesgrouped['gameNumber'] == gameid].copy()
        if gamerow.empty: continue
        
        prizes_df = scratchertables[scratchertables['gameNumber'] == gameid].copy()
        try: price = float(gamerow['price'].values[0])
        except: price = 0
        
        # Calculate % Remaining
        start_sum = prizes_df['Winning Tickets At Start'].sum()
        rem_sum = prizes_df['Winning Tickets Unclaimed'].sum()
        
        if start_sum > 0:
            gamerow['Percent of Prizes Remaining'] = rem_sum / start_sum
        else:
            gamerow['Percent of Prizes Remaining'] = 0
            
        # Defaults for missing data
        cols_needed = ['Overall Rank', 'Rank by Cost', 'Expected Value of Any Prize (as % of cost)', 'Probability of Winning Any Prize']
        for c in cols_needed: gamerow[c] = 0
        
        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)

    # Final Merge
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(columns=['gameName_x'], inplace=True, errors='ignore')
    ratingstable.rename(columns={'gameName_y':'gameName'}, inplace=True)
    
    # Rank
    ratingstable['Rank by Most Available Prizes'] = ratingstable['Percent of Prizes Remaining'].rank(ascending=False)
    ratingstable['Overall Rank'] = ratingstable['Rank by Most Available Prizes'].rank()
    
    ratingstable.to_csv("./MIratingstable.csv", index=False)
    print("Success! Saved MIratingstable.csv")
    
    return ratingstable, scratchertables

if __name__ == "__main__":
    exportMIScratcherRecs()