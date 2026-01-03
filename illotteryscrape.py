#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updated IL Scraper (Selenium Version):
1. Restores the missing RANKING calculations at the end of the script.
2. Handles the 'Start = Remaining' data limitation gracefully.
3. Ensures all numeric columns are properly formatted before ranking.
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

def exportILScratcherRecs():
    # --- SELENIUM SETUP ---
    print("Initializing Browser...")
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
        urls = ["https://www.illinoislottery.com/games-hub/instant-tickets",
                "https://www.illinoislottery.com/games-hub/instant-tickets?page=1",
                "https://www.illinoislottery.com/games-hub/instant-tickets?page=2"]
        
        tixlist = pd.DataFrame()
        print("Scraping Game List...")
        
        # --- 1. SCRAPE GAME HUB PAGES ---
        for url in urls:
            try:
                driver.get(url)
                try:
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "simple-game-card")))
                except: pass
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                pages = soup.find_all('div', class_='simple-game-card card-container__item')
                
                print(f"  Found {len(pages)} games on page.")
                
                for p in pages:
                    link_tag = p.find('a')
                    if not link_tag: continue
                    
                    gameURL = 'https://www.illinoislottery.com' + str(link_tag.get('href'))
                    
                    # Image
                    banner = p.find(class_='simple-game-card__banner')
                    gamePhoto = None
                    if banner:
                        style = banner.get('style', '')
                        if 'url(' in style:
                            img_path = style.replace('background-image:url(', '').replace(')', '').replace(';','')
                            gamePhoto = 'https://www.illinoislottery.com' + img_path

                    # Detail Page
                    try:
                        driver.get(gameURL)
                        time.sleep(0.2)
                        
                        game_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        details_block = game_soup.find('div', class_='itg-details-block')
                        
                        if details_block and details_block.find('table'):
                            table = pd.read_html(io.StringIO(str(details_block.find('table'))))[0]
                            
                            def get_val(key):
                                res = table.loc[table[0]==key, 1]
                                return res.iloc[0] if not res.empty else None

                            gameNumber = get_val('Game Number')
                            overall_str = get_val('Overall Odds')
                            overallodds = 0.0
                            if overall_str:
                                overallodds = float(str(overall_str).replace('1 in ','').replace(' to 1','').replace('1: ',''))
                            
                            price_str = get_val('Price Point')
                            gamePrice = price_str.replace('$','') if price_str else '0'
                            startDate = get_val('Launch Date')
                            
                            new_row = {
                                'price': gamePrice, 'gameNumber': gameNumber, 'gameURL': gameURL,
                                'gamePhoto': gamePhoto, 'overallodds': overallodds, 'startDate': startDate,
                                'endDate': None, 'lastdatetoclaim': None, 'extrachances': None,
                                'secondChance': None, 'dateexported': date.today()
                            }
                            tixlist = pd.concat([tixlist, pd.DataFrame([new_row])], ignore_index=True)
                            print(f"    Processed Game #{gameNumber}")
                    except: continue
            except: continue

        # --- 2. SCRAPE UNCLAIMED PRIZES ---
        print("\nScraping Unclaimed Prizes Table...")
        url_prizes = "https://www.illinoislottery.com/about-the-games/unpaid-instant-games-prizes"
        
        try:
            driver.get(url_prizes)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except: pass
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            tixtables = None
            wrapper = soup.find('div', class_='unclaimed-prizes-table__wrapper')
            if wrapper: tixtables = wrapper.find('table')
            if not tixtables: tixtables = soup.find('table', class_='unclaimed-prizes-table')
            
            if not tixtables:
                print("  > CRITICAL: No prize table found.")
                return None, None

            rows = tixtables.find_all('tr')
            tixdata = pd.DataFrame()
            
            for row in rows:
                cells = row.find_all('td')
                if not cells: continue
                try:
                    gameNameRaw = cells[0].text.strip()
                    gameName = re.split(r'[()]', gameNameRaw)[0].strip()
                    gamePrice = cells[1].text.replace('$','').strip()
                    gameNumRaw = cells[2].text.strip()
                    gameNumber = re.split(r'[()]', gameNumRaw)[0].strip()
                    
                    prizevalues = cells[3].get_text(separator='|').split('|')
                    totalremaining = cells[-1].get_text(separator='|').split('|') # Last col is usually remaining
                    
                    clean_prizes = [x.replace('$','').replace(',','').strip() for x in prizevalues if x.strip()]
                    clean_remain = [x.replace(',','').strip() for x in totalremaining if x.strip()]
                    
                    # IL hides start counts, so we set Start = Unclaimed
                    clean_start = clean_remain 
                    
                    min_len = min(len(clean_prizes), len(clean_start), len(clean_remain))
                    
                    for i in range(min_len):
                        t_row = pd.DataFrame([{
                            'price': gamePrice, 'gameNumber': gameNumber, 'gameName': gameName,
                            'prizeamount': clean_prizes[i],
                            'Winning Tickets At Start': clean_start[i], 
                            'Winning Tickets Unclaimed': clean_remain[i]
                        }])
                        tixdata = pd.concat([tixdata, t_row], ignore_index=True)
                except: continue
                
            tixdata['dateexported'] = date.today()
            
        except Exception as e:
            print(f"Error getting prize table: {e}")
            return None, None

    finally:
        driver.quit()

    # --- 3. MERGE LIST & PRIZES ---
    print("\nMerging Data...")
    
    # Update tixlist based on prize data
    for t in tixlist['gameNumber']:
        game_prizes = tixdata[tixdata['gameNumber'] == t]
        if game_prizes.empty: continue
        
        tixlist.loc[tixlist['gameNumber']==t, 'gameName'] = game_prizes['gameName'].iloc[0]
        
        game_prizes = game_prizes.copy()
        game_prizes['prize_float'] = pd.to_numeric(game_prizes['prizeamount'], errors='coerce')
        top_row = game_prizes.loc[game_prizes['prize_float'].idxmax()]
        
        tixlist.loc[tixlist['gameNumber']==t, 'topprize'] = top_row['prizeamount']
        tixlist.loc[tixlist['gameNumber']==t, 'topprizestarting'] = top_row['Winning Tickets At Start']
        tixlist.loc[tixlist['gameNumber']==t, 'topprizeremain'] = top_row['Winning Tickets Unclaimed']
        
        rem = int(top_row['Winning Tickets Unclaimed'])
        tixlist.loc[tixlist['gameNumber']==t, 'topprizeavail'] = 'Top Prize Claimed' if rem == 0 else np.nan

    tixlist.to_csv("./ILtixlist.csv", index=False)
    
    scratchersall = tixlist.loc[tixlist['gameNumber'] != "Coming Soon!"].copy()
    scratchersall.drop_duplicates(inplace=True)
    scratchersall.to_csv("./ILscratcherslist.csv", index=False)

    # Prepare Tables
    raw_tables = tixdata[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']].copy()
    
    for col in ['Winning Tickets At Start', 'Winning Tickets Unclaimed']:
        raw_tables[col] = pd.to_numeric(raw_tables[col], errors='coerce').fillna(0)
    raw_tables['prizeamount_clean'] = pd.to_numeric(raw_tables['prizeamount'], errors='coerce').fillna(0)

    # --- 4. CALCULATE TOTALS & CREATE 'SCRATCHERTABLES' ---
    gamesgrouped = raw_tables.groupby(['gameNumber','gameName','dateexported'], observed=True)[
        ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    ].sum().reset_index()
    
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    gamesgrouped = gamesgrouped.dropna(subset=['overallodds'])
    
    gamesgrouped['Total at start'] = gamesgrouped['Winning Tickets At Start'] * gamesgrouped['overallodds']
    gamesgrouped['Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * gamesgrouped['overallodds']
    
    gamesgrouped['Non-prize at start'] = gamesgrouped['Total at start'] - gamesgrouped['Winning Tickets At Start']
    gamesgrouped['Non-prize remaining'] = gamesgrouped['Total remaining'] - gamesgrouped['Winning Tickets Unclaimed']
    
    # Build Full Scratchertables
    alltables = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped[gamesgrouped['gameNumber'] == gameid].iloc[0]
        game_prizes = raw_tables[raw_tables['gameNumber'] == gameid].copy()
        game_prizes = game_prizes[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        
        non_prize_row = pd.DataFrame([{
            'gameNumber': gameid, 'gameName': gamerow['gameName'], 'prizeamount': '0',
            'Winning Tickets At Start': gamerow['Non-prize at start'], 'Winning Tickets Unclaimed': gamerow['Non-prize remaining'],
            'dateexported': gamerow['dateexported']
        }])
        
        total_row = pd.DataFrame([{
            'gameNumber': gameid, 'gameName': gamerow['gameName'], 'prizeamount': 'Total',
            'Winning Tickets At Start': gamerow['Total at start'], 'Winning Tickets Unclaimed': gamerow['Total remaining'],
            'dateexported': gamerow['dateexported']
        }])
        
        combined_game_table = pd.concat([game_prizes, non_prize_row, total_row], ignore_index=True)
        alltables = pd.concat([alltables, combined_game_table], ignore_index=True)

    scratchertables = alltables
    scratchertables.to_csv("./ILscratchertables.csv", index=False)

    # --- 5. RATINGS & RANKINGS ---
    currentodds = pd.DataFrame()
    
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped[gamesgrouped['gameNumber'] == gameid].copy()
        if gamerow.empty: continue
        
        start_total = gamerow['Total at start'].values[0]
        curr_total = gamerow['Total remaining'].values[0]
        try: price = float(str(gamerow['price'].values[0]).replace('$',''))
        except: price = 0
        
        prizes_df = raw_tables[raw_tables['gameNumber'] == gameid].copy()
        
        unclaimed_sum = prizes_df['Winning Tickets Unclaimed'].sum()
        
        # Probabilities
        prob_any = unclaimed_sum / curr_total if curr_total > 0 else 0
        gamerow['Probability of Winning Any Prize'] = prob_any
        gamerow['Current Odds of Any Prize'] = (1 / prob_any) if prob_any > 0 else 0
        
        prof_df = prizes_df[prizes_df['prizeamount_clean'] > price]
        prof_unclaimed = prof_df['Winning Tickets Unclaimed'].sum()
        gamerow['Odds of Profit Prize'] = (curr_total / prof_unclaimed) if prof_unclaimed > 0 else 0
        gamerow['Probability of Winning Profit Prize'] = prof_unclaimed / curr_total if curr_total > 0 else 0
        
        # EV
        prizes_df['EV'] = (prizes_df['prizeamount_clean'] - price) * (prizes_df['Winning Tickets Unclaimed'] / curr_total) if curr_total > 0 else 0
        ev_sum = prizes_df['EV'].sum()
        gamerow['Expected Value of Any Prize (as % of cost)'] = ev_sum / price if price > 0 else 0
        
        # Defaults for missing data columns
        gamerow['Change in Current Odds of Any Prize'] = 0 
        gamerow['Change in Odds of Profit Prize'] = 0
        gamerow['Percent of Prizes Remaining'] = 1.0 # Since Start=Remaining
        gamerow['Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0
        
        try:
            ph = scratchersall.loc[scratchersall['gameNumber']==gameid,'gamePhoto'].values
            gamerow['Photo'] = ph[0] if len(ph) > 0 else None
        except: gamerow['Photo'] = None
        
        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)

    # Final Merge
    scratchersall['price'] = pd.to_numeric(scratchersall['price'], errors='coerce')
    currentodds['price'] = pd.to_numeric(currentodds['price'], errors='coerce')
    
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    
    # Cleanup
    cols_drop = ['gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x']
    ratingstable.drop(columns=[c for c in cols_drop if c in ratingstable.columns], inplace=True)
    ratingstable.rename(columns={'gameName_y':'gameName','dateexported_x':'dateexported','topprizeodds_x':'topprizeodds','overallodds_x':'overallodds'}, inplace=True)
    
    # --- ADDED: RANKING CALCULATIONS ---
    # Fills in the 0s for rank columns
    ratingstable['Rank by Best Probability of Winning Any Prize'] = ratingstable['Probability of Winning Any Prize'].rank(ascending=False)
    ratingstable['Rank by Best Probability of Winning Profit Prize'] = ratingstable['Probability of Winning Profit Prize'].rank(ascending=False)
    
    # Since "Change" stats are 0, we rely on EV and Probability for ranking
    ratingstable['Rank by Least Expected Losses'] = ratingstable['Expected Value of Any Prize (as % of cost)'].rank(ascending=False)
    
    # "Most Available Prizes" is usually based on % remaining, which is 100% here. So this rank will be tied.
    ratingstable['Rank by Most Available Prizes'] = ratingstable['Percent of Prizes Remaining'].rank(ascending=False)
    
    # Combined Rank Average
    # We only average the ranks that have meaningful data
    ratingstable['Rank Average'] = (
        ratingstable['Rank by Best Probability of Winning Any Prize'] +
        ratingstable['Rank by Best Probability of Winning Profit Prize'] +
        ratingstable['Rank by Least Expected Losses']
    ) / 3
    
    ratingstable['Overall Rank'] = ratingstable['Rank Average'].rank(ascending=True)
    ratingstable['Rank by Cost'] = ratingstable.groupby('price')['Overall Rank'].rank('dense', ascending=True)

    ratingstable['Stats Page'] = "/illinois-statistics-for-each-scratcher-game"
    
    ratingstable.to_csv("./ILratingstable.csv", index=False)
    print("Success! Files saved.")

    
    ratingstable = ratingstable[['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
       'startDate', 'Days Since Start', 'lastdatetoclaim', 'topprizeodds', 'overallodds','Current Odds of Top Prize',
       'Change in Current Odds of Top Prize', 'Current Odds of Any Prize',
       'Change in Current Odds of Any Prize', 'Odds of Profit Prize','Change in Odds of Profit Prize',
       'Probability of Winning Any Prize','Change in Probability of Any Prize',
       'Probability of Winning Profit Prize','Change in Probability of Profit Prize',
       'StdDev of All Prizes','StdDev of Profit Prizes', 'Odds of Any Prize + 3 StdDevs',
       'Odds of Profit Prize + 3 StdDevs', 'Max Tickets to Buy',
       'Expected Value of Any Prize (as % of cost)',
       'Change in Expected Value of Any Prize',
       'Expected Value of Profit Prize (as % of cost)',
       'Change in Expected Value of Profit Prize',
       'Percent of Prizes Remaining', 'Percent of Profit Prizes Remaining',
       'Ratio of Decline in Prizes to Decline in Losing Ticket',
       'Rank by Best Probability of Winning Any Prize',
       'Rank by Best Probability of Winning Profit Prize',
       'Rank by Least Expected Losses', 'Rank by Most Available Prizes',
       'Rank by Best Change in Probabilities', 'Rank Average', 'Overall Rank','Rank by Cost',
       'Photo','FAQ', 'About', 'Directory', 
       'Data Date','Stats Page', 'gameURL']]
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
    print(ratingstable)
    
    return ratingstable, scratchertables

#exportILScratcherRecs()