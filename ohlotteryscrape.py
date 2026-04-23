#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updated OH Scraper (Selenium Version)
- Fixed ZeroDivisionError in statistical calculations.
- Adds robust checks for missing odds or empty ticket counts.
"""

import pandas as pd
import time
from datetime import datetime, date
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
from urllib.parse import urljoin

def setup_driver():
    """Initializes a Firefox instance optimized for high-volume scraping."""
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    # 'none' strategy allows us to proceed as soon as HTML is available
    firefox_options.page_load_strategy = 'none' 
    
    # Enhanced bot detection avoidance
    firefox_options.set_preference("dom.webdriver.enabled", False)
    firefox_options.set_preference("useAutomationExtension", False)
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=firefox_options)
    driver.set_page_load_timeout(35) # Increased for stability
    return driver

def exportScratcherRecs():
    print("Initializing Ohio Scraper (Circulation Filter Active)...")
    driver = setup_driver()
    today_dt = datetime.now()
    
    try:
        base_url = "https://www.ohiolottery.com"
        category_urls = [f"{base_url}/Games/Scratch-Offs/${p}-Games" for p in [1, 2, 5, 10, 20, 30, 50]]

        tixtables = pd.DataFrame()
        scratchersall_list = []
        unique_game_links = {}

        # --- 1. FIND GAMES (Deduplicated & Grid-Focused) ---
        print("Scanning active categories...")
        unique_game_links = {} 

        for list_url in category_urls:
            try:
                driver.get(list_url)
                # Scroll to ensure the dynamic grid renders
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3) 
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Target ONLY the links inside the scratch-off grid container
                # This prevents picking up nav menus, footers, or sidebars
                grid = soup.select_one('.scratch-off-grid, .game-list-container, #main-content')
                if not grid: grid = soup # Fallback
                
                links = grid.find_all('a', href=re.compile(r'/Games/Scratch-Offs/\$[0-9]+-Games/(?!.*prizes-remaining|.*ended-games)', re.IGNORECASE))

                for link in links:
                    href = link.get('href')
                    if not href: continue
                    full_url = urljoin(base_url, href).split('?')[0].rstrip('/')
                    
                    # Ensure it's a detail page and not a category landing page
                    if full_url.lower().endswith('-games'): continue
                    
                    # Deduplicate using the URL as the key
                    if full_url not in unique_game_links:
                        unique_game_links[full_url] = link.get_text(strip=True)
            except: continue

        print(f"Found {len(unique_game_links)} unique active games. Starting crawl...")

        # --- 2. SCRAPE DETAILS (With Data Recovery) ---
        game_count = 0
        for detail_url, link_text in unique_game_links.items():
            game_count += 1
            if game_count % 25 == 0:
                driver.quit()
                driver = setup_driver()

            try:
                driver.get(detail_url)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
                # Stop loading ads/trackers once H1 is in
                driver.execute_script("window.stop();") 
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Get Game Name from H1
                game_name = soup.find('h1').get_text(strip=True) if soup.find('h1') else "Unknown"
                print(f"  [{game_count}/{len(unique_game_links)}] Scraping: {game_name}")

                # --- TABLE RECOVERY LOGIC ---
                prize_rows = []
                # Ohio often uses specific IDs for prize tables
                tables = soup.find_all('table')
                for tbl in tables:
                    table_text = tbl.get_text().lower()
                    # Only grab tables that actually look like prize structures
                    if 'prize' in table_text and ('remaining' in table_text or 'odds' in table_text):
                        try:
                            # Use 'lxml' if installed for better table parsing
                            df = pd.read_html(io.StringIO(str(tbl)))[0]
                            # Clean column names for matching
                            df.columns = [str(c).lower().strip() for c in df.columns]
                            
                            cols_map = {}
                            for col in df.columns:
                                if 'prize' in col: cols_map[col] = 'prizeamount'
                                elif 'remaining' in col: cols_map[col] = 'Winning Tickets Unclaimed'
                                elif 'total' in col or 'start' in col: cols_map[col] = 'Winning Tickets At Start'
                            
                            if 'prizeamount' in cols_map.values():
                                df.rename(columns=cols_map, inplace=True)
                                # Filter to only the columns we need
                                valid_cols = [c for c in ['prizeamount', 'Winning Tickets Unclaimed', 'Winning Tickets At Start'] if c in df.columns]
                                prize_rows.extend(df[valid_cols].to_dict('records'))
                        except: continue
                
                if not prize_rows:
                    print(f"    ! No prize table found for {game_name}")
                    continue

                # --- PROCESS DATA ---
                prize_df = pd.DataFrame(prize_rows)
                # (Keep your existing numeric cleaning and export logic here)
                # ... (Existing numeric cleanup code from previous version) ...
                prize_df['prizeamount'] = pd.to_numeric(prize_df['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce').fillna(0)
                prize_df['Winning Tickets Unclaimed'] = pd.to_numeric(prize_df['Winning Tickets Unclaimed'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                if 'Winning Tickets At Start' not in prize_df.columns:
                    prize_df['Winning Tickets At Start'] = prize_df['Winning Tickets Unclaimed']
                else:
                    prize_df['Winning Tickets At Start'] = pd.to_numeric(prize_df['Winning Tickets At Start'].astype(str).str.replace(',', ''), errors='coerce').fillna(prize_df['Winning Tickets Unclaimed'])

                prize_df['gameNumber'] = "0" # Default
                prize_df['gameName'] = game_name
                prize_df['dateexported'] = date.today()
                tixtables = pd.concat([tixtables, prize_df], ignore_index=True)

                scratchersall_list.append({
                    'gameName': game_name, 'gameURL': detail_url, 'gamePhoto': gamePhoto,
                    'topprize': prize_df['prizeamount'].max(), 'dateexported': date.today()
                })

            except Exception as e:
                print(f"  ! Skip: {detail_url} (Timeout or page error)")
                continue

    finally:
        driver.quit()

    if scratchersall_list:
        print(f"Collected data for {len(scratchersall_list)} games.")
        
        scratchersall = pd.DataFrame(scratchersall_list)
        scratchersall.to_csv("./OHscratcherslist.csv", index=False)
        tixtables.to_csv("./OHscratchertables.csv", index=False)
    
        scratchersall = scratchersall.drop_duplicates()
        
        #Create scratcherstables df, with calculations of total tix and total tix without prizes
        scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        scratchertables.to_csv("./NYscratchertables.csv", encoding='utf-8')
        
        scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
        scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
        #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
        # Select columns first, then groupby and aggregate
        cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
        gamesgrouped = scratchertables.groupby(
            by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
        gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds', 'topprizeodds']], how='left', on=['gameNumber'])
        #convert columns to numeric
        for col in ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
            if col in gamesgrouped.columns:
                gamesgrouped[col] = gamesgrouped[col].astype(object)
                gamesgrouped[col] = pd.to_numeric(gamesgrouped[col], errors='coerce')
        
        gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start'].astype(float)*gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
        gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
        odds = gamesgrouped['Total remaining'] / gamesgrouped['topprizeremain'].astype('float')
        odds = odds.replace([np.inf, -np.inf], np.nan)
        gamesgrouped['topprizeodds'] = odds
        gamesgrouped.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        
        
        #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
        nonprizetix = gamesgrouped[['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']].copy()
        nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
        nonprizetix.loc[:,'prizeamount'] = 0
        totals = gamesgrouped[['gameNumber','gameName','Total at start','Total remaining','dateexported']].copy()
        totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
        totals.loc[:,'prizeamount'] = "Total"
      
        #loop through each scratcher game id number and add columns for each statistical calculation
        alltables = pd.DataFrame() 
        currentodds = pd.DataFrame()
        for gameid in gamesgrouped['gameNumber']:
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:].copy()

            startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
            tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
            totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
            totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
            price = int(gamerow['price'].values[0])

            prizes = totalremain.loc[:,'prizeamount']


            #add various columns for the scratcher stats that go into the ratings table
            gamerow.loc[:,'Current Odds of Top Prize'] = gamerow.loc[:,'topprizeodds']
            gamerow.loc[:,'Change in Current Odds of Top Prize'] =  (gamerow.loc[:,'Current Odds of Top Prize'] - float(gamerow['topprizeodds'].values[0]))/ float(gamerow['topprizeodds'].values[0])      
            gamerow.loc[:,'Current Odds of Any Prize'] = tixtotal/sum(totalremain.loc[:,'Winning Tickets Unclaimed'])
            gamerow.loc[:,'Change in Current Odds of Any Prize'] =  (gamerow.loc[:,'Current Odds of Any Prize'] - float(gamerow['overallodds'].values[0]))/ float(gamerow['overallodds'].values[0])
            denomUnclaimed = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'])
            gamerow.loc[:,'Odds of Profit Prize'] = tixtotal/denomUnclaimed if denomUnclaimed > 0 else np.nan
            denomStart = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets At Start'])
            startingprofitodds = startingtotal/denomStart if denomStart > 0 else np.nan
            gamerow.loc[:,'Starting Odds of Profit Prize'] = startingprofitodds
            gamerow.loc[:,'Change in Odds of Profit Prize'] =  (gamerow.loc[:,'Odds of Profit Prize'] - startingprofitodds)/startingprofitodds if startingprofitodds > 0 else np.nan
            gamerow.loc[:,'Probability of Winning Any Prize'] = sum(totalremain.loc[:,'Winning Tickets Unclaimed'])/tixtotal if tixtotal > 0 else np.nan
            startprobanyprize = sum(totalremain.loc[:,'Winning Tickets At Start'])/startingtotal if startingtotal > 0 else np.nan
            gamerow.loc[:,'Starting Probability of Winning Any Prize'] = startprobanyprize
            gamerow.loc[:,'Change in Probability of Any Prize'] =  startprobanyprize - gamerow.loc[:,'Probability of Winning Any Prize']  
            gamerow.loc[:,'Probability of Winning Profit Prize'] = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'])/tixtotal if tixtotal > 0 else np.nan
            startprobprofitprize = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets At Start'])/startingtotal if startingtotal > 0 else np.nan
            gamerow.loc[:,'Starting Probability of Winning Profit Prize'] = startprobprofitprize
            gamerow.loc[:,'Change in Probability of Profit Prize'] =  startprobprofitprize - gamerow.loc[:,'Probability of Winning Profit Prize']
            gamerow.loc[:,'StdDev of All Prizes'] = totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()/tixtotal if tixtotal > 0 else np.nan
            gamerow.loc[:,'StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean()/tixtotal if tixtotal > 0 else np.nan
            gamerow.loc[:,'Odds of Any Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Current Odds of Any Prize']+(totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()*3))
            gamerow.loc[:,'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Odds of Profit Prize']+(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean()*3))
            gamerow.loc[:,'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean())
            
            
            #calculate expected value

            totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)

            totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(
                lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal) if startingtotal != 0 else 0,
                axis=1)

            if tixtotal > 0:
                totalremain.loc[:,'Expected Value'] = totalremain.apply(
                    lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
            else:
                totalremain.loc[:,'Expected Value'] = np.nan
            totalremain = totalremain[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
            
            gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
            starting_ev_sum = sum(totalremain['Starting Expected Value'])
            if starting_ev_sum != 0:
                gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value']) - starting_ev_sum) / starting_ev_sum) / price if price > 0 else ((sum(totalremain['Expected Value']) - starting_ev_sum) / starting_ev_sum)
            else:
                gamerow.loc[:,'Change in Expected Value of Any Prize'] = 0
            #gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
            gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
            starting_ev_sum = sum(totalremain.loc[totalremain['prizeamount'] > price, 'Starting Expected Value'])
            if starting_ev_sum != 0 and price > 0:
                gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value']) - starting_ev_sum) / starting_ev_sum) / price
            else:
                gamerow.loc[:,'Change in Expected Value of Profit Prize'] = np.nan
            #gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
            gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
            gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
            chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
            chngAvailPrizes = (tixtotal-startingtotal)/startingtotal if startingtotal != 0 else 0
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0 if chngAvailPrizes == 0 else chngLosingTix/chngAvailPrizes
                    
            gamerow.loc[:,'Photo'] = scratchersall.loc[scratchersall['gameNumber']==gameid,'gamePhoto'].values[0]
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']

            currentodds = pd.concat([currentodds, gamerow], axis=0, ignore_index=True)

            #add non-prize and totals rows with matching columns
            totalremain.loc[:,'Total remaining'] = tixtotal
            totalremain.loc[:,'Prize Probability'] = totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Total remaining']
            totalremain.loc[:,'Percent Tix Remaining'] = totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']
            nonprizetix.loc[:,'Prize Probability'] = nonprizetix.apply(lambda row: (row['Winning Tickets Unclaimed']/tixtotal) if (row['gameNumber']==gameid) & (row['Winning Tickets Unclaimed']>0) else 0,axis=1)
            nonprizetix.loc[:,'Percent Tix Remaining'] =  nonprizetix.loc[nonprizetix['gameNumber']==gameid,'Winning Tickets Unclaimed']/nonprizetix.loc[nonprizetix['gameNumber']==gameid,'Winning Tickets At Start']
            nonprizetix.loc[:,'Starting Expected Value'] = (nonprizetix['prizeamount']-price)*(nonprizetix['Winning Tickets At Start']/startingtotal)
            nonprizetix.loc[:,'Expected Value'] =  (nonprizetix['prizeamount']-price)*(nonprizetix['Winning Tickets Unclaimed']/tixtotal)
            totals.loc[:,'Prize Probability'] = totals.loc[totals['gameNumber']==gameid,'Winning Tickets Unclaimed']/tixtotal
            totals.loc[:,'Percent Tix Remaining'] =  totals.loc[totals['gameNumber']==gameid,'Winning Tickets Unclaimed']/totals.loc[totals['gameNumber']==gameid,'Winning Tickets At Start']
            totals.loc[:,'Starting Expected Value'] = ''
            totals.loc[:,'Expected Value'] = ''
            totalremain = totalremain[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
            totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                             'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
            totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                             'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)

            #add expected values for final totals row
            allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
            
            # Guard against zero-division (happens when scrape returns no starting tickets)
            if not startingtotal or startingtotal == 0:
                print(f"⚠️ OH: startingtotal is 0; skipping Starting Expected Value calc.")
                totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = 0
            else:
                # Also coerce prizeamount in case any value is still a string
                allexcepttotal = allexcepttotal.copy()
                allexcepttotal['prizeamount'] = pd.to_numeric(
                    allexcepttotal['prizeamount'].astype(str).str.replace(r'[\$,]', '', regex=True),
                    errors='coerce'
                )
                totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = (
                    allexcepttotal.apply(
                        lambda row: (row['prizeamount'] - price) * (row['Winning Tickets At Start'] / startingtotal),
                        axis=1,
                    )
                )
            # Coerce numeric columns first
            allexcepttotal = allexcepttotal.copy()
            for _col in ['prizeamount', 'Winning Tickets Unclaimed']:
                if _col in allexcepttotal.columns:
                    allexcepttotal[_col] = pd.to_numeric(
                        allexcepttotal[_col].astype(str).str.replace(r'[\$,]', '', regex=True),
                        errors='coerce'
                    )
            
            # Recompute tixtotal from cleaned column
            tixtotal = allexcepttotal['Winning Tickets Unclaimed'].sum()
            
            if not tixtotal or tixtotal == 0:
                print(f"⚠️ OH: tixtotal is 0; skipping Expected Value calc.")
                totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = 0
            else:
                totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = (
                    allexcepttotal.apply(
                        lambda row: (row['prizeamount'] - price) * (row['Winning Tickets Unclaimed'] / tixtotal),
                        axis=1,
                    )
                )

            alltables = pd.concat([alltables, totalremain], axis=0)

        scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]

        #save scratchers tables
        scratchertables.to_csv("./OHscratchertables.csv", encoding='utf-8')
        
        #create rankings table by merging the list with the tables

        scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
        ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
        ratingstable.drop(labels=['gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x'], axis=1, inplace=True)
        ratingstable.rename(columns={'gameName_y':'gameName','dateexported_x':'dateexported','topprizeodds_x':'topprizeodds','overallodds_x':'overallodds','topprizestarting_y':'topprizestarting', 'topprizeremain_y':'topprizeremain'}, inplace=True)
        #add number of days since the game start date as of date exported
        ratingstable.loc[:,'Days Since Start'] = (pd.to_datetime(ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors = 'coerce')).dt.days
        
        #add rankings columns of all scratchers to ratings table
        ratingstable['Rank by Best Probability of Winning Any Prize'] = (ratingstable['Current Odds of Any Prize'].rank()+ratingstable['Probability of Winning Any Prize'].rank()+ratingstable['Odds of Any Prize + 3 StdDevs'].rank())/3
        ratingstable['Rank by Best Probability of Winning Profit Prize'] = (ratingstable['Odds of Profit Prize'].rank()+ratingstable['Probability of Winning Profit Prize'].rank()+ratingstable['Odds of Profit Prize + 3 StdDevs'].rank())/3
        ratingstable['Rank by Least Expected Losses'] = (ratingstable['Expected Value of Any Prize (as % of cost)'].rank()+ratingstable['Expected Value of Profit Prize (as % of cost)'].rank())/2
        ratingstable['Rank by Most Available Prizes'] = (ratingstable['Percent of Prizes Remaining'].rank()+ratingstable['Percent of Profit Prizes Remaining'].rank()+ratingstable['Ratio of Decline in Prizes to Decline in Losing Ticket'].rank())/3
        ratingstable['Rank by Best Change in Probabilities'] = (ratingstable['Change in Current Odds of Any Prize'].rank()+ratingstable['Change in Current Odds of Top Prize'].rank()
                                                                 +ratingstable['Change in Probability of Any Prize'].rank()+ratingstable['Change in Probability of Profit Prize'].rank()
                                                                 +ratingstable['Expected Value of Any Prize (as % of cost)'].rank()+ratingstable['Expected Value of Profit Prize (as % of cost)'].rank())/6
        ratingstable.loc[:,'Rank Average'] = ratingstable.loc[:, 'Rank by Best Probability of Winning Any Prize':'Rank by Best Change in Probabilities'].mean(axis=1)
        ratingstable.loc[:,'Overall Rank'] = ratingstable.loc[:, 'Rank Average'].rank()
        ratingstable.loc[:,'Rank by Cost'] = ratingstable.groupby('price')['Overall Rank'].rank('dense', ascending=True)
        
        #columns in ratingstable to round to only two decimals
        twodecimalcols = ['Current Odds of Any Prize', 'Odds of Profit Prize', 'Percent of Prizes Remaining', 'Expected Value of Any Prize (as % of cost)']
        ratingstable[twodecimalcols] = ratingstable[twodecimalcols].round(2)
        ratingstable['Max Tickets to Buy'] = ratingstable['Max Tickets to Buy'].round(0)
        
        #save ratingstable
        ratingstable['Stats Page'] = "/ohio-statistics-for-each-scratcher-game"
        ratingstable.to_csv("./OHratingstable.csv", encoding='utf-8')
        
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
        ratingstable = ratingstable.replace([np.inf, -np.inf], 0).infer_objects(copy=False)
        ratingstable = ratingstable.astype(object).fillna('').infer_objects(copy=False)
        
        return ratingstable, scratchertables
    else:
        print("No data collected.")
        return None, None

exportScratcherRecs()