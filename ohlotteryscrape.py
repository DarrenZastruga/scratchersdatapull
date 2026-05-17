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
import os
import random

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
    firefox_options = Options()
    # 'eager' means: Stop waiting for images/trackers once DOM is interactive
    firefox_options.page_load_strategy = 'eager' 
    firefox_options.add_argument("--window-size=1920,1080")
    
    # Standard Stealth Settings
    firefox_options.set_preference("dom.webdriver.enabled", False)
    firefox_options.set_preference("useAutomationExtension", False)
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    
    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
        driver.set_page_load_timeout(30) # Lower this; we don't want to wait 60s anymore
        return driver
    except Exception as e:
        print(f"⚠️ Driver Startup Error: {e}")
        return None

def exportScratcherRecs():
    print("Initializing Ohio Scraper (Circulation Filter Active)...")
    driver = setup_driver()
    if not driver: driver = setup_driver()
    
    today_dt = datetime.now()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        base_url = "https://www.ohiolottery.com"
        category_urls = [f"{base_url}/Games/Scratch-Offs/${p}-Games" for p in [1, 2, 5, 10, 20, 30, 50]]

        tixtables = pd.DataFrame()
        scratchersall_list = []
        unique_game_links = {}

        # --- 1. FIND GAMES ---
        print("Scanning active categories...")
        for list_url in category_urls:
            try:
                driver.get(list_url)
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/Games/Scratch-Offs/']")))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3, 5))
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                links = soup.find_all('a', href=re.compile(r'/Games/Scratch-Offs/\$[0-9]+-Games/', re.IGNORECASE))

                for link in links:
                    href = link.get('href', '').split('?')[0].rstrip('/')
                    if not href or href.lower().endswith('-games') or 'prizes-remaining' in href.lower(): continue
                    full_url = urljoin(base_url, href)
                    if full_url not in unique_game_links:
                        unique_game_links[full_url] = link.get_text(strip=True)
            except: continue

        print(f"Found {len(unique_game_links)} potential games. Checking details...")

        # --- 2. SCRAPE DETAILS ---
        game_count = 0
        for detail_url, link_text in unique_game_links.items():
            game_count += 1
            
            if game_count % 25 == 0:
                print("Refreshing session to clear memory...")
                if driver: driver.quit()
                driver = setup_driver()

            try:
                # Set a local timeout for this specific page load
                driver.set_page_load_timeout(25)
                
                try:
                    driver.get(detail_url)
                except Exception:
                    # If it times out, force a stop and continue anyway
                    driver.execute_script("window.stop();")
                    print(f"    [Diagnostic] Page load timed out, but forcing stop to try and scrape.")

                # --- Continue with Diagnostics and Shadow Piercing ---
                time.sleep(3) 
                print(f"\n--- Diagnostic: {detail_url} ---")
                
                # Check for ANY shadow hosts
                shadow_hosts = driver.execute_script("""
                    return Array.from(document.querySelectorAll('*'))
                                .filter(el => el.shadowRoot)
                                .map(el => ({ tag: el.tagName, class: el.className }));
                """)
                
                # --- B. TRIGGER RENDERING ---
                driver.execute_script("window.scrollTo(0, 500);")
                time.sleep(1)
                driver.execute_script("window.scrollTo(0, 0);")
                driver.execute_script("document.body.click();")
                
                # --- HIGH-PRECISION SHADOW PIERCER ---
                print(f"    [Scraper] Piercing Shadow DOM ...")
                
                # We use a JavaScript 'Promise' to poll the internal Shadow Root
                # This stays active until the table is actually filled with data
                table_html = driver.execute_script("""
                    return new Promise((resolve) => {
                        let attempts = 0;
                        const interval = setInterval(() => {
                            // Target the specific Vue component identified in your debug files
                            const host = document.querySelector('ol-game-detail-scratch-off');
                            const table = host?.shadowRoot?.querySelector('table');
                            
                            // Only resolve once the table has actual prize rows
                            if (table && table.querySelectorAll('tr').length > 2) {
                                clearInterval(interval);
                                resolve(table.outerHTML);
                            }
                            
                            if (attempts > 40) { // Timeout after 20 seconds
                                clearInterval(interval);
                                resolve(null);
                            }
                            attempts++;
                        }, 500);
                    });
                """)

                if not table_html:
                    # Capture what the Shadow Host actually contains
                    shadow_content = driver.execute_script("return document.querySelector('ol-game-detail-scratch-off')?.shadowRoot?.innerHTML;")
                    
                    debug_path = os.path.join(script_dir, f"shadow_debug_{game_count}.html")
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(shadow_content if shadow_content else "Shadow Root was empty or missing.")
                    
                    print(f"    ! Shadow Snapshot saved to: {debug_path}")
                    continue

                print(f"    [Diagnostic] SUCCESS: Captured table data.")

                # --- E. DATA PROCESSING ---
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                game_name = soup.find('h1').get_text(strip=True) if soup.find('h1') else link_text
                og_tag = soup.find('meta', property='og:image')
                gamePhoto = og_tag.get('content') if og_tag else None

                print(f"  [{game_count}] Scraped: {game_name}")

                df = pd.read_html(io.StringIO(table_html))[0]
                df.columns = [str(c).lower().strip() for c in df.columns]
                
                rename_map = {c: 'prizeamount' for c in df.columns if 'prize' in c}
                rename_map.update({c: 'Winning Tickets Unclaimed' for c in df.columns if 'remaining' in c})
                rename_map.update({c: 'Winning Tickets At Start' for c in df.columns if 'total' in c or 'start' in c})
                
                if 'prizeamount' in rename_map.values():
                    df.rename(columns=rename_map, inplace=True)
                    df['prizeamount'] = pd.to_numeric(df['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True), errors='coerce').fillna(0)
                    df['Winning Tickets Unclaimed'] = pd.to_numeric(df['Winning Tickets Unclaimed'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                    
                    # Extract Game ID from URL
                    game_id_match = re.search(r'-(\d+)$', detail_url)
                    df['gameNumber'] = game_id_match.group(1) if game_id_match else str(hash(detail_url))[-5:]
                    df['gameName'] = game_name
                    df['dateexported'] = date.today()
                    tixtables = pd.concat([tixtables, df], ignore_index=True)

                    scratchersall_list.append({
                        'gameName': game_name, 'gameURL': detail_url, 'gamePhoto': gamePhoto,
                        'topprize': df['prizeamount'].max(), 'dateexported': date.today(),
                        'gameNumber': df['gameNumber'].iloc[0]
                    })

            except Exception as e:
                print(f"  ! Error: {detail_url} - {e}")
                continue

    finally:
        if driver: driver.quit()

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