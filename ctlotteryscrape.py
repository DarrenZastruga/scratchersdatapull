#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 15:57:30 2026

@author: michaeljames
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 15:15:48 2026

@author: michaeljames
"""

import pandas as pd
import time
from datetime import date
import re
import io
import numpy as np
import gc
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs

def exportScratcherRecs():
    print("Initializing CT Scraper...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("permissions.default.image", 2)
    
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)

    try:
        # 1. HARVEST THE OFFICIAL 2ND CHANCE LIST FIRST
        print("Fetching official 2nd Chance list...")
        driver.get("https://www.ctlottery.org/ScratchGames/2ndChanceGames")
        time.sleep(5)
        sc_soup = BeautifulSoup(driver.page_source, 'html.parser')
        # Extract all game numbers from the 2nd chance page
        second_chance_ids = set(re.findall(r'/ScratchGames/(\d{3,4})', str(sc_soup)))
        print(f"✅ Identified {len(second_chance_ids)} games with 2nd Chance drawings.")

        # 2. HARVEST MASTER LIST
        url_list = "https://ctlottery.org/ScratchGamesTable"
        driver.get(url_list)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        df_master = pd.read_html(io.StringIO(str(soup.find('table'))))[0]
        df_master.columns = [str(c).strip().lower() for c in df_master.columns]
        
        # Identify columns dynamically from the master list
        col_id = next((c for c in df_master.columns if '#' in c or 'game' in c), df_master.columns[0])
        col_name = next((c for c in df_master.columns if 'name' in c), df_master.columns[1])
        col_price = next((c for c in df_master.columns if 'price' in c), df_master.columns[2])
        
        print(f"Found {len(df_master)} games. Starting detail crawl...")

        all_game_rows = []
        all_prize_tables = []

        for index, row in df_master.iterrows():
            gameNumber = re.sub(r'[^\d]', '', str(row[col_id]))
            if not gameNumber: continue
            
            detail_url = f"https://www.ctlottery.org/ScratchGames/{gameNumber}/"
            driver.get(detail_url)
            
            try:
                WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                continue
            
            # Extract the fully rendered text from the detail page
            detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
            page_text = detail_soup.get_text(" ", strip=True)

            # --- FIXED START DATE EXTRACTION ---
            start_date = ""
            
            # Method 1: Target the description list structure (2026 Standard)
            # Find the label 'dt' and get the following 'dd' value
            start_label = detail_soup.find(lambda tag: tag.name in ['dt', 'td', 'div', 'span'] 
                                         and "Game Start" in tag.text)
            if start_label:
                # Get the next sibling that contains the actual date text
                date_element = start_label.find_next()
                if date_element:
                    start_date_raw = date_element.get_text(strip=True)
                    # Use regex to clean the date (e.g., '01/20/26')
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', start_date_raw)
                    if date_match:
                        start_date = date_match.group(1)

            # Method 2: Fallback to broader text-neighbor sniffing
            if not start_date:
                # Search for 'Game Start' and look at the immediate text following it
                pattern = re.compile(r'Game\s*Start\s*:?\s*', re.IGNORECASE)
                target = detail_soup.find(string=pattern)
                if target:
                    # Look at the parent's next sibling or the text directly after the label
                    sibling_text = target.parent.get_text(" ", strip=True)
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})', sibling_text)
                    if date_match:
                        start_date = date_match.group(1)
            
            # --- FIXED 2ND CHANCE LOGIC (TEXT-BASED) ---
            # Per your suggestion: look for the specific app download text
            second_chance_text = "Download the official CT Lottery 2nd Chance App"
            is_second_chance = "Yes" if second_chance_text in page_text else "No"
            
           # --- FIXED CT PHOTO EXTRACTION ---
            game_photo = ""
            
            # 1. Target the specific ID selector you identified
            img_tag = detail_soup.find('img', id='rollover_image')
            
            if img_tag:
                # Get the relative path from the 'src' attribute
                raw_url = img_tag.get('src')
                if raw_url:
                    # Clean any trailing spaces or query strings
                    raw_url = raw_url.strip().split('?')[0]
                    # Convert to an absolute URL
                    game_photo = urljoin("https://www.ctlottery.org", raw_url)
            
            # 2. Fallback: Search by Game Number if ID is missing or empty
            if not game_photo:
                pattern_match = detail_soup.find('img', src=re.compile(rf'{gameNumber}', re.I))
                if pattern_match:
                    game_photo = urljoin("https://www.ctlottery.org", pattern_match.get('src'))

            print(f"  > Final Photo URL for #{gameNumber}: {game_photo}")
            
            overall_odds = 0
            odds_match = re.search(r'Overall Odds\s*:?\s*(?:1\s*(?::|in)\s*)?([\d\.]+)', page_text, re.IGNORECASE)
            if odds_match: overall_odds = float(odds_match.group(1))

            # Table Parsing
            tixdata = pd.DataFrame()
            tables = detail_soup.find_all('table')
            for tbl in tables:
                temp_df = pd.read_html(io.StringIO(str(tbl)))[0]
                temp_df.columns = [str(c).lower().strip() for c in temp_df.columns]
                if 'unclaimed' in "".join(temp_df.columns):
                    tixdata = temp_df.copy()
                    break

            if not tixdata.empty:
                cols_map = {'prize': 'prizeamount', 'total': 'Winning Tickets At Start', 'unclaimed': 'Winning Tickets Unclaimed'}
                tixdata.rename(columns={c: v for c in tixdata.columns for k, v in cols_map.items() if k in c}, inplace=True)

                # Clean numeric values
                def clean_val(x):
                    return float(re.sub(r'[^\d\.]', '', str(x))) if re.search(r'\d', str(x)) else 0
                
                tixdata['prizeamount'] = tixdata['prizeamount'].apply(clean_val)
                tixdata['Winning Tickets Unclaimed'] = tixdata['Winning Tickets Unclaimed'].apply(clean_val)
                tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets At Start'].apply(clean_val)
                
                tixdata['gameNumber'] = gameNumber
                tixdata['gameName'] = str(row[col_name])
                tixdata['dateexported'] = date.today()
                all_prize_tables.append(tixdata)

                # Summary Metadata for Rankings
                top_row = tixdata.sort_values('prizeamount', ascending=False).iloc[0]
                all_game_rows.append({
                    'price': float(re.sub(r'[^\d\.]', '', str(row[col_price]))) if re.search(r'\d', str(row[col_price])) else 0,
                    'gameName': str(row[col_name]),
                    'gameNumber': gameNumber,
                    'topprize': top_row['prizeamount'],
                    'overallodds': overall_odds,
                    'topprizestarting': top_row['Winning Tickets At Start'],
                    'topprizeremain': top_row['Winning Tickets Unclaimed'],
                    'topprizeavail': np.nan if top_row['Winning Tickets Unclaimed'] > 0 else "Top Prize Claimed",
                    'extrachances': None,
                    'secondChance': is_second_chance,
                    'startDate': start_date,
                    'endDate': "",
                    'lastdatetoclaim': "",
                    'dateexported': date.today(),
                    'gameURL': detail_url,
                    'gamePhoto': game_photo 
                })
                print(f"  > Processed #{gameNumber} with Photo {game_photo}")

        # --- DATA PROCESSING & STATS CONSOLIDATION ---
        if all_game_rows:
            # FIX: Convert the list to a DataFrame before using .loc
            scratchersall = pd.DataFrame(all_game_rows)
            tixtables = pd.concat(all_prize_tables, ignore_index=True)
    
        scratchersall.to_csv("./CTscratcherslist.csv", encoding='utf-8')
        # Create scratcherstables df, with calculations of total tix and total tix without prizes
        scratchertables = tixtables.dropna(subset=['prizeamount']).copy()
        scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(int)
        
        scratchertables.to_csv("./CTscratchertables.csv", encoding='utf-8')
        scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                              != "Coming Soon!", :]
        scratchertables = scratchertables.astype(
            {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
        # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
        gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
        ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
        gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                          'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])
        
        #convert columns to numeric
        for col in ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
            if col in gamesgrouped.columns:
                gamesgrouped[col] = gamesgrouped[col].astype(object)
                gamesgrouped[col] = pd.to_numeric(gamesgrouped[col], errors='coerce')
        
        gamesgrouped.loc[:, 'Total at start'] = gamesgrouped['Winning Tickets At Start'] * \
            gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:, 'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * \
            gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:, 'Non-prize at start'] = gamesgrouped['Total at start'] - \
            gamesgrouped['Winning Tickets At Start']
        gamesgrouped.loc[:, 'Non-prize remaining'] = gamesgrouped['Total remaining'] - \
            gamesgrouped['Winning Tickets Unclaimed']
        try:
            gamesgrouped['topprizeodds'] = gamesgrouped['Total remaining'] / gamesgrouped['topprizeremain'].astype('float')
        except ZeroDivisionError:
            gamesgrouped['topprizeodds'] = 0
        gamesgrouped.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        
        # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
        nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                    'Non-prize at start', 'Non-prize remaining', 'dateexported']].copy()
        nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                           'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
        nonprizetix.loc[:, 'prizeamount'] = 0
        
        totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                               'Total at start', 'Total remaining', 'dateexported']].copy()
        totals.rename(columns={'Total at start': 'Winning Tickets At Start',
                      'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
        totals.loc[:, 'prizeamount'] = "Total"
        
        # loop through each scratcher game id number and add columns for each statistical calculation
        alltables = pd.DataFrame()
        currentodds = pd.DataFrame()
        for gameid in gamesgrouped['gameNumber']:
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:].copy()
            #cast all columns to Object to start to avoid dtype errors when converting to numeric later
            for col in gamerow.columns:
                gamerow[col] = gamerow[col].astype(object)
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
            gamerow.loc[:,'Change in Odds of Profit Prize'] =  (gamerow.loc[:,'Odds of Profit Prize'] - startingprofitodds)/ startingprofitodds if startingprofitodds > 0 else np.nan
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
            gamerow.loc[:,'Odds of Any Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Current Odds of Any Prize']+(totalremain.loc[:,'Winning Tickets Unclaimed'].std()*3))
            gamerow.loc[:,'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Odds of Profit Prize']+(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std()*3))
            gamerow.loc[:,'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std())
            
            
            #calculate expected value

            totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)

            totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)

            totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
            totalremain = totalremain[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
            
            gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
            gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
            gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
            gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
            gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
            gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
            non_prize_start = float(gamerow['Non-prize at start'].iloc[0])
            non_prize_remain = float(gamerow['Non-prize remaining'].iloc[0])
            
            if non_prize_start != 0:
                chngLosingTix = (non_prize_remain - non_prize_start) / non_prize_start
            else:
                chngLosingTix = 0
            
            # Apply the same logic to chngAvailPrizes
            if startingtotal != 0:
                chngAvailPrizes = (tixtotal - startingtotal) / startingtotal
            else:
                chngAvailPrizes = 0
            
            # Assign back to the gamerow DataFrame
            gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = (
                chngLosingTix / chngAvailPrizes if chngAvailPrizes != 0 else 0
            )
            chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
            if chngAvailPrizes != 0:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
            else:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = np.nan
                            
            gamerow.loc[:,'Photo'] = scratchersall.loc[scratchersall['gameNumber'] == gameid,'gamePhoto']
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']

            currentodds = pd.concat([currentodds, gamerow], ignore_index=True)


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
            
            totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)

            alltables = pd.concat([alltables, totalremain], axis=0)

        scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]

        
        #save scratchers tables
        #scratchertables.to_sql('COscratcherstables', engine, if_exists='replace')
        scratchertables.to_csv("./CTscratchertables.csv", encoding='utf-8')
        
        #create rankings table by merging the list with the tables

        scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
        ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
        ratingstable.drop(labels=['gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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

        ratingstable['Stats Page'] = "/connecticut-statistics-for-each-scratcher-game"

        ratingstable.to_csv("./CTratingstable.csv", encoding='utf-8')

        
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
           'Data Date','Stats Page','gameURL']]
        ratingstable = ratingstable.replace([np.inf, -np.inf], 0).infer_objects(copy=False)
        ratingstable = ratingstable.astype(object).fillna('').infer_objects(copy=False)

        print("✅ Success! Files saved for CT.")
        return ratingstable, scratchertables

    finally:
        driver.quit()

exportScratcherRecs()
