#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fixed NH Lottery Scraper
1. FIX: Uses BeautifulSoup to manually parse the 'Odds Breakdown' table, handling the mixed desktop/mobile HTML structure.
2. FIX: Correctly maps 'WIN' (col 1) to Prize and 'PRIZES IN GAME' (col 2) to Unclaimed Count.
3. OUTPUT: Generates populated 'NHscratchertables.csv' and correct 'topprize' data.
"""

import pandas as pd
import time
from datetime import date, datetime
import re
import io
import numpy as np
import gc
import warnings

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup

# Suppress warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

def parse_complex_prize(prize_str):
    """Parses prize strings like '$10,000', '2000/Week', etc."""
    s = str(prize_str).upper().replace(',', '').replace('$', '').strip()
    try:
        if 'WK' in s or 'WEEK' in s:
            amt = float(re.search(r'[\d\.]+', s).group())
            return amt * 52 * 20 
        if 'YR' in s or 'YEAR' in s:
            amt = float(re.search(r'[\d\.]+', s).group())
            return amt * 20
        if 'FREE' in s or 'TICKET' in s: return 0
        if 'ANNUITY' in s: return 0
        clean = re.sub(r'[^\d\.]', '', s)
        return float(clean) if clean else 0
    except:
        return 0

def exportScratcherRecs():
    print("Initializing Firefox...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("permissions.default.image", 2)
    firefox_options.set_preference("plugin.state.flash", 0)
    
    driver = None
    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
    except Exception as e:
        print(f"Error launching Firefox: {e}")
        return None, None

    try:
        url_list = "https://www.nhlottery.com/game-collection/in-store?filters=scratchGame"
        print(f"Fetching game list from {url_list}...")
        driver.get(url_list)
        
        # --- HANDLE "LOAD MORE" ---
        print("  > Checking for 'Load More' buttons...")
        while True:
            try:
                load_btn = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Load More')]"))
                )
                driver.execute_script("arguments[0].click();", load_btn)
                time.sleep(1.5)
            except:
                break

        # Parse List
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        unique_links = {}
        
        links = soup.find_all('a', href=re.compile(r'/(?:scratch|game).*/', re.IGNORECASE))
        for link in links:
            href = link.get('href')
            if href and 'game-collection' not in href and 'filters=' not in href:
                full_url = href if href.startswith('http') else f"https://www.nhlottery.com{href}"
                if full_url not in unique_links:
                    unique_links[full_url] = link.get_text(" ", strip=True)

        print(f"Found {len(unique_links)} potential games. Processing details...")

        all_game_rows = []
        all_prize_tables = []

        for full_url, link_text in unique_links.items():
            try:
                if 'where-to-buy' in full_url or 'about-us' in full_url: continue

                driver.get(full_url)
                
                try:
                    WebDriverWait(driver, 4).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                except: pass

                page_src = driver.page_source
                soup = BeautifulSoup(page_src, 'html.parser')
                full_text = soup.get_text(" ", strip=True)

                # --- 1. GAME NAME ---
                gameName = link_text
                h1 = soup.find('h1')
                if h1: gameName = h1.get_text(strip=True)
                
                # --- 2. GAME NUMBER ---
                gameNumber = "0"
                label_span = soup.find('span', class_='scratch-game-details-page__label', string=re.compile(r'Game Number', re.IGNORECASE))
                if label_span:
                    value_span = label_span.find_next_sibling('span', class_='scratch-game-details-page__value')
                    if value_span: gameNumber = value_span.get_text(strip=True)
                
                if gameNumber == "0":
                    num_match = re.search(r'Game\s*(?:#|No\.?)\s*(\d+)', full_text, re.IGNORECASE)
                    if num_match: gameNumber = num_match.group(1)

                print(f"Processing #{gameNumber}: {gameName}...")

                # --- 3. DATES ---
                startDate = None
                endDate = None
                lastDate = None
                
                # Start Date ("On Sale")
                sale_label = soup.find('span', class_='scratch-game-details-page__label', string=re.compile(r'On Sale', re.IGNORECASE))
                if sale_label:
                    val_span = sale_label.find_next_sibling('span', class_='scratch-game-details-page__value')
                    if val_span:
                        try: startDate = datetime.strptime(val_span.get_text(strip=True), "%m/%d/%Y").strftime("%Y-%m-%d")
                        except: pass
                
                # End Dates
                date_labels = {'Game End': 'endDate', 'Expire': 'lastDate', 'Claim By': 'lastDate'}
                for label, key in date_labels.items():
                    d_match = re.search(rf'{label}:?\s*(\d{{1,2}}/\d{{1,2}}/\d{{4}})', full_text, re.IGNORECASE)
                    if d_match:
                        try:
                            d_str = datetime.strptime(d_match.group(1), "%m/%d/%Y").strftime("%Y-%m-%d")
                            if key == 'endDate': endDate = d_str
                            elif key == 'lastDate': lastDate = d_str
                        except: pass

                # --- 4. PHOTO ---
                gamePhoto = None
                img_div = soup.find('div', class_='scratch-game-details-page__image')
                if img_div:
                    img = img_div.find('img', class_='async-image__img')
                    if img and img.get('src'): gamePhoto = img.get('src')
                
                if not gamePhoto:
                    img = soup.find('img', class_='async-image__img')
                    if img: gamePhoto = img.get('src')

                if gamePhoto:
                    if gamePhoto.startswith('//'): gamePhoto = f"https:{gamePhoto}"
                    elif gamePhoto.startswith('/'): gamePhoto = f"https://www.nhlottery.com{gamePhoto}"

                # --- 5. PRICE ---
                price = 0
                price_tag = soup.find('span', class_=re.compile(r'game-price|scratch-game-details-page__price'))
                if price_tag:
                    price = float(re.sub(r'[^\d\.]', '', price_tag.get_text(strip=True)))
                else:
                    price_match = re.search(r'\$(\d+)\s*Ticket', full_text, re.IGNORECASE)
                    if price_match: price = float(price_match.group(1))

                # --- 6. ODDS ---
                overall_odds = 0
                odds_match = re.search(r'Overall Odds:?\s*1\s*(?:in|[:])\s*([\d\.]+)', full_text, re.IGNORECASE)
                if odds_match: overall_odds = float(odds_match.group(1))

                # --- 7. TABLE PARSING (BS4 Manual) ---
                # We do this manually because NH tables contain desktop AND mobile rows in the same table tag
                topprize = 0
                topprizeremain = 0
                topprizestarting = 0
                
                best_table = pd.DataFrame()
                
                # Find the main table container
                table_container = soup.find('div', class_='table__container')
                if table_container:
                    table = table_container.find('table')
                    if table:
                        # Extract Rows
                        rows = table.find_all('tr')
                        data = []
                        
                        for row in rows:
                            cols = row.find_all('td')
                            # Desktop rows have 4 columns: GET | WIN | PRIZES IN GAME | ODDS
                            if len(cols) == 4:
                                win_col = cols[1].get_text(strip=True)   # WIN (Price)
                                count_col = cols[2].get_text(strip=True) # PRIZES IN GAME
                                
                                # Skip header repetition or Total row if handled elsewhere
                                if "WIN" in win_col or "Total" in win_col:
                                    continue
                                
                                data.append({
                                    'prizeamount': win_col,
                                    'Winning Tickets Unclaimed': count_col
                                })
                        
                        if data:
                            best_table = pd.DataFrame(data)

                if not best_table.empty:
                    # Clean Prize
                    best_table['prizeamount'] = best_table['prizeamount'].apply(parse_complex_prize)
                    
                    # Clean Count
                    best_table['Winning Tickets Unclaimed'] = pd.to_numeric(
                        best_table['Winning Tickets Unclaimed'].astype(str).str.replace(r'[^\d]', '', regex=True),
                        errors='coerce'
                    ).fillna(0)
                    
                    # Logic: Start = Unclaimed (Missing start info)
                    best_table['Winning Tickets At Start'] = best_table['Winning Tickets Unclaimed']

                    best_table['gameNumber'] = gameNumber
                    best_table['gameName'] = gameName
                    best_table['price'] = price
                    best_table['dateexported'] = date.today()
                    
                    # Filter valid rows
                    best_table = best_table[best_table['prizeamount'] > 0]
                    
                    # Add to master list
                    all_prize_tables.append(best_table)
                    
                    # --- TOP PRIZE CALC ---
                    if not best_table.empty:
                        # Sort by prize amount desc to find top prize
                        best_table = best_table.sort_values(by='prizeamount', ascending=False)
                        top_row = best_table.iloc[0]
                        
                        topprize = top_row['prizeamount']
                        topprizeremain = top_row['Winning Tickets Unclaimed']
                        topprizestarting = topprizeremain

                all_game_rows.append({
                    'price': price, 'gameName': gameName, 'gameNumber': gameNumber,
                    'gameURL': full_url, 'gamePhoto': gamePhoto,
                    'topprize': topprize, 'overallodds': overall_odds,
                    'topprizestarting': topprizestarting, 'topprizeremain': topprizeremain,
                    'topprizeavail': "Available" if topprizeremain > 0 else "Claimed",
                    'startDate': startDate, 'endDate': endDate, 'lastdatetoclaim': lastDate,
                    'extrachances': None, 'secondChance': None, 'dateexported': date.today()
                })
                
                gc.collect()

            except Exception as e:
                print(f"  > Error processing {link_text}: {e}")
                continue

    finally:
        driver.quit()

    if not all_game_rows:
        print("No data collected.")
        return None, None

    print("Compiling DataFrames...")
    tixlist = pd.DataFrame(all_game_rows)
    tixlist.to_csv("./NHtixlist.csv", index=False)
    
    if all_prize_tables:
        tixtables = pd.concat(all_prize_tables, ignore_index=True)
        tixtables.to_csv("./NHscratchertables.csv", index=False)
    else:
        tixtables = pd.DataFrame(columns=['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported'])

    scratchersall = tixlist.copy().drop_duplicates()
    scratchersall.to_csv("./NHscratcherslist.csv", index=False)



    # 3. FILTER COLUMNS (Safe)
    cols_to_keep = ['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                    'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL', 'gamePhoto']
    
    for col in cols_to_keep:
        if col not in scratchersall.columns: scratchersall[col] = None
            
    scratchersall = scratchersall[cols_to_keep]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!"]
    scratchersall = scratchersall.drop_duplicates()

    # 4. PREPARE TABLES FOR STATS
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']].copy()
    scratchertables.to_csv("./LAscratchertables.csv", encoding='utf-8', index=False)
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!"]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds', 'gamePhoto']], how='left', on=['gameNumber'])

    gamesgrouped.loc[:, 'Total at start'] = gamesgrouped['Winning Tickets At Start'] * \
        gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * \
        gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Non-prize at start'] = gamesgrouped['Total at start'] - \
        gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:, 'Non-prize remaining'] = gamesgrouped['Total remaining'] - \
        gamesgrouped['Winning Tickets Unclaimed']
    try:
        gamesgrouped['topprizeodds'] = gamesgrouped['Total remaining'] / gamesgrouped['topprizeremain']
    except ZeroDivisionError:
        gamesgrouped['topprizeodds'] = 0
    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

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
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :]
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
            'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
        totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])

        prizes = totalremain.loc[:, 'prizeamount']


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
        
        # calculate expected value

        totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

        totalremain.loc[:, 'Starting Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)

        totalremain.loc[:, 'Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        totalremain = totalremain.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                   'Winning Tickets Unclaimed', 'Starting Expected Value', 'Expected Value', 'dateexported']]

        gamerow.loc[:, 'Expected Value of Any Prize (as % of cost)'] = sum(
            totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:, 'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(
            totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:, 'Expected Value of Profit Prize (as % of cost)'] = sum(
            totalremain.loc[totalremain['prizeamount'] > price, 'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount'] > price, 'Expected Value'])
        gamerow.loc[:, 'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount'] > price, 'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount'] > price, 'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount'] > price, 'Starting Expected Value']))/price if price > 0 else (
            sum(totalremain.loc[totalremain['prizeamount'] > price, 'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount'] > price, 'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount'] > price, 'Starting Expected Value'])
        gamerow.loc[:, 'Percent of Prizes Remaining'] = (
            totalremain.loc[:, 'Winning Tickets Unclaimed']/totalremain.loc[:, 'Winning Tickets At Start']).mean()
        gamerow.loc[:, 'Percent of Profit Prizes Remaining'] = (
            totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets At Start']).mean()
        chngLosingTix = (gamerow.loc[:, 'Non-prize remaining']-gamerow.loc[:,
                         'Non-prize at start'])/gamerow.loc[:, 'Non-prize at start']
        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0
        gamerow.loc[:, 'Photo'] = gamerow.loc[:, 'gamePhoto']
        gamerow.loc[:, 'FAQ'] = None
        gamerow.loc[:, 'About'] = None
        gamerow.loc[:, 'Directory'] = None
        gamerow.loc[:, 'Data Date'] = gamerow.loc[:, 'dateexported']

        currentodds = pd.concat([currentodds, gamerow], axis=0, ignore_index=True)


        # add non-prize and totals rows with matching columns
        totalremain.loc[:, 'Total remaining'] = tixtotal
        totalremain.loc[:, 'Prize Probability'] = totalremain.loc[:,
                                                                  'Winning Tickets Unclaimed']/totalremain.loc[:, 'Total remaining']
        totalremain.loc[:, 'Percent Tix Remaining'] = totalremain.loc[:,
                                                                      'Winning Tickets Unclaimed']/totalremain.loc[:, 'Winning Tickets At Start']
        nonprizetix.loc[:, 'Prize Probability'] = nonprizetix.apply(lambda row: (
            row['Winning Tickets Unclaimed']/tixtotal) if (row['gameNumber'] == gameid) & (row['Winning Tickets Unclaimed'] > 0) else 0, axis=1)
        nonprizetix.loc[:, 'Percent Tix Remaining'] = nonprizetix.loc[nonprizetix['gameNumber'] == gameid,
                                                                      'Winning Tickets Unclaimed']/nonprizetix.loc[nonprizetix['gameNumber'] == gameid, 'Winning Tickets At Start']
        nonprizetix.loc[:, 'Starting Expected Value'] = (
            nonprizetix['prizeamount']-price)*(nonprizetix['Winning Tickets At Start']/startingtotal)
        nonprizetix.loc[:, 'Expected Value'] = (
            nonprizetix['prizeamount']-price)*(nonprizetix['Winning Tickets Unclaimed']/tixtotal)
        totals.loc[:, 'Prize Probability'] = totals.loc[totals['gameNumber']
                                                        == gameid, 'Winning Tickets Unclaimed']/tixtotal
        totals.loc[:, 'Percent Tix Remaining'] = totals.loc[totals['gameNumber'] == gameid,
                                                            'Winning Tickets Unclaimed']/totals.loc[totals['gameNumber'] == gameid, 'Winning Tickets At Start']
        totals.loc[:, 'Starting Expected Value'] = ''
        totals.loc[:, 'Expected Value'] = ''
        totalremain = totalremain.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                   'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)


        # add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount'] != 'Total', :]

        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)

        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]


    # save scratchers tables
    scratchertables.to_csv("./NHscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizeremain_x'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    

    # --- ADD DAYS SINCE START ---
    if 'startDate' in ratingstable.columns and 'dateexported' in ratingstable.columns:
        ratingstable['startDate'] = pd.to_datetime(ratingstable['startDate'], errors='coerce')
        ratingstable['dateexported'] = pd.to_datetime(ratingstable['dateexported'], errors='coerce')
        ratingstable['Days Since Start'] = (ratingstable['dateexported'] - ratingstable['startDate']).dt.days
    # --- End of FIX for date parsing ---
    
    # add rankings columns of all scratchers to ratings table
    ratingstable['Rank by Best Probability of Winning Any Prize'] = (ratingstable['Current Odds of Any Prize'].rank(
    )+ratingstable['Probability of Winning Any Prize'].rank()+ratingstable['Odds of Any Prize + 3 StdDevs'].rank())/3
    ratingstable['Rank by Best Probability of Winning Profit Prize'] = (ratingstable['Odds of Profit Prize'].rank(
    )+ratingstable['Probability of Winning Profit Prize'].rank()+ratingstable['Odds of Profit Prize + 3 StdDevs'].rank())/3
    ratingstable['Rank by Least Expected Losses'] = (ratingstable['Expected Value of Any Prize (as % of cost)'].rank(
    )+ratingstable['Expected Value of Profit Prize (as % of cost)'].rank())/2
    ratingstable['Rank by Most Available Prizes'] = (ratingstable['Percent of Prizes Remaining'].rank(
    )+ratingstable['Percent of Profit Prizes Remaining'].rank()+ratingstable['Ratio of Decline in Prizes to Decline in Losing Ticket'].rank())/3
    ratingstable['Rank by Best Change in Probabilities'] = (ratingstable['Change in Current Odds of Any Prize'].rank()+ratingstable['Change in Current Odds of Top Prize'].rank()
                                                            + ratingstable['Change in Probability of Any Prize'].rank(
    )+ratingstable['Change in Probability of Profit Prize'].rank()
        + ratingstable['Expected Value of Any Prize (as % of cost)'].rank()+ratingstable['Expected Value of Profit Prize (as % of cost)'].rank())/6
    ratingstable.loc[:, 'Rank Average'] = ratingstable.loc[:,
                                                           'Rank by Best Probability of Winning Any Prize':'Rank by Best Change in Probabilities'].mean(axis=1)
    ratingstable.loc[:, 'Overall Rank'] = ratingstable.loc[:,
                                                           'Rank Average'].rank()
    ratingstable.loc[:, 'Rank by Cost'] = ratingstable.groupby(
        'price')['Overall Rank'].rank('dense', ascending=True)

    # columns in ratingstable to round to only two decimals
    twodecimalcols = ['Current Odds of Any Prize', 'Odds of Profit Prize',
                      'Percent of Prizes Remaining', 'Expected Value of Any Prize (as % of cost)']
    ratingstable[twodecimalcols] = ratingstable[twodecimalcols].round(2)
    ratingstable['Max Tickets to Buy'] = ratingstable['Max Tickets to Buy'].round(
        0)

    # save ratingstable

    ratingstable['Stats Page'] = "/new-hampshire-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('MOratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./NHratingstable.csv", encoding='utf-8')

    ratingstable = ratingstable.loc[:, ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance',
                                 'startDate', 'Days Since Start', 'lastdatetoclaim', 'topprizeodds', 'overallodds', 'Current Odds of Top Prize',
                                 'Change in Current Odds of Top Prize', 'Current Odds of Any Prize',
                                 'Change in Current Odds of Any Prize', 'Odds of Profit Prize', 'Change in Odds of Profit Prize',
                                 'Probability of Winning Any Prize', 'Change in Probability of Any Prize',
                                 'Probability of Winning Profit Prize', 'Change in Probability of Profit Prize',
                                 'StdDev of All Prizes', 'StdDev of Profit Prizes', 'Odds of Any Prize + 3 StdDevs',
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
                                 'Rank by Best Change in Probabilities', 'Rank Average', 'Overall Rank', 'Rank by Cost',
                                 'Photo', 'FAQ', 'About', 'Directory',
                                 'Data Date', 'Stats Page','gameURL']]
    ratingstable = ratingstable.replace([np.inf, -np.inf], 0).infer_objects(copy=False)
    ratingstable = ratingstable.astype(object).fillna('').infer_objects(copy=False)


    return ratingstable, scratchertables


exportScratcherRecs()