#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 23:34:32 2022
Updated for robust Hybrid Scraping: API for Data + HTML for Images
"""

import pandas as pd
import requests
from datetime import date, datetime
from dateutil.tz import tzlocal
import numpy as np
from bs4 import BeautifulSoup
import re

# Constants
MAIN_URL = "https://www.arizonalottery.com/scratchers/"
API_BASE_URL = "https://api.arizonalottery.com/v2/Scratchers"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}

def exportScratcherRecs():
    """
    Scrapes Arizona Lottery data.
    1. Gets Game IDs from the main website HTML.
    2. Fetches details for each ID from the official API.
    3. Scrapes the individual game page to find the ticket image.
    """
    print(f"Fetching game IDs from: {MAIN_URL}")
    
    game_ids = set()
    
    try:
        # 1. Get HTML to find Game IDs
        r = requests.get(MAIN_URL, headers=HEADERS)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Strategy A: Find links containing /scratchers/####-
        links = soup.find_all('a', href=re.compile(r'/scratchers/\d+'))
        for link in links:
            href = link['href']
            match = re.search(r'/scratchers/(\d+)', href)
            if match:
                game_ids.add(match.group(1))
        
        # Strategy B: Find text like "#1234" (backup)
        text_matches = re.findall(r'#(\d{4})\b', r.text)
        game_ids.update(text_matches)
        
        print(f"Found {len(game_ids)} unique game IDs: {sorted(list(game_ids))}")
        
    except Exception as e:
        print(f"Error scraping HTML list: {e}")
        return None, None

    tixtables = pd.DataFrame()
    scratchersall_list = []

    # 2. Loop through IDs and hit the API
    for game_id in game_ids:
        api_url = f"{API_BASE_URL}/{game_id}"
        
        try:
            api_r = requests.get(api_url, headers=HEADERS)
            
            if api_r.status_code == 404:
                print(f"    - API 404 for Game #{game_id} (likely expired/hidden). Skipping.")
                continue
            
            api_r.raise_for_status()
            data = api_r.json()
            
            if isinstance(data, list):
                if not data: continue
                game_data = data[0]
            else:
                game_data = data

            # --- Extract Header Info ---
            gameNumber = str(game_data.get('gameNum', game_id))
            gameName = game_data.get('gameName', 'Unknown')
            try:
                gamePrice = float(game_data.get('ticketValue', 0))
            except:
                gamePrice = 0.0
            
            # Dates
            startDate = game_data.get('beginDate')
            if startDate: startDate = startDate.split('T')[0]
            
            endDate = game_data.get('endDate')
            if endDate: endDate = endDate.split('T')[0]
            
            lastDate = game_data.get('lastDate')
            lastdatetoclaim = lastDate.split('T')[0] if lastDate else None
            
            gameOdds = float(game_data.get('gameOdds', 0))
            
            # Construct Web URL
            slug = gameName.lower().replace(' ', '-').replace('#', '').replace('$', '').replace("'", "")
            gameURL = f"https://www.arizonalottery.com/scratchers/{gameNumber}-{slug}/"

            print(f"  > Processing: {gameName} (#{gameNumber})")

            # --- 3. FETCH GAME PHOTO ---
            gamePhoto = None
            
            # Try API first
            if 'image' in game_data and game_data['image']:
                gamePhoto = f"https://www.arizonalottery.com/media/{game_data['image']}"
            elif 'ticketImage' in game_data and game_data['ticketImage']:
                gamePhoto = game_data['ticketImage']

            # HTML Scraping Fallback (Only if API failed)
            if not gamePhoto:
                try:
                    page_r = requests.get(gameURL, headers=HEADERS, timeout=10)
                    if page_r.status_code == 200:
                        page_soup = BeautifulSoup(page_r.text, 'html.parser')
                        
                        # Strategy A: Open Graph Image (Most reliable)
                        og_img = page_soup.find('meta', property='og:image')
                        if og_img and og_img.get('content'):
                            gamePhoto = og_img['content']
                        
                        # Strategy B: Image with Game Number in src
                        if not gamePhoto:
                            img_tag = page_soup.select_one(f"img[src*='{gameNumber}']")
                            if img_tag and img_tag.has_attr('src'):
                                src = img_tag['src'].split('?')[0]
                                if src.startswith('http'):
                                    gamePhoto = src
                                else:
                                    gamePhoto = f"https://www.arizonalottery.com{src}"
                        
                        # Strategy C: Specific Class
                        if not gamePhoto:
                            img_tag = page_soup.select_one(".scratchers-detail-image img")
                            if img_tag and img_tag.has_attr('src'):
                                src = img_tag['src'].split('?')[0]
                                if src.startswith('http'):
                                    gamePhoto = src
                                else:
                                    gamePhoto = f"https://www.arizonalottery.com{src}"
                except Exception as e:
                    pass # Ignore photo errors
            # --- Extract Prize Tiers ---
            prize_tiers = game_data.get('prizeTiers', [])
            
            topprize = 0
            topprizestarting = 0
            topprizeremain = 0
            topprizeodds = 0  # Initialize variable
            
            for tier in prize_tiers:
                # Prize Amount Cleaning
                raw_amt = str(tier.get('prizeAmount', 0))
                disp_title = str(tier.get('displayTitle', raw_amt))
                
                clean_amt_str = disp_title.replace('$', '').replace(',', '').replace('.00', '')
                if 'Million' in clean_amt_str:
                    try:
                        val = float(clean_amt_str.replace(' Million', '').strip())
                        clean_amt = int(val * 1_000_000)
                    except:
                        clean_amt = 0
                elif 'TICKET' in clean_amt_str.upper() or 'FREE' in clean_amt_str.upper():
                    clean_amt = 0 
                else:
                    try:
                        clean_amt = int(float(clean_amt_str))
                    except:
                        clean_amt = 0
                
                prizeamount = clean_amt
                
                # Stats
                start_count = int(tier.get('totalCount', 0))
                remain_count = int(tier.get('count', 0))
                tier_odds = float(tier.get('odds', 0))
                tier_level = int(tier.get('tierLevel', 99))
                
                if prizeamount > topprize:
                    topprize = prizeamount
                    topprizestarting = start_count
                    topprizeremain = remain_count
                    topprizeodds = tier_odds  # Capture the odds for the top prize tier
                
                row_data = {
                    'gameNumber': gameNumber,
                    'gameName': gameName,
                    'price': gamePrice,
                    'prizeamount': prizeamount,
                    'startDate': startDate,
                    'endDate': endDate,
                    'lastdatetoclaim': lastdatetoclaim,
                    'overallodds': gameOdds,
                    'prizeodds': tier_odds,
                    'Winning Tickets At Start': start_count,
                    'Winning Tickets Unclaimed': remain_count,
                    'dateexported': date.today(),
                    'gameURL': gameURL,
                    'tierLevel': tier_level
                }
                tixtables = pd.concat([tixtables, pd.DataFrame([row_data])], ignore_index=True)

            topprizeavail = "Top Prize Claimed" if topprizeremain == 0 else "Available"
            
            scratchersall_list.append({
                'price': gamePrice,
                'gameName': gameName,
                'gameNumber': gameNumber,
                'topprize': topprize,
                'overallodds': gameOdds,
                'topprizestarting': topprizestarting,
                'topprizeremain': topprizeremain,
                'topprizeavail': topprizeavail,
                'topprizeodds': topprizeodds,  # Store the captured odds here
                'extrachances': None,
                'secondChance': None,
                'startDate': startDate,
                'endDate': endDate,
                'lastdatetoclaim': lastdatetoclaim,
                'dateexported': date.today(),
                'gameURL': gameURL,
                'gamePhoto': gamePhoto
            })

        except Exception as e:
            print(f"    - Error processing Game #{game_id}: {e}")
            continue

    # --- SAVE OUTPUTS ---
    if not scratchersall_list:
        print("No data collected.")
        return None, None

    scratchersall = pd.DataFrame(scratchersall_list)
    scratchersall.to_csv("./AZscratcherslist.csv", encoding='utf-8', index=False)
    
    tixtables.to_csv("./AZscratchertables.csv", encoding='utf-8', index=False)
    
    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount','Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel', 'dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber']!= "Coming Soon!", :]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    gamesgrouped = gamesgrouped.merge(scratchersall[[
                                      'gameNumber', 'price', 'topprizeodds', 'overallodds', 'gamePhoto']], how='left', on=['gameNumber'])
    #gamesgrouped.loc[:, 'topprizeodds'] = gamesgrouped.loc[:,'topprizeodds'].str.replace(',', '', regex=True)

    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    gamesgrouped.loc[:, 'Total at start'] = gamesgrouped['Winning Tickets At Start'] * \
        gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * \
        gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Non-prize at start'] = gamesgrouped['Total at start'] - \
        gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:, 'Non-prize remaining'] = gamesgrouped['Total remaining'] - \
        gamesgrouped['Winning Tickets Unclaimed']

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
    nonprizetix = gamesgrouped[['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']].copy()
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped[['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']].copy()
    totals.rename(columns={'Total at start': 'Winning Tickets At Start',
                  'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:, 'prizeamount'] = "Total"


    # loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame()
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :].copy()
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
            'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'tierLevel','dateexported']]
        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])
        print(gameid)
        print(gamerow)
        print(gamerow.columns)

        prizes = totalremain.loc[:, 'prizeamount']
        
        startoddstopprize = tixtotal / totalremain.loc[totalremain['tierLevel']==1, 'Winning Tickets At Start'].values[0]

        # add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:, 'Current Odds of Top Prize'] = float(gamerow['topprizeodds'].values[0])
        gamerow.loc[:, 'Change in Current Odds of Top Prize'] = (gamerow.loc[:, 'Current Odds of Top Prize'] - float(
            startoddstopprize)) / float(startoddstopprize)
        gamerow.loc[:, 'Current Odds of Any Prize'] = tixtotal / \
            sum(totalremain.loc[:, 'Winning Tickets Unclaimed'])
        gamerow.loc[:, 'Change in Current Odds of Any Prize'] = (gamerow.loc[:, 'Current Odds of Any Prize'] - float(
            gamerow['overallodds'].values[0])) / float(gamerow['overallodds'].values[0])
        gamerow.loc[:, 'Odds of Profit Prize'] = tixtotal/sum(
            totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'])
        startingprofitodds = startingtotal / \
            sum(totalremain.loc[totalremain['prizeamount']
                != price, 'Winning Tickets At Start'])
        gamerow.loc[:, 'Starting Odds of Profit Prize'] = startingprofitodds
        gamerow.loc[:, 'Change in Odds of Profit Prize'] = (
            gamerow.loc[:, 'Odds of Profit Prize'] - startingprofitodds) / startingprofitodds
        gamerow.loc[:, 'Probability of Winning Any Prize'] = sum(
            totalremain.loc[:, 'Winning Tickets Unclaimed'])/tixtotal
        startprobanyprize = sum(
            totalremain.loc[:, 'Winning Tickets At Start'])/startingtotal
        gamerow.loc[:, 'Starting Probability of Winning Any Prize'] = startprobanyprize
        gamerow.loc[:, 'Change in Probability of Any Prize'] = startprobanyprize - \
            gamerow.loc[:, 'Probability of Winning Any Prize']
        gamerow.loc[:, 'Probability of Winning Profit Prize'] = sum(
            totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'])/tixtotal
        startprobprofitprize = sum(
            totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets At Start'])/startingtotal
        gamerow.loc[:, 'Starting Probability of Winning Profit Prize'] = startprobprofitprize
        gamerow.loc[:, 'Change in Probability of Profit Prize'] = startprobprofitprize - \
            gamerow.loc[:, 'Probability of Winning Profit Prize']
        gamerow.loc[:, 'StdDev of All Prizes'] = totalremain.loc[:,
                                                                 'Winning Tickets Unclaimed'].std().mean()/tixtotal
        gamerow.loc[:, 'StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']
                                                                    != price, 'Winning Tickets Unclaimed'].std().mean()/tixtotal
        gamerow.loc[:, 'Odds of Any Prize + 3 StdDevs'] = tixtotal / \
            (gamerow.loc[:, 'Current Odds of Any Prize'] +
             (totalremain.loc[:, 'Winning Tickets Unclaimed'].std().mean()*3))
        gamerow.loc[:, 'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:, 'Odds of Profit Prize']+(
            totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'].std().mean()*3))
        gamerow.loc[:, 'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean())

        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        totalremain.loc[:, 'Starting Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[:, 'Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                   'Winning Tickets Unclaimed', 'Starting Expected Value', 'Expected Value', 'dateexported']].copy()

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
        gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes

        gamerow.loc[:, 'Photo'] = gamerow.loc[:,'gamePhoto']
        gamerow.loc[:, 'FAQ'] = None
        gamerow.loc[:, 'About'] = None
        gamerow.loc[:, 'Directory'] = None
        gamerow.loc[:, 'Data Date'] = gamerow.loc[:, 'dateexported']

        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)

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
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                   'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']].copy()
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)

        # add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount'] != 'Total', :]

        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]


    # --- REVISED CONVERSION FOR JSON SERIALIZATION (Simpler) ---
    print("Converting numeric types in scratchertables to JSON-compatible types (using .astype(object))...")
    numeric_cols = scratchertables.select_dtypes(include=np.number).columns
    print(f"Numeric columns identified for conversion: {numeric_cols.tolist()}")
    
    for col in numeric_cols:
        # Force conversion to object dtype, which stores Python native types
        # This handles NaN by converting them to None within the object array.
        try:
            scratchertables[col] = scratchertables[col].astype(object)
            print(f"Converted column '{col}' using astype(object).")
        except Exception as e:
            # Added error handling just in case astype fails for some reason
            print(f"ERROR: Failed to convert column '{col}' using astype(object): {e}")
    
    # Ensure columns that might contain non-numeric strings like 'Total' are object type
    if 'prizeamount' in scratchertables.columns and scratchertables['prizeamount'].dtype != 'object':
        scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(object)
    if 'gameNumber' in scratchertables.columns and scratchertables['gameNumber'].dtype != 'object':
         scratchertables['gameNumber'] = scratchertables['gameNumber'].astype(object) # Game number can be string
    
    print("Final scratchertables dtypes before returning:")
    print(scratchertables.dtypes)
# --- Corrected Diagnostic Check ---
    # Check for problematic NumPy types *within* object columns after conversion attempt
    print("\nDetailed check of types within potentially converted columns...")
    conversion_issue_found = False
    for col in numeric_cols: # Iterate over columns that *should* have been converted
        if col in scratchertables.columns:
            col_dtype = scratchertables[col].dtype
            print(f"  Column '{col}': Reported dtype = {col_dtype}")
            if col_dtype == 'object':
                # Sample the first few non-null values to check their actual Python type
                try:
                    # Using unique types found in a sample is more informative
                    unique_types_in_sample = scratchertables[col].dropna().head(20).apply(type).unique()
                    print(f"    Sampled value types: {unique_types_in_sample}")
                    # Explicitly check for numpy types within the object column
                    numpy_types_present = [t for t in unique_types_in_sample if 'numpy' in str(t)]
                    if numpy_types_present:
                         print(f"    WARNING: NumPy types {numpy_types_present} still present in object column '{col}'!")
                         conversion_issue_found = True
                except Exception as e:
                    print(f"    - Could not inspect types within column '{col}': {e}")
            elif col_dtype == np.int64:
                print(f"    ERROR: Column '{col}' is still {np.int64} despite conversion attempt!")
                conversion_issue_found = True
            elif col_dtype == np.float64:
                 print(f"    WARNING: Column '{col}' is still {np.float64}. This might be acceptable, but was expected to be object.")
                 # Decide if this is truly an error or just a warning
                 # conversion_issue_found = True # Uncomment if float64 is also problematic

    if conversion_issue_found:
        print("-----> WARNING: Potential type conversion issues detected. Review column types above. <-----")
    else:
        print("-----> Type conversion check passed (object columns inspected where applicable). <-----")
    # --- End of Corrected Diagnostic Check ---    
        print(scratchertables.dtypes)

    # save scratchers tables
    #scratchertables.to_sql('AZscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./azscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y',
                      'topprizeodds_y', 'overallodds_y'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported',
                        'topprizeodds_x': 'topprizeodds', 'overallodds_x': 'overallodds'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'])).dt.days

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
    
    # Also convert key columns in ratingstable if they might be numpy types
    # Although less likely to cause issues if not directly serialized as granularly.
    numeric_cols_ratings = [ # Add relevant numeric columns from ratingstable
        'price', 'topprizeremain', 'Days Since Start', # Integers
        'topprizeodds', 'overallodds', 'Current Odds of Top Prize', # Floats that might be np.float64
        # ... include all other numeric columns from ratingstable ...
        'Rank Average', 'Overall Rank', 'Rank by Cost'
    ]
    for col in numeric_cols_ratings:
        if col in ratingstable.columns:
             ratingstable[col] = ratingstable[col].astype(object) # Convert to object/python types
             
    print(scratchertables.columns)
    print(scratchertables)
    print(scratchertables.dtypes)
    # save ratingstable
    print(ratingstable)
    print(ratingstable.columns)
    #ratingstable.to_sql('AZratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./azratingstable.csv", encoding='utf-8')
    ratingstable['Stats Page'] = "/arizona-statistics-for-each-scratcher-game"
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
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
                             
    return ratingstable, scratchertables

#exportScratcherRecs()