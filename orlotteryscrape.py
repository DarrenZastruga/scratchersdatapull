import pandas as pd
import json
import requests
from bs4 import BeautifulSoup
import re
import logging
import time
from datetime import datetime, date
from dateutil.tz import tzlocal
import numpy as np
import io
from urllib.parse import urljoin

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager

def exportScratcherRecs():
    print("Initializing Oregon Hybrid Scraper...")
    
    # --- 1. SETUP SELENIUM ---
    options = Options()
    options.add_argument("--headless")
    # Optimize for speed
    #options.page_load_strategy = 'eager' 
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=options)
    
    # API Configuration
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Ocp-Apim-Subscription-Key': '683ab88d339c4b22b2b276e3c2713809'
    }

    try:
        # --- 2. FETCH MASTER LIST FROM API (Restored & Robust) ---
        print("Fetching master list from API...")
        url_all = "https://api2.oregonlottery.org/instant/GetAll"
        r = requests.get(url_all, headers=headers)
        responsejson = r.json()
        
        # Standardize today
        today = pd.Timestamp.now().normalize()
        tix_list_data = []

        for game in responsejson:
            # Use .get() to avoid KeyErrors and handle potential nulls
            start_dt = pd.to_datetime(game.get('DateAvailable'), errors='coerce')
            claim_dt = pd.to_datetime(game.get('ValidationEndDate'), errors='coerce')
            
            # --- THE "VISIBLE" FILTER ---
            # 1. We must have at least a start date to compare.
            # 2. If claim_dt is missing, we assume it's a new/active game.
            if pd.notnull(start_dt):
                # Logic: Started in the past AND (No claim date OR claim date is in the future)
                if start_dt <= today and (pd.isnull(claim_dt) or today <= claim_dt):
                    tix_list_data.append({
                        'price': float(game.get('TicketPrice', 0)),
                        'gameName': game.get('GameNameTitle', 'Unknown'),
                        'gameNumber': str(game.get('GameNumber')),
                        'topprize': float(game.get('TopPrize', 0)),
                        'topprizeremain': int(game.get('TopPrizesRemaining', 0)),
                        'topprizeavail': 'Available' if int(game.get('TopPrizesRemaining', 0)) > 0 else 'Top Prize Claimed',
                        'startDate': start_dt,
                        'endDate': pd.to_datetime(game.get('GameEndDate'), errors='coerce'),
                        'lastdatetoclaim': claim_dt,
                        'overallodds': float(game.get('OverallOdds', 0)),
                        'secondChance': '2nd Chance' if game.get('SecondChanceDrawDate') else None,
                        'dateexported': date.today(),
                        'gameURL': '', 
                        'gamePhoto': '' 
                    })
        
        tixlist = pd.DataFrame(tix_list_data)
        tixtables = pd.DataFrame()

        # --- 3. DETAIL CRAWL FOR URLS, PHOTOS, AND TABLES ---
        print(f"Starting detailed crawl for {len(tixlist)} games...")
        
        for index, row in tixlist.iterrows():
            game_id = row['gameNumber']
            detail_api = f'https://api2.oregonlottery.org/instant/GetGame?includePrizeTiers=true&gameNumber={game_id}'
            
            try:
                # A. Fetch API Data (Fast & Direct)
                r_detail = requests.get(detail_api, headers=headers)
                # The API returns a list containing one dictionary
                data_list = r_detail.json()
                if not data_list:
                    continue
                game_detail_json = data_list[0] 
                
                # Extract the official web slug from API to build the URL
                game_slug = game_detail_json.get('GameUrl', '').strip('/')
                full_game_url = urljoin("https://www.oregonlottery.org/scratch-its/", game_slug)
                
                # B. Use Selenium & Date-Aware Predictive Construction
                game_photo = ""
                
                # 1. DYNAMIC PREDICTIVE PATH (Using Start Date)
                try:
                    launch_dt = pd.to_datetime(row['startDate'])
                    folder_path = launch_dt.strftime("%Y/%m") # e.g., '2023/08'
                    
                    # Clean name for filename (Remove spaces/special chars)
                    clean_name = re.sub(r'[^a-zA-Z0-9]', '', row['gameName'])
                    
                    # Filename variants Oregon uses
                    variants = [
                        f"{game_id}_{clean_name}_GameTile_1200x1200.jpg",
                        f"{game_id}_{clean_name}_Front_1000w.jpg",
                        f"{game_id}_{clean_name}_Ticket.png"
                    ]
                    
                    for filename in variants:
                        candidate_url = f"https://www.oregonlottery.org/wp-content/uploads/{folder_path}/{filename}"
                        check = requests.head(candidate_url, timeout=2)
                        if check.status_code == 200:
                            game_photo = candidate_url
                            break
                except:
                    pass

                # 2. SHADOW DOM PIERCE (Fallback if prediction fails)
                if not game_photo:
                    driver.get(full_game_url)
                    try:
                        WebDriverWait(driver, 6).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                        time.sleep(1.5) 
                        
                        js_photo = driver.execute_script("""
                            const host = document.querySelector('div.ol-gamedata-scratchit');
                            if (host && host.shadowRoot) {
                                const img = host.shadowRoot.querySelector('img');
                                return img ? (img.srcset || img.src) : null;
                            }
                            return null;
                        """)
                        
                        if js_photo and str(game_id) in str(js_photo):
                            game_photo = str(js_photo).split(',')[0].split(' ')[0]
                            game_photo = urljoin("https://www.oregonlottery.org", game_photo).split('?')[0]
                    except:
                        pass

                # 3. FINAL CRAWL (Hunt for any ID-labeled image on page)
                if not game_photo:
                    detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                    img_match = detail_soup.find('img', src=re.compile(rf"{game_id}"))
                    if img_match:
                        game_photo = urljoin("https://www.oregonlottery.org", img_match.get('src')).split('?')[0]

                # Update Master List
                tixlist.at[index, 'gameURL'] = full_game_url
                tixlist.at[index, 'gamePhoto'] = game_photo
                
                if game_photo:
                    print(f"    - Success: {game_photo}")
                else:
                    print(f"    ! No photo found for {game_id}")
                    
                # D. Process Prize Tiers (FIXED: Accessing dict directly)
                # We use game_detail_json directly because it was defined as data_list[0]
                tixdata = pd.json_normalize(game_detail_json['PrizeTiers'])
                if not tixdata.empty:
                    tixdata.rename(columns={
                        'PrizeAmount': 'prizeamount',
                        'PrizesTotal': 'Winning Tickets At Start', 
                        'PrizesRemaining': 'Winning Tickets Unclaimed'
                    }, inplace=True)
                    tixdata['gameNumber'] = game_id
                    tixdata['gameName'] = row['gameName']
                    tixdata['dateexported'] = date.today()
                    tixtables = pd.concat([tixtables, tixdata], axis=0, ignore_index=True)

                print(f"  > Done: {row['gameName']} (#{game_id})")

            except Exception as e:
                print(f"  ! Error on game {game_id}: {e}")

        # --- 4. EXPORT FINAL FILES ---
        tixlist.to_csv("./ORscratcherslist.csv", index=False)
        tixtables.to_csv("./ORscratchertables.csv", index=False)
        
        scratchertables = tixtables.dropna(subset=['prizeamount']).copy()
        scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(int)
        scratchersall = tixlist.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                                   'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]

        scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                          != "Coming Soon!", :]
        scratchersall = scratchersall.drop_duplicates()

        # Create scratcherstables df, with calculations of total tix and total tix without prizes
        scratchertables = scratchertables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                     'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
        scratchertables.to_csv("./MDscratchertables.csv", encoding='utf-8')
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
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :].copy()

            #cast all columns to Object to start to avoid dtype errors when converting to numeric later
            for col in gamerow.columns:
                gamerow[col] = gamerow[col].astype(object)
            startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
            tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
            totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
                'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
            totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
                'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
            price = int(gamerow['price'].values[0])

            prizes = totalremain.loc[:, 'prizeamount']

            #convert 'Winning Tickets Unclaimed' as numberic to avoid divide by zero warnings
            den = pd.to_numeric(totalremain['Winning Tickets Unclaimed'].iloc[0], errors='coerce')
            if pd.notna(den) and den > 0:
                gamerow.loc[:, 'Current Odds of Top Prize'] = tixtotal / den
            else:
                gamerow.loc[:, 'Current Odds of Top Prize'] = np.nan
                
            # add various columns for the scratcher stats that go into the ratings table
            gamerow['topprizeremain'] = pd.to_numeric(gamerow['topprizeremain'], errors='coerce')

            odds = pd.to_numeric(gamerow.loc[:, 'Total remaining'], errors='coerce') / gamerow.loc[:, 'topprizeremain']
            odds = odds.replace([np.inf, -np.inf], np.nan)
            gamerow.loc[:, 'Current Odds of Top Prize'] = odds
            gamerow.loc[:, 'Change in Current Odds of Top Prize'] = (gamerow.loc[:, 'Current Odds of Top Prize'] - float(
                gamerow['topprizeodds'].values[0])) / float(gamerow['topprizeodds'].values[0])
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
            gamerow.loc[:, 'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'].sum(
            )-totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'].std().mean())

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
            gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                                  == gameid, 'gamePhoto'].values[0]
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
        #scratchertables.to_sql('MDscratcherstables', engine, if_exists='replace')
        scratchertables.to_csv("./MDscratchertables.csv", encoding='utf-8')

        # create rankings table by merging the list with the tables
        scratchersall = scratchersall.astype(object)
        scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                          'price'].apply(pd.to_numeric)
        ratingstable = scratchersall.merge(
            currentodds, how='left', on=['gameNumber', 'price'])
        ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                          'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
        ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                            'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
        # add number of days since the game start date as of date exported
        ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
            ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
        ratingstable['Stats Page'] = "/oregon-statistics-for-each-scratcher-game"
        #ratingstable.to_sql('MDratingstable', engine, if_exists='replace')
        ratingstable.to_csv("./OrRingstable.csv", encoding='utf-8')


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
    finally:
        driver.quit() # Ensure browser always closes

if __name__ == "__main__":
    exportScratcherRecs()   