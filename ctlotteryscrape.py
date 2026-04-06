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

def exportScratcherRecs():
    print("Initializing CT Scraper...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("permissions.default.image", 2)
    
    driver = webdriver.Firefox(service=Service(GeckoDriverManager().install()), options=firefox_options)

    try:
        url_list = "https://ctlottery.org/ScratchGamesTable"
        driver.get(url_list)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        # 1. Harvest Game Master List
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        df_master = pd.read_html(io.StringIO(str(soup.find('table'))))[0]
        df_master.columns = [str(c).strip().lower() for c in df_master.columns]
        
        # Identify columns dynamically
        col_id = next((c for c in df_master.columns if '#' in c or 'game' in c), df_master.columns[0])
        col_name = next((c for c in df_master.columns if 'name' in c), df_master.columns[1])
        
        print(f"Found {len(df_master)} games. Starting detail crawl...")

        all_game_rows = []
        all_prize_tables = []

        for index, row in df_master.iterrows():
            gameNumber = re.sub(r'[^\d]', '', str(row[col_id]))
            if not gameNumber: continue
            
            detail_url = f"https://www.ctlottery.org/ScratchGames/{gameNumber}/"
            driver.get(detail_url)
            
            try:
                # Wait for the prize table or the "Ended" status
                WebDriverWait(driver, 8).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except:
                continue

            detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
            full_text = detail_soup.get_text(" ", strip=True)

            # --- FIX 1: IMPROVED REGEX FOR ODDS ---
            # Handles "Overall Odds 1:3.45" or "Overall Odds: 1 in 4.01"
            overall_odds = 0
            odds_match = re.search(r'Overall Odds\s*:?\s*(?:1\s*[:in]\s*)?([\d\.]+)', full_text, re.IGNORECASE)
            if odds_match:
                overall_odds = float(odds_match.group(1))

            # --- FIX 2: IMPROVED TABLE PARSING ---
            tixdata = pd.DataFrame()
            tables = detail_soup.find_all('table')
            for tbl in tables:
                temp_df = pd.read_html(io.StringIO(str(tbl)))[0]
                temp_df.columns = [str(c).lower().strip() for c in temp_df.columns]
                
                # Check for CT's specific headers (prize, total, unclaimed)
                if any('prize' in c for c in temp_df.columns) and any('unclaimed' in c for c in temp_df.columns):
                    tixdata = temp_df.copy()
                    break

            if not tixdata.empty:
                # Standardize Column Names
                cols_map = {}
                for col in tixdata.columns:
                    if 'prize' in col: cols_map[col] = 'prizeamount'
                    if 'total' in col: cols_map[col] = 'Winning Tickets At Start'
                    if 'unclaimed' in col: cols_map[col] = 'Winning Tickets Unclaimed'
                tixdata.rename(columns=cols_map, inplace=True)

                # Clean Prize Money
                def clean_val(x):
                    return float(re.sub(r'[^\d\.]', '', str(x))) if re.search(r'\d', str(x)) else 0
                
                tixdata['prizeamount'] = tixdata['prizeamount'].apply(clean_val)
                tixdata['Winning Tickets Unclaimed'] = tixdata['Winning Tickets Unclaimed'].apply(clean_val)
                tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets At Start'].apply(clean_val)
                
                # Metadata
                tixdata['gameNumber'] = gameNumber
                tixdata['dateexported'] = date.today()
                all_prize_tables.append(tixdata)

                # Summary Stats
                top_row = tixdata.sort_values('prizeamount', ascending=False).iloc[0]
                all_game_rows.append({
                    'gameNumber': gameNumber,
                    'gameName': str(row[col_name]),
                    'topprize': top_row['prizeamount'],
                    'topprizeremain': top_row['Winning Tickets Unclaimed'],
                    'overallodds': overall_odds,
                    'price': float(re.sub(r'[^\d\.]', '', str(row.get('price', 0)))) if re.search(r'\d', str(row.get('price', 0))) else 0,
                    'dateexported': date.today()
                })
                print(f"  > Processed #{gameNumber}: {top_row['prizeamount']} Top Prize")

        # Create scratcherstables df, with calculations of total tix and total tix without prizes
        scratchertables = tixdata.dropna(subset=['prizeamount']).copy()
        scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(int)
        scratchersall = tixdata.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                                   'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]

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
            odds = gamerow.loc[:, 'total_remaining'] / gamerow.loc[:, 'top_prize_remaining']
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
            gamerow.loc[:, 'Photo'] = tixdata.loc[tixdata['gameNumber']
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
        ratingstable['Stats Page'] = "/maryland-statistics-for-each-scratcher-game"
        #ratingstable.to_sql('MDratingstable', engine, if_exists='replace')
        ratingstable.to_csv("./MDratingstable.csv", encoding='utf-8')
        # write to Google Sheets
        # select a work sheet from its name
        #MDratingssheet = gs.worksheet('MDRatingsTable')
        #MDratingssheet.clear()
        
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
        #set_with_dataframe(worksheet=MDratingssheet, dataframe=ratingstable, include_index=False,
          #                 include_column_header=True, resize=True)
        return ratingstable, scratchertables

    finally:
        driver.quit()

if __name__ == "__main__":
    exportScratcherRecs()