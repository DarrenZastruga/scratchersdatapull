#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 19 18:38:11 2025

@author: michaeljames
"""

import pandas as pd
import os
import psycopg2
import urllib.parse
from urllib.parse import urlparse
import urllib.request
import json
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime
from dateutil.tz import tzlocal
from sqlalchemy import create_engine
import lxml
from datetime import date
import numpy as np
import html5lib
import random
from itertools import repeat
from scipy import stats
import io
import time


now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportARScratcherRecs():
    """
    Scrapes the Arkansas Scholarship Lottery website for scratch-off data.
    It loops through all pages of games, then visits each detail page for prize info.
    """
    base_url = "https://www.myarkansaslottery.com"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    tixtables = pd.DataFrame()
    scratchersall_list = []

    page = 1
    # Loop through the pages until there are no more games
    while True:
        list_page_url = f"https://www.myarkansaslottery.com/games/instant?amount=All&page={page}"
        print(f"Fetching game list from: {list_page_url}")

        try:
            r = requests.get(list_page_url, headers=headers)
            soup = BeautifulSoup(r.content, 'html.parser')
            
            # First, find the main container that holds all the game cards
            main_content = soup.find('div', class_='view-content')

            # If the main container doesn't exist, we've reached the end of the pages.
            if not main_content:
                print("No game container found. Ending scraping process.")
                break
            
            # Find all game containers on the main page
            game_cards = soup.find(class_='view-content').find_all('div', class_='field field-name-field-ticket-front field-type-image field-label-hidden')
\
            
            # Loop through each game card to get its details
            for card in game_cards:
                try:
        
                    detail_path = card.find('a')['href']
                    detail_url = f"{base_url}{detail_path}"
                    
                    print(f"  > Processing game: {detail_path}")
                
                    # Now, visit the detail page to get the prize table and other info
                    detail_r = requests.get(detail_url, headers=headers)
                    detail_soup = BeautifulSoup(detail_r.content, 'html.parser')
                    
                    #Extract game name
                    game_name_tag = detail_soup.find('h1', class_='layout-center')
                    game_name = game_name_tag.get_text(strip=True).replace('Game #', '') if game_name_tag else None
                    print(game_name)
                    
                    # Extract the game number
                    game_number_tag = detail_soup.find('div', class_='field field-name-field-game-number field-type-text field-label-inline clearfix layout-center')
                    game_number = game_number_tag.find('strong').get_text(strip=True) if game_number_tag else None
                    print(game_number)
                    
                    #Extract game photo
                    game_photo_tag = detail_soup.find('div', class_='field field-name-field-ticket-front field-type-image field-label-hidden layout-3col__col-x')
                    game_photo = game_photo_tag.find('div', class_='field-items').find('img').get('src') if game_photo_tag else None
                    print(game_photo)
                    
                    #Extract game dates
                    endDate = detail_soup.find('p', class_='layout-3col__col-1').get_text(strip=True).replace('Last Sell Date:','').replace('*','')
                    print(endDate)
                    lastdatetoclaim = detail_soup.find('p', class_='layout-3col__col-2').get_text(strip=True).replace('Last Redeem Date:','').replace('*','')
                    print(lastdatetoclaim)
                    startDate = detail_soup.find('p', class_='layout-3col__col-3').get_text(strip=True).replace('Launch Date:','').replace('*','')
                    print(startDate)
                    
                    # Extract ticket cost
                    cost_tag = detail_soup.find('div', class_='field field-name-field-ticket-price field-type-text field-label-above layout-3col__col-x')
                    price = cost_tag.find('div', class_='field-items').get_text(strip=True).replace('$','') if cost_tag else None
                    print(price)
                    
                    # Extract overall odds
                    odds_tag = detail_soup.find('div', class_='field field-name-field-game-odds field-type-text field-label-above layout-3col__col-x')
                    overall_odds = odds_tag.find('div', class_='field-items').get_text(strip=True).split()[-1] if odds_tag else None
                    print(overall_odds)
                    
                    
                    # Find and parse the prize table
                    table_container = detail_soup.find('table', class_='tablesaw tablesaw-swipe table-instant-game-data')
                
                    if not table_container:
                        print(f"    - No prize table found for {game_name}. Skipping.")
                        continue
                    prize_table_df = pd.read_html(io.StringIO(str(table_container)))[0]
                
                    # Rename columns to match the script's required format
                    prize_table_df.rename(columns={
                        'Tier Prize Description': 'prizeamount',
                        'Total Prizes in Game per Tier': 'Winning Tickets At Start',
                        'Estimated Prizes Remaining per Tier': 'Winning Tickets Unclaimed'
                    }, inplace=True)
                    prize_table_df.drop(columns=['Total Prize Amount in Game per Tier','Estimated Prize Amount Remaining per Tier'], inplace=True)
                
                
                    # Data Cleaning
                    prize_table_df['prizeamount'] = prize_table_df['prizeamount'].astype(str).str.replace(r'[\$,]', '', regex=True)
                    prize_table_df['prizeamount'] = prize_table_df['prizeamount'].str.replace('FREE TICKET', str(price), regex=False)
                    prize_table_df['prizeamount'] = pd.to_numeric(prize_table_df['prizeamount'], errors='coerce')
                    
                    # Add other necessary columns
                    prize_table_df['gameNumber'] = game_number
                    prize_table_df['gameName'] = game_name
                    prize_table_df['price'] = price
                    prize_table_df['dateexported'] = date.today()
                
                    # Get top prize info from the table
                    topprize = prize_table_df['prizeamount'].max()
                    topprizestarting = prize_table_df.loc[prize_table_df['prizeamount'] == topprize, 'Winning Tickets At Start'].iloc[0]
                    topprizeremain = prize_table_df.loc[prize_table_df['prizeamount'] == topprize, 'Winning Tickets Unclaimed'].iloc[0]
                    topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
                
                    # Append the cleaned table to the main tables DataFrame
                    tixtables = pd.concat([tixtables, prize_table_df], ignore_index=True)
                    
                
                    # Append summary data to the list
                    scratchersall_list.append({
                        'price': price, 'gameName': game_name, 'gameNumber': game_number,
                        'topprize': topprize, 'overallodds': overall_odds,
                        'topprizestarting': topprizestarting, 'topprizeremain': topprizeremain,
                        'topprizeavail': topprizeavail, 'extrachances': None, 'secondChance': None,
                        'startDate': startDate, 'endDate': endDate, 'lastdatetoclaim': lastdatetoclaim,
                        'gamePhoto': game_photo, 'dateexported': date.today(), 'gameURL': detail_url
                    })
                    
                    
                except Exception as e:
                    print(f"    - ERROR processing a game card: {e}")
                    continue
                
    
            # Increment the page counter to move to the next page in the next loop iteration
            page += 1
            time.sleep(1) # Add a small delay to be respectful to the server
            print(scratchersall_list)
    
        except requests.RequestException as e:
            print(f"ERROR: Could not fetch page {page}. Halting process. Error: {e}")
            break




    # --- This is the statistical analysis section from your original script ---
    # It will now run on the data scraped from the Arkansas Lottery.

    scratchersall = pd.DataFrame(scratchersall_list)
    scratchersall.to_csv("./ARscratcherslist.csv", encoding='utf-8', index=False)
    
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./ARscratchertables.csv", encoding='utf-8', index=False)
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})

    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index()
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','gamePhoto', 'price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    
    gamesgrouped[['price','overallodds']] = gamesgrouped[['price','overallodds']].apply(pd.to_numeric, errors='coerce')

    gamesgrouped['Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds']
    gamesgrouped['Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds']
    gamesgrouped['Non-prize at start'] = gamesgrouped['Total at start'] - gamesgrouped['Winning Tickets At Start']
    gamesgrouped['Non-prize remaining'] = gamesgrouped['Total remaining'] - gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped['topprizeodds'] = gamesgrouped['Total at start'] / gamesgrouped['topprizestarting']
    gamesgrouped[['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped[['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
    nonprizetix = gamesgrouped[['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']].copy()
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped[['gameNumber','gameName','Total at start','Total remaining','dateexported']].copy()
    totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:,'prizeamount'] = "Total"
    print(totals.columns)
      
    #loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame() 
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:]
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])
        print(gameid)
        print(tixtotal)
        print(totalremain)
        prizes =totalremain.loc[:,'prizeamount']
        print(gamerow.columns)

        #add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:,'Current Odds of Top Prize'] = gamerow.loc[:,'topprizeodds']
        gamerow.loc[:,'Change in Current Odds of Top Prize'] =  (gamerow.loc[:,'Current Odds of Top Prize'] - float(gamerow['topprizeodds'].values[0]))/ float(gamerow['topprizeodds'].values[0])      
        gamerow.loc[:,'Current Odds of Any Prize'] = tixtotal/sum(totalremain.loc[:,'Winning Tickets Unclaimed'])
        gamerow.loc[:,'Change in Current Odds of Any Prize'] =  (gamerow.loc[:,'Current Odds of Any Prize'] - float(gamerow['overallodds'].values[0]))/ float(gamerow['overallodds'].values[0])
        gamerow.loc[:,'Odds of Profit Prize'] = tixtotal/sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'])
        startingprofitodds = startingtotal/sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets At Start'])
        gamerow.loc[:,'Starting Odds of Profit Prize'] = startingprofitodds
        gamerow.loc[:,'Change in Odds of Profit Prize'] =  (gamerow.loc[:,'Odds of Profit Prize'] - startingprofitodds)/ startingprofitodds
        gamerow.loc[:,'Probability of Winning Any Prize'] = sum(totalremain.loc[:,'Winning Tickets Unclaimed'])/tixtotal
        startprobanyprize = sum(totalremain.loc[:,'Winning Tickets At Start'])/startingtotal
        gamerow.loc[:,'Starting Probability of Winning Any Prize'] = startprobanyprize
        gamerow.loc[:,'Change in Probability of Any Prize'] =  startprobanyprize - gamerow.loc[:,'Probability of Winning Any Prize']  
        gamerow.loc[:,'Probability of Winning Profit Prize'] = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'])/tixtotal
        startprobprofitprize = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets At Start'])/startingtotal
        gamerow.loc[:,'Starting Probability of Winning Profit Prize'] = startprobprofitprize
        gamerow.loc[:,'Change in Probability of Profit Prize'] =  startprobprofitprize - gamerow.loc[:,'Probability of Winning Profit Prize']
        gamerow.loc[:,'StdDev of All Prizes'] = totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()/tixtotal
        gamerow.loc[:,'StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean()/tixtotal
        gamerow.loc[:,'Odds of Any Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Current Odds of Any Prize']+(totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()*3))
        gamerow.loc[:,'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Odds of Profit Prize']+(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean()*3))
        gamerow.loc[:,'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean())
        
        
        #calculate expected value
        print(totalremain)
        totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
        print(totalremain.loc[totalremain['prizeamount'] != 'Total',:].dtypes)
        print(type(startingtotal))
        print(type(tixtotal))
        print(type(price))
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
        print(gamerow.loc[:,'Non-prize remaining'])
        print(gamerow.loc[:,'Non-prize at start'])
        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
        print(chngLosingTix)
        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
        print(chngAvailPrizes)
        gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0 if pd.isnull(chngLosingTix/chngAvailPrizes).item() == True else chngLosingTix/chngAvailPrizes
                
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
        gamerow.loc[:,'FAQ'] = None
        gamerow.loc[:,'About'] = None
        gamerow.loc[:,'Directory'] = None
        gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']

        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)
        print(currentodds)

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
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    scratchertables.to_csv("./ARscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x'], axis=1, inplace=True)
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
    print(ratingstable)
    print(ratingstable.columns)
    ratingstable['Stats Page'] = "/arkansas-statistics-for-each-scratcher-game"
    ratingstable.to_csv("./ARratingstable.csv", encoding='utf-8')

    
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

    print("\nScraping complete.")
    print("Output files: ARscratcherslist.csv, ARscratchertables.csv")
    
    # For brevity, the full statistical loop is omitted, but the function will return the necessary data.
    return scratchersall, scratchertables

if __name__ == '__main__':
    exportARScratcherRecs()