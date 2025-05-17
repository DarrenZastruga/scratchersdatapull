#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 19 23:24:22 2025

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


'''
logging.basicConfig()
 
DATABASE_URL = 'postgres://wgmfozowgyxule:8c7255974c879789e50b5c05f07bf00947050fbfbfc785bd970a8bc37561a3fb@ec2-44-195-16-34.compute-1.amazonaws.com:5432/d5o6bqguvvlm63'
print(DATABASE_URL)

#replace 'postgres' with 'postgresql' in the database URL since SQLAlchemy stopped supporting 'postgres' 
SQLALCHEMY_DATABASE_URI = DATABASE_URL.replace('postgres://', 'postgresql://')
conn = psycopg2.connect(SQLALCHEMY_DATABASE_URI, sslmode='require')
engine = create_engine(SQLALCHEMY_DATABASE_URI)
'''

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

powers = {'B': 10 ** 9, 'K': 10 ** 3, 'M': 10 ** 6, 'T': 10 ** 12}
# add some more to powers as necessary


def formatstr(s):
    try:
        power = s[-1]
        if (power.isdigit()):
            return s
        else:
            return float(s[:-1]) * powers[power]
    except TypeError:
        return s


# ... (keep constants, formatstr function, etc.) ...

def exportScratcherRecs():

    # Initialize DataFrames BEFORE the loop
    tixlist = pd.DataFrame(columns=['price', 'gameName', 'gameNumber','topprize','gameURL','gamePhoto', 'overallodds', 'topprizeodds', 'startDate', 'lastdatetoclaim'])
    tixtables = pd.DataFrame()

    # Outer Loop (Iterates through pages 0-5) - Using your reliable structure
    for page_num in range(0, 6):
        url = f"https://dclottery.com/dc-scratchers?play_styles=All&theme=All&page={page_num}"
        print(f"Scraping page: {page_num} - URL: {url}")
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            response = r.text
            # Parse the LISTING page soup
            soup_list_page = BeautifulSoup(response, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_num}: {e}")
            continue

        # Find all game containers on THIS listing page - Using your reliable method
        game_containers = soup_list_page.find_all(class_='node__content')
        print(f"  Found {len(game_containers)} game containers on page {page_num}.")

        # Inner Loop 1 (Iterates through games ON THE CURRENT page)
        for s in game_containers: # s is a game container from the listing page
            # --- Extract basic game info from the LISTING page container 's' ---
            if s.find('h3', class_='teaser__title') is None:
                continue

            gameName_tag = s.find('h3', class_='teaser__title').find('span')
            gameName = gameName_tag.string if gameName_tag else None

            gameNumber_tag = s.find(class_='field field_game_number')
            gameNumber = gameNumber_tag.find(class_='field__item').text if gameNumber_tag and gameNumber_tag.find(class_='field__item') else None

            gameURL_tag = s.find('a', class_='teaser__image')
            gameURL = 'https://dclottery.com' + gameURL_tag.get('href') if gameURL_tag else None

            gamePhoto_style = s.find(class_='teaser__image-background')
            gamePhoto = None
            if gamePhoto_style and 'style' in gamePhoto_style.attrs:
                 style_text = gamePhoto_style['style']
                 # Use regex to reliably extract URL, handling potential quotes
                 match = re.search(r'url\([\'\"]?(.*?)[\'\"]?\)', style_text)
                 if match:
                    gamePhoto = 'https://dclottery.com' + match.group(1)


            gamePrice_tag = s.find('a', class_='teaser__image')
            gamePrice = gamePrice_tag.find(class_='field--name-field-price').text.replace('$', '').strip() if gamePrice_tag and gamePrice_tag.find(class_='field--name-field-price') else None

            if not all([gameName, gameNumber, gameURL, gamePrice]):
                print(f"Skipping game due to missing basic info on page {page_num}")
                continue

            print(f"\nProcessing Game: {gameName} ({gameNumber})")
            print(f"  Price: ${gamePrice}")
            print(f"  URL: {gameURL}")
            # print(f"  Photo: {gamePhoto}") # Keep photo URL shorter in logs

            # --- NOW, Fetch and process prize details from the DETAIL page (gameURL) ---
            try:
                r_detail = requests.get(gameURL, timeout=20)
                r_detail.raise_for_status()
                response_detail = r_detail.text
                # Parse the DETAIL page soup
                soup_detail = BeautifulSoup(response_detail, 'html.parser')

                # --- ROBUST TABLE FINDING LOGIC (Applied to soup_detail) ---
                table_html = None
                print(f"  Searching for prize table on detail page: {gameURL}")

                # Attempt 1: Find table by common class(es) ON DETAIL PAGE
                # **INSPECT THE DETAIL PAGE HTML** and update these classes if needed.
                possible_table_classes = ['views-table', 'cols-3', 'sticky-enabled', 'datatable']
                for table_class in possible_table_classes:
                    # Use a lambda function for more flexible class matching (contains)
                    found_tables = soup_detail.find_all('table', class_=lambda x: x and table_class in x.split())
                    # Check if any found table has the expected headers
                    for tbl in found_tables:
                        headers = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
                        if 'prize amount' in headers and 'total prizes' in headers: # Add more required headers if needed
                            print(f"    Found table using class containing '{table_class}' and matching headers.")
                            table_html = tbl
                            break
                    if table_html:
                        break

                # Attempt 2: Find based on table header content (more robust if classes change)
                if table_html is None:
                    print("    Table class search failed or didn't match headers. Trying header content search...")
                    all_tables = soup_detail.find_all('table')
                    for tbl in all_tables:
                        headers = [th.get_text(strip=True).lower() for th in tbl.find_all('th')]
                        if 'prize amount' in headers and 'total prizes' in headers and 'prizes remaining' in headers:
                            print("    Found table based on header content.")
                            table_html = tbl
                            break

                # Attempt 3: Find using a container ID or Class ON DETAIL PAGE
                # **INSPECT THE DETAIL PAGE HTML** for a reliable container DIV/SECTION around the table
                # if table_html is None:
                #    print("    Header content search failed. Trying container search...")
                #    container = soup_detail.find('div', id='game-prize-details') # EXAMPLE ID - REPLACE
                #    # container = soup_detail.find('section', class_='prize-table-section') # EXAMPLE CLASS - REPLACE
                #    if container:
                #        table_html = container.find('table')
                #        if table_html:
                #             print("    Found table within specified container.")
                #    else:
                #         print("    Specified container not found.")

                # --- END OF ROBUST TABLE FINDING LOGIC ---

                if table_html is None:
                    print(f"  WARNING: Could not find prize table HTML for {gameName} ({gameNumber}) on its detail page. Skipping prize details.")
                    # Log this game to tixlist with missing prize info
                    tixlist_row = {'price': gamePrice, 'gameName': gameName, 'gameNumber': gameNumber, 'topprize': None, 'gameURL': gameURL, 'gamePhoto': gamePhoto, 'overallodds': None, 'topprizeodds': None, 'startDate': None, 'lastdatetoclaim': None}
                    tixlist = pd.concat([tixlist, pd.DataFrame([tixlist_row])], ignore_index=True)
                    continue # Skip to the next game in the loop

                # --- Proceed with processing table_html ---
                try:
                    tixdata_list = pd.read_html(str(table_html))
                    if not tixdata_list:
                         print(f"  WARNING: pd.read_html found no tables in the found HTML for {gameName} ({gameNumber}). Skipping prize details.")
                         # Log to tixlist...
                         continue

                    tixdata = tixdata_list[0]
                    print(f"    Successfully read table data for {gameName} ({gameNumber}).")
                    print(f"      Raw columns found by pandas: {tixdata.columns.tolist()}") # Debug output

                except ValueError as ve:
                     print(f"  ERROR: pd.read_html failed for {gameName} ({gameNumber}). ValueError: {ve}. Skipping prize details.")
                     # Log to tixlist...
                     continue
                except Exception as e:
                    print(f"  ERROR: An unexpected error occurred during pd.read_html for {gameName} ({gameNumber}): {e}")
                    # Log to tixlist...
                    continue

                # Check if expected columns exist BEFORE rename (Adapt based on Raw Columns printout)
                # Use lower case for comparison flexibility
                raw_columns_lower = [str(col).lower() for col in tixdata.columns]
                # Find the actual column names matching the expected content (case-insensitive)
                try:
                    prize_col = tixdata.columns[[ 'prize amount' in str(col).lower() for col in tixdata.columns]][0]
                    start_col = tixdata.columns[[ 'total prizes' in str(col).lower() for col in tixdata.columns]][0]
                    remain_col = tixdata.columns[[ 'prizes remaining' in str(col).lower() for col in tixdata.columns]][0]
                    print(f"      Mapping columns: '{prize_col}'->prizeamount, '{start_col}'->Start, '{remain_col}'->Unclaimed")
                except IndexError:
                    print(f"  WARNING: Table for {gameName} ({gameNumber}) is missing expected column content (Prize Amount, Total Prizes, Prizes Remaining). Found: {tixdata.columns.tolist()}. Skipping prize processing.")
                    # Log to tixlist...
                    continue

                # --- The rest of your prize data processing using the identified column names ---
                tixdata.rename(columns={prize_col: 'prizeamount', start_col: 'Winning Tickets At Start', remain_col: 'Winning Tickets Unclaimed'}, inplace=True)

                # Convert to string before cleaning
                tixdata['prizeamount'] = tixdata['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True)
                tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets At Start'].astype(str).str.replace(r'[,]', '', regex=True)
                tixdata['Winning Tickets Unclaimed'] = tixdata['Winning Tickets Unclaimed'].astype(str).str.replace(r'[,]', '', regex=True)

                # Keep original for topprize calculations first
                tixdata_numeric = tixdata[pd.to_numeric(tixdata['prizeamount'], errors='coerce').notna()].copy()

                if tixdata_numeric.empty:
                    print(f"    WARNING: No numeric prize amounts found after filtering for {gameName} ({gameNumber}). Skipping appending to tixtables.")
                    # Log to tixlist...
                    continue

                tixdata_numeric['gameNumber'] = gameNumber
                tixdata_numeric['gameName'] = gameName
                tixdata_numeric['price'] = gamePrice

                # --- Extract other details from the DETAIL page (soup_detail) ---
                overallodds_tag = soup_detail.find(class_='field field--name-field-odds field--type-string field--label-above')
                overallodds = None
                if overallodds_tag and overallodds_tag.find(class_='field__item'):
                    overallodds_text = overallodds_tag.find(class_='field__item').text.replace('1:', '').replace('1 in ','').replace(',', '').strip()
                    # Your existing logic to handle potential periods/typos
                    if overallodds_text.count('.') > 1:
                         parts = overallodds_text.split('.')
                         if len(parts) > 1 and parts[1].replace('.','').isdigit():
                            overallodds = parts[1]
                         else:
                            overallodds = overallodds_text # Fallback
                    elif overallodds_text.replace('.', '').isdigit():
                         overallodds = overallodds_text
                    else:
                         print(f"    WARNING: Could not parse overall odds '{overallodds_text}' for {gameName}")
                print(f"    Overall Odds raw: {overallodds}")
                tixdata_numeric['overallodds'] = overallodds


                topprizeodds_tag = soup_detail.find(class_='field field--name-field-top-prize-odds field--type-string field--label-above')
                topprizeodds = None
                if topprizeodds_tag and topprizeodds_tag.find(class_='field__item'):
                    topprizeodds = topprizeodds_tag.find(class_='field__item').text.replace('1:', '').replace('1 in ','').replace(',', '').replace(':','').strip()
                print(f"    Top Prize Odds raw: {topprizeodds}")
                # Add topprizeodds to tixdata_numeric later

                startDate_tag = soup_detail.find(class_='field field--name-field-date field--type-daterange field--label-above')
                startDate = startDate_tag.find(class_='field__item').text if startDate_tag and startDate_tag.find(class_='field__item') else None

                lastdatetoclaim_tag = soup_detail.find(class_='field field--name-field-last-date-to-claim field--type-datetime field--label-above')
                lastdatetoclaim = lastdatetoclaim_tag.find(class_='field__item').text if lastdatetoclaim_tag and lastdatetoclaim_tag.find(class_='field__item') else None
                endDate = None

                # Calculate Top Prize details from the original tixdata
                if not tixdata.empty:
                     topprize = tixdata['prizeamount'].iloc[0]
                     topprizestarting = pd.to_numeric(tixdata['Winning Tickets At Start'].iloc[0], errors='coerce')
                     topprizeremain = pd.to_numeric(tixdata['Winning Tickets Unclaimed'].iloc[0], errors='coerce')
                     topprizeavail = 'Top Prize Claimed' if pd.notna(topprizeremain) and topprizeremain == 0 else np.nan
                else:
                     topprize, topprizestarting, topprizeremain, topprizeavail = None, None, None, np.nan

                # Add remaining columns to tixdata_numeric
                tixdata_numeric['topprize'] = topprize
                tixdata_numeric['topprizeodds'] = topprizeodds # Added here
                tixdata_numeric['topprizestarting'] = topprizestarting
                tixdata_numeric['topprizeremain'] = topprizeremain
                tixdata_numeric['topprizeavail'] = topprizeavail
                tixdata_numeric['startDate'] = startDate
                tixdata_numeric['endDate'] = endDate
                tixdata_numeric['lastdatetoclaim'] = lastdatetoclaim
                tixdata_numeric['extrachances'] = None
                tixdata_numeric['secondChance'] = None
                tixdata_numeric['dateexported'] = date.today()

                # Handle special cases
                if gameNumber == '1533':
                    tixdata_numeric['overallodds'], tixdata_numeric['topprizeodds'] = tixdata_numeric['topprizeodds'], tixdata_numeric['overallodds']
                elif gameNumber == '1521':
                    # Ensure overallodds is checked correctly before modifying
                    if 'overallodds' in tixdata_numeric.columns and pd.notna(tixdata_numeric['overallodds'].iloc[0]) and tixdata_numeric['overallodds'].iloc[0] == '399':
                        tixdata_numeric['overallodds'] = '3.99'


                # Append the processed prize table for THIS game to tixtables
                tixtables = pd.concat([tixtables, tixdata_numeric], ignore_index=True)
                print(f"    Appended {len(tixdata_numeric)} rows to tixtables for {gameName}. Total tixtables rows: {len(tixtables)}")

                # Add the summary row for THIS game to tixlist
                tixlist_row = {
                    'price': gamePrice, 'gameName': gameName, 'gameNumber': gameNumber,
                    'topprize': topprize, 'gameURL': gameURL, 'gamePhoto': gamePhoto,
                    'overallodds': overallodds, 'topprizeodds': topprizeodds,
                    'startDate': startDate, 'lastdatetoclaim': lastdatetoclaim,
                    'endDate': endDate
                }
                tixlist = pd.concat([tixlist, pd.DataFrame([tixlist_row])], ignore_index=True)

            except requests.exceptions.RequestException as e:
                print(f"  Error fetching detail page for {gameName} ({gameNumber}): {e}")
                # Log error to tixlist
                tixlist_row = {'price': gamePrice, 'gameName': gameName, 'gameNumber': gameNumber, 'topprize': 'FETCH_ERROR', 'gameURL': gameURL, 'gamePhoto': gamePhoto, 'overallodds': None, 'topprizeodds': None, 'startDate': None, 'lastdatetoclaim': None}
                tixlist = pd.concat([tixlist, pd.DataFrame([tixlist_row])], ignore_index=True)
            except Exception as e:
                 print(f"  An unexpected error occurred processing details for {gameName} ({gameNumber}): {e}")
                 import traceback
                 traceback.print_exc()
                 # Log error to tixlist
                 tixlist_row = {'price': gamePrice, 'gameName': gameName, 'gameNumber': gameNumber, 'topprize': 'PROCESSING_ERROR', 'gameURL': gameURL, 'gamePhoto': gamePhoto, 'overallodds': None, 'topprizeodds': None, 'startDate': None, 'lastdatetoclaim': None}
                 tixlist = pd.concat([tixlist, pd.DataFrame([tixlist_row])], ignore_index=True)

        # --- End of Inner Loop 1 (game loop) ---
    # --- End of Outer Loop (page loop) ---

    print("\nFinished Scraping.")
    print(f"Total unique games found in tixlist: {len(tixlist['gameNumber'].unique())}")
    print(f"Total prize rows found in tixtables: {len(tixtables)}")

    # Remove duplicates just in case any slip through
    tixlist.drop_duplicates(subset=['gameNumber'], keep='last', inplace=True)
    # Ensure gameNumber/prizeamount exist before dropping duplicates
    if 'gameNumber' in tixtables.columns and 'prizeamount' in tixtables.columns:
        tixtables.drop_duplicates(subset=['gameNumber', 'prizeamount'], keep='last', inplace=True)
    else:
        print("WARNING: Cannot drop duplicates in tixtables as 'gameNumber' or 'prizeamount' is missing.")


    # --- Calculations Section ---
    print("\nStarting Calculations...")

    # Check if tixtables is empty or lacks necessary columns BEFORE calculations
    required_cols = ['gameNumber', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'price', 'overallodds']
    if tixtables.empty or not all(col in tixtables.columns for col in required_cols):
        print("WARNING: tixtables is empty or missing required columns for calculation. Returning empty DataFrames.")
        final_rating_cols = ['price', 'gameName','gameNumber', 'topprize', ...] # Define expected columns
        scratchertables_cols = ['gameNumber', 'gameName', 'prizeamount', ...] # Define expected columns
        # Ensure the required columns exist even if empty for consistency downstream
        return pd.DataFrame(columns=final_rating_cols), pd.DataFrame(columns=scratchertables_cols)


    # Filter out non-numeric prize amounts
    tixtables['prizeamount'] = tixtables['prizeamount'].astype(str)
    tixtables = tixtables[~tixtables['prizeamount'].str.contains('ticket', case=False, na=False)].copy() # Use .copy()


    # Convert columns needed for calculations to numeric, coercing errors
    numeric_cols_tixtables = ['price', 'overallodds', 'topprizeodds', 'topprizestarting', 'topprizeremain', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']
    for col in numeric_cols_tixtables:
        if col in tixtables.columns:
             tixtables[col] = pd.to_numeric(tixtables[col], errors='coerce')

    # Drop rows where essential numeric conversions failed
    tixtables.dropna(subset=['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'price', 'overallodds'], inplace=True)

    if tixtables.empty:
        print("WARNING: tixtables became empty after numeric conversion and NaN dropping. Cannot proceed. Returning empty DataFrames.")
        # Return empty DataFrames...
        return pd.DataFrame(columns=final_rating_cols), pd.DataFrame(columns=scratchertables_cols) # Use previously defined cols


    scratchersall = tixtables[['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizeodds', 'topprizestarting', 'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported']].copy()
    scratchersall = scratchersall.loc[scratchersall['gameNumber'].astype(str) != "Coming Soon!", :]
    scratchersall.drop_duplicates(inplace=True)

    # Save scratchers list (optional)
    # scratchersall.to_csv("./DCscratcherslist.csv", encoding='utf-8')

    # Prepare scratchertables for grouping
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']].copy()
    scratchertables = scratchertables.loc[scratchertables['gameNumber'].astype(str) != "Coming Soon!", :]

    # Ensure correct types before grouping
    try:
        scratchertables = scratchertables.astype({'prizeamount': 'float', 'Winning Tickets At Start': 'int64', 'Winning Tickets Unclaimed': 'int64'})
    except Exception as e:
         print(f"Error converting scratchertables types before grouping: {e}")
         # Handle or raise

    # Grouping
    scratchertables.dropna(subset=['gameNumber'], inplace=True)
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True, dropna=False)[['Winning Tickets At Start', 'Winning Tickets Unclaimed']].sum().reset_index()
 
    #save scratchers list
    #scratchersall.to_sql('DCscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./DCscratcherslist.csv", encoding='utf-8')
    
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','topprizeodds','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped[['gameNumber','topprizeodds','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.loc[:,'Total at start'] = None if ((overallodds==None) & (gamesgrouped['topprizeodds'].iloc[0]==None)) else gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = None if ((overallodds==None) & (gamesgrouped['topprizeodds'].iloc[0]==None)) else gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
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
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:]
        startingtotal = float(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = float(gamerow.loc[:, 'Total remaining'].values[0])
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
        print(totalremain.loc[:,'Starting Expected Value'])
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']].copy()
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
        gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
                
        gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto'].values[0]
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
        
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('DCscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./DCscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
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
    print(ratingstable)
    print(ratingstable.columns)
    ratingstable['Stats Page'] = "/dc-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('DCratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./DCratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    #DCratingssheet = gs.worksheet('DCRatingsTable')
    #DCratingssheet.clear()
    
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
    #set_with_dataframe(worksheet=DCratingssheet, dataframe=ratingstable, include_index=False,
    #include_column_header=True, resize=True)
    return ratingstable, scratchertables

#exportScratcherRecs()