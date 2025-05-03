#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 23:34:32 2022

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


def exportScratcherRecs():
    url = "https://www.arizonalottery.com/scratchers/#all"
    r = requests.get(url)
    response = r.text
    # print(r.text)
    soup = BeautifulSoup(response, 'html.parser')

    tixlist = pd.DataFrame(columns=['gameName', 'gameNumber', 'price', 'gameURL'])
    table = soup.find_all(class_=['section'])
    logos = soup.find_all(class_=['logo'])

    # print(table)
    tixrow = pd.DataFrame()
    for s in table:
        gamenames = s.find(class_='game-name').get_text(strip=True)
        gameURL = s.find(class_='game-name').get('href')
        gameName = gamenames.partition(' #')[0]
        gameNumber = gamenames.partition(' #')[2]
        gamePrice = s.find(class_='col-md-6 price').find('span').get_text(strip=True)
        try:
            gamePhoto = "https://www.arizonalottery.com"+soup.select_one("img[src*='"+gameNumber+"']")["src"].split('?')[0]
        except:
            gamePhoto = None
            continue
        print(gamenames)
        print(gameName)
        print(gameNumber)
        print(gamePrice)
        print(gameURL)
        print(gamePhoto)
            
        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto']] = [
            gamePrice, gameName, gameNumber, gameURL, gamePhoto]

    #tixlist.to_csv("./AZtixlist.csv", encoding='utf-8')
    
    
    tixtables = pd.DataFrame(columns=['gameNumber','gameName','price','prizeamount','startDate','endDate','lastdatetoclaim','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported'])

    for i in tixlist.loc[:, 'gameNumber']:

        url = "https://api.arizonalottery.com/v2/Scratchers/"+i

        payload = "page=0&totalPages=0&pageSize=150\n"
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        }

        response = requests.get(url=url)
        tixdata = response.json()
        scratcherdata = pd.DataFrame.from_dict(tixdata)

        gameName = scratcherdata['gameName'][0]
        gameNumber = scratcherdata['gameNum'][0]
        gamePrice = scratcherdata['ticketValue'][0]
        startDate = datetime.fromisoformat(scratcherdata['beginDate'][0])
        endDate = None if 'endDate' not in scratcherdata else datetime.fromisoformat(scratcherdata['endDate'][0])
        lastdatetoclaim = datetime.fromisoformat(scratcherdata['lastDate'][0])
        gameOdds = float(scratcherdata['gameOdds'][0])
        dateexported = pd.to_datetime(scratcherdata['dateModified'][0],infer_datetime_format=True)

        print('Looping through each prize tier row for scratcher #'+i)
        for row in scratcherdata['prizeTiers']:
            prizetier = pd.DataFrame.from_dict([row])
            if "prizeAmont" in prizetier:
                prizeamount = prizetier['prizeAmount'][0]
            else: 
                prizeamount = prizetier['displayTitle'][0].replace('$','').replace(',','').replace('\n', '')
            if "Million" in prizeamount:
                prizeamount = int(prizeamount.replace(' Million','000000').replace('.',''))
            else:    
                prizeamount = int(prizeamount)
        
            prizeodds = float(prizetier['odds'][0])
            startingprizecount = int(prizetier['totalCount'][0])
            remainingprizecount = int(prizetier['count'][0])
            tixtables.loc[len(tixtables.index), ['gameNumber','gameName','price','prizeamount','startDate','endDate','lastdatetoclaim',
                                                 'overallodds','prizeodds','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported','tierLevel']] = [gameNumber, gameName, gamePrice, prizeamount, 
                                                                                                                                          startDate, endDate, lastdatetoclaim, gameOdds, prizeodds, startingprizecount, remainingprizecount, dateexported, prizetier['tierLevel'][0]]
        
        #tixtables['gameNumber'] = gameNumber
        index = tixtables[tixtables['gameNumber']==gameNumber].index
        tixtables.loc[index,'gameName'] = gameName
        tixtables.loc[index,'price'] = gamePrice
        topprize = tixtables.loc[(tixtables['tierLevel']==1) & (tixtables['gameNumber']==gameNumber),'prizeamount'].iloc[0]
        topprizeodds = tixtables.loc[(tixtables['tierLevel']==1) & (tixtables['gameNumber']==gameNumber),'prizeodds'].iloc[0]
        topprizeremain = tixtables.loc[(tixtables['tierLevel']==1) & (tixtables['gameNumber']==gameNumber),'Winning Tickets Unclaimed'].iloc[0]
        topprizeavail = 'Top Prize Claimed' if topprizeremain==0 else None

        
        tixtables.loc[index,'topprize'] = topprize
        tixtables.loc[index,'topprizeodds'] = topprizeodds
        tixtables.loc[index,'topprizeremain'] = topprizeremain
        tixtables.loc[index,'topprizeavail'] =  topprizeavail                                                                                                                             
        tixtables.loc[index,'extrachances'] = None
        tixtables.loc[index,'secondChance'] = None
                                                                                                                               
    tixtables.to_csv("./AZprizedata.csv", encoding='utf-8')
    print(tixtables.dtypes)
    scratchersall = tixtables[['price','gameName', 'gameNumber','topprize', 'topprizeodds', 'overallodds', 'topprizeremain','topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported']]
    scratchersall = scratchersall.drop_duplicates(subset=['price','gameName', 'gameNumber','topprize', 'topprizeodds', 'overallodds', 'topprizeremain','topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported'])
    scratchersall = scratchersall.loc[scratchersall['gameNumber']!= "Coming Soon!", :]
    #scratchersall = scratchersall.drop_duplicates()
    # save scratchers list
    #scratchersall.to_sql('azscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./azscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount','Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel', 'dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber']!= "Coming Soon!", :]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    # gamesgrouped = gamesgrouped.copy() # .copy() is often redundant after operations like reset_index
    gamesgrouped = gamesgrouped.merge(scratchersall[[
                                      'gameNumber', 'price', 'topprizeodds', 'overallodds']], how='left', on=['gameNumber'])
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
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped[['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes

        print(gameid)
        print(tixlist.dtypes)
        print(type(gameid))
        print(tixlist.loc[tixlist['gameNumber'].astype('int')==gameid, ['gameName','gameNumber','gamePhoto']])
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber'].astype('int')==gameid,['gamePhoto']].values[0]
        gamerow.loc[:, 'FAQ'] = None
        gamerow.loc[:, 'About'] = None
        gamerow.loc[:, 'Directory'] = None
        gamerow.loc[:, 'Data Date'] = gamerow.loc[:, 'dateexported']

        currentodds = currentodds.append(gamerow, ignore_index=True)

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
                                   'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]
        totalremain = totalremain.append(nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']], ignore_index=True)
        totalremain = totalremain.append(totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']], ignore_index=True)
        print(totalremain.columns)

        # add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount'] != 'Total', :]

        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        print(totalremain)
        alltables = alltables.append(totalremain)

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
    ratingstable.rename(columns={'gameName_x': 'gameName', 'dateexported_x': 'dateexported',
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
    
    
                             
    return ratingstable, scratchertables


#exportScratcherRecs()
