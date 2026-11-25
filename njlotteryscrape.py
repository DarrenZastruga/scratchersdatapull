#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New Jersey scratch-off scraper (based on nylotteryscrape.py)

@author: michaeljames
"""

import pandas as pd
import os
import urllib.parse
from urllib.parse import urlparse
import urllib.request
import json
import requests
from bs4 import BeautifulSoup
import re
import logging
from datetime import datetime
from dateutil.tz import tzlocal
import lxml
from datetime import date
import numpy as np
import random
from itertools import repeat
from scipy import stats



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


NJ_GAMES_URL = "https://www.njlottery.com/api/v1/instant-games/games/?size=1000"
NJ_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Referer': 'https://www.njlottery.com/en-us/scratch-offs.html',
}


def _nj_date(ms):
    """NJ API dates are epoch milliseconds."""
    try:
        return datetime.fromtimestamp(int(ms) / 1000).strftime('%Y-%m-%d')
    except (TypeError, ValueError):
        return None


def _nj_fetch_games():
    """Download every page of NJ instant games."""
    games, url, pages = [], NJ_GAMES_URL, 0
    while url and pages < 20:
        r = requests.get(url, headers=NJ_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        games.extend(data.get('games', []))
        nxt = data.get('nextPageUrl')
        url = urllib.parse.urljoin('https://www.njlottery.com', nxt) if nxt else None
        pages += 1
    return games


def _nj_photo(game):
    """NJ API usually omits images; use the official per-game ticket asset."""
    raw = game.get('ticketImageUrl') or game.get('imageUrl')
    if raw:
        return urllib.parse.urljoin('https://www.njlottery.com', str(raw))
    gid = str(game.get('gameId') or '').strip()
    if not gid.isdigit():
        return None
    return f"https://www.njlottery.com/content/dam/portal/images/instant-games/{gid.zfill(5)}/ticket.png"


def exportScratcherRecs():
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()
    dateexported = date.today().strftime('%Y-%m-%d')

    for s in _nj_fetch_games():
        # Only games still on sale / still claimable
        if str(s.get('validationStatus', '')).upper() not in ('ACTIVE', 'ENABLED', ''):
            continue
        tiers = s.get('prizeTiers') or []
        totalprinted = s.get('totalTicketsPrinted')
        price_cents = s.get('ticketPrice')
        if not tiers or not totalprinted or not price_cents:
            continue  # skip rather than write price=0 / bad totals

        gameNumber = str(s['gameId'])
        gameName = str(s.get('gameName', '')).strip()
        gamePrice = str(int(price_cents) // 100) if int(price_cents) % 100 == 0 else str(int(price_cents) / 100)
        gameURL = 'https://www.njlottery.com/en-us/scratch-offs/0' + gameNumber + '.html'
        gamePhoto = _nj_photo(s)
        startDate = _nj_date(s.get('launchDate') or s.get('startDistributionDate'))
        lastdatetoclaim = _nj_date(s.get('disableDate'))
        endDate = _nj_date(s.get('endDistributionDate'))

        rows = []
        for t in tiers:
            amt = t.get('prizeAmount')
            start = t.get('winningTickets')
            paid = t.get('paidTickets', 0) or 0
            if amt is None or start is None:
                continue
            rows.append({'prizeamount': int(amt) // 100,
                         'Winning Tickets At Start': int(start),
                         'Winning Tickets Unclaimed': max(int(start) - int(paid), 0),
                         'prizeDescription': t.get('prizeDescription')})
        if not rows:
            continue
        tixdata = pd.DataFrame(rows).sort_values('prizeamount', ascending=False).reset_index(drop=True)
        # Merge tiers with the same cash value (NJ lists several ways to win the same amount)
        tixdata = tixdata.groupby('prizeamount', as_index=False)[['Winning Tickets At Start', 'Winning Tickets Unclaimed']].sum()
        tixdata = tixdata.sort_values('prizeamount', ascending=False).reset_index(drop=True)

        winners_start = tixdata['Winning Tickets At Start'].sum()
        overallodds = float(totalprinted) / float(winners_start)
        topprize = tixdata['prizeamount'].iloc[0]
        topprizestarting = tixdata['Winning Tickets At Start'].iloc[0]
        topprizeremain = tixdata['Winning Tickets Unclaimed'].iloc[0]

        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'startDate', 'lastdatetoclaim', 'overallodds', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, topprizeremain, startDate, lastdatetoclaim, overallodds, gameURL, gamePhoto]

        tixdata['gameNumber'] = gameNumber
        tixdata['gameName'] = gameName
        tixdata['gamePhoto'] = gamePhoto
        tixdata['gameURL'] = gameURL
        tixdata['price'] = gamePrice
        tixdata['overallodds'] = overallodds
        tixdata['topprizeodds'] = float(totalprinted) / float(topprizestarting)
        tixdata['topprize'] = topprize
        tixdata['topprizestarting'] = topprizestarting
        tixdata['topprizeremain'] = topprizeremain
        tixdata['topprizeavail'] = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
        tixdata['startDate'] = startDate
        tixdata['endDate'] = endDate
        tixdata['lastdatetoclaim'] = lastdatetoclaim
        tixdata['extrachances'] = None
        tixdata['secondChance'] = None
        tixdata['dateexported'] = dateexported
        tixtables = pd.concat([tixtables, tixdata], axis=0)

    if tixtables.empty:
        return pd.DataFrame(), pd.DataFrame()

    if 'prizeamount' in tixtables.columns:
        # Ensure it's string type first to use .str accessor
        tixtables['prizeamount'] = tixtables['prizeamount'].astype(str)
        tixtables['prizeamount'] = tixtables['prizeamount'].str.replace('$', '', regex=False)
        tixtables['prizeamount'] = tixtables['prizeamount'].str.replace(',', '', regex=False)
        # Add any other specific string replacements needed for NJ prize amounts
        tixtables['prizeamount'] = tixtables['prizeamount'].str.replace('k annual installments', '000', regex=False)
        tixtables['prizeamount'] = tixtables['prizeamount'].str.replace('/week/life', '', regex=False) # Handle this case too
    # After cleaning, attempt to convert to numeric, coercing errors for non-convertible rows
    tixtables['prizeamount'] = pd.to_numeric(tixtables['prizeamount'], errors='coerce')
     # Fill any NaNs resulting from coercion if necessary, e.g., with 0 or a specific marker
    tixtables['prizeamount'] = tixtables['prizeamount'].fillna(0) # Or handle as you see fit
     
    tixlist.to_csv("./NJtixlist.csv", encoding='utf-8')
    scratchersall = tixtables[['price','gameName','gameNumber','topprize','overallodds','topprizeodds', 'topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','dateexported', 'gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('NJscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./NJscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./NJscratchertables.csv", encoding='utf-8')
    
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds', 'topprizeodds']], how='left', on=['gameNumber'])
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start'].astype(float)*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total remaining']/gamesgrouped['topprizeremain'].astype('float')
    #print(gamesgrouped.loc[:,'topprizeodds'])
    for col in ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
        gamesgrouped[col] = pd.to_numeric(gamesgrouped[col], errors='coerce')
    
    
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

        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])

        prizes =totalremain.loc[:,'prizeamount']


        #add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:,'Current Odds of Top Prize'] = gamerow.loc[:,'topprizeodds']
        gamerow.loc[:,'Change in Current Odds of Top Prize'] =  (gamerow.loc[:,'Current Odds of Top Prize'] - float(gamerow['topprizeodds'].values[0]))/ float(gamerow['topprizeodds'].values[0])      
        gamerow.loc[:,'Current Odds of Any Prize'] = tixtotal/sum(totalremain.loc[:,'Winning Tickets Unclaimed'])
        gamerow.loc[:,'Change in Current Odds of Any Prize'] =  (gamerow.loc[:,'Current Odds of Any Prize'] - float(gamerow['overallodds'].values[0]))/ float(gamerow['overallodds'].values[0])
        gamerow.loc[:,'Odds of Profit Prize'] = tixtotal/sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'])
        startingprofitodds = startingtotal/sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start'])
        gamerow.loc[:,'Starting Odds of Profit Prize'] = startingprofitodds
        gamerow.loc[:,'Change in Odds of Profit Prize'] =  (gamerow.loc[:,'Odds of Profit Prize'] - startingprofitodds)/ startingprofitodds
        gamerow.loc[:,'Probability of Winning Any Prize'] = sum(totalremain.loc[:,'Winning Tickets Unclaimed'])/tixtotal
        startprobanyprize = sum(totalremain.loc[:,'Winning Tickets At Start'])/startingtotal
        gamerow.loc[:,'Starting Probability of Winning Any Prize'] = startprobanyprize
        gamerow.loc[:,'Change in Probability of Any Prize'] =  startprobanyprize - gamerow.loc[:,'Probability of Winning Any Prize']  
        gamerow.loc[:,'Probability of Winning Profit Prize'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'])/tixtotal
        startprobprofitprize = sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start'])/startingtotal
        gamerow.loc[:,'Starting Probability of Winning Profit Prize'] = startprobprofitprize
        gamerow.loc[:,'Change in Probability of Profit Prize'] =  startprobprofitprize - gamerow.loc[:,'Probability of Winning Profit Prize']
        gamerow.loc[:,'StdDev of All Prizes'] = totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()/tixtotal
        gamerow.loc[:,'StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].std().mean()/tixtotal
        gamerow.loc[:,'Odds of Any Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Current Odds of Any Prize']+(totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()*3))
        gamerow.loc[:,'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Odds of Profit Prize']+(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].std().mean()*3))
        gamerow.loc[:,'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].std().mean())
        
        
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
        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
        gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
                
        gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto'].values[0]
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
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)

        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]

    #save scratchers tables
    #scratchertables.to_sql('NJscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./NJscratchertables.csv", encoding='utf-8')
    
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

    ratingstable['Stats Page'] = "/new-jersey-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('NJratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./NJratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    #NJratingssheet = gs.worksheet('NJRatingsTable')
    #NJratingssheet.clear()
    
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

    #set_with_dataframe(worksheet=MDratingssheet, dataframe=ratingstable, include_index=False,
    #include_column_header=True, resize=True)
    print(f"NJ exported {len(ratingstable)} games; {ratingstable['Photo'].astype(bool).sum()} image URLs.")
    return ratingstable, scratchertables


if __name__ == "__main__":
    exportScratcherRecs()
