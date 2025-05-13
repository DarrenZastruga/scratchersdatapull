#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug  4 23:24:20 2023

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
from lxml import etree
from pandas import json_normalize

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

powers = {'B': 10 ** 9, 'K': 10 ** 3, 'M': 10 ** 6, 'T': 10 ** 12}
# add some more to powers as necessary

def exportScratcherRecs():
    url = "https://api2.oregonlottery.org/instant/GetAll"

    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36',
        'Ocp-Apim-Subscription-Key': '683ab88d339c4b22b2b276e3c2713809'
    }
    r = requests.request("GET", url, headers=headers, data=payload)

    responsejson = r.json()
    tixlist = pd.DataFrame()
    linkslist = pd.DataFrame()
    tixtables = pd.DataFrame(columns=['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed','topprizestarting', 'dateexported'])

    
    #get the game URLs from script tag on the main page and add them to a dataframe to be merged with the data from JSON
    url = "https://www.oregonlottery.org/scratch-its/grid/"
    r = requests.get(url)
    responsehtml = r.text
    soup = BeautifulSoup(responsehtml, 'html.parser')
    scripttag = soup.find('body').find('main', class_='ol-content').find('script')
    links = scripttag.string.split('scratchIts = ')[1].split(';')[0]
    links = json.loads(links)
    for tic in links:
        gameURL = tic['link'].replace('//','/')
        gameID = str(tic['number'])
        gamePhoto = tic['image_preview'][0]
        linkslist.loc[len(linkslist.index), ['gameNumber', 'gameURL', 'gamePhoto']] = [gameID, gameURL, gamePhoto]

    
    # loop through each game in the JSON data and add to the Tixlist dataframe
    for game in responsejson:

        gameName = game['GameNameTitle']
        gameNumber = str(game['GameNumber'])
        gamePrice = game['TicketPrice']
        topprize = game['TopPrize']
        topprizeremain = game['TopPrizesRemaining']
        topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
        startDate = game['DateAvailable']
        endDate = game['GameEndDate']
        lastdatetoclaim = game['ValidationEndDate']
        overallodds = game['OverallOdds']
        extrachances = None
        secondChance = '2nd Chance' if game['SecondChanceDrawDate'] else None
        dateexported = date.today()

        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'lastdatetoclaim', 'overallodds', 'extrachances', 'secondChance', 'dateexported']] = [
            gamePrice, gameName, gameNumber, topprize, topprizeremain, topprizeavail, startDate, endDate, lastdatetoclaim, overallodds, extrachances, secondChance, dateexported]
        
    #merge gameURLs with tix list to start looping through each game page, to get remaining tickets data
    tixlist = pd.merge(tixlist, linkslist, how='left', on=['gameNumber'])
    tixlist['lastdatetoclaim'] = pd.to_datetime(tixlist['lastdatetoclaim'])
    tixlist['startDate'] = pd.to_datetime(tixlist['startDate'])
    tixlist['dateexported'] = pd.to_datetime(tixlist['dateexported'])
    tixlist = tixlist.loc[(tixlist['startDate'] <= datetime.today()) & (tixlist['lastdatetoclaim'] >= datetime.today())]    
    
    tixlist.to_csv("./ORtixlist.csv", encoding='utf-8')
    tixlist = tixlist.loc[(tixlist['startDate'] <= datetime.today()) & (tixlist['lastdatetoclaim'] >= datetime.today())]    
 
    print(range(len(tixlist)))
    for game in tixlist.loc[:,'gameNumber']:
        print(game)
        url = 'https://api2.oregonlottery.org/instant/GetGame?includePrizeTiers=true&gameNumber='+str(game)

        headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36',
        'Ocp-Apim-Subscription-Key': '683ab88d339c4b22b2b276e3c2713809'
        }
        r = requests.request("GET", url, headers=headers, data=payload)
        responsejson = r.json()

        # go down to next level of json response for numbers of prizes
        tixdata = pd.json_normalize(responsejson[0]['PrizeTiers'])
        
        if tixdata.empty!=True:  
            tixdata.rename(columns={'PrizeAmount': 'prizeamount','PrizesTotal': 'Winning Tickets At Start', 'PrizesRemaining': 'Winning Tickets Unclaimed'}, inplace=True)
            tixdata['gameNumber'] = game
            tixdata['gameName'] = tixlist.loc[tixlist['gameNumber']==game, 'gameName'].iloc[0]
            tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets At Start'].astype(int)
            tixdata['Winning Tickets Unclaimed'] = tixdata['Winning Tickets Unclaimed'].astype(int)
            tixdata['topprizestarting'] = tixdata.loc[0, 'Winning Tickets At Start']
            tixdata['dateexported'] = date.today()

            tixtables = pd.concat([tixtables, tixdata], axis=0)

        else:
            continue


    tixtables['Winning Tickets At Start'] = tixtables['Winning Tickets At Start'].astype(int)
    tixtables['Winning Tickets Unclaimed'] = tixtables['Winning Tickets Unclaimed'].astype(int)
                
    tixlist = tixlist.loc[(tixlist['startDate'] <= datetime.today()) & (tixlist['lastdatetoclaim'] >= datetime.today())]    
    tixlist.to_csv("./ORtixlist.csv", encoding='utf-8')
    
    scratchersall = tixlist[['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('OKscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./ORscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes

    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed','topprizestarting', 'dateexported']]

    scratchertables.to_csv("./ORscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall

     # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    gamesgrouped = gamesgrouped.merge(scratchersall[[
                                      'gameNumber', 'price', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :]
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
            'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])

        prizes = totalremain.loc[:, 'prizeamount']

        # add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:, 'Current Odds of Top Prize'] = gamerow.loc[:,
                                                                  'topprizeodds']
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
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
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

    scratchertables = alltables[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]


    # save scratchers tables
    #scratchertables.to_sql('OKscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./ORscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizeremain_x'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
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

    # save ratingstable

    ratingstable['Stats Page'] = "/oregon-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('OKratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./ORratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    #ORratingssheet = gs.worksheet('ORRatingsTable')
    #ORratingssheet.clear()
    ratingstable = ratingstable[['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance',
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
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('', inplace=True)

    #set_with_dataframe(worksheet=ORratingssheet, dataframe=ratingstable, include_index=False,
    #                   include_column_header=True, resize=True)
    return ratingstable, scratchertables

#exportScratcherRecs()