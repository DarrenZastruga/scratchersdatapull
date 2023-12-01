#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 21 19:52:52 2023

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
from bs4 import BeautifulSoup, re
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

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

powers = {'B': 10 ** 9, 'K': 10 ** 3, 'M': 10 ** 6, 'T': 10 ** 12}
# add some more to powers as necessary

def exportOHScratcherRecs():

    urls = ['https://www.ohiolottery.com/Games/ScratchOffs/$1-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$2-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$3-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$5-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/10DollarGames',
                'https://www.ohiolottery.com/Games/ScratchOffs/20DollarGames',
                'https://www.ohiolottery.com/Games/ScratchOffs/$30-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$50-Games']
    
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()
    
    for u in urls:
        r = requests.get(u)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        if soup.find('div', class_='cf moduleContent') == None:
            continue
        else: 
            tables = soup.find('div', class_='cf moduleContent').find_all('li')
            for t in range(len(tables)): 
                #get link from the main page, looping through each page for scratchers in price group
                gameName = tables[t].find('a', class_='igName').text
                print(gameName)
                gameNumber = tables[t].text.strip().split('#')[1]
                print(gameNumber)
                gameURL= 'https://www.ohiolottery.com'+str(tables[t].find('a', class_='igName').get('href'))
                print(gameURL)
                def gamecost(i):
                    switcher = {
                        'https://www.ohiolottery.com/Games/ScratchOffs/$1-Games': 1,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$2-Games': 2,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$3-Games': 3,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$5-Games': 5,
                        'https://www.ohiolottery.com/Games/ScratchOffs/10DollarGames': 10,
                        'https://www.ohiolottery.com/Games/ScratchOffs/20DollarGames': 20,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$30-Games': 30,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$50-Games': 50
                    }
                    return switcher.get(i, "Invalid games url")
    
                gamePrice = gamecost(str(u))
                print(gamePrice)
                r = requests.get(gameURL)
                response = r.text
                soup = BeautifulSoup(response, 'html.parser')
                overallodds = soup.find('div',class_='mobileToggleContent').find('p', class_='odds').text.replace('Overall odds of winning: ', '').replace('1 in ', '')
                if overallodds == '': 
                    continue
                else:
                
                    gamePhoto = 'https://www.ohiolottery.com'+str(soup.find('div',class_='igTicketImg').get('style').replace('background-image: url(', '').replace(');', ''))
                    print(overallodds)
                    print(gamePhoto)
                    table = pd.read_html(str(soup.find('div',class_='tbl_PrizesRemaining').find('table')))[0]
                    table.columns = table.columns.droplevel(0)
                    table.rename(columns={'Prizes':'prizeamount','Remaining':'Winning Tickets Unclaimed'}, inplace=True)
                    table.drop(labels={'Unnamed: 2_level_1'}, axis=1,inplace=True)
                    table['gameName'] = gameName
                    table['gameNumber'] = gameNumber
                    table['prizeamount'] = table['prizeamount'].str.replace('$', '', regex=False).str.replace(',','', regex=False).str.strip().str.replace('.00','', regex=False)
                    table['prizeamount'] = table['prizeamount'].replace({'40K/YR FOR 25 YRS':40000*25, '8333MONTH(100K/YR/10YRS)':100000*10, '1000000 (40k/yr/25yrs)':40000*25, '200K/YR FOR 25 YRS': 200000*25, 
                                                                             '80K/YR FOR 25 YRS': 80000*25, '250000YEAR(250K/YR/10YRS)':250000*10, '5000000(200K/YR/25YRS)':200000*25, '36.5K/YR FOR 10 YRS':36500*10, 
                                                                             '1000WEEK(52K/YR/10YRS)': 52000*10, '1M/YR FOR 20 YRS': 1000000*20, '2000000(80K/YR/25YRS)': 80000*25, '400K/YR FOR 25 YRS': 400000*25, 
                                                                             '1000000(40K/YR/25YRS)':40000*25, '1000000(40k/yr/25yrs)':40000*25, '10K/MO FOR 20 YRS': 10000*12*20, '250K/YR FOR 20 YRS': 250000*20, '50/DY FOR 20 YRS': 50*365*20, 
                                                                             '50K/YR FOR 20 YRS':50000*20, '2000000(80k/YR/25YRS)': 80*25, '2000000 (80K/YR/25YRS)': 80*25, 'ENTRY':gamePrice, 'ENTRY TICKET': gamePrice}).astype('int')
                    table['Winning Tickets At Start'] = table['Winning Tickets Unclaimed'].astype(float)
                    topprize = table.loc[0,'prizeamount']
                    topprizestarting = table.loc[0,'Winning Tickets At Start']
                    topprizeremain = table.loc[0,'Winning Tickets Unclaimed'] 
                    topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
                    startDate = None
                    endDate = None
                    extrachances = None
                    secondChance = None
                    dateexported = date.today()
                    table['dateexported'] = dateexported
                    print(topprizeremain)
                    print(table)
                    tixtables = pd.concat([tixtables, table], axis=0)
                    
                    tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'extrachances', 'secondChance', 'dateexported']] = [
                        gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, overallodds, topprizestarting, topprizeremain, topprizeavail, startDate, endDate, extrachances, secondChance, dateexported]
        
    r = requests.get('https://www.ohiolottery.com/Games/ScratchOffs/Last-Day-to-Redeem')
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')
    lastdaytbl = pd.read_html(str(soup.find('div', class_='cf moduleContent').find('table', class_='purple_table igLDTR_tbl')))[0]
    lastdaytbl.rename(columns={'Game Name':'gameName', 'Game #':'gameNumber', 'Cost': 'gamePrice', 'Last Day to Redeem': 'lastdatetoclaim'}, inplace=True)
    lastdaytbl['gameNumber'] = lastdaytbl['gameNumber'].astype('str')
    tixlist = tixlist.merge(lastdaytbl[['gameNumber', 'lastdatetoclaim']], how="left", on= "gameNumber")
    print(tixlist)
    print(tixlist.columns)

   #tixlist = tixlist.loc[tixlist['overallodds'] != '',:]  
   # print(tixlist.loc[tixlist['overallodds'].isna()==False, :])
    #tixlist['overallodds'] = tixlist['overallodds'].astype(float)
    tixlist.to_csv("./OHtixlist.csv", encoding='utf-8')
    scratchersall = tixlist.loc[:,['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','gamePhoto','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('OHscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./OHscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./OHscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:, ['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    print(gamesgrouped.loc[:,'Non-prize at start'])
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
    nonprizetix = gamesgrouped.loc[:, ['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped.loc[:, ['gameNumber','gameName','Total at start','Total remaining','dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:,'prizeamount'] = "Total"
    print(totals.columns)
      
    #loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame() 
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:]
        print(gameid)
        print(gamerow)
        print(gamerow.loc[:, 'Total at start'].values[0])
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])
        print(gameid)
        print(tixtotal)
        print(totalremain)
        prizes = totalremain.loc[:,'prizeamount']
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

        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0       
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
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
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('OHscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./OHscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
    ratingstable['Stats Page'] = "/ohio-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('OHratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./OHratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    #OHratingssheet = gs.worksheet('OHRatingsTable')
    #OHratingssheet.clear()
    
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
       'Data Date','Stats Page']]
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
    print(ratingstable)
    #set_with_dataframe(worksheet=OHratingssheet, dataframe=ratingstable, include_index=False,
    #include_column_header=True, resize=True)
    return ratingstable, scratchertables

exportOHScratcherRecs()
