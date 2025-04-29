#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  4 08:16:13 2022

@author: michaeljames
"""

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
    
    tixlist = pd.DataFrame()
    tixtables = pd.DataFrame()
    
    #dclottery puts only 20 games on a page, so loop through each of 7 pages
    for i in range(0,6):
        url = "https://dclottery.com/dc-scratchers?play_styles=All&theme=All&page="+str(i)
        r = requests.get(url)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')

        tixinfo = pd.DataFrame()
        table = soup.find_all(class_='node__content')

        #loop through each game on the page
        for s in table:
            if s.find('h3', class_='teaser__title')==None:
                continue
            else:
                gameName = s.find('h3', class_='teaser__title').find('span').string
                gameNumber = s.find(class_='field field_game_number').find(class_='field__item').text
            
                gameURL = 'https://dclottery.com'+s.find('a', class_='teaser__image').get('href')
                print(gameName)
                gamePhoto = 'https://dclottery.com'+s.find(class_='teaser__image-background')['style'].replace('background-image:url(','').replace(')','')
                print(gamePhoto)
                gamePrice = s.find('a', class_='teaser__image').find(class_='field--name-field-price').text.replace('$','').strip()
                topprize = None
                
                print(gameName)
                print(gameNumber)
                print(gameURL)
                print(gamePrice)
                print(topprize)
             
                tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','topprize','gameURL','gamePhoto']] = [
                    gamePrice, gameName, gameNumber, topprize, gameURL, gamePhoto]
                
                for i in tixlist.loc[:,'gameNumber']:
                    url = gameURL
                    r = requests.get(url)
                    response = r.text
                    soup = BeautifulSoup(response, 'html.parser')
        
                    #get the table of ticket numbers
                    tixdata = pd.read_html(str(soup.find(class_='view-content').find('table')))[0]
                    #check if overall odds exists, if not we'll ignore this game
                    overallodds = None if soup.find(class_='field field--name-field-odds field--type-string field--label-above')==None else soup.find(class_='field field--name-field-odds field--type-string field--label-above').find(class_='field__item').text.replace('1:','').replace(',','')
                    
                    #count number of periods in odds number to check if more than one due to type (period instead of colon) and then replace
                    if overallodds != None:
                        overallodds = overallodds[2:] if (overallodds.count('.')>1) else overallodds
                    if (len(tixdata) == 0) & (overallodds == None):
                        tixtables = tixtables.append([])
                    else:
                        tixdata.rename(columns={'Prize Amount':'prizeamount','Total Prizes': 'Winning Tickets At Start', 'Prizes Remaining': 'Winning Tickets Unclaimed'}, inplace=True)
                        print(tixdata.columns)
                        tixdata['prizeamount'] = tixdata['prizeamount'].str.replace('$','',regex=False).str.replace(',','',regex=False)
                        tixdata['gameNumber'] = gameNumber
                        tixdata['gameName'] = gameName
                        tixdata['gamePhoto'] = gamePhoto
                        tixdata['price'] = gamePrice
                        tixdata['overallodds'] = None if overallodds==None else overallodds
                        tixdata['topprize'] = tixdata['prizeamount'].iloc[0]
                        tixdata['topprizeodds'] = None if soup.find(class_='field field--name-field-top-prize-odds field--type-string field--label-above')==None else soup.find(class_='field field--name-field-top-prize-odds field--type-string field--label-above').find(class_='field__item').text.replace('1:','').replace(',','').replace(':','')
                        tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0].astype('float')
                        tixdata['topprizeremain'] = tixdata['Winning Tickets Unclaimed'].iloc[0].astype('float')
                        tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == 0 else np.nan
                        tixdata['startDate'] = soup.find(class_='field field--name-field-date field--type-daterange field--label-above').find(class_='field__item').text
                        tixdata['endDate'] = None
                        tixdata['lastdatetoclaim'] = None if soup.find(class_='field field--name-field-last-date-to-claim field--type-datetime field--label-above')==None else soup.find(class_='field field--name-field-last-date-to-claim field--type-datetime field--label-above').find(class_='field__item').text
                        tixdata['extrachances'] = None
                        tixdata['secondChance'] = None
                        tixdata['dateexported'] = date.today() 

                        #for game 1533, Fat Wallet, the overall odds and top prize odds are switched on the dclottery site 
                        if gameNumber == '1533':
                            tixdata['overallodds'] = tixdata['topprizeodds']
                            tixdata['topprizeodds'] = overallodds
                        elif gameNumber == '1521':
                            #this game, Double Your Money, seems to be missing a decimal point
                            revisedodds = str('3.99')
                            tixdata['overallodds'] = revisedodds if overallodds=='399' else overallodds
                        tixtables = tixtables.append(tixdata)
                    
                    #have to get the game photo link from the game page and add with tixinfo
                    #tixlist['gamePhoto'] = tixdata.loc[tixdata['gameNumber']==gameNumber,'gamePhoto'].iloc[0]
                    tixlist['topprize'] = tixdata.loc[tixdata['gameNumber']==gameNumber,'topprize'].iloc[0]
                    print(tixlist)

    tixlist.to_csv("./DCtixlist.csv", encoding='utf-8')
    print(tixtables.columns)
    print(tixtables)
    tixtables = tixtables.loc[(tixtables['prizeamount']!='Prize Ticket') & (tixtables['prizeamount']!='Prize ticket') & (tixtables['prizeamount']!='PRIZE TICKET'),:]
    scratchersall = tixtables[['price','gameName','gameNumber','topprize','overallodds','topprizeodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('DCscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./DCscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./DCscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','topprizeodds','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped[['gameNumber','topprizeodds','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.loc[:,'Total at start'] = None if ((overallodds==None) & (gamesgrouped['topprizeodds'].iloc[0]==None)) else gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = None if ((overallodds==None) & (gamesgrouped['topprizeodds'].iloc[0]==None)) else gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
    nonprizetix = gamesgrouped[['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    totals = gamesgrouped[['gameNumber','gameName','Total at start','Total remaining','dateexported']]
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
        testdf = totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']]
        print(testdf[~testdf.applymap(np.isreal).all(1)])
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        print(totalremain.loc[:,'Starting Expected Value'])
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

        currentodds = currentodds.append(gamerow, ignore_index=True)
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
        totalremain = totalremain.append(nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']], ignore_index=True)
        totalremain = totalremain.append(totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']], ignore_index=True)
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = alltables.append(totalremain)

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
    ratingstable.drop(labels=['gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
       'Data Date','Stats Page']]
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
    print(ratingstable)
    #set_with_dataframe(worksheet=DCratingssheet, dataframe=ratingstable, include_index=False,
    #include_column_header=True, resize=True)
    return ratingstable, scratchertables

exportScratcherRecs()
