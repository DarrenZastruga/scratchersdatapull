#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updated IL Scraper (Selenium Version):
1. Restores the missing RANKING calculations at the end of the script.
2. Handles the 'Start = Remaining' data limitation gracefully.
3. Ensures all numeric columns are properly formatted before ranking.
"""

import pandas as pd
import time
from datetime import date, datetime
from dateutil.tz import tzlocal
import re
import io
import numpy as np

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportScratcherRecs():
    # --- SELENIUM SETUP ---
    print("Initializing Browser...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0")

    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
    except Exception as e:
        print(f"Error launching Firefox: {e}")
        return None, None

    try:
        urls = ["https://www.illinoislottery.com/games-hub/instant-tickets",
                "https://www.illinoislottery.com/games-hub/instant-tickets?page=1",
                "https://www.illinoislottery.com/games-hub/instant-tickets?page=2"]
        
        tixlist = pd.DataFrame()
        print("Scraping Game List...")
        
        # --- 1. SCRAPE GAME HUB PAGES ---
        for url in urls:
            try:
                driver.get(url)
                try:
                    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "simple-game-card")))
                except: pass
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                pages = soup.find_all('div', class_='simple-game-card card-container__item')
                
                print(f"  Found {len(pages)} games on page.")
                
                for p in pages:
                    link_tag = p.find('a')
                    if not link_tag: continue
                    
                    gameURL = 'https://www.illinoislottery.com' + str(link_tag.get('href'))
                    
                    # Image
                    banner = p.find(class_='simple-game-card__banner')
                    gamePhoto = None
                    if banner:
                        style = banner.get('style', '')
                        if 'url(' in style:
                            img_path = style.replace('background-image:url(', '').replace(')', '').replace(';','')
                            gamePhoto = 'https://www.illinoislottery.com' + img_path

                    # Detail Page
                    try:
                        driver.get(gameURL)
                        time.sleep(0.2)
                        
                        game_soup = BeautifulSoup(driver.page_source, 'html.parser')
                        details_block = game_soup.find('div', class_='itg-details-block')
                        
                        if details_block and details_block.find('table'):
                            table = pd.read_html(io.StringIO(str(details_block.find('table'))))[0]
                            
                            def get_val(key):
                                res = table.loc[table[0]==key, 1]
                                return res.iloc[0] if not res.empty else None

                            gameNumber = get_val('Game Number')
                            overall_str = get_val('Overall Odds')
                            overallodds = 0.0
                            if overall_str:
                                overallodds = float(str(overall_str).replace('1 in ','').replace(' to 1','').replace('1: ',''))
                            
                            price_str = get_val('Price Point')
                            gamePrice = price_str.replace('$','') if price_str else '0'
                            startDate = get_val('Launch Date')
                            
                            new_row = {
                                'price': gamePrice, 'gameNumber': gameNumber, 'gameURL': gameURL,
                                'gamePhoto': gamePhoto, 'overallodds': overallodds, 'startDate': startDate,
                                'endDate': None, 'lastdatetoclaim': None, 'extrachances': None,
                                'secondChance': None, 'dateexported': date.today()
                            }
                            tixlist = pd.concat([tixlist, pd.DataFrame([new_row])], ignore_index=True)
                            print(f"    Processed Game #{gameNumber}")
                    except: continue
            except: continue

        # --- 2. SCRAPE UNCLAIMED PRIZES ---
        print("\nScraping Unclaimed Prizes Table...")
        url_prizes = "https://www.illinoislottery.com/about-the-games/unpaid-instant-games-prizes"
        
        try:
            driver.get(url_prizes)
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
            except: pass
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            tixtables = pd.DataFrame()
            wrapper = soup.find('div', class_='unclaimed-prizes-table__wrapper')
            if wrapper: tixtables = wrapper.find('table')
            if not tixtables: tixtables = soup.find('table', class_='unclaimed-prizes-table')
            
            if not tixtables:
                print("  > CRITICAL: No prize table found.")
                return None, None

            rows = tixtables.find_all('tr')
            tixdata = pd.DataFrame()
            
            for row in rows:
                cells = row.find_all('td')
                if not cells: continue
                try:
                    gameNameRaw = cells[0].text.strip()
                    gameName = re.split(r'[()]', gameNameRaw)[0].strip()
                    gamePrice = cells[1].text.replace('$','').strip()
                    gameNumRaw = cells[2].text.strip()
                    gameNumber = re.split(r'[()]', gameNumRaw)[0].strip()
                    
                    prizevalues = cells[3].get_text(separator='|').split('|')
                    totalremaining = cells[-1].get_text(separator='|').split('|') # Last col is usually remaining
                    
                    clean_prizes = [x.replace('$','').replace(',','').strip() for x in prizevalues if x.strip()]
                    clean_remain = [x.replace(',','').strip() for x in totalremaining if x.strip()]
                    
                    # IL hides start counts, so we set Start = Unclaimed
                    clean_start = clean_remain 
                    
                    min_len = min(len(clean_prizes), len(clean_start), len(clean_remain))
                    
                    for i in range(min_len):
                        t_row = pd.DataFrame([{
                            'price': gamePrice, 'gameNumber': gameNumber, 'gameName': gameName,
                            'prizeamount': clean_prizes[i],
                            'Winning Tickets At Start': clean_start[i], 
                            'Winning Tickets Unclaimed': clean_remain[i]
                        }])
                        tixdata['dateexported'] = date.today()
                        tixdata = pd.concat([tixdata, t_row], ignore_index=True)
                except: continue
                
            
        except Exception as e:
            print(f"Error getting prize table: {e}")
            return None, None

    finally:
        driver.quit()

    # --- 3. MERGE LIST & PRIZES ---
    print("\nMerging Data...")
    
    # Update tixlist based on prize data
    for t in tixlist['gameNumber']:
        game_prizes = tixdata[tixdata['gameNumber'] == t]
        if game_prizes.empty: continue
        
        tixlist.loc[tixlist['gameNumber']==t, 'gameName'] = game_prizes['gameName'].iloc[0]
        
        game_prizes = game_prizes.copy()
        game_prizes['prize_float'] = pd.to_numeric(game_prizes['prizeamount'], errors='coerce')
        top_row = game_prizes.loc[game_prizes['prize_float'].idxmax()]
        
        tixlist.loc[tixlist['gameNumber']==t, 'topprize'] = top_row['prizeamount']
        tixlist.loc[tixlist['gameNumber']==t, 'topprizestarting'] = top_row['Winning Tickets At Start']
        tixlist.loc[tixlist['gameNumber']==t, 'topprizeremain'] = top_row['Winning Tickets Unclaimed']
        
        rem = int(top_row['Winning Tickets Unclaimed'])
        if 'topprizeavail' not in tixlist.columns: tixlist['topprizeavail'] = ''
        tixlist.loc[tixlist['gameNumber']==t, 'topprizeavail'] = tixlist.loc[tixlist['gameNumber']==t, 'topprizeavail'].astype(str)
        tixlist.loc[tixlist['gameNumber']==t, 'topprizeavail'] = 'Top Prize Claimed' if rem == 0 else np.nan

    tixlist.to_csv("./ILtixlist.csv", index=False)
    
    scratchersall = tixlist.loc[tixlist['gameNumber'] != "Coming Soon!"].copy()
    scratchersall.drop_duplicates(inplace=True)
    scratchersall.to_csv("./ILscratcherslist.csv", index=False)

    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixdata[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./ILscratchertables.csv", encoding='utf-8')
    
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start'].astype(float)*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total remaining'] / gamesgrouped['topprizeremain'].astype(float).replace(0, np.nan)
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
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:].copy()

        startingtotal_val = gamerow.loc[:, 'Total at start'].values[0]
        if pd.isna(startingtotal_val):
            continue  # skip games with no odds data
        startingtotal = int(startingtotal_val)
        tixtotal_val = gamerow.loc[:, 'Total remaining'].values[0]
        if pd.isna(tixtotal_val) or tixtotal_val == 0:
            continue
        tixtotal = int(tixtotal_val)
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])

        prizes =totalremain.loc[:,'prizeamount']


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
    #scratchertables.to_sql('NYscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./ILscratchertables.csv", encoding='utf-8')
    
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

    ratingstable['Stats Page'] = "/illinois-statistics-for-each-scratcher-game"
    
    ratingstable.to_csv("./ILratingstable.csv", index=False)
    print("Success! Files saved.")

    
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
    ratingstable = ratingstable.replace([np.inf, -np.inf], 0).infer_objects(copy=False)
    ratingstable = ratingstable.astype(object).fillna('').infer_objects(copy=False)

    
    return ratingstable, scratchertables

exportScratcherRecs()