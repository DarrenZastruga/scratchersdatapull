#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 23:34:32 2022
Updated to fix merge issues, inf values, and missing topprizeodds.
"""

import pandas as pd
import requests
from datetime import date, datetime
from dateutil.tz import tzlocal
import numpy as np
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import io

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportScratcherRecs():
    """
    Scrapes Kansas Lottery data.
    1. Gets list of games from /instantgameslist
    2. Parses the detail page HTML (DIV structure) for prizes and stats.
    3. Calculates detailed statistics and rankings.
    """
    base_url = "https://www.kslottery.com"
    list_url = "https://www.kslottery.com/instantgameslist"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    print(f"Fetching KS game list from: {list_url}")

    tixtables = pd.DataFrame()
    scratchersall_list = []

    try:
        r = requests.get(list_url, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        
        # Find detail page links
        game_links = soup.find_all('a', href=re.compile(r'InstantGame/\?gameid='))
        
        unique_game_links = {}
        for link in game_links:
            href = link['href']
            full_url = urljoin(base_url, href)
            unique_game_links[full_url] = link

        print(f"Found {len(unique_game_links)} potential games.")

        for detail_url, link_tag in unique_game_links.items():
            try:
                # print(f"  > Processing: {detail_url}")
                
                # --- FETCH DETAIL PAGE ---
                detail_r = requests.get(detail_url, headers=headers)
                detail_soup = BeautifulSoup(detail_r.content, 'html.parser')
                full_text = detail_soup.get_text(" ", strip=True)
                
                # --- EXTRACT HEADER INFO ---
                game_id_match = re.search(r'gameid=(\d+)', detail_url)
                game_number = game_id_match.group(1) if game_id_match else "0"
                
                game_name = "Unknown Game"
                meta_title = detail_soup.find('meta', attrs={'name':'twitter:title'})
                if meta_title:
                    game_name = meta_title['content']
                else:
                    img_tag = detail_soup.find('img', id='ticketArtImage')
                    if img_tag and img_tag.has_attr('title'):
                        game_name = img_tag['title'].replace(' Ticket Art', '').title()

                price = 0.0
                price_tag = detail_soup.find('h3', string=re.compile(r'Retail Selling Price', re.I))
                if price_tag:
                    p_match = re.search(r'\$(\d+)', price_tag.get_text())
                    if p_match: price = float(p_match.group(1))
                
                start_date = None
                date_tag = detail_soup.find('h3', string=re.compile(r'Game Start Date', re.I))
                if date_tag:
                    d_match = re.search(r'(\d{2}/\d{2}/\d{4})', date_tag.get_text())
                    if d_match: 
                        try:
                            dt = datetime.strptime(d_match.group(1), '%m/%d/%Y')
                            start_date = dt.strftime('%Y-%m-%d')
                        except: pass

                overall_odds = 0.0
                odds_tag = detail_soup.find('b', string=re.compile(r'Overall odds', re.I))
                if odds_tag:
                    o_match = re.search(r'1 in ([\d\.]+)', odds_tag.get_text())
                    if o_match: overall_odds = float(o_match.group(1))

                gamePhoto = None
                img_tag = detail_soup.find('img', id='ticketArtImage')
                if img_tag and img_tag.has_attr('src'):
                    gamePhoto = urljoin(base_url, img_tag['src'])

                print(f"  > Processing: {game_name} (#{game_number})")

                # --- EXTRACT PRIZE TABLE ---
                prize_vals = []
                remain_vals = []
                
                prize_header = detail_soup.find('b', string=re.compile(r'Prize', re.I))
                if prize_header:
                    parent_div = prize_header.find_parent('div').parent
                    prize_vals = [x.get_text(strip=True) for x in parent_div.find_all('div', class_='col-6')]
                
                remain_header = detail_soup.find('b', string=re.compile(r'Remaining', re.I))
                if remain_header:
                    parent_div = remain_header.find_parent('div').parent
                    remain_vals = [x.get_text(strip=True) for x in parent_div.find_all('div', class_='col-6')]

                prize_table_df = pd.DataFrame()
                
                if len(prize_vals) > 0 and len(remain_vals) > 0:
                    min_len = min(len(prize_vals), len(remain_vals))
                    data = {
                        'prizeamount': prize_vals[:min_len],
                        'Winning Tickets Unclaimed': remain_vals[:min_len]
                    }
                    prize_table_df = pd.DataFrame(data)
                    # Use Unclaimed as proxy for Start since Start is unknown
                    prize_table_df['Winning Tickets At Start'] = prize_table_df['Winning Tickets Unclaimed']

                if prize_table_df.empty:
                    print(f"    - No prize table found for {game_name}. Skipping.")
                    continue

                # --- CLEAN DATA ---
                prize_table_df['prizeamount'] = (
                    prize_table_df['prizeamount']
                    .astype(str)
                    .str.replace(r'[$,]', '', regex=True)
                    .str.replace(r'Ticket', '0', regex=True, case=False)
                    .str.strip()
                )
                prize_table_df['prizeamount'] = pd.to_numeric(prize_table_df['prizeamount'], errors='coerce').fillna(0)
                
                for col in ['Winning Tickets Unclaimed', 'Winning Tickets At Start']:
                    prize_table_df[col] = (
                        prize_table_df[col]
                        .astype(str)
                        .str.replace(r'[,]', '', regex=True)
                        .str.strip()
                    )
                    prize_table_df[col] = pd.to_numeric(prize_table_df[col], errors='coerce').fillna(0)

                # Metadata
                prize_table_df['gameNumber'] = game_number
                prize_table_df['gameName'] = game_name
                prize_table_df['price'] = price
                prize_table_df['dateexported'] = date.today()
                
                # Top Prize Stats
                topprize = prize_table_df['prizeamount'].max()
                topprizestarting = 0
                topprizeremain = 0
                
                if topprize > 0:
                    top_rows = prize_table_df[prize_table_df['prizeamount'] == topprize]
                    if not top_rows.empty:
                        topprizestarting = top_rows['Winning Tickets At Start'].iloc[0]
                        topprizeremain = top_rows['Winning Tickets Unclaimed'].iloc[0]
                
                topprizeavail = "Available" if topprizeremain > 0 else "Claimed"
                
                # Calculate estimated topprizeodds if missing
                # Proxy: If we don't know total tickets, we can't calculate exact odds.
                # We will placeholder it as 0.
                topprizeodds = 0
                
                tixtables = pd.concat([tixtables, prize_table_df], ignore_index=True)

                scratchersall_list.append({
                    'gameNumber': game_number,
                    'gameName': game_name,
                    'price': price,
                    'topprize': topprize,
                    'overallodds': overall_odds,
                    'topprizestarting': topprizestarting,
                    'topprizeremain': topprizeremain,
                    'topprizeavail': topprizeavail,
                    'startDate': start_date,
                    'endDate': None,
                    'lastdatetoclaim': None,
                    'gameURL': detail_url,
                    'gamePhoto': gamePhoto,
                    'dateexported': date.today(),
                    'extrachances': None,
                    'secondChance': None,
                    'topprizeodds': topprizeodds
                })

            except Exception as e:
                print(f"    - Error processing {detail_url}: {e}")
                continue
    
    except Exception as e:
        print(f"Critical Error scraping KS list: {e}")
        return None, None

    if not scratchersall_list:
        print("No data collected.")
        return None, None

    scratchersall = pd.DataFrame(scratchersall_list)
    scratchersall.to_csv("./KSscratcherslist.csv", encoding='utf-8', index=False)
    tixtables.to_csv("./KSscratchertables.csv", encoding='utf-8', index=False)

    print("Done! Saved KSscratcherslist.csv and KSscratchertables.csv")
    
    # --- STATISTICAL ANALYSIS ---
    print("Running statistical analysis...")
    
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    
    # Ensure numeric types
    for col in ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
        scratchertables[col] = pd.to_numeric(scratchertables[col], errors='coerce').fillna(0)

    # Grouping
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index()
    
    # Merge with scratchersall to get odds and price
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','gamePhoto', 'price','topprizestarting','topprizeremain','overallodds', 'topprizeodds']], how='left', on=['gameNumber'])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    
    # Calculate Totals
    gamesgrouped['Total at start'] = gamesgrouped['Winning Tickets Unclaimed'] * gamesgrouped['overallodds']
    gamesgrouped['Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * gamesgrouped['overallodds']
    gamesgrouped['Non-prize at start'] = gamesgrouped['Total at start'] - gamesgrouped['Winning Tickets At Start']
    gamesgrouped['Non-prize remaining'] = gamesgrouped['Total remaining'] - gamesgrouped['Winning Tickets Unclaimed']
    
    # Calc Top Prize Odds (Safely)
    gamesgrouped['topprizeodds'] = gamesgrouped.apply(
        lambda x: x['Total at start'] / x['topprizestarting'] if x['topprizestarting'] > 0 else 0, axis=1
    )

    # Loop for detailed stats
    alltables = pd.DataFrame() 
    currentodds = pd.DataFrame()
    
    for gameid in gamesgrouped['gameNumber'].unique():
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:].copy()
        
        startingtotal = gamerow['Total at start'].values[0]
        tixtotal = gamerow['Total remaining'].values[0]
        price = gamerow['price'].values[0]
        
        # Pull table for this game
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid)].copy()
        
        # --- SAFEGUARDED CALCULATIONS ---
        
        # Top Prize Odds
        # Use calculated if valid, else scraped, else 0
        tp_odds = gamerow['topprizeodds'].values[0]
        if tp_odds == 0 or np.isinf(tp_odds) or np.isnan(tp_odds):
             tp_odds = 0 # Default if unknown
        
        gamerow['Current Odds of Top Prize'] = tp_odds
        gamerow['Change in Current Odds of Top Prize'] = 0 

        # Any Prize Odds
        total_unclaimed = totalremain['Winning Tickets Unclaimed'].sum()
        if total_unclaimed > 0:
            gamerow['Current Odds of Any Prize'] = tixtotal / total_unclaimed
        else:
            gamerow['Current Odds of Any Prize'] = 0
            
        gamerow['Change in Current Odds of Any Prize'] = 0
        
        
        
        # Profit Prize Odds
        profit_unclaimed = totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets Unclaimed'].sum()
        if profit_unclaimed > 0:
            gamerow['Odds of Profit Prize'] = tixtotal / profit_unclaimed
        else:
            gamerow['Odds of Profit Prize'] = 0
        
            
        # Starting Profit Odds (Use current if starting unknown)
        profit_start = totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets At Start'].sum()
        if profit_start > 0 and startingtotal > 0:
             start_profit_odds = startingtotal / profit_start
        else:
             start_profit_odds = gamerow['Odds of Profit Prize'].values[0] # Fallback
             
        gamerow['Starting Odds of Profit Prize'] = start_profit_odds
        
        if start_profit_odds > 0:
            gamerow['Change in Odds of Profit Prize'] = (gamerow['Odds of Profit Prize'] - start_profit_odds) / start_profit_odds
        else:
            gamerow['Change in Odds of Profit Prize'] = 0
            
        # Probabilities
        if tixtotal > 0:
            gamerow['Probability of Winning Any Prize'] = total_unclaimed / tixtotal
            gamerow['Probability of Winning Profit Prize'] = profit_unclaimed / tixtotal
        else:
            gamerow['Probability of Winning Any Prize'] = 0
            gamerow['Probability of Winning Profit Prize'] = 0
            
        # Placeholders for Change in Prob (assume 0 change if start unknown)
        gamerow['Change in Probability of Any Prize'] = 0
        gamerow['Change in Probability of Profit Prize'] = 0
        
        # Std Devs
        if tixtotal > 0:
            gamerow['StdDev of All Prizes'] = totalremain['Winning Tickets Unclaimed'].std() / tixtotal
            gamerow['StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std() / tixtotal
        else:
            gamerow['StdDev of All Prizes'] = 0
            gamerow['StdDev of Profit Prizes'] = 0
            
        # 3 Std Devs (Inf check)
        std_all = gamerow['StdDev of All Prizes'].values[0]
        if std_all > 0:
             gamerow['Odds of Any Prize + 3 StdDevs'] = tixtotal / (gamerow['Current Odds of Any Prize'] + (std_all * 3))
        else:
             gamerow['Odds of Any Prize + 3 StdDevs'] = 0
             
        # 3 Std Devs Profit (Inf check)
        std_all = gamerow['StdDev of Profit Prizes'].values[0]
        if std_all > 0:
             gamerow['Odds of Profit Prize + 3 StdDevs'] = tixtotal / (gamerow['Odds of Profit Prize'] + (std_all * 3))
        else:
             gamerow['Odds of Profit Prize + 3 StdDevs'] = 0
             
        # Max Tickets
        profit_tix_count = totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()
        profit_std = totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std()
        
        if (profit_tix_count - profit_std) > 0:
            gamerow['Max Tickets to Buy'] = tixtotal / (profit_tix_count - profit_std)
        else:
            gamerow['Max Tickets to Buy'] = 0
            
        # Expected Value
        if tixtotal > 0:
             totalremain['Expected Value'] = totalremain.apply(lambda r: (r['prizeamount'] - price) * (r['Winning Tickets Unclaimed'] / tixtotal), axis=1)
             ev_sum = totalremain['Expected Value'].sum()
             gamerow['Expected Value of Any Prize (as % of cost)'] = ev_sum / price if price > 0 else 0
        else:
             gamerow['Expected Value of Any Prize (as % of cost)'] = 0
             
        gamerow['Change in Expected Value of Any Prize'] = 0
        gamerow['Expected Value of Profit Prize (as % of cost)'] = 0 # Placeholder
        gamerow['Change in Expected Value of Profit Prize'] = 0
        
        # Percent Remaining
        if profit_start > 0:
             gamerow['Percent of Profit Prizes Remaining'] = profit_unclaimed / profit_start
        else:
             gamerow['Percent of Profit Prizes Remaining'] = 0
             
        gamerow['Percent of Prizes Remaining'] = 0
        gamerow['Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0
        
        
        gamerow['FAQ'] = None
        gamerow['About'] = None
        gamerow['Directory'] = None
        gamerow['Data Date'] = gamerow['dateexported']

        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)

    # --- FINAL MERGE ---
    # Fix: Ensure no columns are dropped that we need, and handle suffixes
    
    # First, handle INFs before rank
    currentodds.replace([np.inf, -np.inf], 0, inplace=True)
    
    # Rename columns to avoid collision/suffix issues if keys overlap
    # We want the 'currentodds' version of calculations
    
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber', 'price'])
    
    # Resolve suffixes if any (e.g. gameName_x, gameName_y)
    if 'gameName_y' in ratingstable.columns:
        ratingstable.rename(columns={'gameName_y': 'gameName'}, inplace=True)
    elif 'gameName_x' in ratingstable.columns:
         ratingstable.rename(columns={'gameName_x': 'gameName'}, inplace=True)
         
    if 'topprizeodds_y' in ratingstable.columns:
         ratingstable['topprizeodds'] = ratingstable['topprizeodds_y']
    elif 'topprizeodds_x' in ratingstable.columns:
         ratingstable['topprizeodds'] = ratingstable['topprizeodds_x']
         
    # Handle infinite values again globally
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna(0, inplace=True)
    
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
    ratingstable['Stats Page'] = "/kansas-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('KSratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./KSratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    #KSratingssheet = gs.worksheet('KSRatingsTable')
    #KSratingssheet.clear()
    
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
    #set_with_dataframe(worksheet=KSratingssheet, dataframe=ratingstable, include_index=False,
    #include_column_header=True, resize=True)
    return ratingstable, scratchertables

#exportScratcherRecs()