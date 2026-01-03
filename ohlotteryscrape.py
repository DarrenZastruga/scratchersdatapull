#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Updated OH Scraper (Selenium Version)
- Fixed ZeroDivisionError in statistical calculations.
- Adds robust checks for missing odds or empty ticket counts.
"""

import pandas as pd
import time
from datetime import date
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

def exportScratcherRecs():
    """
    Scrapes Ohio Lottery using Selenium (Firefox).
    """
    
    # --- SELENIUM SETUP (FIREFOX) ---
    print("Initializing Firefox...")
    firefox_options = Options()
    firefox_options.add_argument("--headless")
    
    # Set User Agent
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:91.0) Gecko/20100101 Firefox/91.0")

    try:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
    except Exception as e:
        print(f"Error launching Firefox. Ensure Firefox is installed.\nDetails: {e}")
        return None, None

    try:
        base_url = "https://www.ohiolottery.com"
        
        category_urls = [
            "https://www.ohiolottery.com/Games/Scratch-Offs/$1-Games",
            "https://www.ohiolottery.com/Games/Scratch-Offs/$2-Games",
            "https://www.ohiolottery.com/Games/Scratch-Offs/$5-Games",
            "https://www.ohiolottery.com/Games/Scratch-Offs/$10-Games",
            "https://www.ohiolottery.com/Games/Scratch-Offs/$20-Games",
            "https://www.ohiolottery.com/Games/Scratch-Offs/$30-Games",
            "https://www.ohiolottery.com/Games/Scratch-Offs/$50-Games"
        ]

        tixtables = pd.DataFrame()
        scratchersall_list = []
        unique_game_links = {}

        # --- 1. FIND GAMES ---
        print("Scanning categories for games...")
        
        for list_url in category_urls:
            try:
                driver.get(list_url)
                
                try:
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.TAG_NAME, "a"))
                    )
                except:
                    continue
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                links = soup.find_all('a', class_='igName')
                if not links:
                    links = soup.find_all('a', href=re.compile(r'/Games/Scratch-Offs/', re.IGNORECASE))

                for link in links:
                    href = link.get('href')
                    if not href: continue
                    
                    if href.startswith('/'):
                        full_url = base_url + href
                    else:
                        full_url = href
                    
                    lower = full_url.lower()
                    if '-games' in lower and lower.endswith('-games'): continue
                    if 'prizes-remaining' in lower: continue
                    
                    if full_url not in unique_game_links:
                        unique_game_links[full_url] = link.get_text(strip=True)
            
            except Exception as e:
                print(f"Error checking {list_url}: {e}")
                continue

        print(f"Found {len(unique_game_links)} unique games.")

        # --- 2. SCRAPE DETAILS ---
        for detail_url, link_text in unique_game_links.items():
            try:
                driver.get(detail_url)
                time.sleep(1)
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                full_text = soup.get_text(" ", strip=True)
                
                # -- Metadata --
                game_name = "Unknown"
                h1 = soup.find('h1')
                if h1: game_name = h1.get_text(strip=True)
                elif link_text: game_name = link_text
                
                game_number = "0"
                num_match = re.search(r'Game Number[:\s]*#?(\d+)', full_text, re.IGNORECASE)
                if num_match: game_number = num_match.group(1)
                else:
                    num_match = re.search(r'#(\d{3,4})', game_name)
                    if num_match: game_number = num_match.group(1)
                
                price = 0.0
                price_match = re.search(r'\$([0-9]+)-Games', detail_url, re.IGNORECASE)
                if price_match:
                    price = float(price_match.group(1))
                else:
                    p_match = re.search(r'Price:?\s*\$?(\d+)', full_text, re.IGNORECASE)
                    if p_match: price = float(p_match.group(1))

                overall_odds = 0.0
                odds_match = re.search(r'1 in ([\d\.]+)', full_text, re.IGNORECASE)
                if odds_match: overall_odds = float(odds_match.group(1))

                # --- IMAGE EXTRACTION ---
                gamePhoto = None
                img_tag = None
                
                if game_name and game_name != "Unknown":
                    img_tag = soup.find('img', alt=game_name)
                
                if not img_tag:
                    img_tag = soup.find('img', src=re.compile(r'Ticket', re.IGNORECASE))
                    
                if not img_tag:
                    potential_imgs = soup.find_all('img', src=re.compile(r'Scratch-Offs', re.IGNORECASE))
                    for img in potential_imgs:
                        src = img.get('src', '').lower()
                        if 'social' not in src and 'twitter' not in src and 'facebook' not in src and 'share' not in src:
                            img_tag = img
                            break

                if img_tag:
                    src = img_tag.get('src')
                    if src.startswith('/'): gamePhoto = base_url + src
                    else: gamePhoto = src

                print(f"  > Processing: {game_name} (#{game_number})")

                # -- Prizes --
                prize_rows = []
                
                tables = soup.find_all('table')
                for tbl in tables:
                    txt = tbl.get_text().lower()
                    if 'prize' in txt and ('remaining' in txt or 'total' in txt):
                        try:
                            df = pd.read_html(io.StringIO(str(tbl)))[0]
                            cols_map = {}
                            for col in df.columns:
                                c_low = str(col).lower()
                                if 'prize' in c_low: cols_map[col] = 'prizeamount'
                                elif 'remaining' in c_low: cols_map[col] = 'Winning Tickets Unclaimed'
                                elif 'total' in c_low: cols_map[col] = 'Winning Tickets At Start'
                            if 'prizeamount' in cols_map.values():
                                df.rename(columns=cols_map, inplace=True)
                                prize_rows.extend(df.to_dict('records'))
                        except: pass
                
                if not prize_rows:
                    matches = re.findall(r'\$\s*([\d,]+\.?\d*)[^\d\n<]+([\d,]+)(?:\s|$)', full_text)
                    for m in matches:
                        p_amt, cnt = m[0], m[1]
                        if len(p_amt) < 15 and len(cnt) < 10:
                            prize_rows.append({'prizeamount': p_amt, 'Winning Tickets Unclaimed': cnt})

                if not prize_rows:
                    continue

                prize_df = pd.DataFrame(prize_rows)
                
                if 'prizeamount' in prize_df.columns:
                    prize_df['prizeamount'] = prize_df['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True).str.split('(').str[0]
                    prize_df['prizeamount'] = pd.to_numeric(prize_df['prizeamount'], errors='coerce').fillna(0)
                
                for c in ['Winning Tickets Unclaimed', 'Winning Tickets At Start']:
                    if c in prize_df.columns:
                        prize_df[c] = prize_df[c].astype(str).str.replace(r'[,]', '', regex=True)
                        prize_df[c] = pd.to_numeric(prize_df[c], errors='coerce').fillna(0)
                    else:
                        prize_df[c] = 0

                if 'Winning Tickets At Start' not in prize_df.columns or prize_df['Winning Tickets At Start'].sum() == 0:
                    prize_df['Winning Tickets At Start'] = prize_df['Winning Tickets Unclaimed']

                prize_df['gameNumber'] = game_number
                prize_df['gameName'] = game_name
                prize_df['price'] = price
                prize_df['dateexported'] = date.today()
                
                tixtables = pd.concat([tixtables, prize_df], ignore_index=True)
                
                topprize = prize_df['prizeamount'].max()
                tp_row = prize_df[prize_df['prizeamount'] == topprize]
                remain_tp = tp_row['Winning Tickets Unclaimed'].iloc[0] if not tp_row.empty else 0
                
                scratchersall_list.append({
                    'gameNumber': game_number,
                    'gameName': game_name,
                    'price': price,
                    'topprize': topprize,
                    'overallodds': overall_odds,
                    'topprizestarting': 0,
                    'topprizeremain': remain_tp,
                    'topprizeavail': "Available" if remain_tp > 0 else "Claimed",
                    'startDate': None,
                    'gameURL': detail_url,
                    'gamePhoto': gamePhoto,
                    'dateexported': date.today(),
                    'topprizeodds': 0
                })

            except Exception as e:
                # print(f"    Error: {e}")
                continue

    finally:
        print("Closing Browser...")
        driver.quit()

    if scratchersall_list:
        print(f"Collected data for {len(scratchersall_list)} games.")
        
        scratchersall = pd.DataFrame(scratchersall_list)
        scratchersall.to_csv("./OHscratcherslist.csv", index=False)
        tixtables.to_csv("./OHscratchertables.csv", index=False)
    
        scratchersall = scratchersall.drop_duplicates()
        
        # --- STATISTICAL ANALYSIS (With ZeroDiv Fixes) ---
        scratchertables = tixtables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
        scratchertables = scratchertables.astype({'prizeamount': 'float', 'Winning Tickets At Start': 'float', 'Winning Tickets Unclaimed': 'float'})
        
        gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
        gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
        
        gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
        gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start'] * gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * gamesgrouped['overallodds'].astype(float)
        
        # Avoid division by zero in topprizeodds
        gamesgrouped['topprizeodds'] = gamesgrouped.apply(
            lambda row: row['Total at start'] / row['topprizestarting'] if row['topprizestarting'] > 0 else 0, axis=1
        )
        
        gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        
        currentodds = pd.DataFrame()
        
        for gameid in gamesgrouped['gameNumber']:
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:].copy()
            
            start_val = gamerow.loc[:, 'Total at start'].values[0]
            remain_val = gamerow.loc[:, 'Total remaining'].values[0]
            
            startingtotal = int(start_val) if not np.isnan(start_val) else 0
            tixtotal = int(remain_val) if not np.isnan(remain_val) else 0
            
            totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']].copy()
            totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
            
            price = float(gamerow['price'].values[0])
            
            # --- SAFE CALCULATIONS (Checked for 0 denominators) ---
            
            # Top Prize
            tpo = float(gamerow['topprizeodds'].values[0])
            gamerow.loc[:,'Current Odds of Top Prize'] = tpo
            gamerow.loc[:,'Change in Current Odds of Top Prize'] = 0 # Cannot calc change effectively without history/start
            
            # Any Prize
            unclaimed_sum = totalremain['Winning Tickets Unclaimed'].sum()
            if unclaimed_sum > 0:
                gamerow.loc[:,'Current Odds of Any Prize'] = tixtotal / unclaimed_sum
            else:
                gamerow.loc[:,'Current Odds of Any Prize'] = 0
            
            overall_odds_val = float(gamerow['overallodds'].values[0])
            if overall_odds_val > 0:
                curr_any = gamerow.loc[:,'Current Odds of Any Prize'].values[0]
                gamerow.loc[:,'Change in Current Odds of Any Prize'] = (curr_any - overall_odds_val) / overall_odds_val
            else:
                gamerow.loc[:,'Change in Current Odds of Any Prize'] = 0

            # Profit Prize
            profit_unclaimed = totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets Unclaimed'].sum()
            profit_start = totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets At Start'].sum()
            
            if profit_unclaimed > 0:
                gamerow.loc[:,'Odds of Profit Prize'] = tixtotal / profit_unclaimed
            else:
                gamerow.loc[:,'Odds of Profit Prize'] = 0
                
            if profit_start > 0 and startingtotal > 0:
                start_profit_odds = startingtotal / profit_start
                curr_profit_odds = gamerow.loc[:,'Odds of Profit Prize'].values[0]
                if start_profit_odds > 0:
                    gamerow.loc[:,'Change in Odds of Profit Prize'] = (curr_profit_odds - start_profit_odds) / start_profit_odds
                else:
                    gamerow.loc[:,'Change in Odds of Profit Prize'] = 0
            else:
                gamerow.loc[:,'Change in Odds of Profit Prize'] = 0

            # Probabilities
            if tixtotal > 0:
                gamerow.loc[:,'Probability of Winning Any Prize'] = unclaimed_sum / tixtotal
                gamerow.loc[:,'Probability of Winning Profit Prize'] = profit_unclaimed / tixtotal
                
                # EV (Safe)
                totalremain['Expected Value'] = totalremain.apply(lambda row: (row['prizeamount'] - price) * (row['Winning Tickets Unclaimed'] / tixtotal), axis=1)
                ev_sum = totalremain['Expected Value'].sum()
                gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = ev_sum / price if price > 0 else ev_sum
            else:
                gamerow.loc[:,'Probability of Winning Any Prize'] = 0
                gamerow.loc[:,'Probability of Winning Profit Prize'] = 0
                gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = 0

            # Std Devs
            if tixtotal > 0:
                std_all = totalremain['Winning Tickets Unclaimed'].std()
                if np.isnan(std_all): std_all = 0
                gamerow.loc[:,'StdDev of All Prizes'] = std_all / tixtotal
                
                std_prof = totalremain.loc[totalremain['prizeamount'] > price, 'Winning Tickets Unclaimed'].std()
                if np.isnan(std_prof): std_prof = 0
                gamerow.loc[:,'StdDev of Profit Prizes'] = std_prof / tixtotal
            else:
                gamerow.loc[:,'StdDev of All Prizes'] = 0
                gamerow.loc[:,'StdDev of Profit Prizes'] = 0

            # Metadata
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']
            
            # Add safe defaults for other columns if needed
            cols_needed = ['Odds of Any Prize + 3 StdDevs', 'Odds of Profit Prize + 3 StdDevs', 'Max Tickets to Buy']
            for c in cols_needed:
                if c not in gamerow.columns: gamerow[c] = 0

            currentodds = pd.concat([currentodds, gamerow], axis=0, ignore_index=True)

        
        # Merge & Format
        scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
        ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
        
        if 'gameName_y' in ratingstable.columns:
            ratingstable.rename(columns={'gameName_y':'gameName'}, inplace=True)
        elif 'gameName_x' in ratingstable.columns:
            ratingstable.rename(columns={'gameName_x':'gameName'}, inplace=True)
            
        ratingstable['Stats Page'] = "/ohio-statistics-for-each-scratcher-game"
        
        ratingstable = ratingstable[['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail',
           'overallodds','Current Odds of Top Prize',
           'Change in Current Odds of Top Prize', 'Current Odds of Any Prize',
           'Change in Current Odds of Any Prize', 'Odds of Profit Prize','Change in Odds of Profit Prize',
           'Probability of Winning Any Prize',
           'Probability of Winning Profit Prize',
           'StdDev of All Prizes','StdDev of Profit Prizes', 'Odds of Any Prize + 3 StdDevs',
           'Odds of Profit Prize + 3 StdDevs', 'Max Tickets to Buy',
           'Expected Value of Any Prize (as % of cost)',
           'Photo','FAQ', 'About', 'Directory', 
           'Data Date','Stats Page', 'gameURL']]
           
        ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
        ratingstable.fillna(0, inplace=True)
        
        print(ratingstable.head())
        
        return ratingstable, scratchertables
    else:
        print("No data collected.")
        return None, None


#exportScratcherRecs()