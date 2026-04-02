#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Rhode Island Lottery Scraper
1. FIX: Saves to absolute path with timestamp to prevent OS-level TimeoutErrors.
2. FIX: Added robust float parsing to handle website typos (e.g., '3.50.').
"""

import pandas as pd
import time
from datetime import date, datetime
import re
import warnings
import gc
import uuid
import os
import numpy as np

# Selenium Imports
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup

warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)

# --- CONFIGURATION ---
COOL_DOWN_SECONDS = 3  
# Create a unique output directory to avoid file locks
OUTPUT_DIR = os.path.abspath(os.path.dirname(__file__))

def parse_complex_prize(prize_str):
    if not prize_str: return 0
    s = str(prize_str).upper().replace(',', '').replace('$', '').strip()
    try:
        multiplier = 1
        if 'MILLION' in s or 'MIL' in s: multiplier = 1000000
        elif 'THOUSAND' in s: multiplier = 1000
        s = re.sub(r'(MILLION|MIL|THOUSAND)', '', s)
        
        if 'WK' in s or 'WEEK' in s:
            amt = float(re.search(r'[\d\.]+', s).group())
            return amt * 52 * 20 
        if 'YR' in s or 'YEAR' in s:
            amt = float(re.search(r'[\d\.]+', s).group())
            return amt * 20
        
        # Extract only valid float characters
        match = re.search(r'(\d+\.?\d*)', s)
        if match: 
            return float(match.group(1)) * multiplier
        return 0
    except: return 0

def clean_date(date_str):
    try: return datetime.strptime(date_str.strip(), "%B %d, %Y").strftime("%Y-%m-%d")
    except: return None

def init_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--width=1920")
    options.add_argument("--height=1080")
    options.page_load_strategy = 'normal' 
    
    try:
        service = Service() 
        driver = webdriver.Firefox(service=service, options=options)
    except:
        service = Service(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=options)
    return driver

def get_detail(soup, label_pattern):
    try:
        label = soup.find(lambda tag: tag.name in ['strong', 'b', 'span'] and re.search(label_pattern, tag.get_text(), re.IGNORECASE))
        if not label: return None

        parent = label.parent
        strings = list(parent.stripped_strings)
        
        for i, s in enumerate(strings):
            if re.search(label_pattern, s, re.IGNORECASE):
                if i + 1 < len(strings):
                    return strings[i+1] 
        return None
    except: return None

def scrape_single_game(driver, btn_index, all_rows, all_tables):
    xpath = "//a[contains(text(), 'More Info')] | //button[contains(text(), 'More Info')]"
    
    try:
        buttons = [b for b in driver.find_elements(By.XPATH, xpath) if b.is_displayed()]
        if btn_index >= len(buttons): return
        btn = buttons[btn_index]

        gameName = "Unknown Game"
        try:
            card = btn.find_element(By.XPATH, "./ancestor::div[contains(@class, 'card') or contains(@class, 'item')][1]")
            raw_text = driver.execute_script("return arguments[0].textContent;", card).strip()
            lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
            for line in lines:
                if len(line) > 3 and "$" not in line and "NEW" not in line.upper() and "MORE INFO" not in line.upper():
                    gameName = line
                    break
        except: pass

        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
        time.sleep(1) 
        driver.execute_script("arguments[0].click();", btn)
        
        try:
            WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.ID, "instantGameModalBody")))
        except TimeoutException:
            print(f"    ! Timeout: Modal did not open for {gameName}.")
            return 
            
        try: WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, "#instantGameModalBody .loader")))
        except: pass
        
        try:
            WebDriverWait(driver, 8).until(lambda d: "Game Number" in d.find_element(By.ID, "instantGameModalBody").text)
            time.sleep(0.5) 
        except: pass
        
        modal = driver.find_element(By.ID, "instantGameModalBody")
        soup = BeautifulSoup(modal.get_attribute('innerHTML'), 'html.parser')
        full_text = soup.get_text(" ", strip=True)

        gameNumber = "0"
        val = get_detail(soup, "Game Number")
        if val: gameNumber = re.sub(r'[^\d]', '', val)
        
        if gameNumber == "0" or not gameNumber:
            match = re.search(r'Game Number:?\s*(\d+)', full_text, re.IGNORECASE)
            if match: gameNumber = match.group(1)
        
        if gameNumber == "0" or not gameNumber: 
            gameNumber = f"unknown-{uuid.uuid4().hex[:6]}"

        if any(g['gameNumber'] == gameNumber for g in all_rows if "unknown" not in str(gameNumber)):
            return 

        price = 0
        val = get_detail(soup, "Ticket Price")
        if val: price = parse_complex_prize(val)
        if price == 0:
            match = re.search(r'Ticket Price:?\s*\$([\d\.]+)', full_text, re.IGNORECASE)
            if match: price = float(match.group(1))

        topprize = 0
        val = get_detail(soup, "Top Prize")
        if val: topprize = parse_complex_prize(val)
        
        startDate = None; endDate = None; lastDate = None
        val = get_detail(soup, "On Sale")
        if val: startDate = clean_date(val)
        val = get_detail(soup, "Game Ended")
        if val: endDate = clean_date(val)
        val = get_detail(soup, "Last date to claim")
        if val: lastDate = clean_date(val)

        overall_odds = 0
        # Robust regex for odds to ignore trailing periods/typos
        match = re.search(r'Overall Odds.*?1\s*(?:in|:)\s*(\d+\.?\d*)', full_text, re.IGNORECASE)
        if match: overall_odds = float(match.group(1))

        gamePhoto = None
        img = soup.find('img', id="imgTicketDesktop")
        if img and img.get('src'): gamePhoto = f"https://www.rilot.com{img.get('src')}"
        
        if "unknown" in str(gameNumber):
            gameURL = "https://www.rilot.com/en-us/instantgames.html"
        else:
            gameURL = f"https://www.rilot.com/en-us/instantgames.html#game-{gameNumber}"

        best_table = pd.DataFrame()
        tbl = soup.find('table')
        if tbl:
            headers = [th.get_text(strip=True).upper() for th in tbl.find_all('th')]
            idx_prize = -1; idx_total = -1; idx_remain = -1
            for i, h in enumerate(headers):
                if 'AMOUNT' in h or 'PRIZE' in h: idx_prize = i
                if 'TOTAL' in h: idx_total = i
                if 'REMAIN' in h: idx_remain = i
            
            if idx_prize != -1:
                data = []
                rows = tbl.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if not cols: continue
                    max_idx = max(idx_prize, idx_total, idx_remain)
                    if len(cols) <= max_idx: continue
                    
                    p_val = cols[idx_prize].get_text(strip=True)
                    t_val = cols[idx_total].get_text(strip=True) if idx_total != -1 else "0"
                    r_val = cols[idx_remain].get_text(strip=True) if idx_remain != -1 else "0"
                    
                    if '$' in p_val or any(c.isdigit() for c in p_val):
                        data.append({'prizeamount': p_val, 'Winning Tickets At Start': t_val, 'Winning Tickets Unclaimed': r_val})
                if data: best_table = pd.DataFrame(data)

        topprizeremain = 0; topprizestarting = 0
        if not best_table.empty:
            best_table['prizeamount'] = best_table['prizeamount'].apply(parse_complex_prize)
            best_table['Winning Tickets At Start'] = pd.to_numeric(best_table['Winning Tickets At Start'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0)
            best_table['Winning Tickets Unclaimed'] = pd.to_numeric(best_table['Winning Tickets Unclaimed'].astype(str).str.replace(r'[^\d]', '', regex=True), errors='coerce').fillna(0)
            best_table['gameNumber'] = gameNumber
            best_table['gameName'] = gameName
            best_table['price'] = price
            best_table['dateexported'] = date.today()
            
            best_table = best_table[best_table['prizeamount'] > 0]
            all_tables.append(best_table)
            
            best_table = best_table.sort_values(by='prizeamount', ascending=False)
            if not best_table.empty:
                topprize = best_table.iloc[0]['prizeamount']
                topprizeremain = best_table.iloc[0]['Winning Tickets Unclaimed']
                topprizestarting = best_table.iloc[0]['Winning Tickets At Start']

        print(f"    + Scraped #{gameNumber}: {gameName} (${price})")

        all_rows.append({
            'price': price, 'gameName': gameName, 'gameNumber': gameNumber,
            'gameURL': gameURL, 'gamePhoto': gamePhoto, 'topprize': topprize, 
            'overallodds': overall_odds, 'topprizestarting': topprizestarting, 
            'topprizeremain': topprizeremain,
            'topprizeavail': "Available" if topprizeremain > 0 else "Claimed",
            'startDate': startDate, 'endDate': endDate, 'lastdatetoclaim': lastDate,
            'extrachances': None, 'secondChance': None, 'dateexported': date.today()
        })

    except Exception as e:
        print(f"    ! Error processing game: {type(e).__name__} - {str(e).strip()}")
    finally:
        try:
            close_btn = driver.find_element(By.CSS_SELECTOR, "#instantGameModal .close")
            driver.execute_script("arguments[0].click();", close_btn)
            WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.ID, "instantGameModalBody")))
        except:
            try: webdriver.ActionChains(driver).send_keys(u'\ue00c').perform() 
            except: pass
            time.sleep(1)

def get_filter_count(tab_name):
    driver = init_driver()
    count = 0
    try:
        driver.get("https://www.rilot.com/en-us/instantgames.html")
        time.sleep(5)
        
        tab_xpath = "//*[contains(text(), 'ACTIVE')]" if tab_name == "ACTIVE" else "//*[contains(text(), 'ENDED') and not(contains(text(), 'EXTENDED'))]"
        filter_id = "#xsInstantsFilter-ACTIVE" if tab_name == "ACTIVE" else "#xsInstantsFilter-ENDED"
        
        driver.execute_script("window.scrollTo(0, 0);")
        tab = driver.find_element(By.XPATH, tab_xpath)
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(3)
        
        filter_container = driver.find_element(By.CSS_SELECTOR, f"{filter_id} .filters")
        filters = filter_container.find_elements(By.TAG_NAME, "a")
        price_filters = [f for f in filters if "$" in f.text]
        count = len(price_filters)
    except: pass
    finally:
        driver.quit()
        gc.collect()
    return count

def process_filter_group(tab_name, filter_index, all_rows, all_tables):
    driver = init_driver()
    try:
        driver.get("https://www.rilot.com/en-us/instantgames.html")
        time.sleep(5)
        
        try:
            cookie = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
            driver.execute_script("arguments[0].click();", cookie)
        except: pass

        if tab_name == "ACTIVE":
            tab_xpath = "//*[contains(text(), 'ACTIVE')]"
            filter_id = "#xsInstantsFilter-ACTIVE"
        else:
            tab_xpath = "//*[contains(text(), 'ENDED') and not(contains(text(), 'EXTENDED'))]"
            filter_id = "#xsInstantsFilter-ENDED"

        driver.execute_script("window.scrollTo(0, 0);")
        tab = driver.find_element(By.XPATH, tab_xpath)
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(3)

        filter_container = driver.find_element(By.CSS_SELECTOR, f"{filter_id} .filters")
        filters = filter_container.find_elements(By.TAG_NAME, "a")
        price_filters = [f for f in filters if "$" in f.text]
        
        if filter_index >= len(price_filters):
            return 
            
        target_filter = price_filters[filter_index]
        filter_text = target_filter.text.replace("\n", " ")
        print(f"  > Processing {tab_name} Filter [{filter_index}]: {filter_text}")
        
        driver.execute_script("arguments[0].click();", target_filter)
        
        try: WebDriverWait(driver, 5).until(EC.invisibility_of_element_located((By.CSS_SELECTOR, ".loader")))
        except: pass
        time.sleep(3) 

        for _ in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)

        xpath = "//a[contains(text(), 'More Info')] | //button[contains(text(), 'More Info')]"
        buttons = [b for b in driver.find_elements(By.XPATH, xpath) if b.is_displayed()]
        print(f"    > Found {len(buttons)} visible games in this group.")
        
        for idx in range(len(buttons)):
            scrape_single_game(driver, idx, all_rows, all_tables)

    except Exception as e:
        print(f"  > Phase Error: {e}")
    finally:
        driver.quit()
        gc.collect() 

def run_phase(tab_name, all_rows, all_tables):
    print(f"\n--- Phase: {tab_name} (Group Batching) ---")
    print("  > Counting filter groups...")
    filter_count = get_filter_count(tab_name)
    print(f"  > Found {filter_count} groups to process.")
    
    for i in range(filter_count):
        process_filter_group(tab_name, i, all_rows, all_tables)
        print(f"  > Group {i} complete. Resting CPU for {COOL_DOWN_SECONDS}s...")
        time.sleep(COOL_DOWN_SECONDS)

def exportRIScratcherRecs():
    all_game_rows = []
    all_prize_tables = []
    
    run_phase("ACTIVE", all_game_rows, all_prize_tables)
    run_phase("ENDED", all_game_rows, all_prize_tables)
    
    print("\nCompiling DataFrames...")
    if not all_game_rows:
        print("No games collected. Saving aborted to prevent data loss.")
        return None, None

    # Determine unique filenames based on time to bypass OS locks
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tixlist_path = os.path.join(OUTPUT_DIR, f"RItixlist_{timestamp}.csv")
    tables_path = os.path.join(OUTPUT_DIR, f"RIscratchertables_{timestamp}.csv")
    ratings_path = os.path.join(OUTPUT_DIR, f"RIratingstable_{timestamp}.csv")

    tixlist = pd.DataFrame(all_game_rows)
    tixlist = tixlist.drop_duplicates(subset=['gameNumber'])
    
    try:
        tixlist.to_csv(tixlist_path, index=False)
        print(f"Saved: {tixlist_path}")
    except Exception as e:
        print(f"CRITICAL ERROR SAVING CSV: {e}")

    if all_prize_tables:
        tixtables = pd.concat(all_prize_tables, ignore_index=True)
    else:
        tixtables = pd.DataFrame(columns=['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported'])
    
    try:
        tixtables.to_csv(tables_path, index=False)
        print(f"Saved: {tables_path}")
    except Exception as e:
        pass

   
    print("Compiling DataFrames...")
    tixlist = pd.DataFrame(all_game_rows)
    tixlist.to_csv("./NHtixlist.csv", index=False)
   
    if all_prize_tables:
        tixtables = pd.concat(all_prize_tables, ignore_index=True)
        tixtables.to_csv("./NHscratchertables.csv", index=False)
    else:
        tixtables = pd.DataFrame(columns=['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported'])
 
    scratchersall = tixlist.copy().drop_duplicates()
    scratchersall.to_csv("./NHscratcherslist.csv", index=False)
 
 
 
    # 3. FILTER COLUMNS (Safe)
    cols_to_keep = ['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                    'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL', 'gamePhoto']
    
    for col in cols_to_keep:
        if col not in scratchersall.columns: scratchersall[col] = None
            
    scratchersall = scratchersall[cols_to_keep]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!"]
    scratchersall = scratchersall.drop_duplicates()
 
    # 4. PREPARE TABLES FOR STATS
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']].copy()
    scratchertables.to_csv("./LAscratchertables.csv", encoding='utf-8', index=False)
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!"]
 
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds', 'gamePhoto']], how='left', on=['gameNumber'])
 
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
 
    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']].copy()
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0
 
    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
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
        totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
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
 
        totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
 
        totalremain.loc[:, 'Starting Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
 
        totalremain.loc[:, 'Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        totalremain = totalremain.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
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
        gamerow.loc[:, 'Photo'] = gamerow.loc[:, 'gamePhoto']
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
        totalremain = totalremain.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
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
 
    scratchertables = alltables.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]
 
 
    # save scratchers tables
    scratchertables.to_csv("./NHscratchertables.csv", encoding='utf-8')
 
    # create rankings table by merging the list with the tables
 
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizeremain_x'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    
 
    # --- ADD DAYS SINCE START ---
    if 'startDate' in ratingstable.columns and 'dateexported' in ratingstable.columns:
        ratingstable['startDate'] = pd.to_datetime(ratingstable['startDate'], errors='coerce')
        ratingstable['dateexported'] = pd.to_datetime(ratingstable['dateexported'], errors='coerce')
        ratingstable['Days Since Start'] = (ratingstable['dateexported'] - ratingstable['startDate']).dt.days
    # --- End of FIX for date parsing ---
    
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
 
    ratingstable['Stats Page'] = "/new-hampshire-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('MOratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./NHratingstable.csv", encoding='utf-8')
 
    ratingstable = ratingstable.loc[:, ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance',
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
    
    return ratingstable, scratchertables

if __name__ == "__main__":
    exportRIScratcherRecs()