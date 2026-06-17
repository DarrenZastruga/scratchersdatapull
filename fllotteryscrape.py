import pandas as pd
import time
import io
import re
import gc
from datetime import date
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.firefox import GeckoDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import numpy as np

# Cache driver path
GECKO_PATH = GeckoDriverManager().install()

def get_driver():
    options = Options()
    options.add_argument("--headless")
    options.page_load_strategy = 'normal' 
    options.set_preference("permissions.default.image", 2)
    service = Service(GECKO_PATH)
    driver = webdriver.Firefox(service=service, options=options)
    driver.set_window_size(1920, 1080)
    return driver

def exportFLScratcherRecs():
    print("Initializing Florida Scraper (Master List Name Mapping)...")
    driver = get_driver()
    base_url = "https://floridalottery.com"
    
    try:
        # 1. HARVEST MASTER LIST AND NAMES
        print("Fetching Florida catalog and mapping names...")
        driver.get(f"{base_url}/games/scratch-offs/top-remaining-prizes")
        
        # Wait for any catalog link to render
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/scratch-offs/view']")))

        # --- 1. HARVEST MASTER LIST: name_map AND price_map ---
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        CANONICAL_PRICES = {1, 2, 3, 5, 10, 20, 30, 50}
        PRICE_LABEL_RE = re.compile(r'\$\s*(\d+)\b(?!\d)')

        def _nearest_price(anchor):
            """Walk up from a game link to find the nearest ancestor whose text contains
            a canonical FL ticket-price label (e.g., '$50 GAMES'). Returns float or 0."""
            node = anchor
            for _ in range(10):
                node = node.parent
                if node is None:
                    return 0.0
                for tag in node.find_all(
                        ['h1', 'h2', 'h3', 'h4', 'h5', 'span', 'div'],
                        string=PRICE_LABEL_RE, limit=8
                ):
                    for m in PRICE_LABEL_RE.finditer(tag.get_text(" ", strip=True)):
                        v = int(m.group(1))
                        if v in CANONICAL_PRICES:
                            return float(v)
            return 0.0

        name_map = {}
        price_map = {}
        game_links = []

        for a in soup.find_all('a', href=True):
            if "/games/scratch-offs/view" not in a['href']:
                continue
            full_url = urljoin(base_url, a['href'])
            gid = parse_qs(urlparse(full_url).query).get('id', [''])[0]
            if not gid or gid in name_map:
                continue

            clean_name = re.sub(
                r'\s*\(\s*(?:GAME\s*ID|#)\s*:?\s*\d+\s*\)', '',
                a.get_text(strip=True), flags=re.IGNORECASE
            ).strip()
            name_map[gid] = clean_name
            price_map[gid] = _nearest_price(a)
            game_links.append(full_url)

        print(f"✅ Mapped {len(name_map)} games "
              f"({sum(1 for p in price_map.values() if p > 0)} with price). Starting crawl...")

        all_prize_tables = []
        summary_data = []

        # 2. DETAIL CRAWL
        for i, link in enumerate(game_links):
            if i > 0 and i % 10 == 0:
                driver.quit()
                driver = get_driver()
                print(f"--- Session Reset (Game {i+1}) ---")

            try:
                # Use the pre-built ID and Name
                current_id = parse_qs(urlparse(link).query).get('id', [''])[0]
                game_name = name_map.get(current_id, f"Game {current_id}")

                driver.get(link)
                WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                time.sleep(1) 
                
                detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
                page_text = detail_soup.get_text(" ", strip=True)

                # --- FIXED PHOTO EXTRACTION ---
                # 1. Target the specific image container or class for the ticket art
                # Florida uses classes like 'scratch-off-ticket' or 'game-detail-img'
                img_tag = detail_soup.find('img', class_=re.compile(r'scratch|ticket|game-img', re.I))
                
                game_photo = ""
                if img_tag:
                    # Check 'data-src' first (for lazy-loaded images) then fall back to 'src'
                    raw_photo_url = img_tag.get('data-src') or img_tag.get('src')
                    if raw_photo_url:
                        game_photo = urljoin(base_url, raw_photo_url)
                
                # 2. Fallback: If no specific class is found, look for any image with 'game' in the filename
                if not game_photo:
                    fallback_img = detail_soup.find('img', src=re.compile(r'game|ticket', re.I))
                    if fallback_img:
                        game_photo = urljoin(base_url, fallback_img.get('src'))

                # --- PRICE & ODDS ---
                overall_odds = 0
                odds_match = re.search(
                    r'Overall Odds\s*[:in]*\s*1[:\-in]*([\d\.,]+)',
                    page_text, re.IGNORECASE
                )
                if odds_match:
                    overall_odds = float(odds_match.group(1).replace(',', ''))

                price = price_map.get(current_id, 0.0)
                if price == 0.0:
                    print(f"    ⚠ No price found for game {current_id} ({game_name}); "
                          f"skipping to avoid bad simulation data.")
                    continue  # never write price=0 — it poisons every EV/payout metric

                # --- TABLE PARSING ---
                tables = pd.read_html(io.StringIO(str(detail_soup)))
                df = next((t.copy() for t in tables if any('remaining' in str(c).lower() for c in t.columns)), None)
                
                if df is not None:
                    df.columns = [str(c).lower().strip() for c in df.columns]
                    rem_col = next((c for c in df.columns if 'remaining' in c), None)
                    prz_col = next((c for c in df.columns if 'amount' in c or 'prize' in c and 'remaining' not in c), None)

                    if rem_col and prz_col:
                        df = df.rename(columns={prz_col: 'prizeamount'})

                        def split_qty(val, part):
                            match = re.search(r'(\d[\d,]*)\s+of\s+(\d[\d,]*)', str(val))
                            return float(match.group(part).replace(',', '')) if match else 0

                        df['Winning Tickets Unclaimed'] = df[rem_col].apply(lambda x: split_qty(x, 1))
                        df['Winning Tickets At Start'] = df[rem_col].apply(lambda x: split_qty(x, 2))
                        df['prizeamount'] = pd.to_numeric(df['prizeamount'].astype(str).str.replace(r'[^\d\.]', '', regex=True), errors='coerce').fillna(0)

                        df['gameNumber'], df['gameName'], df['dateexported'] = current_id, game_name, date.today()
                        all_prize_tables.append(df)
                        
                        top_row = df.sort_values('prizeamount', ascending=False).iloc[0]
                        summary_data.append({
                            'gameNumber': current_id, 'gameName': game_name, 'price': price,
                            'topprize': top_row['prizeamount'], 'overallodds': overall_odds,
                            'topprizestarting': top_row['Winning Tickets At Start'],
                            'topprizeremain': top_row['Winning Tickets Unclaimed'],
                            'topprizeavail': "Yes" if top_row['Winning Tickets Unclaimed'] > 0 else "No",
                            'gameURL': link, 'gamePhoto': game_photo, 'dateexported': date.today(),
                            'startDate': "", 'extrachances': "No", 'secondChance': "No", 'lastdatetoclaim': ""
                        })
                        print(f"  > Scraped: {game_name}")

                gc.collect()

            except Exception as e:
                print(f"    - Error on {link}: {e}")

        # 3. STATISTICAL ENGINE AND EXPORT
        if all_prize_tables:
            scratchertables_base = pd.concat(all_prize_tables, ignore_index=True)
            scratchersall = pd.DataFrame(summary_data)
            scratchersall.to_csv("./FLscratcherslist.csv", index=False)
            
            # Calculations...
            scratchertables = scratchertables_base.dropna(subset=['prizeamount']).copy()
            scratchertables['prizeamount'] = pd.to_numeric(scratchertables['prizeamount'], errors='coerce').fillna(0).astype(int)
            
            scratchertables.to_csv("./FLscratchertables.csv", encoding='utf-8')
            scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                                  != "Coming Soon!", :]
            scratchertables['gameNumber'] = scratchertables['gameNumber'].astype(str)
            scratchersall['gameNumber'] = scratchersall['gameNumber'].astype(str)
            scratchertables = scratchertables.astype(
                {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
            # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
            gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
            ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
            gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                              'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])
        
            #convert columns to numeric
            for col in ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
                if col in gamesgrouped.columns:
                    gamesgrouped[col] = gamesgrouped[col].astype(object)
                    gamesgrouped[col] = pd.to_numeric(gamesgrouped[col], errors='coerce')
            
            gamesgrouped.loc[:, 'Total at start'] = gamesgrouped['Winning Tickets At Start'] * \
                gamesgrouped['overallodds'].astype(float)
            gamesgrouped.loc[:, 'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * \
                gamesgrouped['overallodds'].astype(float)
            gamesgrouped.loc[:, 'Non-prize at start'] = gamesgrouped['Total at start'] - \
                gamesgrouped['Winning Tickets At Start']
            gamesgrouped.loc[:, 'Non-prize remaining'] = gamesgrouped['Total remaining'] - \
                gamesgrouped['Winning Tickets Unclaimed']
            try:
                gamesgrouped['topprizeodds'] = gamesgrouped['Total remaining'] / gamesgrouped['topprizeremain'].astype('float')
            except ZeroDivisionError:
                gamesgrouped['topprizeodds'] = 0
            gamesgrouped.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        
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
                gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :].copy()
        
                #cast all columns to Object to start to avoid dtype errors when converting to numeric later
                for col in gamerow.columns:
                    gamerow[col] = gamerow[col].astype(object)
                startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
                tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
                totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
                    'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
                totalremain = totalremain.astype(object)
                totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
                    'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
                price = int(gamerow['price'].values[0])
                prizes = totalremain.loc[:, 'prizeamount']
        
                #convert 'Winning Tickets Unclaimed' as numberic to avoid divide by zero warnings
                den = pd.to_numeric(totalremain['Winning Tickets Unclaimed'].iloc[0], errors='coerce')
                if pd.notna(den) and den > 0:
                    gamerow.loc[:, 'Current Odds of Top Prize'] = tixtotal / den
                else:
                    gamerow.loc[:, 'Current Odds of Top Prize'] = np.nan
                    
                # add various columns for the scratcher stats that go into the ratings table
                gamerow['topprizeremain'] = pd.to_numeric(gamerow['topprizeremain'], errors='coerce')
        
                odds = pd.to_numeric(gamerow.loc[:, 'Total remaining'], errors='coerce') / gamerow.loc[:, 'topprizeremain']
                odds = odds.replace([np.inf, -np.inf], np.nan)
                gamerow.loc[:, 'Current Odds of Top Prize'] = odds
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
                # Clean prizeamount before numeric conversion
                totalremain = totalremain.astype(object) 
                totalremain['prizeamount'] = totalremain['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True)
                #totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
                   # 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric, errors='coerce')
                # --- FIX: pandas 2.1+ refuses to coerce int64 values into a StringDtype column.
                # Cast source values to string and assign by position (.values) so it works
                # regardless of the destination column dtype.
                _dst_cols = ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']
                _src_cols = ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']
                
                # Ensure destination columns are string-typed (avoids future dtype drift)
                for _c in _dst_cols:
                    if _c in totalremain.columns:
                        totalremain[_c] = totalremain[_c].astype('string')
                
                # Cast source values to string and assign positionally
                totalremain.loc[:, _dst_cols] = (
                    totalremain.loc[:, _src_cols]
                      .astype('string')
                      .to_numpy()
                )
                # --- Coerce ALL numeric columns before any math ---
                for _col in ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
                    if _col in totalremain.columns:
                        totalremain[_col] = pd.to_numeric(
                            totalremain[_col].astype(str).str.replace(r'[\$,]', '', regex=True),
                            errors='coerce'
                        )
                
                # Recompute startingtotal from the cleaned numeric column
                startingtotal = totalremain.loc[
                    totalremain['prizeamount'].notna(), 'Winning Tickets At Start'
                ].sum()
                
                if not startingtotal or startingtotal == 0:
                    print(f"⚠️ FL: startingtotal is 0 for game; skipping EV calc.")
                    totalremain['Starting Expected Value'] = 0
                else:
                    _ev_mask = totalremain['prizeamount'].notna() & totalremain['Winning Tickets At Start'].notna()
                    totalremain.loc[_ev_mask, 'Starting Expected Value'] = totalremain[_ev_mask].apply(
                        lambda row: (row['prizeamount'] - price) * (row['Winning Tickets At Start'] / startingtotal),
                        axis=1,
                    )
                totalremain.loc[:, 'Expected Value'] = totalremain.apply(lambda row: (
                    row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
                totalremain = totalremain.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                           'Winning Tickets Unclaimed', 'Starting Expected Value', 'Expected Value', 'dateexported']]
                # --- UPDATED STATISTICAL CALCULATIONS ---
                # Define common sums to use for guards
                total_ev_any = sum(totalremain['Expected Value'])
                starting_ev_any = sum(totalremain['Starting Expected Value'])
                
                # Expected Value Any Prize
                gamerow.loc[:, 'Expected Value of Any Prize (as % of cost)'] = total_ev_any / price if price > 0 else total_ev_any
                
                # GUARD: Prevent ZeroDivisionError for Any Prize Change
                if starting_ev_any != 0:
                    gamerow.loc[:, 'Change in Expected Value of Any Prize'] = ((total_ev_any - starting_ev_any) / starting_ev_any) / price if price > 0 else (total_ev_any - starting_ev_any) / starting_ev_any
                else:
                    gamerow.loc[:, 'Change in Expected Value of Any Prize'] = 0
    
                # Profit Prize Calculations
                profit_mask = totalremain['prizeamount'] > price
                total_ev_profit = sum(totalremain.loc[profit_mask, 'Expected Value'])
                starting_ev_profit = sum(totalremain.loc[profit_mask, 'Starting Expected Value'])
                
                gamerow.loc[:, 'Expected Value of Profit Prize (as % of cost)'] = total_ev_profit / price if price > 0 else total_ev_profit
                
                # GUARD: Prevent ZeroDivisionError for Profit Prize Change (The fix for your crash)
                if starting_ev_profit != 0:
                    gamerow.loc[:, 'Change in Expected Value of Profit Prize'] = ((total_ev_profit - starting_ev_profit) / starting_ev_profit) / price if price > 0 else (total_ev_profit - starting_ev_profit) / starting_ev_profit
                else:
                    gamerow.loc[:, 'Change in Expected Value of Profit Prize'] = 0
    
                # Remaining Stats with Guards
                gamerow.loc[:, 'Percent of Prizes Remaining'] = (totalremain.loc[:, 'Winning Tickets Unclaimed'] / totalremain.loc[:, 'Winning Tickets At Start']).mean()
                
                if not totalremain.loc[profit_mask].empty:
                    gamerow.loc[:, 'Percent of Profit Prizes Remaining'] = (totalremain.loc[profit_mask, 'Winning Tickets Unclaimed'] / totalremain.loc[profit_mask, 'Winning Tickets At Start']).mean()
                else:
                    gamerow.loc[:, 'Percent of Profit Prizes Remaining'] = 0
                
                chngLosingTix = (gamerow.loc[:, 'Non-prize remaining'] - gamerow.loc[:, 'Non-prize at start']) / gamerow.loc[:, 'Non-prize at start']
                chngAvailPrizes = (tixtotal - startingtotal) / startingtotal if startingtotal != 0 else 0
                
                # GUARD: Prevent ZeroDivisionError for Ratio calculation
                if chngAvailPrizes != 0:
                    gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix / chngAvailPrizes
                else:
                    gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0
                gamerow.loc[:,'Photo'] = scratchersall.loc[scratchersall['gameNumber'] == gameid,'gamePhoto']
                gamerow.loc[:,'FAQ'] = None
                gamerow.loc[:,'About'] = None
                gamerow.loc[:,'Directory'] = None
                gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']

                currentodds = pd.concat([currentodds, gamerow], ignore_index=True)


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
            #scratchertables.to_sql('COscratcherstables', engine, if_exists='replace')
            scratchertables.to_csv("./FLscratchertables.csv", encoding='utf-8')
            
            #create rankings table by merging the list with the tables

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

            ratingstable['Stats Page'] = "/florida-statistics-for-each-scratcher-game"

            ratingstable.to_csv("./FLratingstable.csv", encoding='utf-8')

            
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
            ratingstable = ratingstable.replace([np.inf, -np.inf], 0).infer_objects(copy=False)
            ratingstable = ratingstable.astype(object).fillna('').infer_objects(copy=False)

            print("✅ Success! Files saved for FL.")
            return ratingstable, scratchertables

    finally:
        driver.quit()

if __name__ == "__main__":
    exportFLScratcherRecs()
