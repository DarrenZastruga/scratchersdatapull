import pandas as pd
import requests
from datetime import date, datetime
from dateutil.tz import tzlocal
import numpy as np
from bs4 import BeautifulSoup
import io
import re
from urllib.parse import urljoin

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportScratcherRecs():
    """
    Scrapes the Minnesota Lottery website for scratch-off data.
    Fixed: Ensures 'topprizestarting', 'topprizeremain', and 'gamePhoto' columns exist
    to prevent KeyErrors during statistical analysis.
    """
    base_url = "https://www.mnlottery.com"
    list_page_url = "https://www.mnlottery.com/games/scratch"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    print(f"Fetching game list from: {list_page_url}")
    
    tixtables = pd.DataFrame()
    scratchersall_list = []

    try:
        r = requests.get(list_page_url, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        
        game_links = soup.find_all('a', href=re.compile(r'/games/scratch/'))
        
        unique_game_links = {}
        for link in game_links:
            href = link['href'].strip()
            if href.rstrip('/') == "/games/scratch": continue
            
            slug = href.rstrip('/').split('/')[-1]
            is_year_archive = slug.isdigit() and len(slug) == 4 and int(slug) < 2026 
            
            if not is_year_archive and href.count('/') >= 3: 
                unique_game_links[href] = link

        print(f"Found {len(unique_game_links)} potential games.")

        for detail_path, link_tag in unique_game_links.items():
            try:
                detail_url = urljoin(base_url, detail_path)
                
                # --- 1. EXTRACT NAME & PRICE ---
                card_text = link_tag.get_text(" ", strip=True)
                
                price_match = re.search(r'\$(\d+)', card_text)
                price = float(price_match.group(1)) if price_match else 0.0
                
                game_name = "Unknown Game"
                if '$' in card_text:
                    game_name = card_text.split('$')[0].strip()
                elif "Learn More" in card_text:
                    game_name = card_text.replace("Learn More", "").strip()
                
                if game_name == "Unknown Game" or len(game_name) < 2:
                    slug = detail_path.strip('/').split('/')[-1]
                    game_name = slug.replace('-', ' ').title()
                
                game_name = game_name.replace("NEW!", "").strip()

                print(f"  > Processing: {game_name} ({detail_url})")

                # --- 2. FETCH DETAIL PAGE ---
                detail_r = requests.get(detail_url, headers=headers)
                detail_soup = BeautifulSoup(detail_r.content, 'html.parser')
                full_text = detail_soup.get_text(" ", strip=True)
                
                # --- 3. EXTRACT GAME NUMBER ---
                game_number = "0"
                
                game_num_match = re.search(r'Game\s*Number\s*[:\.]?\s*(\d{3,4})', full_text, re.IGNORECASE)
                if game_num_match:
                    game_number = game_num_match.group(1)
                
                if game_number == "0":
                    game_num_match = re.search(r'Game\s*#\s*(\d{3,4})', full_text, re.IGNORECASE)
                    if game_num_match:
                        game_number = game_num_match.group(1)

                # --- 4. EXTRACT PRIZE TABLE ---
                tables = detail_soup.find_all('table')
                prize_table_df = pd.DataFrame()
                
                for tbl in tables:
                    try:
                        df = pd.read_html(io.StringIO(str(tbl)))[0]
                        cols_map = {}
                        
                        for col in df.columns:
                            c_low = str(col).lower()
                            if ('win' in c_low or 'prize' in c_low) and 'prizeamount' not in cols_map.values():
                                if 'number' not in c_low and 'count' not in c_low:
                                    cols_map[col] = 'prizeamount'
                            elif ('odd' in c_low or 'chance' in c_low or 'prizes*' in c_low) and 'odds_text' not in cols_map.values():
                                cols_map[col] = 'odds_text'
                            elif ('number' in c_low or 'count' in c_low or 'total' in c_low) and 'Winning Tickets At Start' not in cols_map.values():
                                cols_map[col] = 'Winning Tickets At Start'
                        
                        if 'prizeamount' in cols_map.values() and 'Winning Tickets At Start' in cols_map.values():
                            df.rename(columns=cols_map, inplace=True)
                            prize_table_df = df
                            break 
                    except Exception:
                        continue
                
                if prize_table_df.empty:
                    print(f"    - No valid prize table found for {game_name}. Skipping.")
                    continue

                # --- 5. CLEAN DATA ---
                if 'prizeamount' in prize_table_df.columns:
                    prize_table_df['prizeamount'] = (
                        prize_table_df['prizeamount']
                        .astype(str)
                        .str.replace(r'[$,]', '', regex=True)
                        .str.replace(r'Ticket', '0', regex=True, case=False)
                        .str.strip()
                    )
                    prize_table_df['prizeamount'] = pd.to_numeric(prize_table_df['prizeamount'], errors='coerce').fillna(0)
                
                if 'Winning Tickets At Start' in prize_table_df.columns:
                    prize_table_df['Winning Tickets At Start'] = (
                        prize_table_df['Winning Tickets At Start']
                        .astype(str)
                        .str.replace(r'[,]', '', regex=True)
                        .str.strip()
                    )
                    prize_table_df['Winning Tickets At Start'] = pd.to_numeric(prize_table_df['Winning Tickets At Start'], errors='coerce').fillna(0)
                else:
                    prize_table_df['Winning Tickets At Start'] = 0

                prize_table_df['Winning Tickets Unclaimed'] = prize_table_df['Winning Tickets At Start']

                # Overall Odds
                overall_odds_match = re.search(r'odds.*?1 in (\d+(?:\.\d+)?)', full_text, re.IGNORECASE)
                if overall_odds_match:
                    try:
                        overall_odds = float(overall_odds_match.group(1))
                    except ValueError:
                        overall_odds = 0.0
                else:
                    overall_odds = 0.0

                # --- 6. CALCULATE TOP PRIZE STATS ---
                # Required for the merge step later
                topprize = 0
                topprizestarting = 0
                topprizeremain = 0
                
                if not prize_table_df.empty:
                    topprize = prize_table_df['prizeamount'].max()
                    # Find rows matching top prize
                    top_rows = prize_table_df[prize_table_df['prizeamount'] == topprize]
                    if not top_rows.empty:
                        topprizestarting = top_rows['Winning Tickets At Start'].iloc[0]
                        # MN doesn't give remaining counts, so we default to start to avoid NaN errors
                        topprizeremain = topprizestarting

                # --- 7. EXTRACT PHOTO (Placeholder) ---
                # Find an image that looks like the game card
                game_photo = None
                img_tag = detail_soup.find('img', alt=re.compile(game_name, re.IGNORECASE))
                if img_tag and img_tag.has_attr('src'):
                    game_photo = urljoin(base_url, img_tag['src'])

                # Assign Metadata
                prize_table_df['gameNumber'] = game_number
                prize_table_df['gameName'] = game_name
                prize_table_df['price'] = price
                prize_table_df['dateexported'] = date.today()
                
                cols_to_keep = ['gameNumber', 'gameName', 'price', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']
                final_table_cols = [c for c in cols_to_keep if c in prize_table_df.columns]
                
                tixtables = pd.concat([tixtables, prize_table_df[final_table_cols]], ignore_index=True)

                scratchersall_list.append({
                    'gameNumber': game_number,
                    'gameName': game_name,
                    'price': price,
                    'topprize': topprize,
                    'overallodds': overall_odds,
                    'startDate': None,
                    'gameURL': detail_url,
                    'gamePhoto': game_photo, # Added to fix KeyError
                    'topprizestarting': topprizestarting, # Added to fix KeyError
                    'topprizeremain': topprizeremain,     # Added to fix KeyError
                    'topprizeavail': 'Unknown',           # Added for completeness
                    'dateexported': date.today()
                })

            except Exception as e:
                print(f"    - Error processing {detail_path}: {e}")
                continue

        # --- 8. STATISTICAL CALCULATIONS ---
        if not scratchersall_list:
            print("No data collected.")
            return

        scratchersall = pd.DataFrame(scratchersall_list)
        scratchersall.to_csv("./MNscratcherslist.csv", encoding='utf-8', index=False)
        
        # Ensure correct types for merge
        scratchersall['price'] = pd.to_numeric(scratchersall['price'], errors='coerce')
        scratchersall['overallodds'] = pd.to_numeric(scratchersall['overallodds'], errors='coerce')
        
        # Prepare Tables
        scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        scratchertables = scratchertables.astype({'prizeamount': 'float', 'Winning Tickets At Start': 'float', 'Winning Tickets Unclaimed': 'float'})
        
        # Group & Merge
        gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
        
        # This MERGE previously failed because keys were missing. They are now present.
        gamesgrouped = gamesgrouped.merge(
            scratchersall[['gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds', 'gamePhoto']], 
            how='left', 
            on=['gameNumber']
        )
        
        # Basic Stats Calculation
        # Note: Since Unclaimed == Start for MN (due to missing data), 'Total remaining' will equal 'Total at start'
        gamesgrouped['Total at start'] = gamesgrouped['Winning Tickets At Start'] * gamesgrouped['overallodds']
        gamesgrouped['Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * gamesgrouped['overallodds']
        
        # Prevent division by zero if topprizestarting is 0
        gamesgrouped['topprizeodds'] = gamesgrouped.apply(
            lambda x: x['Total at start'] / x['topprizestarting'] if x['topprizestarting'] > 0 else 0, axis=1
        )

        # Save Final Outputs
        scratchertables.to_csv("./MNscratchertables.csv", encoding='utf-8', index=False)
        gamesgrouped.to_csv("./MNgamesgrouped.csv", encoding='utf-8', index=False) # Optional debug output
        
        print(f"Done! Processed {len(scratchersall)} games.")
        print("Saved MNscratcherslist.csv and MNscratchertables.csv")
        
        scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
        scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
        #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
        gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
        gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds', 'gamePhoto']], how='left', on=['gameNumber'])
        print(gamesgrouped.columns)
        print(gamesgrouped[['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
        gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
        gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
        gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
        print(gamesgrouped.loc[:,'topprizeodds'])
        gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        
        
        #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
        nonprizetix = gamesgrouped[['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
        nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
        nonprizetix.loc[:,'prizeamount'] = 0
        print(nonprizetix.columns)
        totals = gamesgrouped[['gameNumber','gameName','Total at start','Total remaining','dateexported']]
        totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
        totals.loc[:,'prizeamount'] = "Total"
        print(totals.columns)
          
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
                    
            gamerow.loc[:,'Photo'] = gamerow.loc[:,'gamePhoto']
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']

            currentodds = pd.concat([currentodds, gamerow], ignore_index=True)
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
            totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                             'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
            totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                             'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
            print(totalremain.columns)
            
            #add expected values for final totals row
            allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
            
            totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
            print(totalremain)
            alltables = pd.concat([alltables, totalremain], axis=0)

        scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        print(scratchertables.columns)   
        
        #save scratchers tables
        #scratchertables.to_sql('COscratcherstables', engine, if_exists='replace')
        scratchertables.to_csv("./COscratchertables.csv", encoding='utf-8')
        
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
        ratingstable['Stats Page'] = "/colorado-statistics-for-each-scratcher-game"
        #ratingstable.to_sql('NMratingstable', engine, if_exists='replace')
        ratingstable.to_csv("./COratingstable.csv", encoding='utf-8')
        # write to Google Sheets
        # select a work sheet from its name
        #COratingssheet = gs.worksheet('CORatingsTable')
        #COratingssheet.clear()
        
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
        #set_with_dataframe(worksheet=COratingssheet, dataframe=ratingstable, include_index=False,
        #include_column_header=True, resize=True)
        return ratingstable, scratchertables

    except Exception as e:
        print(f"Fatal error: {e}")
        return None, None

#exportScratcherRecs()