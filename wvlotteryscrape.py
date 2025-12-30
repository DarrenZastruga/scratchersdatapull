import pandas as pd
import requests
import json
from datetime import date, datetime
from dateutil.tz import tzlocal
import numpy as np
from bs4 import BeautifulSoup
import io
import re # Import the regular expression module

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportScratcherRecs():
    """
    Scrapes the West Virginia Lottery website. 
    1. Extracts main game list from Next.js hydration data.
    2. Extracts prize tables using a cascade of methods:
       - Strict JSON Parsing (Preferred)
       - Regex Pattern Matching (Robust Fallback)
       - HTML Table Parsing (Legacy Fallback)
    """
    base_url = "https://wvlottery.com"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    tixtables = pd.DataFrame()
    scratchersall_list = []
    
    # --- 1. FETCH MAIN LIST ---
    list_page_url = f"{base_url}/games/scratch-offs"
    print(f"Fetching game data from: {list_page_url}")
    
    try:
        r = requests.get(list_page_url, headers=headers)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, 'html.parser')
        
        games_on_page = []
        all_script_tags = soup.find_all('script')
        
        # Parse Main Page Script
        for script_tag in all_script_tags:
            script_content = script_tag.get_text()
            if 'scratchOffs' in script_content:
                found_data = scan_and_extract_json(script_content, '"scratchOffs"')
                if found_data:
                    games_on_page = found_data
                    print(f"  > Successfully parsed {len(games_on_page)} games from main listing.")
                    break 

        if not games_on_page:
            print("No game data found on main page. Exiting.")
            return None, None
        
        # --- 2. PROCESS EACH GAME ---
        for game_data in games_on_page:
            try:
                # Extract basic fields
                game_number = game_data.get("gameNumber")
                top_prize = game_data.get("topPrize")
                slug = game_data.get("slug")
                title = game_data.get("title")
                start_date = game_data.get("startDate")
                end_date = game_data.get("endDate")
                claim_end_date = game_data.get("claimEndDate")
                ticket_price = game_data.get("ticketPrice")
                odds = game_data.get("odds")
                
                # Normalize types
                price = float(ticket_price) if ticket_price is not None else 0.0
                try:
                    top_prize_float = float(top_prize) if top_prize is not None else 0.0
                except (ValueError, TypeError):
                    top_prize_float = 0.0

                game_photo_data = game_data.get("image", {})
                game_photo = game_photo_data.get("url") if isinstance(game_photo_data, dict) else None

                if not slug:
                    print(f"  - Skipping game '{title}' due to missing slug.")
                    continue

                detail_url = f"{base_url}/games/scratch-offs/{slug}"
                print(f"  > Processing game details for: {title} (#{game_number})")

                # --- 3. FETCH DETAIL PAGE ---
                try:
                    detail_r = requests.get(detail_url, headers=headers)
                    detail_r.raise_for_status()
                    detail_soup = BeautifulSoup(detail_r.content, 'html.parser')
                    
                    prize_table_df = pd.DataFrame()
                    found_method = "None"

                    # Get all script content as one big string for regex searching
                    detail_scripts = detail_soup.find_all('script')
                    full_script_text = "\n".join([ds.get_text() for ds in detail_scripts])

                    # STRATEGY A: Try Strict JSON Parser (Best for clean data)
                    if 'prizeDetails' in full_script_text:
                        # Try extracting directly
                        prize_details_list = scan_and_extract_json(full_script_text, '"prizeDetails"')
                        
                        # If direct extraction failed, try finding it inside a scratchOffs list
                        if not prize_details_list and 'scratchOffs' in full_script_text:
                            scratch_list = scan_and_extract_json(full_script_text, '"scratchOffs"')
                            if scratch_list and len(scratch_list) > 0 and 'prizeDetails' in scratch_list[0]:
                                prize_details_list = scratch_list[0]['prizeDetails']

                        if prize_details_list:
                            prize_table_df = pd.DataFrame(prize_details_list)
                            prize_table_df.rename(columns={
                                'prize': 'prizeamount',
                                'totalPrizes': 'Winning Tickets At Start',
                                'remainingPrizes': 'Winning Tickets Unclaimed'
                            }, inplace=True)
                            found_method = "JSON Parser"

                    # STRATEGY B: Regex Pattern Match (Fallback for messy JSON)
                    # This looks for the raw data pattern: "prize":10,"totalPrizes":100,"remainingPrizes":50
                    if prize_table_df.empty:
                        # Pattern matches escaped or unescaped keys, and captures the 3 numbers
                        # pattern: "prize" : NUMBER , "totalPrizes" : NUMBER , "remainingPrizes" : NUMBER
                        regex_pattern = r'(?:\\|)"prize(?:\\|)"\s*:\s*([\d\.]+)\s*,\s*(?:\\|)"totalPrizes(?:\\|)"\s*:\s*(\d+)\s*,\s*(?:\\|)"remainingPrizes(?:\\|)"\s*:\s*(\d+)'
                        
                        matches = re.findall(regex_pattern, full_script_text)
                        if matches:
                            # matches is a list of tuples: [('10', '91486', '85443'), ...]
                            prize_table_df = pd.DataFrame(matches, columns=['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed'])
                            found_method = "Regex Extraction"

                    # STRATEGY C: HTML Table (Legacy Fallback)
                    if prize_table_df.empty:
                        html_table = detail_soup.find('table')
                        if html_table:
                            try:
                                html_df = pd.read_html(io.StringIO(str(html_table)))[0]
                                html_df.rename(columns={
                                    'Prize Amount': 'prizeamount',
                                    'Total Prizes': 'Winning Tickets At Start',
                                    'Prizes Remaining': 'Winning Tickets Unclaimed'
                                }, inplace=True)
                                if 'prizeamount' in html_df.columns:
                                    html_df['prizeamount'] = html_df['prizeamount'].astype(str).str.replace(r'[$,]', '', regex=True)
                                prize_table_df = html_df
                                found_method = "HTML Table"
                            except Exception:
                                pass

                    # --- 4. PROCESS PRIZE DATA ---
                    topprizestarting = 0
                    topprizeremain = 0

                    if not prize_table_df.empty:
                        # Ensure numeric types
                        for col in ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
                            if col in prize_table_df.columns:
                                prize_table_df[col] = pd.to_numeric(prize_table_df[col], errors='coerce').fillna(0)

                        # Add Metadata
                        prize_table_df['gameNumber'] = game_number
                        prize_table_df['gameName'] = title
                        prize_table_df['price'] = price
                        prize_table_df['dateexported'] = date.today()

                        # Calculate Top Prize Stats
                        # Try exact match first
                        if top_prize_float in prize_table_df['prizeamount'].values:
                            topprizestarting = prize_table_df.loc[prize_table_df['prizeamount'] == top_prize_float, 'Winning Tickets At Start'].iloc[0]
                            topprizeremain = prize_table_df.loc[prize_table_df['prizeamount'] == top_prize_float, 'Winning Tickets Unclaimed'].iloc[0]
                        else:
                            # Fallback: Max prize in table
                            if not prize_table_df.empty:
                                actual_top_prize = prize_table_df['prizeamount'].max()
                                top_prize_float = actual_top_prize
                                topprizestarting = prize_table_df.loc[prize_table_df['prizeamount'] == actual_top_prize, 'Winning Tickets At Start'].iloc[0]
                                topprizeremain = prize_table_df.loc[prize_table_df['prizeamount'] == actual_top_prize, 'Winning Tickets Unclaimed'].iloc[0]

                        # Append to master table
                        tixtables = pd.concat([tixtables, prize_table_df], ignore_index=True)
                        # print(f"    + Found prize data via {found_method}")
                    else:
                        print(f"    - Warning: No prize data found for {title}.")

                    topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
                    clean_odds = str(odds).lower().replace("1 in ", "").strip() if odds else None

                    scratchersall_list.append({
                        'gameNumber': game_number,
                        'gameName': title,
                        'title': title,
                        'slug': slug,
                        'price': price,
                        'ticketPrice': ticket_price,
                        'topprize': top_prize_float,
                        'topPrize': top_prize,
                        'overallodds': clean_odds,
                        'odds': odds,
                        'startDate': start_date,
                        'endDate': end_date,
                        'claimEndDate': claim_end_date,
                        'lastdatetoclaim': claim_end_date,
                        'topprizestarting': topprizestarting,
                        'topprizeremain': topprizeremain,
                        'topprizeavail': topprizeavail,
                        'dateexported': date.today(),
                        'gameURL': detail_url,
                        'gamePhoto': game_photo
                    })

                except Exception as e:
                    print(f"    - ERROR processing detail page for {title}: {e}")
                    continue

            except Exception as e:
                print(f"  - Error processing game item: {e}")
                continue
    
    except requests.RequestException as e:
        print(f"ERROR: Could not fetch list page. Error: {e}")

    # --- SAVE ---
    if not scratchersall_list:
        print("Scraping finished, but no data was collected. Exiting.")
        return None, None
        
    scratchersall = pd.DataFrame(scratchersall_list)
    print("Saving data to CSV...")
    scratchersall.to_csv("./WVscratcherslist.csv", encoding='utf-8', index=False)
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./COscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds','gamePhoto']], how='left', on=['gameNumber'])
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
        totalremain = totalremain.append(nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']], ignore_index=True)
        totalremain = totalremain.append(totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']], ignore_index=True)
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
       'Data Date','Stats Page']]
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
    print(ratingstable)
    #set_with_dataframe(worksheet=COratingssheet, dataframe=ratingstable, include_index=False,
    #include_column_header=True, resize=True)
    return ratingstable, scratchertables

def scan_and_extract_json(script_content, target_key):
    """
    Robust scanner that loops through ALL occurrences of `target_key` in the script.
    It returns the first successfully parsed JSON array.
    """
    try:
        # 1. Global Unescape
        unescaped_content = script_content.replace('\\"', '"').replace('\\\\', '\\')
        
        search_start_idx = 0
        
        while True:
            key_idx = unescaped_content.find(target_key, search_start_idx)
            if key_idx == -1: return None 
            
            search_start_idx = key_idx + len(target_key)
            search_cursor = search_start_idx
            list_start_idx = -1
            
            # Scan forward for [
            found_structure = False
            for i in range(search_cursor, min(search_cursor + 100, len(unescaped_content))):
                char = unescaped_content[i]
                if char == '[':
                    list_start_idx = i
                    found_structure = True
                    break
                elif char in [':', ' ', '\n', '\r', '\t']:
                    continue
                else:
                    break
            
            if not found_structure: continue

            # Bracket Counter
            bracket_count = 0
            json_snippet = ""
            in_string = False
            escape_next = False
            
            try:
                for i in range(list_start_idx, len(unescaped_content)):
                    char = unescaped_content[i]
                    json_snippet += char
                    
                    if escape_next:
                        escape_next = False
                        continue
                    if char == '\\':
                        escape_next = True
                        continue
                    if char == '"':
                        in_string = not in_string
                        continue
                    
                    if not in_string:
                        if char == '[':
                            bracket_count += 1
                        elif char == ']':
                            bracket_count -= 1
                            if bracket_count == 0:
                                return json.loads(json_snippet)
            except Exception:
                continue

    except Exception:
        return None
    return None

#exportScratcherRecs()