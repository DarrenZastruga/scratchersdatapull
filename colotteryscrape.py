import pandas as pd
import requests
import json
import io
import re
import gc
import time
import random
import numpy as np
from datetime import date, datetime
from dateutil.tz import tzlocal
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def exportScratcherRecs():
    print("Initializing Memory-Safe CO Scraper...")
    base_url = "https://www.coloradolottery.com"
    list_page_url = "https://www.coloradolottery.com/en/player-tools/scratch-insider/"
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    })

    try:
        r = session.get(list_page_url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'html.parser')
        main_table = soup.find('table')
        table_rows = main_table.find('tbody').find_all('tr')
        print(f"✅ Found {len(table_rows)} games in the catalog.")
    except Exception as e:
        print(f"❌ Failed to fetch catalog. Error: {e}")
        return None, None

    all_prize_tables = []
    summary_data = []

    for i, row in enumerate(table_rows):
        if i > 0 and i % 15 == 0:
            time.sleep(5)

        try:
            cells = row.find_all('td')
            if len(cells) < 10: continue

            link_tag = cells[0].find('a')
            if not link_tag: continue

            game_name = link_tag.get_text(strip=True)
            detail_url = urljoin(base_url, link_tag['href'])
            
            # --- SCRAPE MISSING COLUMNS FROM MASTER LIST ---
            game_num = cells[1].get_text(strip=True)
            price = float(cells[2].get_text(strip=True).replace('$', ''))
            start_date = cells[3].get_text(strip=True)
            last_date = cells[4].get_text(strip=True)
            odds_str = cells[8].get_text(strip=True).split(' in ')[-1]
            overall_odds = float(odds_str) if odds_str else 1.0
            
            # Check for 2nd Chance / eXTRA Chances
            chance_val = cells[9].get_text(strip=True)
            has_chance = "Yes" if "➞" in chance_val or (chance_val.isdigit() and int(chance_val) > 0) else "No"

            print(f"  > Processing: {game_name}")

            dr = session.get(detail_url, timeout=20)
            dsoup = BeautifulSoup(dr.content, 'html.parser')
            
            # --- PHOTO EXTRACTION ---
            # Colorado usually identifies the primary ticket with 'img-responsive'
            img_tag = dsoup.find('img', class_='img-responsive')
            game_photo = urljoin(base_url, img_tag['src']) if img_tag else ""
            # --- FIXED CO PHOTO EXTRACTION ---
            # 1. Target the specific selector path you identified
            img_tag = dsoup.select_one('#uncovered > img')
            
            game_photo = ""
            if img_tag:
                raw_url = img_tag.get('src')
                if raw_url:
                    game_photo = urljoin(base_url, raw_url)
            
            # 2. Fallback: If #uncovered is missing, look for alt text "Front - Uncovered"
            if not game_photo:
                fallback_img = dsoup.find('img', alt=re.compile(r'Front\s*-\s*Uncovered', re.I))
                if fallback_img:
                    game_photo = urljoin(base_url, fallback_img.get('src'))
            
            # 3. Last Resort: Use the old .img-responsive logic
            if not game_photo:
                old_tag = dsoup.find('img', class_='img-responsive')
                if old_tag:
                    game_photo = urljoin(base_url, old_tag.get('src'))
            
            
            prize_tag = dsoup.find('table', class_='respond')
            if prize_tag:
                df = pd.read_html(io.StringIO(str(prize_tag)))[0].copy()
                df.columns = [str(c).lower().strip() for c in df.columns]
                
                df = df.rename(columns={
                    'prize amount': 'prizeamount',
                    'winning tickets': 'Winning Tickets At Start'
                })
            
                df['prizeamount'] = df['prizeamount'].astype(str).str.replace(r'[\$,]', '', regex=True)
                df['prizeamount'] = pd.to_numeric(df['prizeamount'], errors='coerce').fillna(0)
                df['Winning Tickets At Start'] = pd.to_numeric(df['Winning Tickets At Start'], errors='coerce').fillna(0)
                df['Winning Tickets Unclaimed'] = df['Winning Tickets At Start']
                
                # Metadata for individual prize rows
                df['gameNumber'] = game_num
                df['gameName'] = game_name
                df['dateexported'] = date.today()
                all_prize_tables.append(df)
                
                # --- CAPTURE TOP PRIZE STATS FOR RATINGSTABLE ---
                top_p = df['prizeamount'].max()
                top_row = df[df['prizeamount'] == top_p].iloc[0]
                
                summary_data.append({
                    'price': price,
                    'gameName': game_name,
                    'gameNumber': game_num,
                    'topprize': top_p,
                    'overallodds': overall_odds,
                    'topprizestarting': top_row['Winning Tickets At Start'],
                    'topprizeremain': top_row['Winning Tickets Unclaimed'],
                    'topprizeavail': np.nan if top_row['Winning Tickets Unclaimed'] > 0 else "Top Prize Claimed",
                    'extrachances': has_chance,
                    'secondChance': has_chance,
                    'startDate': start_date,
                    'endDate': "",
                    'lastdatetoclaim': last_date,
                    'dateexported': date.today(),
                    'gameURL': detail_url,
                    'gamePhoto': game_photo
                })
                

            del dsoup
            gc.collect()

        except Exception as e:
            print(f"    - Error on game {i}: {e}")

    # --- PROCESSING AND SAVING ---
    if all_prize_tables:
        tixtables = pd.concat(all_prize_tables, ignore_index=True)
        # Convert the list of dicts to a proper DataFrame to avoid KeyError
        scratchersall = pd.DataFrame(summary_data)
        
        # Clean scratchertables for the stats engine
        scratchertables = tixtables.loc[:, ['gameNumber', 'gameName', 'prizeamount', 
                                            'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']].copy()
        scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(int)
        
        tixtables = tixtables.loc[(tixtables['prizeamount']!='Prize Ticket') & (tixtables['prizeamount']!='Prize ticket') & (tixtables['prizeamount']!='PRIZE TICKET'),:]
       
        #save scratchers list
        #scratchersall.to_sql('NMscratcherlist', engine, if_exists='replace')
        scratchersall.to_csv("./COscratcherslist.csv", encoding='utf-8')
        
        #Create scratcherstables df, with calculations of total tix and total tix without prizes
        scratchertables.to_csv("./COscratchertables.csv", encoding='utf-8')
        scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
        scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
        #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
        gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
        gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizestarting','topprizeremain','overallodds', 'gamePhoto']], how='left', on=['gameNumber'])
        
        #convert columns to numeric
        for col in ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
            if col in gamesgrouped.columns:
                gamesgrouped[col] = gamesgrouped[col].astype(object)
                gamesgrouped[col] = pd.to_numeric(gamesgrouped[col], errors='coerce')
     
        gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
        gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
        gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
        gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
        gamesgrouped.replace([np.inf, -np.inf], np.nan, inplace=True)

        
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
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:].copy()
            #cast all columns to Object to start to avoid dtype errors when converting to numeric later
            for col in gamerow.columns:
                gamerow[col] = gamerow[col].astype(object)
            startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
            tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
            totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
            totalremain[['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
            price = int(gamerow['price'].values[0])

            prizes =totalremain.loc[:,'prizeamount']


            #add various columns for the scratcher stats that go into the ratings table
            gamerow.loc[:,'Current Odds of Top Prize'] = gamerow.loc[:,'topprizeodds']
            gamerow.loc[:,'Change in Current Odds of Top Prize'] =  (gamerow.loc[:,'Current Odds of Top Prize'] - float(gamerow['topprizeodds'].values[0]))/ float(gamerow['topprizeodds'].values[0])      
            gamerow.loc[:,'Current Odds of Any Prize'] = tixtotal/sum(totalremain.loc[:,'Winning Tickets Unclaimed'])
            gamerow.loc[:,'Change in Current Odds of Any Prize'] =  (gamerow.loc[:,'Current Odds of Any Prize'] - float(gamerow['overallodds'].values[0]))/ float(gamerow['overallodds'].values[0])
            denomUnclaimed = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'])
            gamerow.loc[:,'Odds of Profit Prize'] = tixtotal/denomUnclaimed if denomUnclaimed > 0 else np.nan
            denomStart = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets At Start'])
            startingprofitodds = startingtotal/denomStart if denomStart > 0 else np.nan
            gamerow.loc[:,'Starting Odds of Profit Prize'] = startingprofitodds
            gamerow.loc[:,'Change in Odds of Profit Prize'] =  (gamerow.loc[:,'Odds of Profit Prize'] - startingprofitodds)/ startingprofitodds if startingprofitodds > 0 else np.nan
            gamerow.loc[:,'Probability of Winning Any Prize'] = sum(totalremain.loc[:,'Winning Tickets Unclaimed'])/tixtotal if tixtotal > 0 else np.nan
            startprobanyprize = sum(totalremain.loc[:,'Winning Tickets At Start'])/startingtotal if startingtotal > 0 else np.nan
            gamerow.loc[:,'Starting Probability of Winning Any Prize'] = startprobanyprize
            gamerow.loc[:,'Change in Probability of Any Prize'] =  startprobanyprize - gamerow.loc[:,'Probability of Winning Any Prize']  
            gamerow.loc[:,'Probability of Winning Profit Prize'] = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'])/tixtotal if tixtotal > 0 else np.nan
            startprobprofitprize = sum(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets At Start'])/startingtotal if startingtotal > 0 else np.nan
            gamerow.loc[:,'Starting Probability of Winning Profit Prize'] = startprobprofitprize
            gamerow.loc[:,'Change in Probability of Profit Prize'] =  startprobprofitprize - gamerow.loc[:,'Probability of Winning Profit Prize']
            gamerow.loc[:,'StdDev of All Prizes'] = totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()/tixtotal if tixtotal > 0 else np.nan
            gamerow.loc[:,'StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean()/tixtotal if tixtotal > 0 else np.nan
            gamerow.loc[:,'Odds of Any Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Current Odds of Any Prize']+(totalremain.loc[:,'Winning Tickets Unclaimed'].std()*3))
            gamerow.loc[:,'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Odds of Profit Prize']+(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std()*3))
            gamerow.loc[:,'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std())
            
            
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
            if chngAvailPrizes != 0:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
            else:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = np.nan
                            
            gamerow.loc[:,'Photo'] = gamerow.loc[:,'gamePhoto']
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']

            currentodds = pd.concat([currentodds, gamerow], ignore_index=True)


            #add non-prize and totals rows with matching columns
            totalremain.loc[:,'Total remaining'] = tixtotal
            totalremain.loc[:,'Prize Probability'] = totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Total remaining']
            totalremain.loc[:,'Percent Tix Remaining'] = totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']
            game_nonprize = nonprizetix.loc[nonprizetix['gameNumber']==gameid].copy()
            game_nonprize.loc[:,'Prize Probability'] = game_nonprize['Winning Tickets Unclaimed']/tixtotal if tixtotal > 0 else 0
            game_nonprize.loc[:,'Percent Tix Remaining'] = game_nonprize['Winning Tickets Unclaimed']/game_nonprize['Winning Tickets At Start']
            game_nonprize.loc[:,'Starting Expected Value'] = (game_nonprize['prizeamount']-price)*(game_nonprize['Winning Tickets At Start']/startingtotal)
            game_nonprize.loc[:,'Expected Value'] = (game_nonprize['prizeamount']-price)*(game_nonprize['Winning Tickets Unclaimed']/tixtotal)
            
            game_totals = totals.loc[totals['gameNumber']==gameid].copy()
            game_totals.loc[:,'Prize Probability'] = game_totals['Winning Tickets Unclaimed']/tixtotal if tixtotal > 0 else 0
            game_totals.loc[:,'Percent Tix Remaining'] = game_totals['Winning Tickets Unclaimed']/game_totals['Winning Tickets At Start']
            game_totals.loc[:,'Starting Expected Value'] = ''
            game_totals.loc[:,'Expected Value'] = ''
            totalremain = pd.concat([totalremain, game_nonprize[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                     'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
            totalremain = pd.concat([totalremain, game_totals[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                     'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)

            
            #add expected values for final totals row
            allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
            
            totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)

            alltables = pd.concat([alltables, totalremain], axis=0)

        scratchertables = alltables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]

        
        #save scratchers tables
        #scratchertables.to_sql('COscratcherstables', engine, if_exists='replace')
        scratchertables.to_csv("./COscratchertables.csv", encoding='utf-8')
        
        #create rankings table by merging the list with the tables

        scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
        ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
        ratingstable.drop(labels=['gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
        ratingstable.rename(columns={'gameName_y':'gameName','dateexported_x':'dateexported','topprizeodds_x':'topprizeodds','overallodds_x':'overallodds','topprizestarting_y':'topprizestarting', 'topprizeremain_y':'topprizeremain'}, inplace=True)
        #add number of days since the game start date as of date exported
        ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(ratingstable['dateexported'])- pd.to_datetime(ratingstable['startDate'], format='mixed', errors='coerce')).dt.days

        
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

        ratingstable['Stats Page'] = "/colorado-statistics-for-each-scratcher-game"

        ratingstable.to_csv("./COratingstable.csv", encoding='utf-8')

        
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
           'Data Date','Stats Page','gameURL']]
        ratingstable = ratingstable.replace([np.inf, -np.inf], 0).infer_objects(copy=False)
        ratingstable = ratingstable.astype(object).fillna('').infer_objects(copy=False)
   
        print("✅ Success! Files saved for CO.")
        return ratingstable, scratchertables

if __name__ == "__main__":
    exportScratcherRecs()