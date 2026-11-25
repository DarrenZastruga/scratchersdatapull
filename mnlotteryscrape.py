#!/usr/bin/env python3
"""MN WordPress scratch games; returns ratings and prize tiers to the dual-write runner."""
import requests
import pandas as pd
import numpy as np
import io
import re
import time
from bs4 import BeautifulSoup
from datetime import date
from urllib.parse import urljoin, urlparse

BASE = 'https://www.mnlottery.com'
HEADERS = {'User-Agent': 'Mozilla/5.0'}

def fetch_html(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    return response.text

def game_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    links = set()
    for a in soup.select('a[href]'):
        url = urljoin(BASE, a['href'])
        parsed = urlparse(url)
        if parsed.hostname in ('mnlottery.com', 'www.mnlottery.com') and re.fullmatch(r'/games/scratch/[^/]+/?', parsed.path):
            links.add(BASE + parsed.path.rstrip('/') + '/')
    if not links:
        raise RuntimeError('MN: no scratch-game detail links; refusing empty export')
    return sorted(links)

def parse_game(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    intro = soup.select_one('section[data-mnl-game-id][data-mnl-game-price]')
    table = soup.select_one('table.b-scratch-game-prizes-odds__table')
    if intro is None or table is None:
        raise ValueError(f'MN {url}: missing current game metadata/prize table')
    gid = str(intro.get('data-mnl-game-id') or '').strip()
    name = str(intro.get('data-mnl-game-name') or '').strip()
    price = float(intro['data-mnl-game-price'])
    odds_match = re.search(r'Overall Odds\s*1\s*in\s*([\d.]+)', intro.get_text(' ', strip=True), re.I)
    if not gid.isdigit() or not name or price <= 0 or not odds_match:
        raise ValueError(f'MN {url}: invalid number, name, price or overall odds')
    odds = float(odds_match.group(1))
    # Mobile cells repeat labels, so remove only label elements before reading.
    for label in table.select('.b-scratch-game-prizes-odds__cell-label'):
        label.decompose()
    df = pd.read_html(io.StringIO(str(table)), displayed_only=False)[0]
    df = df.rename(columns={'Prize': 'prizeamount', 'Total Prizes**': 'Winning Tickets At Start',
                            'Prizes Remaining': 'Winning Tickets Unclaimed'})
    required = ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']
    for col in required:
        raw = df[col].astype(str).str.replace(r'[$,]', '', regex=True).str.strip()
        if col == 'prizeamount':
            raw = raw.str.replace(r'^(?:free )?ticket$', str(price), regex=True, case=False)
        df[col] = pd.to_numeric(raw, errors='coerce')
    if df[required].isna().any().any() or (df[required] < 0).any().any():
        raise ValueError(f'MN {gid}: invalid prize amounts or counts')
    claimed = pd.to_numeric(df['Prizes Claimed'].astype(str).str.replace(',', '', regex=False), errors='coerce')
    if not ((df['Winning Tickets At Start'] - claimed) == df['Winning Tickets Unclaimed']).all():
        raise ValueError(f'MN {gid}: claimed/remaining inventory does not reconcile')
    if odds <= 1 or df['Winning Tickets At Start'].sum() <= 0 or df['Winning Tickets Unclaimed'].sum() <= 0:
        raise ValueError(f'MN {gid}: unusable inventory/odds')
    df = df[required].copy().sort_values('prizeamount', ascending=False)
    df['gameNumber'], df['gameName'], df['dateexported'] = gid, name, date.today().isoformat()
    top = df.iloc[0]
    image = next((img for img in intro.select('img[src]') if 'ticket' in str(img.get('alt', '')).lower()), None)
    photo = urljoin(BASE, image['src']) if image is not None else ''
    summary = {'gameNumber': gid, 'gameName': name, 'price': price, 'overallodds': odds,
               'topprize': top['prizeamount'], 'topprizestarting': top['Winning Tickets At Start'],
               'topprizeremain': top['Winning Tickets Unclaimed'],
               'topprizeavail': 'Top Prize Claimed' if top['Winning Tickets Unclaimed'] == 0 else '',
               'gamePhoto': photo, 'gameURL': url, 'dateexported': date.today().isoformat(),
               'startDate': None, 'endDate': None, 'lastdatetoclaim': None,
               'extrachances': None, 'secondChance': '2nd Chance' if intro.select_one('[data-mnl-action="second_chance_enter"]') else None}
    return summary, df

def exportScratcherRecs():
    summaries, tables = [], []
    links = game_links(fetch_html(BASE + '/games/scratch/'))
    print(f'MN: discovered {len(links)} game pages')
    for url in links:
        try:
            summary, table = parse_game(fetch_html(url), url)
            summaries.append(summary)
            tables.append(table)
            print(f"MN: {summary['gameNumber']} {summary['gameName']}, {len(table)} prize tiers")
        except Exception as exc:
            print(f'MN: failed {url}: {exc}')
        time.sleep(0.25)
    if not summaries:
        raise RuntimeError('MN: no valid games collected; refusing empty output')
    return calculate_ratings(summaries, pd.concat(tables, ignore_index=True))

def calculate_ratings(scratchersall_list, tixtables):
    scratchersall = pd.DataFrame(scratchersall_list)
    scratchersall.to_csv("./MNscratcherslist.csv", encoding='utf-8', index=False)
    
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./MNscratchertables.csv", encoding='utf-8', index=False)
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})

    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index()
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','gamePhoto', 'price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)

    for col in ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']:
        if col in gamesgrouped.columns:
            gamesgrouped[col] = gamesgrouped[col].astype(object)
            gamesgrouped[col] = pd.to_numeric(gamesgrouped[col], errors='coerce')
    
    gamesgrouped[['price','overallodds']] = gamesgrouped[['price','overallodds']].apply(pd.to_numeric, errors='coerce')
    
    gamesgrouped['Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds']
    gamesgrouped['Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds']
    gamesgrouped['Non-prize at start'] = gamesgrouped['Total at start'] - gamesgrouped['Winning Tickets At Start']
    gamesgrouped['Non-prize remaining'] = gamesgrouped['Total remaining'] - gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped['topprizeodds'] = gamesgrouped['Total at start'] / gamesgrouped['topprizestarting']
    gamesgrouped[['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped[['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    gamesgrouped.replace([np.inf, -np.inf], np.nan, inplace=True)
    
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
        denomUnclaimed = sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'])
        gamerow.loc[:,'Odds of Profit Prize'] = tixtotal/denomUnclaimed if denomUnclaimed > 0 else np.nan
        denomStart = sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start'])
        startingprofitodds = startingtotal/denomStart if denomStart > 0 else np.nan
        gamerow.loc[:,'Starting Odds of Profit Prize'] = startingprofitodds
        gamerow.loc[:,'Change in Odds of Profit Prize'] =  (gamerow.loc[:,'Odds of Profit Prize'] - startingprofitodds)/ startingprofitodds if startingprofitodds > 0 else np.nan
        gamerow.loc[:,'Probability of Winning Any Prize'] = sum(totalremain.loc[:,'Winning Tickets Unclaimed'])/tixtotal if tixtotal > 0 else np.nan
        startprobanyprize = sum(totalremain.loc[:,'Winning Tickets At Start'])/startingtotal if startingtotal > 0 else np.nan
        gamerow.loc[:,'Starting Probability of Winning Any Prize'] = startprobanyprize
        gamerow.loc[:,'Change in Probability of Any Prize'] =  startprobanyprize - gamerow.loc[:,'Probability of Winning Any Prize']  
        gamerow.loc[:,'Probability of Winning Profit Prize'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'])/tixtotal if tixtotal > 0 else np.nan
        startprobprofitprize = sum(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start'])/startingtotal if startingtotal > 0 else np.nan
        gamerow.loc[:,'Starting Probability of Winning Profit Prize'] = startprobprofitprize
        gamerow.loc[:,'Change in Probability of Profit Prize'] =  startprobprofitprize - gamerow.loc[:,'Probability of Winning Profit Prize']
        gamerow.loc[:,'StdDev of All Prizes'] = totalremain.loc[:,'Winning Tickets Unclaimed'].std().mean()/tixtotal if tixtotal > 0 else np.nan
        gamerow.loc[:,'StdDev of Profit Prizes'] = totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].std().mean()/tixtotal if tixtotal > 0 else np.nan
        gamerow.loc[:,'Odds of Any Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Current Odds of Any Prize']+(totalremain.loc[:,'Winning Tickets Unclaimed'].std()*3))
        gamerow.loc[:,'Odds of Profit Prize + 3 StdDevs'] = tixtotal/(gamerow.loc[:,'Odds of Profit Prize']+(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].std()*3))
        gamerow.loc[:,'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed'].std())
        
        
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

        gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0 if pd.isnull(chngLosingTix/chngAvailPrizes).item() == True else chngLosingTix/chngAvailPrizes
                
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
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
    scratchertables.to_csv("./MNscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables

    scratchersall['price'] = scratchersall['price'].astype(object)
    scratchersall['price'] = pd.to_numeric(scratchersall['price'], errors='coerce')
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

    ratingstable['Stats Page'] = "/minnesota-statistics-for-each-scratcher-game"
    ratingstable.to_csv("./MNratingstable.csv", encoding='utf-8')

    
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


if __name__ == "__main__":
    exportScratcherRecs()
