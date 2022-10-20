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
import logging.handlers
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
from psycopg2.extensions import register_adapter, AsIs
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import df2gspread as d2g


#logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
try:
    service_account_info = json.load(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
except KeyError:
    service_account_info = "Google application service account info not available"
    #logger.info("Google application service account info not available!")
    #raise

#service_account_info = json.loads(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
credentials = service_account.Credentials.from_service_account_info(
    service_account_info)

#credentials = Credentials.from_service_account_file('./scratcherstats_googleacct_credentials.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# open a google sheet
gs = gc.open_by_key('1vAgFDVBit4C6H2HUnOd90imbtkCjOl1ekKychN2uc4o')
# select a work sheet from its name
testratingssheet = gs.worksheet('TestRatingsTable')

    
DATABASE_URL = 'postgres://wgmfozowgyxule:8c7255974c879789e50b5c05f07bf00947050fbfbfc785bd970a8bc37561a3fb@ec2-44-195-16-34.compute-1.amazonaws.com:5432/d5o6bqguvvlm63'
print(DATABASE_URL)

#replace 'postgres' with 'postgresql' in the database URL since SQLAlchemy stopped supporting 'postgres' 
SQLALCHEMY_DATABASE_URI = DATABASE_URL.replace('postgres://', 'postgresql://')
conn = psycopg2.connect(SQLALCHEMY_DATABASE_URI, sslmode='require')
engine = create_engine(SQLALCHEMY_DATABASE_URI)

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
logger.info(f'Running lotteryscrape.py at: {now}')

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

        
def exportVAScratcherRecs():
    url = "https://www.valottery.com/api/v1/scratchers"

    payload = "page=0&totalPages=0&pageSize=150\n"
    headers = {
      'Accept': 'application/json',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'Cookie': 'ASP.NET_SessionId=ezrqoarnfcq140zkskvnr5wv; SC_ANALYTICS_GLOBAL_COOKIE=e9c1d1d7d8304a159112163a356e9d7f|False'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    tixlist = response.json()
    
    tixtables = pd.DataFrame()
    #with open('scratcherlist.txt') as json_file:
        #tixlist = json.load(json_file)
    for t in tixlist['data']:
        ticID = t['GameID']
        closing = t['IsClosingSoon']
        PayoutNumber = t['PayoutNumber']
        print(ticID)

        ticketurl = 'https://www.valottery.com/scratchers/'+str(ticID)
        #ticketurl = 'https://www.valottery.com/scratchers/2057'
        
        r = requests.get(ticketurl)
        gameNum = r.text
        soup = BeautifulSoup(gameNum, 'html.parser')
        try:
            table = soup.select('#scratcher-detail-container > div > div:nth-child(3) > div:nth-child(4) > div > table')
            tableData = pd.read_html(str(table))[0]
            
        except ValueError as e:
            print(e) # ValueError: No tables found
            try:
                table = soup.select('#scratcher-detail-container > div > div:nth-child(3) > div:nth-child(5) > div > table')
                tableData = pd.read_html(str(table))[0]
            except ValueError as e:
                print(e) # ValueError: No tables found 
                continue
            
        tableData['prizeamount'] = tableData['Prize Amount'].replace('*','')
        tableData['gameNumber'] = soup.find('h2', class_='title-display').find('small').get_text()
        tableData['gameName'] = soup.find('h2', class_='title-display').find(text=True, recursive=False).strip()
        tableData['price'] = soup.find('h2', class_='ticket-price-display').get_text()
        tableData['overallodds'] = soup.find('p', class_='odds-display').find('span').get_text()
        tableData['topprize'] = soup.find('h2', class_='top-prize-display').get_text().replace('*','')
        tableData['topprizeodds'] = soup.find('p', class_='odds-display').find('br').find('span').get_text()
        tableData['topprizeremain'] = tableData.iloc[0,2]
        tableData['extrachances'] = 'eXTRA Chances' if soup.find('p', text=re.compile('eXTRA Chances')) else np.nan
        tableData['secondChance'] = '2nd Chance' if soup.find('p', text=re.compile('2nd Chance')) else np.nan
        tableData['startDate'] = soup.find_all('h2', class_='start-date-display')[0].get_text()
        tableData['endDate'] = soup.find_all('h2', class_='start-date-display')[1].get_text() if (closing == True & PayoutNumber != 0) else np.nan
        tableData['lastdatetoclaim'] = soup.find_all('h2', class_='start-date-display')[2].get_text() if (closing == True & PayoutNumber != 0) else np.nan
        tableData['topprizeavail'] = 'Top Prize Claimed' if tableData.iloc[0,2] == 0 else np.nan
        tableData['dateexported'] = date.today()  

        #print(tableData)
        if tableData.empty:
            continue
        else:
            tixtables = tixtables.append(tableData)
    
    #remove characters from numeric values
    tixtables['gameNumber'] = tixtables['gameNumber'].replace('#','', regex = True)
    tixtables['prizeamount'] = tixtables['prizeamount'].str.replace('*','', regex = True)
    tixtables['prizeamount'] = tixtables['prizeamount'].str.replace(',','', regex = True)
    tixtables['prizeamount'] = tixtables['prizeamount'].replace({r'\$':''}, regex = True)
    tixtables['price'] = tixtables['price'].replace({r'\$':''}, regex = True)
    tixtables['topprize'] = tixtables['topprize'].str.replace('*','', regex = True)
    tixtables['topprize'] = tixtables['topprize'].str.replace(',','', regex = True)
    tixtables['topprize'] = tixtables['topprize'].replace({r'\$':''}, regex = True)
    
    #convert text top prizes by calculating the ammounts
    #converts the tax prizes to the $50k + 4% tax rate, $2k/week*52 weeks/yr*10yrs, and Live Spin to the max prize $500,000 
    tixtables['topprize'] = tixtables['topprize'].replace({'50000 + Taxes':50000*1.04,'2K/Wk for 10 Yrs':2000*52*10, 'Live Spin':500000}) 
    tixtables['prizeamount'] = tixtables['prizeamount'].replace({'50000 + Taxes':50000*1.04,'2K/Wk for 10 Yrs':2000*52*10, 'Live Spin':500000}) 
    tixtables['topprize'] = tixtables['topprize'].apply(formatstr).astype('int64')
    
    scratchersall = tixtables[['price','gameName','gameNumber','topprize','topprizeodds','overallodds','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    scratchersall.to_sql('VAscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./VAscratcherslist.csv", encoding='utf-8')
    
    # write to Google Sheets
    testratingssheet.clear()
    set_with_dataframe(worksheet=testratingssheet, dataframe=scratchersall, include_index=False,
    include_column_header=True, resize=True)
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
   
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall[['gameNumber','price','topprizeodds','overallodds']], how='left', on=['gameNumber'])
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped.loc[:,'topprizeodds'].str.replace(',','', regex = True)
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    
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
        print(gamerow)

        #add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:,'Current Odds of Top Prize'] = tixtotal/totalremain.loc[0,'Winning Tickets Unclaimed']
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
        #totalremain.loc[:,'Starting Expected Value'] = ''
        #totalremain.loc[:,'Expected Value'] = ''
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
        
        #function to get url for photo based on game number, like a case-swtich statement
        def photolink(i):
            switcher = {
                '1841':"https://www.valottery.com/-/media/VAL/Images/Scratcher-Game-Tiles/1841_Extreme-Millions_teaser.ashx",
                '1874':"https://www.valottery.com/-/media/val/images/scratcher-game-tiles/1874_super_cash_frenzy_teaser.ashx",
                '1888':"https://www.valottery.com/-/media/val/images/scratcher-game-tiles/1888_30k-cash-party_teaser.ashx",
                '1895':"https://www.valottery.com/-/media/val/images/scratcher-game-tiles/1895_100x-the-money_teaser.ashx",
                '1773':"https://www.valottery.com/-/media/VAL/Images/Scratcher-Game-Tiles/1773_Jewel-7s_teaser.ashx",
                '1948':"https://www.valottery.com/-/media/VAL/Images/Scratcher-Game-Tiles/1948_teaser.ashx",
                i:"https://www.valottery.com/-/media/val/images/digital-scratcher-teaser-images/" + i + "_teaser.ashx"
                }

            return switcher.get(i,"Invalid game number")
                    
        gamerow.loc[:,'Photo'] = photolink(str(gameid))
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
    scratchertables.to_sql('VAscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./VAscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gameName_x','dateexported_y','topprizeodds_y','overallodds_y'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_x':'gameName','dateexported_x':'dateexported','topprizeodds_x':'topprizeodds','overallodds_x':'overallodds'}, inplace=True)
    #add number of days since the game start date as of date exported
    ratingstable.loc[:,'Days Since Start'] = (pd.to_datetime(ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'])).dt.days
    
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
    ratingstable.to_sql('VAratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./VAratingstable.csv", encoding='utf-8')
    return ratingstable, scratchertables

def exportAZScratcherRecs():
    url = "https://www.arizonalottery.com/scratchers/#all"
    r = requests.get(url)
    response = r.text
    # print(r.text)
    soup = BeautifulSoup(response, 'html.parser')

    tixlist = pd.DataFrame(columns=['gameName', 'gameNumber', 'price', 'gameURL'])
    table = soup.find_all(class_=['section'])
    logos = soup.find_all(class_=['logo'])

    # print(table)
    tixrow = pd.DataFrame()
    for s in table:
        gamenames = s.find(class_='game-name').string
        gameURL = s.find(class_='game-name').get('href')
        gameName = gamenames.partition(' #')[0]
        gameNumber = gamenames.partition(' #')[2]
        gamePrice = s.find(class_='col-md-6 price').find('span').string
        try:
            gamePhoto = "https://www.arizonalottery.com"+soup.select_one("img[src*='"+gameNumber+"']")["src"].split('?')[0]
        except:
            gamePhoto = None
            continue
        print(gamenames)
        print(gameName)
        print(gameNumber)
        print(gamePrice)
        print(gameURL)
        print(gamePhoto)
            
        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto']] = [
            gamePrice, gameName, gameNumber, gameURL, gamePhoto]

    #tixlist.to_csv("./AZtixlist.csv", encoding='utf-8')
    
    
    tixtables = pd.DataFrame(columns=['gameNumber','gameName','price','prizeamount','startDate','endDate','lastdatetoclaim','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported'])

    for i in tixlist.loc[:, 'gameNumber']:

        url = "https://api.arizonalottery.com/v2/Scratchers/"+i

        payload = "page=0&totalPages=0&pageSize=150\n"
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        }

        response = requests.get(url=url)
        tixdata = response.json()
        scratcherdata = pd.DataFrame.from_dict(tixdata)

        gameName = scratcherdata['gameName'][0]
        gameNumber = scratcherdata['gameNum'][0]
        gamePrice = scratcherdata['ticketValue'][0]
        startDate = datetime.fromisoformat(scratcherdata['beginDate'][0])
        endDate = None if 'endDate' not in scratcherdata else datetime.fromisoformat(scratcherdata['endDate'][0])
        lastdatetoclaim = datetime.fromisoformat(scratcherdata['lastDate'][0])
        gameOdds = scratcherdata['gameOdds'][0]
        dateexported = pd.to_datetime(scratcherdata['dateModified'][0],infer_datetime_format=True)

        print('Looping through each prize tier row for scratcher #'+i)
        for row in scratcherdata['prizeTiers']:
            prizetier = pd.DataFrame.from_dict([row])
            prizeamount = prizetier['prizeAmount'][0]
            prizeodds = prizetier['odds'][0]
            startingprizecount = prizetier['totalCount'][0]
            remainingprizecount = prizetier['count'][0]
            tixtables.loc[len(tixtables.index), ['gameNumber','gameName','price','prizeamount','startDate','endDate','lastdatetoclaim',
                                                 'overallodds','prizeodds','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported','tierLevel']] = [gameNumber, gameName, gamePrice, prizeamount, 
                                                                                                                                          startDate, endDate, lastdatetoclaim, gameOdds, prizeodds, startingprizecount, remainingprizecount, dateexported, prizetier['tierLevel'][0]]
        
        #tixtables['gameNumber'] = gameNumber
        index = tixtables[tixtables['gameNumber']==gameNumber].index
        tixtables.loc[index,'gameName'] = gameName
        tixtables.loc[index,'price'] = gamePrice
        topprize = tixtables.loc[(tixtables['tierLevel']==1) & (tixtables['gameNumber']==gameNumber),'prizeamount'].iloc[0]
        topprizeodds = tixtables.loc[(tixtables['tierLevel']==1) & (tixtables['gameNumber']==gameNumber),'prizeodds'].iloc[0]
        topprizeremain = tixtables.loc[(tixtables['tierLevel']==1) & (tixtables['gameNumber']==gameNumber),'Winning Tickets Unclaimed'].iloc[0]
        topprizeavail = 'Top Prize Claimed' if topprizeremain==0 else None

        
        tixtables.loc[index,'topprize'] = topprize
        tixtables.loc[index,'topprizeodds'] = topprizeodds
        tixtables.loc[index,'topprizeremain'] = topprizeremain
        tixtables.loc[index,'topprizeavail'] =  topprizeavail                                                                                                                             
        tixtables.loc[index,'extrachances'] = None
        tixtables.loc[index,'secondChance'] = None
                                                                                                                               
    #tixtables.to_csv("./AZprizedata.csv", encoding='utf-8')
    scratchersall = tixtables[['price','gameName', 'gameNumber','topprize', 'topprizeodds', 'overallodds', 'topprizeremain','topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported']]
    scratchersall = scratchersall.drop_duplicates(subset=['price','gameName', 'gameNumber','topprize', 'topprizeodds', 'overallodds', 'topprizeremain','topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported'])
    scratchersall = scratchersall.loc[scratchersall['gameNumber']!= "Coming Soon!", :]
    print(scratchersall.dtypes)
    #scratchersall = scratchersall.drop_duplicates()
    # save scratchers list
    scratchersall.to_sql('AZscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./azscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount','Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel', 'dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber']!= "Coming Soon!", :]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(by=['gameNumber', 'gameName', 'dateexported'],group_keys=False)['Winning Tickets At Start', 'Winning Tickets Unclaimed'].sum().reset_index(level=['gameNumber','gameName','dateexported']).copy()
    gamesgrouped = gamesgrouped.merge(scratchersall[[
                                      'gameNumber', 'price', 'topprizeodds', 'overallodds']], how='left', on=['gameNumber'])
    #gamesgrouped.loc[:, 'topprizeodds'] = gamesgrouped.loc[:,'topprizeodds'].str.replace(',', '', regex=True)

    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    gamesgrouped.loc[:, 'Total at start'] = gamesgrouped['Winning Tickets At Start'] * \
        gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'] * \
        gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Non-prize at start'] = gamesgrouped['Total at start'] - \
        gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:, 'Non-prize remaining'] = gamesgrouped['Total remaining'] - \
        gamesgrouped['Winning Tickets Unclaimed']

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
    nonprizetix = gamesgrouped[['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped[['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start',
                  'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:, 'prizeamount'] = "Total"


    # loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame()
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :].copy()
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
            'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'tierLevel','dateexported']]
        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed','tierLevel']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])
        print(gameid)
        print(gamerow)
        print(gamerow.columns)

        prizes = totalremain.loc[:, 'prizeamount']
        
        startoddstopprize = tixtotal / totalremain.loc[totalremain['tierLevel']==1, 'Winning Tickets At Start'].values[0]

        # add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:, 'Current Odds of Top Prize'] = float(gamerow['topprizeodds'].values[0])
        gamerow.loc[:, 'Change in Current Odds of Top Prize'] = (gamerow.loc[:, 'Current Odds of Top Prize'] - float(
            startoddstopprize)) / float(startoddstopprize)
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
        gamerow.loc[:, 'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean())

        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)


        testdf = totalremain[[
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']]
        totalremain.loc[:, 'Starting Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[:, 'Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
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
        gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes

        print(gameid)
        print(tixlist.dtypes)
        print(type(gameid))
        print(tixlist.loc[tixlist['gameNumber'].astype('int')==gameid, ['gameName','gameNumber','gamePhoto']])
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber'].astype('int')==gameid,['gamePhoto']].values[0]
        gamerow.loc[:, 'FAQ'] = None
        gamerow.loc[:, 'About'] = None
        gamerow.loc[:, 'Directory'] = None
        gamerow.loc[:, 'Data Date'] = gamerow.loc[:, 'dateexported']

        currentodds = currentodds.append(gamerow, ignore_index=True)

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
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                   'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]
        totalremain = totalremain.append(nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']], ignore_index=True)
        totalremain = totalremain.append(totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']], ignore_index=True)
        print(totalremain.columns)

        # add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount'] != 'Total', :]

        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        print(totalremain)
        alltables = alltables.append(totalremain)

    scratchertables = alltables[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]

    # save scratchers tables
    scratchertables.to_sql('azscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./azscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y',
                      'topprizeodds_y', 'overallodds_y'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_x': 'gameName', 'dateexported_x': 'dateexported',
                        'topprizeodds_x': 'topprizeodds', 'overallodds_x': 'overallodds'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'])).dt.days

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
    print(ratingstable)
    print(ratingstable.columns)
    ratingstable.to_sql('AZratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./azratingstable.csv", encoding='utf-8')
    return ratingstable, scratchertables

#function to create an array of prizes by their probability for all scratchers still unclaimed
def generateWeightedList(prizes, prizeNumbers, tixTotal, tixinRoll):
    weighted_list = []
    for prize in prizes:
        prizeCount = int(prizeNumbers.loc[prizeNumbers['prizeamount']==prize,'Winning Tickets Unclaimed'].values[0])
        weighted_list.extend(repeat(prize,prizeCount))
    
    random.shuffle(weighted_list)
    return weighted_list

#calculate the optimal bankroll from the bet amount (ticket cost), the negative probability (probability of losing, factoring in stdDev), 
#the longest expected losing streak (from odds and stdDev), and the Risk Coeeficient - higher if risk-averse (e.g., 5 or higher) or lower if more risk-tolerant (e.g. 2 or less))
def optimalbankroll(cost, stDev, probability, odds, riskCoeff):
    negProbability = 1 - (probability + stDev*3)
    LonglosingStreak = np.round(abs(np.log(float(odds))/np.log(float(negProbability))))
    bankroll = cost*LonglosingStreak*riskCoeff
    d = dict()
    d['Longest Losing Streak'] = LonglosingStreak
    d['Optimal Bankroll'] = bankroll
    return d

allSimOutcomes = pd.DataFrame()
allSimTables = pd.DataFrame()                

def clusterloop(ratingstable, scratchertables, prizetype, stddevs, riskCoeff):
    
    #determine the size of the roll based on the scratcher price
    def rollsize(price):
            switcher = {
                0: 100,
                1: 200,
                2: 100,
                3: 100,
                5: 40,
                10: 40,
                20: 20,
                30: 20
                }
            return switcher.get(price,"Invalid game price")
        
    simulations = pd.DataFrame()
    simTable = pd.DataFrame(columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size',  
                                     'Ticket Number','Ticket Outcome','Cluster Number','Cluster Outcome','Total Tally'])
    simOutcomes = pd.DataFrame(columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size', 'Any Prize Probability',
                                        'Profit Prize Probability','Number of Tickets', 'Number of Clusters', 'Number of Prizes', 'Number of Profit Prizes', 'Observed Prize Frequency', 'Observed Profit Prize Frequency',
                                        'Average Cluster Outcome', 'Median Cluster Outcome', 'StdDev Ticket Outcome', 'StdDev Any Prizes', 'StdDev Profit Prizes', 'Final Tally', 'Prizes Descriptive Stats'])
                                        
    
    #loop through each game generating outcomes for a sample of tickets in clusters    
    for gameid in ratingstable.loc[:,'gameNumber']:
        print(gameid)
        price = float(ratingstable.loc[ratingstable['gameNumber']==gameid,'price'].values[0])
        print(price)
        gamename = ratingstable.loc[ratingstable['gameNumber']==gameid,'gameName'].values[0]
        prizes = scratchertables.loc[(scratchertables['gameNumber']==gameid) & (scratchertables['prizeamount']!="Total"),'prizeamount']
        totalprizes = scratchertables.loc[(scratchertables['gameNumber']==gameid) & (scratchertables['prizeamount']!="Total"),['prizeamount','Winning Tickets Unclaimed']]
        totaltixstarting = scratchertables.loc[(scratchertables['gameNumber']==gameid) & (scratchertables['prizeamount']=="Total"),'Winning Tickets At Start'].values[0]
        totaltixremain = int(scratchertables.loc[(scratchertables['gameNumber']==gameid) & (scratchertables['prizeamount']=="Total"),'Winning Tickets Unclaimed'].values[0])
        print(totaltixremain)
        tixinRoll = rollsize(price)
        print(tixinRoll)
        
        
        #settings for the simulation based on function parameters
        if (prizetype == "profit"):
            gameprob = float(ratingstable.loc[ratingstable['gameNumber']==gameid,'Probability of Winning Profit Prize'].values[0])
            oddsprizes = float(ratingstable.loc[ratingstable['gameNumber']==gameid,'Odds of Profit Prize'].values[0])
            stDevpct = float(totalprizes.loc[(totalprizes['prizeamount']!=price) & (totalprizes['prizeamount']!="0"), 'Winning Tickets Unclaimed'].std().mean()/totaltixremain)
            totalprizesremain = totalprizes.loc[(totalprizes['prizeamount']!=price) & (totalprizes['prizeamount']!="0"),'Winning Tickets Unclaimed'].sum()
        
        elif (prizetype == "any"):   
            gameprob = float(ratingstable.loc[ratingstable['gameNumber']==gameid,'Probability of Winning Any Prize'].values[0])
            oddsprizes = float(ratingstable.loc[ratingstable['gameNumber']==gameid,'Current Odds of Any Prize'].values[0])
            stDevpct = float(totalprizes.loc[totalprizes['prizeamount']!="0", 'Winning Tickets Unclaimed'].std().mean()/totaltixremain)
            totalprizesremain = totalprizes.loc[totalprizes['prizeamount']!="0",'Winning Tickets Unclaimed'].sum()
        
        print(oddsprizes)
        print(stddevs)
        print(stDevpct)
        print(gameprob)
        #probability plus std dev percent for max probability
        print(gameprob-(stDevpct*stddevs))
        #probability converted to decimal odds, expanding number of tickets by subtracting standard deviations so the 1 in X number grows with more Std Devs
        print(1/(gameprob-(stDevpct*stddevs)))
        
        #get a cluster size by taking the probability any prize then converting to decimal odds, expanding number of tickets by subtracting standard deviations so the 1 in X number grows with more Std Devs
        clustersize = int(np.round(1/(gameprob-(stDevpct*stddevs)),0))
        print(clustersize) 
        #description of the parameters to add to the file name
        description = prizetype+"-"+str(stddevs)+"stDevs"+"-RiskCoeff"+str(riskCoeff)
        
        #Get the sample size of total game, and them a number of clusters in hte sample
        sampleSize = np.round(totaltixremain/(1+(totaltixremain*np.power(0.03, 2))))
        print(sampleSize)
        clusterSample = np.round(sampleSize/clustersize)
        print(clusterSample)
        
        #use above numbers to generate a randomly shuffled list of prizes, then select a set to form a roll of scratcher tickets
        weightedList = generateWeightedList(prizes, totalprizes, totaltixstarting, tixinRoll)
        startpos = random.randint(0,tixinRoll)
        endpos = startpos+tixinRoll
        roll = weightedList[startpos:endpos]
        print(roll)
        
        #use function to get the optimal bankroll amount from the probability, standard deviations, and risk factor
        #divide stDev by totaltixremain so that it is a percentage of total, like the game prize probability figure
        bankroll = optimalbankroll(price, stDevpct, gameprob, clustersize, riskCoeff)
        longlosingstreak = bankroll['Longest Losing Streak']
        bankroll = bankroll['Optimal Bankroll']
        print(longlosingstreak)
        print(bankroll)
        
        tally = 0
        clusterCount = 1
        
        
        #loop through each ticket in the cluster
        print(len(roll))
        simCluster = pd.DataFrame(columns=simTable.columns)
        
        #pull tickets from cluster until the total tally sucks up the bankroll amount or until it goes through sample of clusters
        #while (tally >= -(bankroll)) & (clusterCount <= clusterSample):
        
        #Maybe change this run until the ticket sample size instead of bankroll?
        while (clusterCount <= clusterSample):
            randnum = random.randint(0,tixinRoll)
            print(randnum)
            
            cluster = []
            clusterOutcome = 0
 
            #but first check if the number of tickets purchased will exceed number left in the roll
            if (len(roll) - randnum) < (clustersize):
               clustergroup = roll[(randnum):(tixinRoll)]
               print(clustergroup)
               
               #get new roll to get remainder of cluster
               startpos = randnum
               endpos = startpos+tixinRoll
               roll = weightedList[startpos:endpos]
               print(roll)
               
               #get new cluster starting at first ticket in roll
               startpos = 0
               endpos = clustersize-len(clustergroup)
               clustergroup.extend(roll[startpos:endpos])
               print(clustergroup)
            
            else:
                #get the cluster as it is from the same roll
                clustergroup = roll[(randnum):(randnum+clustersize)]
            print(clustergroup)
            print(len(clustergroup))
            
            tic = 1
            
            
            #loop through each possible ticket up to the possible tickets in cluster
            while tic <= len(clustergroup):
                print(tic)
                print(len(clustergroup))
                print(clustersize)

                ticOutcome = float(clustergroup[tic-1])
                print(ticOutcome)
                tally = tally + ticOutcome - price
                print(tally)
                clusterOutcome = clusterOutcome + ticOutcome - price
                print(clusterOutcome)
                cluster = [gameid, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize, 
                                tic, ticOutcome, clusterCount, clusterOutcome, tally]
                print(cluster)  
                
                #add cluster to dataframe of each cluster for this gameid, so it can be used for stats
                simCluster.loc[len(simCluster)] = cluster
                
                #add cluster outcome to a dataframe for all clusters of all games
                simTable.loc[len(simTable)] = cluster
                print(simTable.shape)
                
                #advance the ticket count by one
                tic = tic + 1
            

            print(simCluster.shape)
            #advance the cluster count by one
            clusterCount = clusterCount + 1
            
        #compile stats for comparison table
        probAnyPrize = ratingstable.loc[ratingstable['gameNumber']==gameid,'Probability of Winning Any Prize'].values[0]
        probProfitPrize = ratingstable.loc[ratingstable['gameNumber']==gameid,'Probability of Winning Profit Prize'].values[0]
        numTickets = simCluster['Ticket Number'].count()
        numClusters = clusterCount-1
        numPrizes = simCluster.loc[simCluster['Ticket Outcome']>0, 'Ticket Outcome'].count()
        numProfitPrizes = simCluster.loc[simCluster['Ticket Outcome']>price,'Ticket Outcome'].count()
        prizeFreq = numPrizes/numTickets
        profitprizeFreq = numProfitPrizes/numTickets
        avgClusterOutcome = simCluster['Cluster Outcome'].mean()
        medianClusterOutcome = simCluster['Cluster Outcome'].median()
        stdevTicketOutcome = simCluster['Ticket Outcome'].std()
        stdevAnyprizes = simCluster.loc[simCluster['Ticket Outcome']>0,'Ticket Outcome'].std()
        stdevAnyprizes = simCluster.loc[simCluster['Ticket Outcome']>price,'Ticket Outcome'].std()
        finalTally = simCluster['Cluster Outcome'].sum()
        prizesstats = simCluster.loc[simCluster['Ticket Outcome']>0, 'Ticket Outcome'].describe()
        
        result = [gameid, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize, probAnyPrize, probProfitPrize,
                  numTickets, numClusters, numPrizes, numProfitPrizes, prizeFreq, profitprizeFreq, avgClusterOutcome, medianClusterOutcome, 
                  stdevTicketOutcome, stdevAnyprizes, stdevAnyprizes, finalTally, prizesstats]
        print(result)
        #add stats to a dataframe of stats for each game
        simOutcomes.loc[len(simOutcomes)] = result
        
        print(simTable)
        print(simOutcomes)
    
    allSimOutcomes.append(simOutcomes, ignore_index=False)
    allSimTables.append(simTable, ignore_index=False)
    
    simTable.to_csv("/Users/michaeljames/Documents/scratchersdatapull/simTable_"+description+"2.csv", encoding='utf-8')
    simOutcomes.to_csv("/Users/michaeljames/Documents/scratchersdatapull/simOutcomes_"+description+"2.csv", encoding='utf-8')

    allSimTables.to_csv("/Users/michaeljames/Documents/scratchersdatapull/simTable_0-3StDevs.csv", encoding='utf-8')
    allSimOutcomes.to_csv("/Users/michaeljames/Documents/scratchersdatapull/simOutcomes_0-3StDevs.csv", encoding='utf-8')

    return simTable, simOutcomes
    

prizetypes = ['any','profit']
stdeviations = [0, 3]
'''
#loop through each number of std devs and for whether any prize probability and then profit prizes
for t in prizetypes:
    for std in stdeviations:
        clusterloop(ratingstable, scratchertables, t, std, 2)        
'''                    
                
exportVAScratcherRecs()
exportAZScratcherRecs()

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
logger.info(f'Finishing lotteryscrape.py at: {now}')
'''
scheduler = BlockingScheduler()
scheduler.add_job(exportVAScratcherRecs, 'cron', hour=0, minute=30)
scheduler.add_job(exportAZScratcherRecs, 'cron', hour=0, minute=30)
scheduler.start()
'''

