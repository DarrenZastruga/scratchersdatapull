import df2gspread as d2g
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import gspread
import pandas as pd
import os
import psycopg2
import urllib.parse
from urllib.parse import urlparse
import urllib.request
import json
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
from bs4 import BeautifulSoup
import re
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
from pandas import json_normalize
import io


# logging.basicConfig()

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
try:
    service_account_info = json.loads(
        os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
except KeyError:
    service_account_info = "Google application service account info not available"
    #logger.info("Google application service account info not available!")
    # raise

#service_account_info = json.loads(os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON'))
credentials = Credentials.from_service_account_info(
    service_account_info, scopes=scopes)

#credentials = Credentials.from_service_account_file('./scratcherstats_googleacct_credentials.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# open a google sheet
gs = gc.open_by_key('1vAgFDVBit4C6H2HUnOd90imbtkCjOl1ekKychN2uc4o')

# connect to postgres database on Heroku
#DATABASE_URL = 'postgres://wgmfozowgyxule:8c7255974c879789e50b5c05f07bf00947050fbfbfc785bd970a8bc37561a3fb@ec2-44-195-16-34.compute-1.amazonaws.com:5432/d5o6bqguvvlm63'
# print(DATABASE_URL)

# replace 'postgres' with 'postgresql' in the database URL since SQLAlchemy stopped supporting 'postgres'
#SQLALCHEMY_DATABASE_URI = DATABASE_URL.replace('postgres://', 'postgresql://')
#conn = psycopg2.connect(SQLALCHEMY_DATABASE_URI, sslmode='require')
#engine = create_engine(SQLALCHEMY_DATABASE_URI)

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


# function to auto-download images from scratchers site if necessary
def download_image(url, file_path, file_name):
    full_path = file_path + file_name
    urllib.request.urlretrieve(url, full_path)


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
    # with open('scratcherlist.txt') as json_file:
    #tixlist = json.load(json_file)
    for t in tixlist['data']:
        ticID = t['GameID']
        print(ticID)
        closing = t['IsClosingSoon']
        PayoutNumber = t['PayoutNumber']

        ticketurl = 'https://www.valottery.com/scratchers/'+str(ticID)
        #ticketurl = 'https://www.valottery.com/scratchers/2057'

        r = requests.get(ticketurl)
        gameNum = r.text
        soup = BeautifulSoup(gameNum, 'html.parser')
        
        try:
            if soup.find('div', id='scratcher-detail-container') == None:
               continue
            else:
                table = soup.find('div', id='scratcher-detail-container').find('table',class_='table table-responsive scratcher-prize-table')
                table = str(table).split('!--')[0]+str(table).split('!-- } -->')[1]
                tableData = pd.read_html(io.StringIO(table))[0]
                print(tableData)
                
        except ValueError as e:
            print(e) # ValueError: No tables found
            try:
                table = soup.select('#scratcher-detail-container > div > div:nth-child(3) > div:nth-child(5) > div > table')
                tableData = pd.read_html(io.StringIO(table))[0]
            except ValueError as e:
                print(e) # ValueError: No tables found 
                continue
                
        tableData['prizeamount'] = tableData['Prize Amount'].replace('*','')
        tableData['gameNumber'] = soup.find('h2', class_='title-display').find('small').get_text()
        tableData['gameName'] = soup.find('h2', class_='title-display').find(string=True, recursive=False).strip()
        tableData['price'] = soup.find('h2', class_='ticket-price-display').get_text()
        tableData['overallodds'] = soup.find('p', class_='odds-display').find('span').get_text()
        tableData['topprize'] = soup.find('h2', class_='top-prize-display').get_text().replace('*','')
        tableData['topprizeodds'] = soup.find('p', class_='odds-display').find('br').find('span').get_text()
        tableData['topprizeremain'] = tableData.iloc[0,2]
        tableData['extrachances'] = 'eXTRA Chances' if soup.find('p', string=re.compile('eXTRA Chances')) else np.nan
        tableData['secondChance'] = '2nd Chance' if soup.find('p', string=re.compile('2nd Chance')) else np.nan
        tableData['startDate'] = soup.find_all('h2', class_='start-date-display')[0].get_text()
        print(soup.find_all('h2', class_='start-date-display'))
        if len(soup.find_all('h2', class_='start-date-display')) > 1 & closing == True:
            tableData['endDate'] = soup.find_all('h2', class_='start-date-display')[1].get_text()
            tableData['lastdatetoclaim'] = soup.find_all('h2', class_='start-date-display')[2].get_text()
        else: 
            tableData['endDate'] = np.nan
            tableData['lastdatetoclaim'] = np.nan
        tableData['topprizeavail'] = 'Top Prize Claimed' if tableData.iloc[0,2] == 0 else np.nan
        tableData['dateexported'] = date.today()  
        tableData['gameURL'] = ticketurl

        if tableData.empty:
            continue
        else:
            tixtables = pd.concat([tixtables,tableData], axis=0)

    # remove characters from numeric values
    tixtables['gameNumber'] = tixtables['gameNumber'].replace(
        '#', '', regex=True)
    tixtables['prizeamount'] = tixtables['prizeamount'].str.replace(
        re.escape('*'), '', regex=True)
    tixtables['prizeamount'] = tixtables['prizeamount'].str.replace(
        ',', '', regex=True)
    tixtables['prizeamount'] = tixtables['prizeamount'].str.replace(r'\$','', regex = True)
    tixtables['price'] = tixtables['price'].str.replace(r'\$','', regex = True)
    tixtables['topprize'] = tixtables['topprize'].str.replace(
        re.escape('*'), '', regex=True)
    tixtables['topprize'] = tixtables['topprize'].str.replace(
        ',', '', regex=True)
    tixtables['topprize'] = tixtables['topprize'].str.replace(r'\$','', regex = True)

    # convert text top prizes by calculating the ammounts
    # converts the tax prizes to the $50k + 4% tax rate, $2k/week*52 weeks/yr*10yrs, and Live Spin to the max prize $500,000
    tixtables['topprize'] = tixtables['topprize'].replace(
        {'50000 + Taxes': 50000*1.04, '2K/Wk for 10 Yrs': 2000*52*10, 'Live Spin': 500000, '10K Month for 10 Yrs': 10000*12*10})
    tixtables['prizeamount'] = tixtables['prizeamount'].replace(
        {'50000 + Taxes': 50000*1.04, '2K/Wk for 10 Yrs': 2000*52*10, 'Live Spin': 500000, '10K Month for 10 Yrs': 10000*12*10}).astype('int64')
    tixtables['topprize'] = tixtables['topprize'].apply(
        formatstr).astype('int64')

    scratchersall = tixtables.loc[:, ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeodds', 'overallodds', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('VAscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./VAscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:, ['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables.to_csv("./VAscratchertables.csv", encoding='utf-8')
    print(scratchertables.dtypes)

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:,[
                                      'gameNumber', 'price', 'topprizeodds', 'overallodds']], how='left', on=['gameNumber'])
    gamesgrouped.loc[:, 'topprizeodds'] = gamesgrouped.loc[:,
                                                           'topprizeodds'].str.replace(',', '', regex=True)
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

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Current Odds of Top Prize'] = tixtotal / \
            totalremain.loc[0, 'Winning Tickets Unclaimed']
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
        #totalremain.loc[:,'Starting Expected Value'] = ''
        #totalremain.loc[:,'Expected Value'] = ''

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
        # function to get url for photo based on game number, like a case-swtich statement
        def photolink(i):
            switcher = {
                '1841': "https://www.valottery.com/-/media/VAL/Images/Scratcher-Game-Tiles/1841_Extreme-Millions_teaser.ashx",
                '1874': "https://www.valottery.com/-/media/val/images/scratcher-game-tiles/1874_super_cash_frenzy_teaser.ashx",
                '1888': "https://www.valottery.com/-/media/val/images/scratcher-game-tiles/1888_30k-cash-party_teaser.ashx",
                '1895': "https://www.valottery.com/-/media/val/images/scratcher-game-tiles/1895_100x-the-money_teaser.ashx",
                '1773': "https://www.valottery.com/-/media/VAL/Images/Scratcher-Game-Tiles/1773_Jewel-7s_teaser.ashx",
                '1948': "https://www.valottery.com/-/media/VAL/Images/Scratcher-Game-Tiles/1948_teaser.ashx",
                i: "https://www.valottery.com/-/media/val/images/digital-scratcher-teaser-images/" +
                i + "_teaser.ashx"
            }

            return switcher.get(i, "Invalid game number")

        gamerow.loc[:, 'Photo'] = photolink(str(gameid))
        gamerow.loc[:, 'FAQ'] = None
        gamerow.loc[:, 'About'] = None
        gamerow.loc[:, 'Directory'] = None
        gamerow.loc[:, 'Data Date'] = gamerow.loc[:, 'dateexported']

        currentodds = pd.concat([currentodds, gamerow], axis=0)


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
    #scratchertables.to_sql('VAscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./VAscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y',
                      'topprizeodds_y', 'overallodds_y'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported',
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
    ratingstable['Stats Page'] = "/virginia-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('VAratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./VAratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    VAratingssheet = gs.worksheet('VARatingsTable')
    VAratingssheet.clear()
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

    set_with_dataframe(worksheet=VAratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportAZScratcherRecs():
    url = "https://www.arizonalottery.com/scratchers/#all"
    r = requests.get(url)
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')

    tixlist = pd.DataFrame(
        columns=['gameName', 'gameNumber', 'price', 'gameURL'])
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
            gamePhoto = "https://www.arizonalottery.com" + \
                soup.select_one(
                    "img[src*='"+gameNumber+"']")["src"].split('?')[0]
        except:
            gamePhoto = None
            continue


        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, gameURL, gamePhoto]

    #tixlist.to_csv("./AZtixlist.csv", encoding='utf-8')

    tixtables = pd.DataFrame(columns=['gameNumber', 'gameName', 'price', 'prizeamount', 'startDate', 'endDate',
                             'lastdatetoclaim', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported','gameURL'])

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
        endDate = None if 'endDate' not in scratcherdata else datetime.fromisoformat(
            scratcherdata['endDate'][0])
        lastdatetoclaim = datetime.fromisoformat(scratcherdata['lastDate'][0])
        gameOdds = scratcherdata['gameOdds'][0]
        dateexported = pd.to_datetime(
            scratcherdata['dateModified'][0])

        print('Looping through each prize tier row for scratcher #'+i)
        for row in scratcherdata['prizeTiers']:
            prizetier = pd.DataFrame.from_dict([row])
            if "prizeAmont" in prizetier:
                prizeamount = prizetier['prizeAmount'][0]
            else: 
                prizeamount = prizetier['displayTitle'][0].replace('$','').replace(',','')
                if "Million" in prizeamount:
                    prizeamount = int(prizeamount.replace(' Million','000000').replace('.',''))
                else:    
                    prizeamount = int(prizeamount)
            prizeodds = prizetier['odds'][0]
            startingprizecount = prizetier['totalCount'][0]
            remainingprizecount = prizetier['count'][0]
            tixtables.loc[len(tixtables.index), ['gameNumber', 'gameName', 'price', 'prizeamount', 'startDate', 'endDate', 'lastdatetoclaim',
                                                 'overallodds', 'prizeodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported', 'tierLevel']] = [gameNumber, gameName, gamePrice, prizeamount,
                                                                                                                                                                       startDate, endDate, lastdatetoclaim, gameOdds, prizeodds, startingprizecount, remainingprizecount, dateexported, prizetier['tierLevel'][0]]

        #tixtables['gameNumber'] = gameNumber
        index = tixtables[tixtables['gameNumber'] == gameNumber].index
        tixtables.loc[index, 'gameName'] = gameName
        tixtables.loc[index, 'price'] = gamePrice
        topprize = tixtables.loc[(tixtables['tierLevel'] == 1) & (
            tixtables['gameNumber'] == gameNumber), 'prizeamount'].iloc[0]
        topprizeodds = tixtables.loc[(tixtables['tierLevel'] == 1) & (
            tixtables['gameNumber'] == gameNumber), 'prizeodds'].iloc[0]
        topprizeremain = tixtables.loc[(tixtables['tierLevel'] == 1) & (
            tixtables['gameNumber'] == gameNumber), 'Winning Tickets Unclaimed'].iloc[0]
        topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else None

        tixtables.loc[index, 'topprize'] = topprize
        tixtables.loc[index, 'topprizeodds'] = topprizeodds
        tixtables.loc[index, 'topprizeremain'] = topprizeremain
        tixtables.loc[index, 'topprizeavail'] = topprizeavail
        tixtables.loc[index, 'extrachances'] = None
        tixtables.loc[index, 'secondChance'] = None
        tixtables['gameURL'] = gameURL

    #tixtables.to_csv("./AZprizedata.csv", encoding='utf-8')
    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'topprizeodds', 'overallodds', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.drop_duplicates(subset=['price', 'gameName', 'gameNumber', 'topprize', 'topprizeodds', 'overallodds',
                                                  'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported'])
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    #scratchersall = scratchersall.drop_duplicates()
    # save scratchers list
    #scratchersall.to_sql('AZscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./azscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'tierLevel', 'dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(by=['gameNumber', 'gameName', 'dateexported'], group_keys=False).agg({
        'Winning Tickets At Start':'sum', 'Winning Tickets Unclaimed':'sum'}).reset_index(level=['gameNumber', 'gameName', 'dateexported']).copy()
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
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

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start',
                  'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:, 'prizeamount'] = "Total"

    # loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame()
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(
            gamesgrouped['gameNumber'] == gameid), :].copy()
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
            'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'tierLevel', 'dateexported']]
        totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'tierLevel']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'tierLevel']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])


        prizes = totalremain.loc[:, 'prizeamount']

        startoddstopprize = tixtotal / \
            totalremain.loc[totalremain['tierLevel'] ==
                            1, 'Winning Tickets At Start'].values[0]

        # add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:, 'Current Odds of Top Prize'] = float(
            gamerow['topprizeodds'].values[0])
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
        gamerow.loc[:, 'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'].sum(
        )-totalremain.loc[totalremain['prizeamount'] != price, 'Winning Tickets Unclaimed'].std().mean())

        totalremain.loc[:, ['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

        testdf = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']]
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

        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber'].astype(
            'int') == gameid, ['gamePhoto']].values[0]
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
    #scratchertables.to_sql('azscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./azscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y',
                      'topprizeodds_y', 'overallodds_y'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported',
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

    ratingstable['Stats Page'] = "/arizona-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('AZratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./azratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select sheet by name
    AZratingssheet = gs.worksheet('AZRatingsTable')
    AZratingssheet.clear()
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

    set_with_dataframe(worksheet=AZratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportMOScratcherRecs():
    url = "https://www.molottery.com/scratchers/?type=all"

    payload = ""
    headers = {
        'Cookie': 'incap_ses_1458_2143742=iIdGEMj+ECj51ZUHpts7FGw4XWMAAAAAfGo/0vdFFuY6L5QELZdtTg==; lottery-track=42ca0a2.5ec2cd1330bff; nlbi_2143742=ECYdLKjyZG8ItsXPsZUu4QAAAACousb+YvwyeM3QBYQGjqaT; visid_incap_2143742=2sOJh/4QR9Kzu8WOBe7lTlMyXWMAAAAAQUIPAAAAAABEyHQLItGNVhorQjN8YfiH'
    }

    r = requests.request("GET", url, headers=headers, data=payload)
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')

    tixlist = pd.DataFrame(
        columns=['gameName', 'gameNumber', 'price', 'gameURL'])
    table = soup.find_all(class_=['scratchers-list__item'])

    tixrow = pd.DataFrame()
    tixtables = pd.DataFrame(columns=['gameNumber', 'gameName', 'price', 'prizeamount', 'startDate', 'endDate',
                             'lastdatetoclaim', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported'])

    # loop through the HTML sections to get the top level game data
    for s in table:
        itemback = s.find(class_=['scratchers-list__back'])
        gameName = itemback.find(
            class_='scratchers-list__title').find('span').string
        gameNumber = itemback.find(
            class_='scratchers-list__num').string.replace('#', '')
        gamePrice = itemback.find_all(
            class_='scratchers-list__value')[2].string.replace('$', '')
        topprize = itemback.find_all(
            class_='scratchers-list__value')[3].string.replace('$', '')
        startDate = itemback.find_all(
            class_='scratchers-list__value')[0].string
        endDate = itemback.find_all(class_='scratchers-list__value')[1].string
        totalMoneyWon = itemback.find_all(
            class_='scratchers-list__value')[4].string.replace('$', '').replace(',', '')
        totalMoneyUnclaimed = itemback.find_all(
            class_='scratchers-list__value')[5].string.replace('$', '').replace(',', '')
        totalMoneyStart = int(totalMoneyWon)+int(totalMoneyUnclaimed)
        percentMoneyClaimed = int(totalMoneyWon)/int(totalMoneyStart)

        gameURL = 'https://www.molottery.com/scratchers/'+gameNumber

        # download the MO scratcher tile images to folder
        file_name = "moscratchers_"+gameNumber+'.gif'
        #download_image(gameURL, './gameimages/', file_name)

        # get photo from where it was saved
        gamePhoto = 'https://www.scratcherstats.com/wp-content/uploads/gameimages/'+file_name
        #gamePhoto = 'https://www.molottery.com/sites/default/files/scratchers/tile/'+gameNumber+'.gif'


        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'startDate', 'endDate', 'Total Prize Money at start', 'Total Prize Money Won', 'Total Prize Money remaining', 'Percent of Prize Money Won', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, startDate, endDate, totalMoneyStart, totalMoneyWon, totalMoneyUnclaimed, percentMoneyClaimed, gameURL, gamePhoto]

        # go to the individual page for this scratcher game and get the table data
        url = "https://www.molottery.com/scratchers/"+gameNumber

        payload = ''
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
        }

        r = requests.get(url=url, headers=headers)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        singletixinfo = soup.find(
            class_=['scratchers-single__info scratchers-single-info'])
        lastdatetoclaim = singletixinfo.find_all(
            class_='scratchers-single-info__body')[2].string
        overallodds = singletixinfo.find_all(
            class_='scratchers-single-info__body')[5].string.replace('1 in ', '')

        tixdata = soup.find(class_=['table-mo table_highlight-first'])
        tixdata = pd.read_html(io.StringIO(str(tixdata)))[0]
        tixdata['gameNumber'] = gameNumber
        tixdata['gameName'] = gameName
        tixdata['price'] = gamePrice
        tixdata['overallodds'] = overallodds
        tixdata['topprize'] = tixdata.iloc[-1,
                                           0].replace('$', '').replace(',', '')
        tixdata['topprizestarting'] = tixdata.iloc[-1, 1]
        tixdata['topprizeremain'] = tixdata.iloc[-1, 2]
        tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata.iloc[-1,
                                                                       2] == 0 else np.nan
        tixdata['startDate'] = startDate
        tixdata['endDate'] = endDate
        tixdata['lastdatetoclaim'] = lastdatetoclaim
        tixdata['extrachances'] = None
        tixdata['secondChance'] = None
        tixdata['dateexported'] = date.today()
        tixdata['gameURL'] = gameURL
        tixdata.rename(columns={'Prize Level': 'prizeamount', 'Total Prizes': 'Winning Tickets At Start',
                       'Unclaimed Prizes': 'Winning Tickets Unclaimed'}, inplace=True)
        tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
            '$', '').str.replace(',', '')


        if tixdata.empty:
            continue
        else:
            tixtables = pd.concat([tixtables, tixdata], axis=0)

    tixlist.to_csv("./MOtixlist.csv", encoding='utf-8')

    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('MOscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./MOscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./MOscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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

        testdf = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']]

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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('MOscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./MOscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
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

    ratingstable['Stats Page'] = "/missouri-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('MOratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./MOratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    MOratingssheet = gs.worksheet('MORatingsTable')
    MOratingssheet.clear()
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

    set_with_dataframe(worksheet=MOratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportOKScratcherRecs():
    url = "https://www.lottery.ok.gov/scratchers/get"

    payload = {}
    headers = {
        'Cookie': 'BIGipServer~ONE-ARMED-PUBLIC~olc_www.lottery.ok.gov.app~olc_www.lottery.ok.gov_pool=rd5o00000000000000000000ffffac100f46o80'
    }

    r = requests.request("GET", url, headers=headers, data=payload)
    response = r.json()

    tixlist = pd.DataFrame()
    tixrow = pd.DataFrame()
    tixtables = pd.DataFrame(columns=['gameNumber', 'gameName', 'price', 'prizeamount', 'startDate', 'endDate',
                             'lastdatetoclaim', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported'])

    # loop through the HTML sections to get the top level game data
    for game in response['Games']:

        gameName = game['Name']
        gameNumber = game['GameId']
        gameURL = 'https://www.lottery.ok.gov/scratchers/'+str(game['Id'])
        gamePrice = game['Price']
        topprize = game['TopPrize']
        topprizeremain = game['TopPrizesRemaining']
        startDate = datetime.fromtimestamp(
            int(game['StartDate'][6:-2])/1000).strftime("%m/%d/%Y")
        endDate = datetime.fromtimestamp(
            int(game['EndDate'][6:-2])/1000).strftime("%m/%d/%Y")
        lastdatetoclaim = datetime.fromtimestamp(
            int(game['RedemptionEnd'][6:-2])/1000).strftime("%m/%d/%Y")
        overallodds = game['OverallOdds']

        gamePhoto = 'https://content.lottery.ok.gov/games/face/' + \
            str(gameNumber)+'.jpg'


        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'startDate', 'endDate', 'lastdatetoclaim', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, startDate, endDate, lastdatetoclaim, gameURL, gamePhoto]

        # go down to next level of json response for numbers of prizes
        tixdata = pd.json_normalize(game['Prizes'])

        if tixdata.empty:
            continue
        else:
            tixdata.rename(columns={'GameId': 'gameNumber', 'GameName': 'gameName', 'PrizeAmount': 'prizeamount',
                           'TotalPrizes': 'Winning Tickets At Start', 'RemainingPrizes': 'Winning Tickets Unclaimed'}, inplace=True)
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            tixdata['overallodds'] = overallodds
            tixdata['topprize'] = topprize
            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[-1]
            tixdata['topprizeremain'] = topprizeremain
            tixdata['topprizeavail'] = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = endDate
            tixdata['lastdatetoclaim'] = lastdatetoclaim
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = date.today()
            tixdata['gameURL'] = gameURL
            tixtables = pd.concat([tixtables, tixdata], axis=0)

    tixlist.to_csv("./OKtixlist.csv", encoding='utf-8')

    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('OKscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./OKscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./OKscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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

        testdf = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']]

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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('OKscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./OKscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
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

    ratingstable['Stats Page'] = "/oklahoma-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('OKratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./OKratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    OKratingssheet = gs.worksheet('OKRatingsTable')
    OKratingssheet.clear()
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

    set_with_dataframe(worksheet=OKratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportCAScratcherRecs():
    url = "https://www.calottery.com/api/games/scratchers"

    payload = {}
    headers = {
        'Cookie': 'ASP.NET_SessionId=m1rgg4qyl0bxd4jn3rvsp4im; TS01cd3c19=015bea3ff9502221abae359a88b2633aec8fa691da49da7aa7011903437c886cc19df2a8062a5a70120821678507bd5b9e9a51fc72; persistence=!ZchdtSMJwLrQ2gjc2We5XtPITzc1BhfQG73Fr9y9//LtBYBnimGW/tuEZ/+ixs+af5dnJKUTj5pTcQ=='
    }

    r = requests.request("GET", url, headers=headers, data=payload)
    # print(r)
    response = r.json()

    tixlist = pd.DataFrame()
    tixrow = pd.DataFrame()
    tixtables = pd.DataFrame(columns=['gameNumber', 'gameName', 'price', 'prizeamount', 'startDate', 'endDate',
                             'lastdatetoclaim', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported'])

    # loop through the HTML sections to get the top level game data
    for game in response['games']:
        gameName = game['name']
        gameNumber = game['gameNumber']
        gameURL = game['productPage']
        gamePrice = game['price']
        topprize = game['topPrizeTier']['value']
        topprizeremain = game['topPrizeTier']['numberOfPrizesPending']
        startDate = datetime.strftime(datetime.fromtimestamp(
            abs(int(game['goToMarketDate']))/1000), "%m/%d/%Y")
        endDate = None if game['retailSalesEndDate'] == None else datetime.fromtimestamp(
            int(game['retailSalesEndDate'])/1000).strftime("%m/%d/%Y")
        lastdatetoclaim = None if game['retailSalesEndDate'] == None else datetime.fromtimestamp(
            int(game['lastDayToClaimDate'])/1000).strftime("%m/%d/%Y")
        overallodds = game['cashOdds']
        gamePhoto = game['cardImage']

        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'startDate', 'endDate', 'lastdatetoclaim', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, startDate, endDate, lastdatetoclaim, gameURL, gamePhoto]

        # go down to next level of json response for numbers of prizes
        tixdata = pd.json_normalize(game['prizeTiers'])

        if tixdata.empty:
            tixtables = pd.concat([tixtables, []], axis=0)
        else:
            tixdata.rename(columns={'value': 'prizeamount', 'totalNumberOfPrizes': 'Winning Tickets At Start',
                           'numberOfPrizesPending': 'Winning Tickets Unclaimed'}, inplace=True)
            tixdata['gameNumber'] = gameNumber
            tixdata['gameName'] = gameName
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            tixdata['overallodds'] = overallodds
            tixdata['topprize'] = topprize
            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0]
            tixdata['topprizeremain'] = topprizeremain
            tixdata['topprizeavail'] = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = endDate
            tixdata['lastdatetoclaim'] = lastdatetoclaim
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = date.today()
            tixdata['gameURL'] = gameURL
            tixtables = pd.concat([tixtables, tixdata], axis=0)

    tixlist.to_csv("./CAtixlist.csv", encoding='utf-8')

    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('CAscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./CAscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./CAscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables = scratchertables.astype(
        {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]

    # save scratchers tables
    #scratchertables.to_sql('CAscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./CAscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
    ratingstable['Stats Page'] = "/california-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('CAratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./CAratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    CAratingssheet = gs.worksheet('CARatingsTable')
    CAratingssheet.clear()

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
    
    set_with_dataframe(worksheet=CAratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportNMScratcherRecs():
    url = "https://www.nmlottery.com/games/scratchers/"
    r = requests.get(url)
    response = r.text
    # print(r.text)
    soup = BeautifulSoup(response, 'html.parser')

    tixlist = pd.DataFrame()
    table = soup.find(
        'div', id='scratchers-results').find_all(class_='scratcher-content')
    # print(table)

    # get list of end dates from another page on NM site
    url = "https://www.nmlottery.com/games/scratchers/games-ending/"
    r = requests.get(url)
    response = r.text
    dateslist = BeautifulSoup(response, 'html.parser')
    endDateslist = pd.read_html(io.StringIO(str(dateslist.find('table'))))[0]

    tixtables = pd.DataFrame()

    for s in table:
        gameName = s.find('h3').string
        gameNumber = s.find(
            class_='game-number').text.replace('Game Number: ', '')
        gamePhoto = soup.select_one("img[src*='"+gameNumber+"']")["src"]
        gameURL = 'https://www.nmlottery.com/games/scratchers'
        gamePrice = s.find(class_='price').string.replace('$', '')
        topprize = s.find(
            class_='top-prize').text.replace('Top Prize: $', '').replace(',', '')
        startDate = datetime.strftime(datetime.strptime(s.find(
            class_='start-date').text.replace('Start Date: ', ''), "%B %d, %Y"), "%m/%d/%Y")
        overallodds = s.find(class_='prizes-and-odds').text
        # if there's no odds text for the game, leave it empty for now and calculate later
        try:
            oddstextindex = overallodds.index(': 1 in ')+7
            overallodds = overallodds[oddstextindex:oddstextindex+4]
        except ValueError as e:
            print(e)  # ValueError: substring not found if there's no text on odds
            overallodds = None
            continue


        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'startDate', 'overallodds', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, startDate, overallodds, gameURL, gamePhoto]

        tixdata = pd.read_html(io.StringIO(str(s.find(class_='data'))))[0]

        if len(tixdata) == 0:
            tixtables = pd.concat([tixtables, pd.Series()], axis=0)
        else:
            tixdata.rename(columns={'Prize:': 'prizeamount', 'Approx. # of Prizes:': 'Winning Tickets At Start',
                           'Approx. Prizes Remaining:': 'Winning Tickets Unclaimed'}, inplace=True)
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                '$', '').str.replace(',', '')
            tixdata['gameNumber'] = gameNumber
            tixdata['gameName'] = gameName
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            # if overallodds text not available, calculate overallodds by top prize odds x number of top prizes at start
            tixdata['overallodds'] = tixdata['Approx. Odds 1 in:'].iloc[0] * \
                tixdata['Winning Tickets At Start'].iloc[0] if overallodds == None else overallodds
            tixdata['topprize'] = topprize
            tixdata['topprizeodds'] = tixdata['Approx. Odds 1 in:'].iloc[0]
            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0]
            tixdata['topprizeremain'] = tixdata['Winning Tickets Unclaimed'].iloc[0]
            tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == 0 else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = endDateslist.loc[endDateslist['Game #']
                                                  == gameNumber, 'End Date']
            tixdata['lastdatetoclaim'] = endDateslist.loc[endDateslist['Game #']
                                                          == gameNumber, 'Last Day to Redeem']
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = date.today()
            tixdata['gameURL'] = gameURL

            tixtables = pd.concat([tixtables, tixdata], axis=0)

    tixlist.to_csv("./NMtixlist.csv", encoding='utf-8')

    tixtables = tixtables.loc[(tixtables['prizeamount'] != 'Prize Ticket') & (
        tixtables['prizeamount'] != 'Prize ticket') & (tixtables['prizeamount'] != 'PRIZE TICKET'), :]
    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('NMscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./NMscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./NMscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables = scratchertables.astype(
        {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('NMscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./NMscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
    ratingstable['Stats Page'] = "/new-mexico-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('NMratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./NMratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    NMratingssheet = gs.worksheet('NMRatingsTable')
    NMratingssheet.clear()

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

    set_with_dataframe(worksheet=NMratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportMDScratcherRecs():
    url = "https://www.mdlottery.com/wp-admin/admin-ajax.php?action=jquery_shortcode&shortcode=scratch_offs&atts=%7B%22null%22%3A%22null%22%7D"

    payload={}
    headers = {
      'Cookie': 'incap_ses_1460_1865635=w3c/LNWgrCQpiy7OpfZCFGDjoWMAAAAAev9ixbH7jEujWZqUXKOQKw==; visid_incap_1865635=T+iNyhKJQdimr2R9QqfrhWDjoWMAAAAAQUIPAAAAAABfT6Fd0RLq527L2FIKx4i1',
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    r = requests.request("GET", url, headers=headers, data=payload)
    response = r.text

    soup = BeautifulSoup(response, 'html.parser')
    table = soup.find_all('li', class_= 'ticket')
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()
    
    for s in table:
        gameName = s.find(class_='name').string

        gameNumber = s.find(class_='gamenumber').string.replace('Game #','')
        print(gameNumber)
        gamePhoto = s.find(class_='magnific-img').get('href')
        gameURL = 'https://www.mdlottery.com/games/scratch-offs/#prize_details_'+str(gameNumber)
        gamePrice = s.find(class_='price').string.replace('$','')
        topprize = s.find(class_='topprize').string.replace('$','').replace(',','')
        topprizeremain = s.find(class_='topremaining').string
        startDate = s.find(class_='launchdate').text
        overallodds = s.find(class_='probability').text
        
        dateexported = s.find('div', id = 'prize_details_'+gameNumber).find('p').text.replace('Records Last Updated:','')
        
        tixdata = pd.read_html(str(s.find('table')))[0]
        

        
        print(gameName)
        print(gameNumber)
        print(gamePrice)
        print(gameURL)
        print(gamePhoto)
        print(topprize)
        print(topprizeremain)
        print(startDate)
        print(overallodds)
        print(dateexported)
            
        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','topprize','topprizeremain', 'startDate','overallodds','gameURL','gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, topprizeremain, startDate, overallodds, gameURL, gamePhoto]

        print(tixdata)
        if len(tixdata) == 0:
            tixtables = tixtables.append([])
        else:
            print(tixdata.columns)
            tixdata.rename(columns={'Prize Amount':'prizeamount','Start': 'Winning Tickets At Start', 'Remaining*': 'Winning Tickets Unclaimed'}, inplace=True)
            print(tixdata.columns)
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace('$','').str.replace(',','').str.replace("\(Digital Spin\)",'').replace('Big Spin',50000)
            tixdata['gameNumber'] = gameNumber
            tixdata['gameName'] = gameName
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            tixdata['gameURL'] = gameURL
            #if overallodds text not available, calculate overallodds by top prize odds x number of top prizes at start
            tixdata['overallodds'] = tixdata['Approx. Odds 1 in:'].iloc[0]*tixdata['Winning Tickets At Start'].iloc[0] if overallodds==None else overallodds
            tixdata['topprize'] = topprize
            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0]
            tixdata['topprizeremain'] = topprizeremain
            tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == 0 else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = None
            tixdata['lastdatetoclaim'] = None
            tixdata['extrachances'] = "Digital Spin" if "(Digital Spin)" in tixdata['prizeamount'].str.replace('$','').str.replace(',','') else "Big Spin" if "Big Spin" in tixdata['prizeamount'].str.replace('$','').str.replace(',','') else None 
            tixdata['secondChance'] = None
            tixdata['dateexported'] = dateexported
            print(tixdata)
            print(tixdata.columns)
            tixtables = pd.concat([tixtables, tixdata], axis=0)

    tixtables = tixtables.astype(
        {'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})

    tixtables['prizeamount'] = pd.to_numeric(tixtables['prizeamount'], errors='coerce')  # Replace invalid values with NaN
    
    scratchertables = tixtables.dropna(subset=['prizeamount'])
    scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(int)
    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]

    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('MDscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./MDscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./MDscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables = scratchertables.astype(
        {'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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

    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]

    # save scratchers tables
    #scratchertables.to_sql('MDscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./MDscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
    ratingstable['Stats Page'] = "/maryland-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('MDratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./MDratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    MDratingssheet = gs.worksheet('MDRatingsTable')
    MDratingssheet.clear()

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

    set_with_dataframe(worksheet=MDratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportNYScratcherRecs():
    url = "https://nylottery.ny.gov/drupal-api/api/v2/scratch_off_data?_format=json"

    payload = {}
    headers = {}

    r = requests.request("GET", url, headers=headers, data=payload)
    response = r.json()
    rows = response['rows']
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()

    for s in rows:
        gameName = s['title']
        gameNumber = s['game_number']
        gamePhoto = s['art'][0]['uri']
        gameURL = 'https://nylottery.ny.gov/scratch-off-game?game=' + \
            str(gameNumber)
        gamePrice = str(s['ticket_price'].replace('$', '')).replace('.00', '')
        topprize = s['top_prize_amount'].replace('$', '').replace(',', '')
        topprizeremain = s['top_prize_remaining']
        overallodds = pd.to_numeric(s['overall_odds'].replace('1 in ', '').replace('Cash Odds ', '').replace('1 in:\t', '').replace(
            '1 in\t', '').replace('Odds of Winning Cash Prize 1 in: ', '').replace('1  in ', ''), errors='coerce')
        startDate = s['release_date']
        lastdatetoclaim = s['prizes_thru_date']
        dateexported = datetime.strftime(
            datetime.fromtimestamp(int(s['last_updated'])), "%m/%d/%Y")

        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'startDate', 'lastdatetoclaim', 'overallodds', 'gameURL', 'gamePhoto']] = [
            gamePrice, gameName, gameNumber, topprize, topprizeremain, startDate, lastdatetoclaim, overallodds, gameURL, gamePhoto]

        #get odds_prizes from json on game page
        url = "https://nylottery.ny.gov/drupal-api/api/v2/scratch_off_game?game="+str(gameNumber)

        payload={}
        headers = {}
    
        r = requests.request("GET", url, headers=headers, data=payload)
        response = r.json()
        tixdata = pd.json_normalize(response['odds_prizes'])
        print(tixdata)

        if len(tixdata) == 0:
            tixtables = pd.concat([tixtables, []], axis=0)
        else:
            tixdata = tixdata.loc[:, ['title', 'overall_odds',
                               'prize_amount', 'prizes_paid_out', 'prizes_remaining']]
            tixdata.rename(columns={'prize_amount': 'prizeamount',
                           'prizes_remaining': 'Winning Tickets Unclaimed'}, inplace=True)
            tixdata['Winning Tickets At Start'] = tixdata['prizes_paid_out'].astype(
                'int')+tixdata['Winning Tickets Unclaimed'].astype('int')
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                '$', '').str.replace(',', '').str.lower()
            if tixdata['prizeamount'].iloc[0].find(' a week for life')>0:
                print(tixdata['prizeamount'][0])
                tixdata.at[0,'prizeamount'] = str(int(tixdata['prizeamount'].iloc[0].replace(' a week for life',''))*52*50)
            elif tixdata['prizeamount'][0].find('k/wk/life') > 0:
                tixdata.at[0, 'prizeamount'] = str(
                    int(tixdata['prizeamount'].iloc[0].replace('k/wk/life', '000'))*52*50)
            elif tixdata['prizeamount'].iloc[0].find('k annual installments') > 0:
                tixdata.at[0, 'prizeamount'] = str(
                    int(tixdata['prizeamount'].iloc[0].replace('$','').replace('k annual installments', '000'))*60)
            elif tixdata['prizeamount'].iloc[0].find('/week/life') > 0:
                tixdata.at[0, 'prizeamount'] = str(
                    int(tixdata['prizeamount'].iloc[0].replace('/week/life', ''))*52*50)
            else:
                tixdata['prizeamount'].iloc[0] = tixdata['prizeamount'].iloc[0]

            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                'free take 5 fp', gamePrice, regex=True).astype('str')
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                'free c4l qp', gamePrice, regex=True).astype('str')
            tixdata['prizeamount'] = tixdata['prizeamount'].replace('.00', '')
            tixdata['prizeamount'] = tixdata['prizeamount'].replace(
                '10.00', '10')


            tixdata['gameNumber'] = gameNumber
            tixdata['gameName'] = gameName
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            #tixdata['overallodds'] = overallodds

            tixdata['overallodds'] = float(tixdata['overall_odds'].iloc[0].replace('1 in ', '').replace(
                ',', ''))*float(tixdata['Winning Tickets At Start'].iloc[0]) if pd.isna(overallodds) else overallodds

            tixdata['topprizeodds'] = tixdata['overall_odds'].iloc[0].replace(
                '1 in ', '').replace(',', '')

            tixdata['topprize'] = topprize

            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0]
            tixdata['topprizeremain'] = topprizeremain
            tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == '0' else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = None
            tixdata['lastdatetoclaim'] = lastdatetoclaim
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = dateexported
            tixdata['gameURL'] = gameURL

            tixtables = pd.concat([tixtables, tixdata], axis=0)

    tixlist.to_csv("./NYtixlist.csv", encoding='utf-8')
    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizeodds', 'topprizestarting',
                               'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('NYscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./NYscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./NYscratchertables.csv", encoding='utf-8')

    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables = scratchertables.astype(
        {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds', 'topprizeodds']], how='left', on=['gameNumber'])

    gamesgrouped.loc[:, 'Total at start'] = gamesgrouped['Winning Tickets At Start'].astype(
        float)*gamesgrouped['overallodds'].astype(float)
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
    # print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('NYscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./NYscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables
    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
    ratingstable['Stats Page'] = "/new-york-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('NYratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./NYratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    NYratingssheet = gs.worksheet('NYRatingsTable')
    NYratingssheet.clear()

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
    
    set_with_dataframe(worksheet=NYratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportDCScratcherRecs():

    tixlist = pd.DataFrame()
    tixtables = pd.DataFrame()

    # dclottery puts only 20 games on a page, so loop through each of 7 pages
    for i in range(0, 6):
        url = "https://dclottery.com/dc-scratchers?play_styles=All&theme=All&page=" + \
            str(i)
        r = requests.get(url)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')

        tixinfo = pd.DataFrame()
        table = soup.find_all(class_='node__content')

        # loop through each game on the page
        for s in table:
            if s.find('h3', class_='teaser__title') == None:
                continue
            else:
                gameName = s.find(
                    'h3', class_='teaser__title').find('span').string
                gameNumber = s.find(class_='field field_game_number').find(
                    class_='field__item').text

                gameURL = 'https://dclottery.com' + \
                    s.find('a', class_='teaser__image').get('href')

                gamePhoto = 'https://dclottery.com'+s.find(class_='teaser__image-background')[
                    'style'].replace('background-image:url(', '').replace(')', '')

                gamePrice = s.find('a', class_='teaser__image').find(
                    class_='field--name-field-price').text.replace('$', '').strip()
                topprize = None


                tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'gameURL', 'gamePhoto']] = [
                    gamePrice, gameName, gameNumber, topprize, gameURL, gamePhoto]

                for i in tixlist.loc[:, 'gameNumber']:
                    url = gameURL
                    r = requests.get(url)
                    response = r.text
                    soup = BeautifulSoup(response, 'html.parser')

                    # get the table of ticket numbers
                    tixdata = pd.read_html(
                        io.StringIO(str(soup.find(class_='view-content').find('table'))))[0]
                    # check if overall odds exists, if not we'll ignore this game
                    overallodds = None if soup.find(class_='field field--name-field-odds field--type-string field--label-above') == None else soup.find(
                        class_='field field--name-field-odds field--type-string field--label-above').find(class_='field__item').text.replace('1:', '').replace(',', '')

                    # count number of periods in odds number to check if more than one due to type (period instead of colon) and then replace
                    if overallodds != None:
                        overallodds = overallodds[2:] if (
                            overallodds.count('.') > 1) else overallodds
                    if (len(tixdata) == 0) & (overallodds == None):
                        tixtables = pd.concat([tixtables, []], axis=0)
                    else:
                        tixdata.rename(columns={'Prize Amount': 'prizeamount', 'Total Prizes': 'Winning Tickets At Start',
                                       'Prizes Remaining': 'Winning Tickets Unclaimed'}, inplace=True)
                        tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                            '$', '', regex=False).str.replace(',', '', regex=False)
                        tixdata['gameNumber'] = gameNumber
                        tixdata['gameName'] = gameName
                        tixdata['gamePhoto'] = gamePhoto
                        tixdata['price'] = gamePrice
                        tixdata['overallodds'] = None if overallodds == None else overallodds
                        tixdata['topprize'] = tixdata['prizeamount'].iloc[0]
                        tixdata['topprizeodds'] = None if soup.find(class_='field field--name-field-top-prize-odds field--type-string field--label-above') == None else soup.find(
                            class_='field field--name-field-top-prize-odds field--type-string field--label-above').find(class_='field__item').text.replace('1:', '').replace(',', '').replace(':', '')
                        tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0].astype(
                            'float')
                        tixdata['topprizeremain'] = tixdata['Winning Tickets Unclaimed'].iloc[0].astype(
                            'float')
                        tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == 0 else np.nan
                        tixdata['startDate'] = soup.find(
                            class_='field field--name-field-date field--type-daterange field--label-above').find(class_='field__item').text
                        tixdata['endDate'] = None
                        tixdata['lastdatetoclaim'] = None if soup.find(class_='field field--name-field-last-date-to-claim field--type-datetime field--label-above') == None else soup.find(
                            class_='field field--name-field-last-date-to-claim field--type-datetime field--label-above').find(class_='field__item').text
                        tixdata['extrachances'] = None
                        tixdata['secondChance'] = None
                        tixdata['dateexported'] = date.today()
                        tixdata['gameURL'] = gameURL

                        # for game 1533, Fat Wallet, the overall odds and top prize odds are switched on the dclottery site
                        if gameNumber == '1533':
                            tixdata['overallodds'] = tixdata['topprizeodds']
                            tixdata['topprizeodds'] = overallodds
                        elif gameNumber == '1521':
                            # this game, Double Your Money, seems to be missing a decimal point
                            revisedodds = str('3.99')
                            tixdata['overallodds'] = revisedodds if overallodds == '399' else overallodds
                        tixtables = pd.concat([tixtables, tixdata], axis=0)

                    # have to get the game photo link from the game page and add with tixinfo
                    #tixlist['gamePhoto'] = tixdata.loc[tixdata['gameNumber']==gameNumber,'gamePhoto'].iloc[0]
                    tixlist['topprize'] = tixdata.loc[tixdata['gameNumber']
                                                      == gameNumber, 'topprize'].iloc[0]


    tixlist.to_csv("./DCtixlist.csv", encoding='utf-8')
    tixtables = tixtables.loc[(tixtables['prizeamount'] != 'Prize Ticket') & (
        tixtables['prizeamount'] != 'Prize ticket') & (tixtables['prizeamount'] != 'PRIZE TICKET'), :]
    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizeodds', 'topprizestarting',
                               'topprizeremain', 'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('DCscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./DCscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables.to_csv("./DCscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables = scratchertables.astype(
        {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'topprizeodds', 'overallodds']], how='left', on=['gameNumber'])

    gamesgrouped.loc[:, 'Total at start'] = None if ((overallodds == None) & (
        gamesgrouped['topprizeodds'].iloc[0] == None)) else gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Total remaining'] = None if ((overallodds == None) & (
        gamesgrouped['topprizeodds'].iloc[0] == None)) else gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:, 'Non-prize at start'] = gamesgrouped['Total at start'] - \
        gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:, 'Non-prize remaining'] = gamesgrouped['Total remaining'] - \
        gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0
    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start',
                  'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:, 'prizeamount'] = "Total"

    # loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame()
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :]
        startingtotal = float(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = float(gamerow.loc[:, 'Total remaining'].values[0])
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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('DCscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./DCscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
    ratingstable['Stats Page'] = "/dc-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('DCratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./DCratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    DCratingssheet = gs.worksheet('DCRatingsTable')
    DCratingssheet.clear()

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

    set_with_dataframe(worksheet=DCratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportNCScratcherRecs():
    url = "https://nclottery.com/scratch-off-prizes-remaining"
    r = requests.get(url)
    response = r.text
    # print(r.text)
    soup = BeautifulSoup(response, 'html.parser')
    tixlist = pd.DataFrame()
    table = soup.find_all('table', class_='datatable')

    # get list of end dates from another page on NC site
    url = "https://nclottery.com/scratch-off-games-ending"
    r = requests.get(url)
    response = r.text
    dateslist = BeautifulSoup(response, 'html.parser')
    endDateslist = pd.read_html(io.StringIO(str(dateslist.find('table'))))[0]

    tixtables = pd.DataFrame()

    # loop through the HTML converting the data table to a dataframe, and the values out of hte still-HTML ticketdetails section
    for s in table:
        #get the game details
        tixdetails = s.find(class_='ticketdetails')
        gameName = tixdetails.find(class_='gamename').text
        print(gameName)
        gameNumber = tixdetails.find('span', class_='gamenumber').text.replace('Game Number: ','')
        gamePhoto = 'https://nclottery.com'+str(tixdetails.find(class_='gamethumb').find('a').get('href'))
        gameURL = 'https://nclottery.com'+str(tixdetails.find(class_='gamename').find('a').get('href'))
        print(gameURL)
        #if gameName!="The Bigger Spin":
        #    continue
        #else:
        #get more details from game page
        r = requests.get(gameURL)
        response = r.text
        details = BeautifulSoup(response, 'html.parser')
        detailstbl = pd.read_html(str(details.find(class_='datatable prizes')))[0]
        print(detailstbl)
        tixinfo = details.find_all('div',class_='part')[1]
        print(tixinfo)
        gamePrice = tixinfo.find('span',attrs={'class':'price value'}).string.replace('$','')
        print(gamePrice)

        topprize = tixinfo.find('span',attrs={'class':'topprize value'}).string.replace('$','').replace(',','')
        overallodds = tixinfo.find('span',attrs={'class':'odds value'}).string.replace('1 in ','')
        startDate = tixinfo.find('span',attrs={'class':'status value'}).string.replace('Released ','')
        lastdatetoclaim = None
        endDate = None

        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'gameURL', 'gamePhoto', 'topprize', 'overallodds', 'startDate', 'endDate', 'lastdatetoclaim']] = [
            gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, overallodds, startDate, endDate, lastdatetoclaim]

        # get the data from the table for this game
        tixdata = pd.read_html(io.StringIO(str(s)))[0]
        tixdata = tixdata.droplevel(0, axis=1)
        tixdata = tixdata.dropna(axis=0, inplace=False)

        if len(tixdata) == None:
            tixtables = pd.concat([tixtables, []], axis=0)
        else:
            tixdata.rename(columns={'Value': 'prizeamount', 'Total': 'Winning Tickets At Start',
                           'Remaining': 'Winning Tickets Unclaimed'}, inplace=True)
            # in addition to removing dollar signs and commans, changing the text for the Bigger Spin second game game to the minimum possible of $400,000
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                '$', '', regex=False).str.replace(',', '', regex=False)
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                'The Bigger Spin (400000 to 2 Million)', '400000', regex=False)
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace(
                'The Bigger Spin(400000 to 2 Million)', '400000', regex=False)
            tixdata['gameNumber'] = gameNumber
            tixdata['gameName'] = gameName
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            # if overallodds text not available, calculate overallodds by top prize odds x number of top prizes at start
            tixdata['overallodds'] = overallodds
            tixdata['topprize'] = topprize

            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0]
            tixdata['topprizeremain'] = tixdata['Winning Tickets Unclaimed'].iloc[0]
            tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == 0 else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = endDate
            tixdata['lastdatetoclaim'] = lastdatetoclaim
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = date.today()
            tixdata['gameURL'] = gameURL
            tixtables = pd.concat([tixtables, tixdata], axis=0)


    tixlist.to_csv("./NCtixlist.csv", encoding='utf-8')

    tixtables = tixtables.loc[(tixtables['prizeamount'] != 'Prize Ticket') & (
        tixtables['prizeamount'] != 'Prize ticket') & (tixtables['prizeamount'] != 'PRIZE TICKET'), :]
    scratchersall = tixtables.loc[:,['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('NCscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./NCscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    #scratchertables['prizeamount'] = scratchertables['prizeamount'].str.replace('The Bigger Spin(400000 to 2 Million)','400000')
    scratchertables.to_csv("./NCscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]
    scratchertables = scratchertables.astype(
        {'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizestarting', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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

    gamesgrouped.loc[:, ['price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = gamesgrouped.loc[:, [
        'price', 'topprizeodds', 'overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('NCscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./NCscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizestarting_x', 'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizestarting_y': 'topprizestarting', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
    # add number of days since the game start date as of date exported
    ratingstable.loc[:, 'Days Since Start'] = (pd.to_datetime(
        ratingstable['dateexported']) - pd.to_datetime(ratingstable['startDate'], errors='coerce')).dt.days

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
    ratingstable['Stats Page'] = "/north-carolina-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('NCratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./NCratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    NCratingssheet = gs.worksheet('NCRatingsTable')
    NCratingssheet.clear()

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

    set_with_dataframe(worksheet=NCratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables

def exportFLScratcherRecs():
    url = "https://www.flalottery.com/remainingPrizes"
    r = requests.get(url)
    response = r.text
            #print(r.text)
    soup = BeautifulSoup(response, 'html.parser')
    tixlist = pd.DataFrame()
    table = soup.find('div', class_='gameContent').find('table')
    table = pd.read_html(io.StringIO(str(table)))[0]
    
    tixtables = pd.DataFrame()
    
    #loop through each row of the data table and get data from the game page
    for s in table['Game Number']:
        gameName = table.loc[table['Game Number'] == s,'Game Name'].iloc[0]
        gameNumber = s
        gameURL = 'https://www.flalottery.com/scratch-offsGameDetails?gameNumber='+str(gameNumber)
        gamePrice = table.loc[table['Game Number'] == s,'Ticket Cost'].iloc[0].replace('$','')
        topprize = table.loc[table['Game Number'] == s,'Top Prize'].iloc[0].replace('$','').replace(',','')
        topprizeremain = table.loc[table['Game Number'] == s,'Top Prizes Remaining'].iloc[0].split(' of ')[0]

    
        #get more details from game page
        r = requests.get(gameURL)
        response = r.text
        details = BeautifulSoup(response, 'html.parser').find('div',class_='ticketDetailsContent').find('table', class_='scratchOdds')
        gamePhoto = "http://www.flalottery.com/scratch/"+str(gameNumber)+"pic.jpg"
        
        overallodds = None
        
        for p in BeautifulSoup(response, 'html.parser').find('div',class_='ticketDetailsContent').find_all('p'):
            if 'Launch Date: ' in p.text: 
                dates = p.text
                startDate = dates[dates.index('Launch Date:')+len('Launch Date:'):dates.index('End Date:')].strip()
                print(dates[dates.index('End Date:')+len('End Date:'):dates.index('Redemption Deadline:')].strip())
                endDate = None if dates[dates.index('End Date:')+len('End Date:'):dates.index('Redemption Deadline:')].strip()=="TBA" else dates[dates.index('End Date:')+len('End Date:'):dates.index('Redemption Deadline:')].strip()
                lastdatetoclaim = None if dates[dates.index('Redemption Deadline:')+len('Redemption Deadline:'):].strip()=="TBA" else dates[dates.index('Redemption Deadline:')+len('Redemption Deadline:'):].strip()
            elif 'Overall Odds: ' in p.text:
                overallodds = p.text.replace('Overall Odds: 1-in-','')

    
        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto','topprize','topprizeremain','overallodds','startDate','endDate','lastdatetoclaim']] = [
            gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, topprizeremain, overallodds, startDate, endDate, lastdatetoclaim]
    
        #get the data from the table for this game
        tixdata = pd.read_html(io.StringIO(str(details)))[0]
    
        if len(tixdata) == None:
            tixtables = pd.concat([tixtables, []], axis=0, ignore_index=True)
        else:
            tixdata.rename(columns={'Prize Amount':'prizeamount','Total Prizes': 'Winning Tickets At Start', 'Prizes Remaining': 'Winning Tickets Unclaimed'}, inplace=True)
            #in addition to removing dollar signs and commans, changing the text for the Bigger Spin second game game to the minimum possible of $400,000
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace('$','',regex=False).str.replace(',','',regex=False).str.replace('.00','',regex=False)
            #if prize is an amount per week for life, then strip text and multiply by 52 weeks for 50 years
            tixdata['prizeamount'] = tixdata['prizeamount'].replace({'1000/Week for Life':1000*52*50,'500/Week for Life':500*52*50, '1MM/YR/LF (MIN 25)':1000000*25, 
                                                                     '250000YR/LIFE':250000*50, '150000YR/LIFE':150000*50, '50000YR/LIFE':50000*50, '25000YR/LIFE':25000*50}).astype('int')
            tixdata['gameNumber'] = gameNumber
            tixdata['gameName'] = gameName
            tixdata['gamePhoto'] = gamePhoto
            tixdata['price'] = gamePrice
            #if overallodds text not available, calculate overallodds by top prize odds x number of top prizes at start
            tixdata['overallodds'] = overallodds
            tixdata['topprize'] = topprize
            tixdata['topprizeodds'] = tixdata['Odds of Winning'].iloc[0].replace('1-in-','').replace(',','')

            tixdata['topprizestarting'] = tixdata['Winning Tickets At Start'].iloc[0]
            tixdata['topprizeremain'] = topprizeremain
            tixdata['topprizeavail'] = 'Top Prize Claimed' if tixdata['Winning Tickets Unclaimed'].iloc[0] == 0 else np.nan
            tixdata['startDate'] = startDate
            tixdata['endDate'] = endDate
            tixdata['lastdatetoclaim'] = lastdatetoclaim
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = date.today() 
            tixdata['gameURL'] = gameURL

            tixtables = pd.concat([tixtables, tixdata], axis=0)

            
    tixlist.to_csv("./FLtixlist.csv", encoding='utf-8')


    scratchersall = tixtables.loc[:,['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','dateexported', 'gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('FLscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./FLscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./FLscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0
    
    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:,'prizeamount'] = "Total"

      
    #loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame() 
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:]
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0       
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
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)

        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)

        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']] 
    
    #save scratchers tables
    #scratchertables.to_sql('FLscratcherstables', engine, if_exists='replace')
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
    ratingstable['Stats Page'] = "/florida-statistics-for-each-scratch-off-game"
    #ratingstable.to_sql('FLratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./FLratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    FLratingssheet = gs.worksheet('FLRatingsTable')
    FLratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)

    set_with_dataframe(worksheet=FLratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportILScratcherRecs():
    #urls = ["https://www.illinoislottery.com/games-hub/instant-tickets",
          #  "https://www.illinoislottery.com/games-hub/instant-tickets?page=1",
           # "https://www.illinoislottery.com/games-hub/instant-tickets?page=2"]
    
    urls = ["https://webcache.googleusercontent.com/search?q=cache:https://www.illinoislottery.com/games-hub/instant-tickets",
            "https://webcache.googleusercontent.com/search?q=cache:https://www.illinoislottery.com/games-hub/instant-tickets?page=1",
            "https://webcache.googleusercontent.com/search?q=cache:https://www.illinoislottery.com/games-hub/instant-tickets?page=2"]
    tixlist = pd.DataFrame()
    #loop through each page of scratcher games, from list above. 
    for u in urls:
        url = u
        r = requests.get(url)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        pages = soup.find_all('div', class_='simple-game-card card-container__item')
        #loop through each row of the data table and get data from the game page
        for p in pages:
            #gameURL = 'https://www.illinoislottery.com'+str(p.find('a').get('href'))
            
            gameURL = 'https://webcache.googleusercontent.com/search?q=cache:https://www.illinoislottery.com'+str(p.find('a').get('href'))
            r = requests.get(gameURL)
            response = r.text
            soup = BeautifulSoup(response, 'html.parser')
            table = soup.find('div', class_='itg-details-block')
            print(table)

            if table==None:
                continue
            else:
                table = pd.read_html(io.StringIO(str(table.find('table'))))[0]
                gameNumber = table.loc[table[0]=='Game Number',1].iloc[0]
                overallodds = float(table.loc[table[0]=='Overall Odds',1].iloc[0].replace('1 in ','').replace(' to 1','').replace('1: ',''))
                gamePrice = table.loc[table[0]=='Price Point',1].iloc[0].replace('$','')
                startDate = table.loc[table[0]=='Launch Date',1].iloc[0]
                #gamePhoto = 'https://www.illinoislottery.com'+str(p.find(class_='simple-game-card__banner').get('style').replace('background-image:url(', '').replace(')', ''))
                
                gamePhoto = 'https://webcache.googleusercontent.com/search?q=cache:https://www.illinoislottery.com'+str(p.find(class_='simple-game-card__banner').get('style').replace('background-image:url(', '').replace(')', ''))
                endDate = None
                lastdatetoclaim = None
                extrachances = None
                secondChance = None
                dateexported = date.today()
    
        
        
                tixlist.loc[len(tixlist.index), ['price', 'gameNumber','gameURL','gamePhoto','overallodds','startDate','endDate','lastdatetoclaim', 'extrachances', 'secondChance', 'dateexported']] = [
                    gamePrice, gameNumber, gameURL, gamePhoto, overallodds, startDate, endDate, lastdatetoclaim, extrachances, secondChance, dateexported]
            
    #get the data from the big table of prizes for all games

    #url = "https://www.illinoislottery.com/about-the-games/unpaid-instant-games-prizes"
    
    url = "https://webcache.googleusercontent.com/search?q=cache:https://www.illinoislottery.com/about-the-games/unpaid-instant-games-prizes"
    r = requests.get(url)
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')

    tixtables = soup.find('div', class_='unclaimed-prizes-table__wrapper').find('table', class_='unclaimed-prizes-table unclaimed-prizes-table--itg')
    rows = tixtables.find_all('tr')
    
    tixdata = pd.DataFrame()
    for row in rows:
        ticketrow = pd.DataFrame()
        if row.find_all('td') == []: 
            continue
        else:  
            try:
                #tic = row.find(class_='unclaimed-prizes-table__row')
                gameName = re.split(r'[()]',row.find_all('td')[0].text)[0]
                gamePrice = row.find_all('td')[1].text.replace('$','')
                gameNumber = re.split(r'[()]',row.find_all('td')[2].text)[0]
                prizevalues = row.find_all('td')[3].text.splitlines()[2:-2]
                
                for x in prizevalues:
                    prizeamount = x.strip().replace('$','').replace(',','')
                    if prizeamount =='': 
                        continue
                    else:
                        ticketrow.loc[len(ticketrow.index), ['price', 'gameNumber','gameName', 'prizeamount']] = [
                            gamePrice, gameNumber, gameName, prizeamount]
                 
                totalatstart = str(row.find_all('td')[4]).split('>')[1:-1]
                totalatstart = list(map(lambda x: x.replace('<br/', '').replace('</td', '').replace(',',''), totalatstart))
                totalremaining = str(row.find_all('td')[5]).split('>')[1:-1]
                totalremaining = list(map(lambda x: x.replace('<br/', '').replace('</td', '').replace(',',''), totalremaining))
        
                ticketrow['Winning Tickets At Start'] = totalatstart
                ticketrow['Winning Tickets Unclaimed'] = totalremaining
 
                tixdata = pd.concat([tixdata, ticketrow])

            except:
                pass
    
    tixdata['dateexported'] = date.today()

    #add topprize stats from tixdata foir each gameNumber in tixlist
    for t in tixlist['gameNumber']:

        try:   
            tixlist.loc[tixlist['gameNumber']==t,'gameName'] = tixdata.loc[tixdata['gameNumber']==t,'gameName'].iloc[-1]
            tixlist.loc[tixlist['gameNumber']==t,'topprize'] = tixdata.loc[tixdata['gameNumber']==t,'prizeamount'].iloc[-1]
            tixlist.loc[tixlist['gameNumber']==t,'topprizestarting'] = tixdata.loc[tixdata['gameNumber']==t,'Winning Tickets At Start'].iloc[-1]
            tixlist.loc[tixlist['gameNumber']==t,'topprizeremain'] = tixdata.loc[tixdata['gameNumber']==t,'Winning Tickets Unclaimed'].iloc[-1]
            tixlist.loc[tixlist['gameNumber']==t,'topprizeavail'] = 'Top Prize Claimed' if tixdata.loc[tixdata['gameNumber']==t,'Winning Tickets Unclaimed'].iloc[-1]==0 else np.nan
        except: 
            continue

    tixlist.to_csv("./ILtixlist.csv", encoding='utf-8')


    scratchersall = tixlist.loc[:, ['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','dateexported', 'gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()

    #save scratchers list
    #scratchersall.to_sql('ILscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./ILscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixdata.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./ILscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    
    #drop rows missing a game name, because those don't have a game page with overall odds to use for calculations
    gamesgrouped = gamesgrouped.loc[pd.isnull(gamesgrouped['overallodds'])==False,:]
 
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start'].astype(float)*gamesgrouped['overallodds'].astype(float)

    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'].astype(float)*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start'].astype(float)/gamesgrouped['topprizestarting'].astype(float)
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:,'prizeamount'] = "Total"

      
    #loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame() 
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        if gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),'gameName'].iloc[0]==None:
            continue
        else:
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:]
            print(gamerow)
            startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
            tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
            totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
            totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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
            totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
            totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
            totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
            
            gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
            gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
            gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
            gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
            gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
            gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
            chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
            chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
            try:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
            except ZeroDivisionError:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0                      
            
            gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto'].values[0]
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']
    
            currentodds = pd.concat([currentodds, gamerow], axis=0)
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
            totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
            totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
            totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
            print(totalremain.columns)
            
            #add expected values for final totals row
            allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
            
            totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)

            alltables = pd.concat([alltables, totalremain], axis=0, ignore_index=True)

    
    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]

    
    #save scratchers tables
    #scratchertables.to_sql('FLscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./ILscratchertables.csv", encoding='utf-8')
    
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
    ratingstable['Stats Page'] = "/statistics-for-each-illinois-instant-ticket-scratcher-game"
    #ratingstable.to_sql('FLratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./ILratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    ILratingssheet = gs.worksheet('ILRatingsTable')
    ILratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
    print(ratingstable)
    set_with_dataframe(worksheet=ILratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportKSScratcherRecs():
    url = "https://www.kslottery.com/games/instantgameslist"
    r = requests.get(url)
    response = r.text
    #print(r.text)
    soup = BeautifulSoup(response, 'html.parser')

    tixlist = pd.DataFrame()
    tixlist = pd.read_html(io.StringIO(str(soup.find('table', id = 'gametable'))))[0]
    tixlist.rename(columns={0:'gamePrice',1:'gameNumber',2:'gameName', 3:'gameType',4: 'startDate',5:'topprize',6:'topprizeremain',7:'prizeLevel1',8:'level1remain',9:'prizeLevel2',10:'level2remain'}, inplace=True)
    tixlist = tixlist[tixlist['gameType']=='S']
    tixlist['gameNumber'] = tixlist['gameNumber'].astype(str)

    
    tixtables = pd.DataFrame()
    
    for s in tixlist.loc[:,'gameNumber']:
        print(s)
        print(tixlist.loc[tixlist['gameNumber']==s,'gameNumber'])
        #get table of data for each scratcher page
        gameURL = 'https://www.kslottery.com/games/instants/?gameid='+str(s)
        print(gameURL)
        r = requests.get(gameURL)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        #get table of remaining tickets
        page = soup.find('div', class_='page-container')
        tixdata = pd.read_html(io.StringIO(str(page.find('table'))))[1]
        
        tixdata.rename(columns={'Prize':'prizeamount','Remaining':'Winning Tickets Unclaimed'}, inplace=True)
        gameHeader = soup.find('div', class_='col-xs-12 col-md-12 cat-item').find('h2').string
        print(gameHeader)
        gamePrice = gameHeader.split(' - ')[0].replace('$','')
        gameName = gameHeader.split(' - ')[1].strip()
        gameNumber = gameHeader.split(' - ')[2].strip().replace('Game# ','')

        #gamePhoto = 'https://www.kslottery.com'+soup.select_one("img[src*='"+"_"+str(gameNumber)+"']")["src"]
        gamePhoto = 'https://www.kslottery.com'+str(soup.find_all('table')[0].find('img')['src'])
        
        overallodds = tixdata.iloc[-1,0].replace('Overall odds of winning are 1 in ','')
        topprize = tixlist.loc[tixlist['gameNumber']==s,'topprize'].iloc[0]
        topprizestarting = tixdata.iloc[-2,1]
        topprizeavail = 'Top Prize Claimed' if tixlist.loc[tixlist['gameNumber']==s,'topprizeremain'].iloc[0] == 0 else np.nan
        
        print(gamePrice)
        print(gameName)
        print(gameNumber)
        print(gamePhoto)
        print(overallodds)
        print(topprizestarting)
        print(topprizeavail)
            
        print(tixlist.columns)
        tixlist.loc[tixlist['gameNumber']==s,'price'] = gamePrice
        tixlist.loc[tixlist['gameNumber']==s,'gameURL'] = gameURL
        tixlist.loc[tixlist['gameNumber']==s,'gamePhoto'] = gamePhoto
        tixlist.loc[tixlist['gameNumber']==s,'topprizestarting'] = topprizestarting
        tixlist.loc[tixlist['gameNumber']==s,'topprizeavail'] = topprizeavail
        tixlist.loc[tixlist['gameNumber']==s,'overallodds'] = overallodds
        tixlist.loc[tixlist['gameNumber']==s,'endDate'] = None
        tixlist.loc[tixlist['gameNumber']==s,'lastdatetoclaim'] = None
        tixlist.loc[tixlist['gameNumber']==s,'extrachances'] = None
        tixlist.loc[tixlist['gameNumber']==s,'secondChance'] = None
        tixlist.loc[tixlist['gameNumber']==s,'dateexported'] = date.today()

        if len(tixdata) == 0:
            tixtables = pd.concat([tixtables, []], axis=0, ignore_index=True)
        else:
            tixdata = tixdata.iloc[:-1].replace('Free Ticket',gamePrice)
            tixdata = tixdata.iloc[:-1].replace('Free 1 Ticket',gamePrice)
            tixdata['prizeamount'] = tixdata['prizeamount'].str.replace('$','', regex=False).str.replace(',','', regex=False)
            tixdata['gameNumber'] = str(gameNumber)
            tixdata['gameName'] = gameName
            #since Kansas doesn't post the Winning Tickets At Start, just set as Winning Tickets Unclaimed
            tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets Unclaimed']                                                                        
            tixdata['gamePhoto'] = tixlist.loc[tixlist['gameNumber']==s,'gamePhoto'].iloc[0]
            tixdata['price'] = gamePrice
            #if overallodds text not available, calculate overallodds by top prize odds x number of top prizes at start
            tixdata['overallodds'] = overallodds
            tixdata['topprize'] = topprize
            tixdata['topprizestarting'] = topprizestarting
            tixdata['topprizeremain'] = tixlist.loc[tixlist['gameNumber']==s,'topprizeremain'].iloc[0]
            tixdata['topprizeavail'] = topprizeavail
            tixdata['startDate'] = tixlist.loc[tixlist['gameNumber']==s,'startDate'].iloc[0]
            tixdata['endDate'] = None
            tixdata['lastdatetoclaim'] = None
            tixdata['extrachances'] = None
            tixdata['secondChance'] = None
            tixdata['dateexported'] = date.today() 
            print(tixdata)
            
            tixtables = pd.concat([tixtables, tixdata], axis=0)
        
    
    tixlist.to_csv("./KStixlist.csv", encoding='utf-8')
    tixtables = tixtables.loc[(tixtables['prizeamount']!='Prize Ticket') & (tixtables['prizeamount']!='Prize ticket') & (tixtables['prizeamount']!='PRIZE TICKET'),:]
    scratchersall = tixlist.loc[:,['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','gamePhoto','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('KSscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./KSscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./KSscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:,['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    print(gamesgrouped.loc[:,'Non-prize at start'])
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
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
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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

        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
  
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()

        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']

        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal

        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0       
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
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
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('KSscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./KSscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
    KSratingssheet = gs.worksheet('KSRatingsTable')
    KSratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    set_with_dataframe(worksheet=KSratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportOHScratcherRecs():

    urls = ['https://www.ohiolottery.com/Games/ScratchOffs/$1-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$2-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$3-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$5-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/10DollarGames',
                'https://www.ohiolottery.com/Games/ScratchOffs/20DollarGames',
                'https://www.ohiolottery.com/Games/ScratchOffs/$30-Games',
                'https://www.ohiolottery.com/Games/ScratchOffs/$50-Games']
    
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()
    
    for u in urls:
        r = requests.get(u)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        if soup.find('div', class_='cf moduleContent') == None:
            continue
        else: 
            tables = soup.find('div', class_='cf moduleContent').find_all('li')
            for t in range(len(tables)): 
                #get link from the main page, looping through each page for scratchers in price group
                gameName = tables[t].find('a', class_='igName').text
                print(gameName)
                gameNumber = tables[t].text.strip().split('#')[1]
                print(gameNumber)
                gameURL= 'https://www.ohiolottery.com'+str(tables[t].find('a', class_='igName').get('href'))
                print(gameURL)
                def gamecost(i):
                    switcher = {
                        'https://www.ohiolottery.com/Games/ScratchOffs/$1-Games': 1,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$2-Games': 2,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$3-Games': 3,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$5-Games': 5,
                        'https://www.ohiolottery.com/Games/ScratchOffs/10DollarGames': 10,
                        'https://www.ohiolottery.com/Games/ScratchOffs/20DollarGames': 20,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$30-Games': 30,
                        'https://www.ohiolottery.com/Games/ScratchOffs/$50-Games': 50
                    }
                    return switcher.get(i, "Invalid games url")
    
                gamePrice = gamecost(str(u))
                print(gamePrice)
                r = requests.get(gameURL)
                response = r.text
                soup = BeautifulSoup(response, 'html.parser')
                overallodds = soup.find('div',class_='mobileToggleContent').find('p', class_='odds').text.replace('Overall odds of winning: ', '').replace('1 in ', '')
                if overallodds == '': 
                    continue
                else:
                    gamePhoto = 'https://www.ohiolottery.com'+str(soup.find('div',class_='igTicketImg').get('style').replace('background-image: url(', '').replace(');', ''))
                    print(overallodds)
                    print(gamePhoto)
                    table = pd.read_html(io.StringIO(str(soup.find('div',class_='tbl_PrizesRemaining').find('table'))))[0]
                    table.columns = table.columns.droplevel(0)
                    table.rename(columns={'Prizes':'prizeamount','Remaining':'Winning Tickets Unclaimed'}, inplace=True)
                    table.drop(labels={'Unnamed: 2_level_1'}, axis=1,inplace=True)
                    table['gameName'] = gameName
                    table['gameNumber'] = gameNumber
                    table['prizeamount'] = table['prizeamount'].str.replace('$', '', regex=False).str.replace(',','', regex=False).str.strip().str.replace('.00','', regex=False)
                    table['prizeamount'] = table['prizeamount'].replace({'40K/YR FOR 25 YRS':40000*25, '8333MONTH(100K/YR/10YRS)':100000*10, '1000000 (40k/yr/25yrs)':40000*25, '200K/YR FOR 25 YRS': 200000*25, 
                                                                             '80K/YR FOR 25 YRS': 80000*25, '250000YEAR(250K/YR/10YRS)':250000*10, '5000000(200K/YR/25YRS)':200000*25, '36.5K/YR FOR 10 YRS':36500*10, 
                                                                             '1000WEEK(52K/YR/10YRS)': 52000*10, '1M/YR FOR 20 YRS': 1000000*20, '2000000(80K/YR/25YRS)': 80000*25, '400K/YR FOR 25 YRS': 400000*25, 
                                                                             '1000000(40K/YR/25YRS)':40000*25, '1000000(40k/yr/25yrs)':40000*25, '10K/MO FOR 20 YRS': 10000*12*20, '250K/YR FOR 20 YRS': 250000*20, '50/DY FOR 20 YRS': 50*365*20, 
                                                                             '50K/YR FOR 20 YRS':50000*20, '2000000(80k/YR/25YRS)': 80*25, '2000000 (80K/YR/25YRS)': 80*25, '1000000 (40K/YR/25YRS)': 1000000, '15000000 (600K/YR/25YRS)': 15000000, 'ENTRY':gamePrice, 'ENTRY TICKET': gamePrice}).astype('int')
                    table['Winning Tickets At Start'] = table['Winning Tickets Unclaimed']
                    topprize = table.loc[0,'prizeamount']
                    topprizestarting = table.loc[0,'Winning Tickets At Start']
                    topprizeremain = table.loc[0,'Winning Tickets Unclaimed'] 
                    topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
                    startDate = None
                    endDate = None
                    extrachances = None
                    secondChance = None
                    dateexported = date.today()
                    table['dateexported'] = dateexported
                    print(topprizeremain)
                    print(table)
                    tixtables = pd.concat([tixtables, table], axis=0)
                    
                    tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'extrachances', 'secondChance', 'dateexported']] = [
                        gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, overallodds, topprizestarting, topprizeremain, topprizeavail, startDate, endDate, extrachances, secondChance, dateexported]
            
    r = requests.get('https://www.ohiolottery.com/games/scratch-offs/last-day-to-redeem')
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')
    lastdaytbl = pd.read_html(io.StringIO(str(soup.find('div', class_='cf moduleContent').find('table', class_='purple_table igLDTR_tbl'))))[0]
    lastdaytbl.rename(columns={'Game Name':'gameName', 'Game #':'gameNumber', 'Cost': 'gamePrice', 'Last Day to Redeem': 'lastdatetoclaim'}, inplace=True)
    lastdaytbl['gameNumber'] = lastdaytbl['gameNumber'].astype('str')
    print(lastdaytbl)
    tixlist = tixlist.merge(lastdaytbl.loc[:, ['gameNumber', 'lastdatetoclaim']], how="left", on= "gameNumber")
    print(tixlist)
    print(tixlist.columns)

    tixlist.to_csv("./OHtixlist.csv", encoding='utf-8')
    scratchersall = tixlist.loc[:, ['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','gamePhoto','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('OHscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./OHscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./OHscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:,['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    print(gamesgrouped.loc[:,'Non-prize at start'])
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
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
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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

        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
  
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()

        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']

        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal

        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0       
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
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
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('OHscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./OHscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
    ratingstable['Stats Page'] = "/ohio-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('OHratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./OHratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    OHratingssheet = gs.worksheet('OHRatingsTable')
    OHratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    set_with_dataframe(worksheet=OHratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables

def exportTXScratcherRecs():

    url = "https://www.texaslottery.com/export/sites/lottery/Games/Scratch_Offs/all.html"
    r = requests.get(url)
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')
    tixlist = pd.read_html(io.StringIO(str(soup.find('table'))))[0]
    tixlist = tixlist.loc[~tixlist['Game Number'].isna()]
    tixlist.rename(columns={'Game Number':'gameNumber', 'Start Date':'startDate', 'Ticket Price':'price', 'Game Name':'gameName', 
                            'Prize Amount':'topprize', 'Prizes Printed':'topprizestarting', 'Prizes Claimed':'topprizeremaining'}, inplace=True)
    tixlist['gameNumber'] =  tixlist['gameNumber'].astype('int').astype('str')
    tixlist['price'] = tixlist['price'].str.replace('$','').astype('int')
    
    
    #get all the game hyperlinks by looping through the table
    links = pd.DataFrame()
    a_tags = soup.find('table').find_all('a')
    for a in a_tags:
        links.loc[len(links.index), ['gameNumber','gameURL']] = [a.string, a.get('href')]
    print(links)
    tixlist = tixlist.merge(links, how='left', on=['gameNumber'])
    
    #get the tables that have end dates and last dates to claim, and combine them, the merge with tixlist
    gameendtbls = "https://www.texaslottery.com/export/sites/lottery/Games/Scratch_Offs/closing.html"
    r = requests.get(gameendtbls)
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')
    tables = soup.find_all('table')
    closingtable = pd.DataFrame()
    for tbl in tables: 
        tbl = pd.read_html(io.StringIO(str(tbl)))[0]
        print(tbl)
        closingtable = pd.concat([closingtable ,tbl], axis=0, ignore_index=True)    
    closingtable.rename(columns={'Game Name':'gameName', 'Game Number':'gameNumber', 'End of Game Date':'endDate', 'Last Day to Redeem Prizes':'lastdatetoclaim'}, inplace=True)
    closingtable['gameNumber'] = closingtable['gameNumber'].astype('str')
    closingtable.drop(labels=['gameName'], axis=1, inplace=True)
    print(closingtable)
    print(closingtable.columns)
    tixlist = tixlist.merge(closingtable, how='left', on='gameNumber')
    print(tixlist)
    print(tixlist.columns)
    tixlist['extrachances'] = None
    tixlist['secondChance'] = None
    
    tixlist.to_csv("./TXtixlist.csv", encoding='utf-8')

        
    #loop through each row of the data table and get data from the game page
    tixdata = pd.DataFrame()
    for t in range(len(tixlist)):
        print(t)
        print(tixlist.loc[t,:])
        print(tixlist.loc[t,'gameURL'])
        gameURL = 'https://www.texaslottery.com'+str(tixlist.loc[t,'gameURL'])
        print(gameURL)
        r = requests.get(gameURL)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        details = soup.find_all('div', class_='large-4 cell')[1].find_all('p')
        for x in details: 
            if x.string==None:
                continue
            elif 'Overall odds of winning any prize' in x.string: 
                overallodds  = x.string.split('1 in ')[1][0:4] 
                print(overallodds )
            elif 'Claimed as of ' in x.string:
                dateexported = x.string.split('as of ')[1].split('.')[0]
            else: 
                continue
        print(overallodds)
        print(dateexported)
        gamePhoto = 'https://www.texaslottery.com' + soup.find('div', class_='large-4 cell').find('img').get('src')
        print(gamePhoto)
        gameNumber = soup.find('div', class_='large-12 cell').find_all('div', class_='text-center')[0].text.split(' - ')[0].replace('Game No. ','')
        print(gameNumber)
        gameName = soup.find('div', class_='large-12 cell').find_all('div', class_='text-center')[0].text.split(' - ')[1]
        print(gameName)
        
        table = pd.read_html(io.StringIO(str(soup.find_all('div', class_='large-4 cell')[2].find('table', class_='large-only'))))[0]
        table.rename(columns={'Amount':'prizeamount', 'No. in Game*': 'Winning Tickets At Start'}, inplace=True)
        table['No. Prizes Claimed'] = table['No. Prizes Claimed'].replace('---',0)
        table['Winning Tickets At Start'] = table['Winning Tickets At Start'].replace('---',0)
        table['Winning Tickets Unclaimed'] = table['Winning Tickets At Start'].astype('int')-table['No. Prizes Claimed'].astype('int')
        table['gameName'] = gameName
        table['gameNumber'] = gameNumber
        table['dateexported'] = dateexported
        table['prizeamount'] = table['prizeamount'].str.replace('$','', regex=False).str.replace(',','', regex=False)
    
        topprizeremain = table['Winning Tickets Unclaimed'].iloc[0]
        topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
        
        tixlist.loc[t,'topprizeremain'] = topprizeremain
        tixlist.loc[t,'overallodds'] = overallodds
        tixlist.loc[t,'dateexported'] = dateexported
        tixlist.loc[t,'topprizeavail'] = topprizeavail
        tixlist.loc[t,'gameURL'] = gameURL
        tixlist.loc[t,'gamePhoto'] = gamePhoto
        
        print(table)
        tixdata = pd.concat([tixdata, table], axis=0, ignore_index=True)
      
        
    tixlist.to_csv("./TXtixlist.csv", encoding='utf-8')

    print(tixlist.columns)

    scratchersall = tixlist.loc[:, ['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances',
                             'secondChance','startDate','endDate','lastdatetoclaim','dateexported', 'gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()

    #save scratchers list
    #scratchersall.to_sql('ILscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./TXscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixdata.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./TXscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    
    #drop rows missing a game name, because those don't have a game page with overall odds to use for calculations
    gamesgrouped = gamesgrouped.loc[pd.isnull(gamesgrouped['overallodds'])==False,:]
    
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:,['gameName','gameNumber','overallodds']])
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start'].astype(float)*gamesgrouped['overallodds'].astype(float)
    print(gamesgrouped.loc[:,['gameName','gameNumber','Total at start']])
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed'].astype(float)*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start'].astype(float)/gamesgrouped['topprizestarting'].astype(float)
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
    totals.rename(columns={'Total at start': 'Winning Tickets At Start', 'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:,'prizeamount'] = "Total"

      
    #loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame() 
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        if gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),'gameName'].iloc[0]==None:
            continue
        else:
            gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid),:]
            print(gamerow)
            startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
            tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
            totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid),['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
            totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
            price = int(gamerow['price'].values[0])
            print(gameid)
            print(tixtotal)
            print(totalremain)
            prizes = totalremain.loc[:,'prizeamount']
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
            totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
            print(totalremain.loc[totalremain['prizeamount'] != 'Total',:].dtypes)
            print(type(startingtotal))
            print(type(tixtotal))
            print(type(price))
            testdf = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']]
            print(testdf[~testdf.applymap(np.isreal).all(1)])
            totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            print(totalremain.loc[:,'Starting Expected Value'])
            totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
            totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
            
            gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
            gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
            gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
            gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
            gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
            gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()
            chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']
            chngAvailPrizes = (tixtotal-startingtotal)/startingtotal
            try:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
            except ZeroDivisionError:
                gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0        
            print(tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto'])
            gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto'].values[0]
            gamerow.loc[:,'FAQ'] = None
            gamerow.loc[:,'About'] = None
            gamerow.loc[:,'Directory'] = None
            gamerow.loc[:,'Data Date'] = gamerow.loc[:,'dateexported']
    
            currentodds = pd.concat([currentodds, gamerow], axis=0, ignore_index=True)
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
            totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
            totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
            totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
            
            print(totalremain.columns)
            
            #add expected values for final totals row
            allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
            
            totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
            totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
            print(totalremain)
            alltables = pd.concat([alltables, totalremain], axis=0)

    
    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('TXscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./TXscratchertables.csv", encoding='utf-8')
    
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
    ratingstable['Stats Page'] = "/texas-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('TXratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./TXratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    TXratingssheet = gs.worksheet('TXRatingsTable')
    TXratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    ratingstable.replace([np.inf, -np.inf], 0, inplace=True)
    ratingstable.fillna('',inplace=True)
    print(ratingstable)
    set_with_dataframe(worksheet=TXratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables

def exportKYScratcherRecs():

    url = 'https://www.kylottery.com/apps/scratch_offs/available_games.html'
    
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()

    r = requests.get(url)
    response = r.text
    soup = BeautifulSoup(response, 'html.parser')
    tixdata = soup.find_all('div', class_='panel panel-info')

    for t in tixdata: 
        #get link from the main page, looping through each page for scratchers in price group
        gameName = t.find('div', class_='panel-heading klc-clickable-item').text.strip().split('-')[0].strip()
        print(gameName)
        #gameNumber = t.find('div', class_='panel-heading klc-clickable-item').text.strip().split('-')[1].strip()
        gameNumber = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[6].text
        print(gameNumber)
        gameURL = 'https://www.kylottery.com/apps/scratch_offs/prizes_remaining.html'+t.find('div', class_='panel-heading klc-clickable-item').get('href')
        print(gameURL)
        startDate = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[0].text
        print(startDate)
        endDate = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[1].text
        lastdatetoclaim = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[2].text
        gamePrice = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[3].text.replace('$','')
        topprize = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[4].text.replace('$','').replace('*','').replace(',','')
        overallodds = t.find_all('div', class_='col-md-6')[1].find('div').find_all('b')[5].text.replace('1:','')
        gamePhoto = 'https://www.kylottery.com'+t.find_all('div',class_='col-md-6')[0].find('img').get('src')
        print(gamePhoto)
        dateexported = date.today() if t.find_all('div', class_='col-md-6')[1].find_all('div')[1].find('b').text ==None else t.find_all('div', class_='col-md-6')[1].find_all('div')[1].find('b').text
        print(dateexported)
        
        table = pd.read_html(io.StringIO(str(t.find_all('div', class_='col-md-6')[1].find('table'))))[0]
        if len(table)==0:
            continue
        else:
            print(table)
            table.rename(columns={'Prize Amount':'prizeamount','Prizes Remaining':'Winning Tickets Unclaimed'}, inplace=True)
            table['gameName'] = gameName
            table['gameNumber'] = gameNumber
            table['prizeamount'] = table['prizeamount'].str.replace('$', '', regex=False).str.replace(',','', regex=False)
            #table['Winning Tickets Unclaimed'] = table['Winning Tickets Unclaimed'].str.replace(',', '', regex=False)
            table['Winning Tickets Unclaimed'] = table['Winning Tickets Unclaimed'].replace({'0 - Last Top Prize Claimed':0}).astype('int')
            table['Winning Tickets At Start'] = table['Winning Tickets Unclaimed']
            
            topprizestarting = table.loc[0,'Winning Tickets At Start']
            topprizeremain = table.loc[0,'Winning Tickets Unclaimed'] 
            topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
            extrachances = None
            secondChance = None
            table['dateexported'] = dateexported
            print(topprizeremain)
            print(table)
            tixtables = pd.concat([tixtables, table], axis=0)
            
            tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'lastdatetoclaim', 'extrachances', 'secondChance', 'dateexported']] = [
                gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, overallodds, topprizestarting, topprizeremain, topprizeavail, startDate, endDate, lastdatetoclaim, extrachances, secondChance, dateexported]

    tixlist.to_csv("./KYtixlist.csv", encoding='utf-8')
    print(tixlist.columns)
    scratchersall = tixlist.loc[:,['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','gamePhoto','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('KYscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./KYscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./KYscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:,['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    print(gamesgrouped.loc[:,'Non-prize at start'])
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
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
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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

        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
  
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()

        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']

        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal

        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0       
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
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
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('KYscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./KYscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
    ratingstable['Stats Page'] = "/kentucky-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('KYratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./KYratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    KYratingssheet = gs.worksheet('KYRatingsTable')
    KYratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    set_with_dataframe(worksheet=KYratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables


def exportWAScratcherRecs():

    urls = ['https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$1',
            'https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$2',
            'https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$3',
            'https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$5',
            'https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$10',
            'https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$20',
            'https://walottery.com/Scratch/TopPrizesRemaining.aspx?price=$30']
    
    for url in urls:
        
        tixtables = pd.DataFrame()
        tixlist = pd.DataFrame()
        
        
        #go to game page for tix data
        r = requests.get(url)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        gameURL = str('https://walottery.com/Scratch/'+str(soup.find_all('div', class_='prizes-remaining-item')[0].find('a')['href']))
        r = requests.get(gameURL)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')

    
        #get game data from JSON loaded into Script tag
        scripts = soup.find_all('script', string=re.compile('WaLottery.Scratch.data'))
        jsontext = scripts[0].string.split('all: ')[1].split('ads: ')[0].replace("JSON.parse('",'').replace('{"Games":[', '')
        jsontext = '['+jsontext.split('],"Result"')[0]+']'
        gamesdata = json.loads(jsontext)

        for game in gamesdata:
            gameNumber = game['Id']
            gameName = game['GameName'].replace("&#39;","'")
            gamePrice = game['Cost']
            gamePhoto = game['GridImageUrl']
            overallodds = float(game['OverallOdds'].replace('1 in ',''))
            startDate = str(game['SalesStartDate'])
            endDate = str(game['SalesEndDate'])
            lastdatetoclaim = str(game['RedeemEndDate'])
            dateexported = str(game['LastUpdated'])
            print(game)
            print(gameNumber)
            print(gameName)
            print(gamePrice)
            print(gamePhoto)
            print(overallodds)
            print(startDate)
            print(endDate)
            print(lastdatetoclaim)
            print(dateexported)
            
            tixtable = json_normalize(game['Prizes'])
            tixtable.rename(columns={'PrizeAmount':'prizeamount','TotalPrizesNumber':'Winning Tickets At Start', 'PrizesRemainingNumber':'Winning Tickets Unclaimed'}, inplace=True)
            tixtable['gameName'] = gameName
            tixtable['gameNumber'] = gameNumber
            tixtable['dateexported'] = dateexported
            tixtable['prizeamount'] = tixtable['prizeamount'].str.replace('$','', regex=False).str.replace(',','', regex=False)
            tixtable['prizeamount'] = tixtable['prizeamount'].replace({'40000/yr/25 years':40000*25,'80000/yr/25yrs':80000*25,'80000/yr/25 years':80000*25, '150000/yr/10yrs':150000*25, 'LIFE': 1000*52*50}).astype('int')
            print(tixtable)
            tixtables = pd.concat([tixtables, tixtable], axis=0, ignore_index=True)
            
            topprize = tixtable.loc[0,'prizeamount']
            topprizestarting = tixtable.loc[0,'Winning Tickets At Start']
            topprizeremain = tixtable.loc[0,'Winning Tickets Unclaimed']
            topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
            extrachances = None
            secondChance = None
      
            tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'lastdatetoclaim', 'extrachances', 'secondChance', 'dateexported']] = [
                gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, overallodds, topprizestarting, topprizeremain, topprizeavail, startDate, endDate, lastdatetoclaim, extrachances, secondChance, dateexported]

    tixlist.to_csv("./WAtixlist.csv", encoding='utf-8')
    scratchersall = tixlist.loc[:, ['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','gamePhoto','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    #scratchersall.to_sql('WAscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./WAscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./WAscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:,['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    print(gamesgrouped.loc[:,'Non-prize at start'])
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
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
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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

        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
  
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()

        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']

        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal

        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0       
        #gamerow.loc[:,'Photo'] = tixlist.loc[tixlist['gameNumber']==gameid,'gamePhoto']
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
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    #scratchertables.to_sql('WAscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./WAscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
    ratingstable['Stats Page'] = "/washington-statistics-for-each-scratcher-game"
    #ratingstable.to_sql('WAratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./WAratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    WAratingssheet = gs.worksheet('WARatingsTable')
    WAratingssheet.clear()
    
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    set_with_dataframe(worksheet=WAratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables
    

def exportORScratcherRecs():
    url = "https://api2.oregonlottery.org/instant/GetAll"

    payload = {}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36',
        'Ocp-Apim-Subscription-Key': '683ab88d339c4b22b2b276e3c2713809'
    }
    r = requests.request("GET", url, headers=headers, data=payload)

    responsejson = r.json()
    tixlist = pd.DataFrame()
    linkslist = pd.DataFrame()
    tixtables = pd.DataFrame(columns=['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed','topprizestarting', 'dateexported'])

    
    #get the game URLs from script tag on the main page and add them to a dataframe to be merged with the data from JSON
    url = "https://www.oregonlottery.org/scratch-its/grid/"
    r = requests.get(url)
    responsehtml = r.text
    soup = BeautifulSoup(responsehtml, 'html.parser')
    scripttag = soup.find('body').find('main', class_='ol-content').find('script')
    links = scripttag.string.split('scratchIts = ')[1].split(';')[0]
    links = json.loads(links)
    for tic in links:
        gameURL = tic['link'].replace('//','/')
        gameID = str(tic['number'])
        gamePhoto = tic['image_preview'][0]
        linkslist.loc[len(linkslist.index), ['gameNumber', 'gameURL', 'gamePhoto']] = [gameID, gameURL, gamePhoto]

    
    # loop through each game in the JSON data and add to the Tixlist dataframe
    for game in responsejson:

        gameName = game['GameNameTitle']
        gameNumber = str(game['GameNumber'])
        gamePrice = game['TicketPrice']
        topprize = game['TopPrize']
        topprizeremain = game['TopPrizesRemaining']
        topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan
        startDate = game['DateAvailable']
        endDate = game['GameEndDate']
        lastdatetoclaim = game['ValidationEndDate']
        overallodds = game['OverallOdds']
        extrachances = None
        secondChance = '2nd Chance' if game['SecondChanceDrawDate'] else None
        dateexported = date.today()

        tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'lastdatetoclaim', 'overallodds', 'extrachances', 'secondChance', 'dateexported']] = [
            gamePrice, gameName, gameNumber, topprize, topprizeremain, topprizeavail, startDate, endDate, lastdatetoclaim, overallodds, extrachances, secondChance, dateexported]
        
    #merge gameURLs with tix list to start looping through each game page, to get remaining tickets data
    tixlist = pd.merge(tixlist, linkslist, how='left', on=['gameNumber'])
    tixlist['lastdatetoclaim'] = pd.to_datetime(tixlist['lastdatetoclaim'])
    tixlist['startDate'] = pd.to_datetime(tixlist['startDate'])
    tixlist['dateexported'] = pd.to_datetime(tixlist['dateexported'])
    tixlist = tixlist.loc[(tixlist['startDate'] <= datetime.today()) & (tixlist['lastdatetoclaim'] >= datetime.today())]    
    
    tixlist.to_csv("./ORtixlist.csv", encoding='utf-8')
    tixlist = tixlist.loc[(tixlist['startDate'] <= datetime.today()) & (tixlist['lastdatetoclaim'] >= datetime.today())]    

    for game in tixlist.loc[:,'gameNumber']:
        url = 'https://api2.oregonlottery.org/instant/GetGame?includePrizeTiers=true&gameNumber='+str(game)

        headers = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36',
        'Ocp-Apim-Subscription-Key': '683ab88d339c4b22b2b276e3c2713809'
        }
        r = requests.request("GET", url, headers=headers, data=payload)
        responsejson = r.json()

        # go down to next level of json response for numbers of prizes
        tixdata = pd.json_normalize(responsejson[0]['PrizeTiers'])
        
        if tixdata.empty!=True:  
            tixdata.rename(columns={'PrizeAmount': 'prizeamount','PrizesTotal': 'Winning Tickets At Start', 'PrizesRemaining': 'Winning Tickets Unclaimed'}, inplace=True)
            tixdata['gameNumber'] = game
            tixdata['gameName'] = tixlist.loc[tixlist['gameNumber']==game, 'gameName'].iloc[0]
            tixdata['Winning Tickets At Start'] = tixdata['Winning Tickets At Start'].astype(int)
            tixdata['Winning Tickets Unclaimed'] = tixdata['Winning Tickets Unclaimed'].astype(int)
            tixdata['topprizestarting'] = tixdata.loc[0, 'Winning Tickets At Start']
            tixdata['dateexported'] = date.today()

            tixtables = pd.concat([tixtables, tixdata], axis=0)

        else:
            continue


    tixtables['Winning Tickets At Start'] = tixtables['Winning Tickets At Start'].astype(int)
    tixtables['Winning Tickets Unclaimed'] = tixtables['Winning Tickets Unclaimed'].astype(int)
                
    tixlist = tixlist.loc[(tixlist['startDate'] <= datetime.today()) & (tixlist['lastdatetoclaim'] >= datetime.today())]    
    tixlist.to_csv("./ORtixlist.csv", encoding='utf-8')
    
    scratchersall = tixlist.loc[:, ['price', 'gameName', 'gameNumber', 'topprize', 'overallodds', 'topprizeremain',
                               'topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported','gameURL']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber']
                                      != "Coming Soon!", :]
    scratchersall = scratchersall.drop_duplicates()

    # save scratchers list
    #scratchersall.to_sql('OKscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./ORscratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes

    scratchertables = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed','topprizestarting', 'dateexported']]

    scratchertables.to_csv("./ORscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber']
                                          != "Coming Soon!", :]

    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall

    gamesgrouped = scratchertables.groupby(['gameNumber', 'gameName', 'dateexported'], observed=True).sum(
    ).reset_index(level=['gameNumber', 'gameName', 'dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, [
                                      'gameNumber', 'price', 'topprizeremain', 'overallodds']], how='left', on=['gameNumber'])

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
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped.loc[:,['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']]
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

        testdf = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']]

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
        gamerow.loc[:, 'Photo'] = tixlist.loc[tixlist['gameNumber']
                                              == gameid, 'gamePhoto'].values[0]
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
    #scratchertables.to_sql('OKscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./ORscratchertables.csv", encoding='utf-8')

    # create rankings table by merging the list with the tables

    scratchersall.loc[:, 'price'] = scratchersall.loc[:,
                                                      'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(
        currentodds, how='left', on=['gameNumber', 'price'])
    ratingstable.drop(labels=['gameName_x', 'dateexported_y', 'overallodds_y',
                      'topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
    ratingstable.rename(columns={'gameName_y': 'gameName', 'dateexported_x': 'dateexported', 'topprizeodds_x': 'topprizeodds',
                        'overallodds_x': 'overallodds', 'topprizeremain_y': 'topprizeremain'}, inplace=True)
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

    ratingstable['Stats Page'] = "/oregon-statistics-for-each-scratch-it-game"
    #ratingstable.to_sql('ORratingstable', engine, if_exists='replace')
    ratingstable.to_csv("./ORratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    ORratingssheet = gs.worksheet('ORRatingsTable')
    ORratingssheet.clear()
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

    set_with_dataframe(worksheet=ORratingssheet, dataframe=ratingstable, include_index=False,
                       include_column_header=True, resize=True)
    return ratingstable, scratchertables

def exportMSScratcherRecs():

    urls = ['https://www.mslottery.com/gamestatus/active/',
           'https://www.mslottery.com/gamestatus/ending/']
    
    tixtables = pd.DataFrame()
    tixlist = pd.DataFrame()
    
    for url in urls:
        r = requests.get(url)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        if soup.find_all('div',class_='col-lg-3 gamebox') == None:
            continue
        else:
            tixurls = soup.find_all('div',class_='col-lg-3 gamebox')
        
            for t in tixurls: 
                gameURL = t.find('a').get('href')
                print(gameURL)
                gamePhoto = t.find('img').get('data-src')
                print(gamePhoto)
                
                r = requests.get(gameURL)
                response = r.text
                soup = BeautifulSoup(response, 'html.parser')
                gameName = soup.find('h1', class_='entry-title').string

                tixdata = pd.read_html(io.StringIO(str(soup.find('table', class_='juxtable'))))[0]
                gameNumber = tixdata.loc[tixdata[0]=='Game Number',1].iloc[0]
                gamePrice = tixdata.loc[tixdata[0]=='Ticket Price',1].iloc[0].replace('$','')
                topprize = tixdata.loc[tixdata[0]=='Top Prize',1].iloc[0].replace('$','').replace(',','')
                overallodds = tixdata.loc[tixdata[0]=='Overall Odds',1].iloc[0].replace('1:','')
                startDate = tixdata.loc[tixdata[0]=='Launch Date',1].iloc[0]
                endDate = tixdata.loc[tixdata[0]=='End Date',1].iloc[0] if len(tixdata.loc[tixdata[0]=='End Date',1]) > 0 else None
                lastdatetoclaim = tixdata.loc[tixdata[0]=='Claim Deadline',1].iloc[0] if len(tixdata.loc[tixdata[0]=='Claim Deadline',1]) > 0 else None
                extrachances = None
                dateexported = date.today()
                
                print(gameName)
                print(gamePrice)
                print(topprize)
                print(overallodds)
                print(startDate)
                print(endDate)
                print(gameNumber)
                
                table = pd.read_html(io.StringIO(str(soup.find_all('table')[1])))[0]
                secondChance = '2nd Chance'if table.iloc[0,2]>0 else None
                table.columns = table.columns.droplevel(0)
                table.rename(columns={table.columns[0]:'prizeamount', table.columns[1]: 'Winning Tickets At Start', table.columns[2]: 'Winning Tickets Unclaimed'}, inplace=True)
                table['prizeamount'] = table['prizeamount'].str.replace('$','', regex=False).str.replace(',','', regex=False)
                table['gameNumber'] = gameNumber
                table['gameName'] = gameName
                table['dateexported'] = dateexported
                print(table)
                tixtables = pd.concat([tixtables, table], axis=0)

                topprizeremain = table.loc[0,'Winning Tickets Unclaimed']
                topprizestarting = table.loc[0,'Winning Tickets At Start']
                topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan

                tixlist.loc[len(tixlist.index), ['price', 'gameName', 'gameNumber','gameURL','gamePhoto', 'topprize', 'overallodds', 'topprizestarting', 'topprizeremain', 'topprizeavail', 'startDate', 'endDate', 'lastdatetoclaim', 'extrachances', 'secondChance', 'dateexported']] = [
                gamePrice, gameName, gameNumber, gameURL, gamePhoto, topprize, overallodds, topprizestarting, topprizeremain, topprizeavail, startDate, endDate, lastdatetoclaim, extrachances, secondChance, dateexported]
    
    tixlist.to_csv("./MStixlist.csv", encoding='utf-8')
    print(tixlist.columns)
    scratchersall = tixlist.loc[:,['price','gameName','gameNumber','topprize','overallodds','topprizestarting','topprizeremain','topprizeavail','extrachances','secondChance','startDate','endDate','lastdatetoclaim','gamePhoto','dateexported']]
    scratchersall = scratchersall.loc[scratchersall['gameNumber'] != "Coming Soon!",:]
    scratchersall = scratchersall.drop_duplicates()
    
    #save scratchers list
    scratchersall.to_csv("./MSscratcherslist.csv", encoding='utf-8')
    
    #Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables.loc[:,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','dateexported']]
    scratchertables.to_csv("./MSscratchertables.csv", encoding='utf-8')
    scratchertables = scratchertables.loc[scratchertables['gameNumber'] != "Coming Soon!",:]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    #Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    gamesgrouped = scratchertables.groupby(['gameNumber','gameName','dateexported'], observed=True).sum().reset_index(level=['gameNumber','gameName','dateexported'])
    gamesgrouped = gamesgrouped.merge(scratchersall.loc[:, ['gameNumber','gamePhoto','price','topprizestarting','topprizeremain','overallodds']], how='left', on=['gameNumber'])
    print(gamesgrouped.columns)
    print(gamesgrouped.loc[:,['gameNumber','overallodds','Winning Tickets At Start','Winning Tickets Unclaimed']])
    gamesgrouped.rename(columns={'gamePhoto':'Photo'}, inplace=True)
    gamesgrouped.loc[:,'Total at start'] = gamesgrouped['Winning Tickets At Start']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Total remaining'] = gamesgrouped['Winning Tickets Unclaimed']*gamesgrouped['overallodds'].astype(float)
    gamesgrouped.loc[:,'Non-prize at start'] = gamesgrouped['Total at start']-gamesgrouped['Winning Tickets At Start']
    print(gamesgrouped.loc[:,'Non-prize at start'])
    gamesgrouped.loc[:,'Non-prize remaining'] = gamesgrouped['Total remaining']-gamesgrouped['Winning Tickets Unclaimed']
    gamesgrouped.loc[:,'topprizeodds'] = gamesgrouped['Total at start']/gamesgrouped['topprizestarting']
    print(gamesgrouped.loc[:,'topprizeodds'])
    gamesgrouped.loc[:,['price','topprizeodds','overallodds', 'Winning Tickets At Start','Winning Tickets Unclaimed']] = gamesgrouped.loc[:, ['price','topprizeodds','overallodds', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
    
    
    #create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then concat to scratcherstables
    nonprizetix = gamesgrouped.loc[:,['gameNumber','gameName','Non-prize at start','Non-prize remaining','dateexported']]
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start', 'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:,'prizeamount'] = 0
    print(nonprizetix.columns)
    totals = gamesgrouped.loc[:,['gameNumber','gameName','Total at start','Total remaining','dateexported']]
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
        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
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

        totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']] = totalremain.loc[:, ['prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed']].apply(pd.to_numeric)
  
        totalremain.loc[:,'Starting Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[:,'Expected Value'] = totalremain.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Starting Expected Value','Expected Value','dateexported']]
        
        gamerow.loc[:,'Expected Value of Any Prize (as % of cost)'] = sum(totalremain['Expected Value'])/price if price > 0 else sum(totalremain['Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Any Prize'] = ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))/price if price > 0 else ((sum(totalremain['Expected Value'])-sum(totalremain['Starting Expected Value']))/sum(totalremain['Starting Expected Value']))
        gamerow.loc[:,'Expected Value of Profit Prize (as % of cost)'] = sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])/price if price > 0 else sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])
        gamerow.loc[:,'Change in Expected Value of Profit Prize'] = ((sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/price if price > 0 else (sum(totalremain.loc[totalremain['prizeamount']>price,'Expected Value'])-sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value']))/sum(totalremain.loc[totalremain['prizeamount']>price,'Starting Expected Value'])
        gamerow.loc[:,'Percent of Prizes Remaining'] = (totalremain.loc[:,'Winning Tickets Unclaimed']/totalremain.loc[:,'Winning Tickets At Start']).mean()
        gamerow.loc[:,'Percent of Profit Prizes Remaining'] = (totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets Unclaimed']/totalremain.loc[totalremain['prizeamount']>price,'Winning Tickets At Start']).mean()

        chngLosingTix = (gamerow.loc[:,'Non-prize remaining']-gamerow.loc[:,'Non-prize at start'])/gamerow.loc[:,'Non-prize at start']

        chngAvailPrizes = (tixtotal-startingtotal)/startingtotal

        try:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes
        except ZeroDivisionError:
            gamerow.loc[:,'Ratio of Decline in Prizes to Decline in Losing Ticket'] = 0   
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
        totalremain = totalremain.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber']==gameid,['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)
        
        #add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount']!='Total',:]
        
        totalremain.loc[totalremain['prizeamount']!='Total','Starting Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal),axis=1)
        totalremain.loc[totalremain['prizeamount']!='Total','Expected Value'] = allexcepttotal.apply(lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal),axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables.loc[:, ['gameNumber','gameName','prizeamount','Winning Tickets At Start','Winning Tickets Unclaimed','Prize Probability','Percent Tix Remaining','Starting Expected Value','Expected Value','dateexported']]
    print(scratchertables.columns)   
    
    #save scratchers tables
    scratchertables.to_csv("./MSscratchertables.csv", encoding='utf-8')
    
    #create rankings table by merging the list with the tables
    print(currentodds.dtypes)
    print(scratchersall.dtypes)
    scratchersall.loc[:,'price'] = scratchersall.loc[:,'price'].apply(pd.to_numeric)
    ratingstable = scratchersall.merge(currentodds, how='left', on=['gameNumber','price'])
    ratingstable.drop(labels=['gamePhoto', 'gameName_x','dateexported_y','overallodds_y','topprizestarting_x','topprizeremain_x', 'prizeamount'], axis=1, inplace=True)
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
    ratingstable['Stats Page'] = "/mississippi-statistics-for-each-scratch-off-game"
    ratingstable.to_csv("./MSratingstable.csv", encoding='utf-8')
    # write to Google Sheets
    # select a work sheet from its name
    MSratingssheet = gs.worksheet('MSRatingsTable')
    MSratingssheet.clear()
    
    ratingstable = ratingstable.loc[:, ['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    set_with_dataframe(worksheet=MSratingssheet, dataframe=ratingstable, include_index=False,
    include_column_header=True, resize=True)
    return ratingstable, scratchertables

# function to create an array of prizes by their probability for all scratchers still unclaimed


def generateWeightedList(prizes, prizeNumbers, tixTotal, tixinRoll):
    weighted_list = []
    for prize in prizes:
        prizeCount = int(
            prizeNumbers.loc[prizeNumbers['prizeamount'] == prize, 'Winning Tickets Unclaimed'].values[0])
        weighted_list.extend(repeat(prize, prizeCount))

    random.shuffle(weighted_list)
    return weighted_list

# calculate the optimal bankroll from the bet amount (ticket cost), the negative probability (probability of losing, factoring in stdDev),
# the longest expected losing streak (from odds and stdDev), and the Risk Coeeficient - higher if risk-averse (e.g., 5 or higher) or lower if more risk-tolerant (e.g. 2 or less))


def optimalbankroll(cost, stDev, probability, odds, riskCoeff):
    negProbability = 1 - (probability + stDev*3)
    LonglosingStreak = np.round(
        abs(np.log(float(odds))/np.log(float(negProbability))))
    bankroll = cost*LonglosingStreak*riskCoeff
    d = dict()
    d['Longest Losing Streak'] = LonglosingStreak
    d['Optimal Bankroll'] = bankroll
    return d


allSimOutcomes = pd.DataFrame()
allSimTables = pd.DataFrame()


def clusterloop(ratingstable, scratchertables, prizetype, stddevs, riskCoeff):

    # determine the size of the roll based on the scratcher price
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
        return switcher.get(price, "Invalid game price")

    simulations = pd.DataFrame()
    simTable = pd.DataFrame(columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size',
                                     'Ticket Number', 'Ticket Outcome', 'Cluster Number', 'Cluster Outcome', 'Total Tally'])
    simOutcomes = pd.DataFrame(columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size', 'Any Prize Probability',
                                        'Profit Prize Probability', 'Number of Tickets', 'Number of Clusters', 'Number of Prizes', 'Number of Profit Prizes', 'Observed Prize Frequency', 'Observed Profit Prize Frequency',
                                        'Average Cluster Outcome', 'Median Cluster Outcome', 'StdDev Ticket Outcome', 'StdDev Any Prizes', 'StdDev Profit Prizes', 'Final Tally', 'Prizes Descriptive Stats'])

    # loop through each game generating outcomes for a sample of tickets in clusters
    for gameid in ratingstable.loc[:, 'gameNumber']:
        print(gameid)
        price = float(
            ratingstable.loc[ratingstable['gameNumber'] == gameid, 'price'].values[0])
        print(price)
        gamename = ratingstable.loc[ratingstable['gameNumber']
                                    == gameid, 'gameName'].values[0]
        prizes = scratchertables.loc[(scratchertables['gameNumber'] == gameid) & (
            scratchertables['prizeamount'] != "Total"), 'prizeamount']
        totalprizes = scratchertables.loc[(scratchertables['gameNumber'] == gameid) & (
            scratchertables['prizeamount'] != "Total"), ['prizeamount', 'Winning Tickets Unclaimed']]
        totaltixstarting = scratchertables.loc[(scratchertables['gameNumber'] == gameid) & (
            scratchertables['prizeamount'] == "Total"), 'Winning Tickets At Start'].values[0]
        totaltixremain = int(scratchertables.loc[(scratchertables['gameNumber'] == gameid) & (
            scratchertables['prizeamount'] == "Total"), 'Winning Tickets Unclaimed'].values[0])
        print(totaltixremain)
        tixinRoll = rollsize(price)
        print(tixinRoll)

        # settings for the simulation based on function parameters
        if (prizetype == "profit"):
            gameprob = float(ratingstable.loc[ratingstable['gameNumber']
                             == gameid, 'Probability of Winning Profit Prize'].values[0])
            oddsprizes = float(
                ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Odds of Profit Prize'].values[0])
            stDevpct = float(totalprizes.loc[(totalprizes['prizeamount'] != price) & (
                totalprizes['prizeamount'] != "0"), 'Winning Tickets Unclaimed'].std().mean()/totaltixremain)
            totalprizesremain = totalprizes.loc[(totalprizes['prizeamount'] != price) & (
                totalprizes['prizeamount'] != "0"), 'Winning Tickets Unclaimed'].sum()

        elif (prizetype == "any"):
            gameprob = float(ratingstable.loc[ratingstable['gameNumber']
                             == gameid, 'Probability of Winning Any Prize'].values[0])
            oddsprizes = float(
                ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Current Odds of Any Prize'].values[0])
            stDevpct = float(totalprizes.loc[totalprizes['prizeamount'] !=
                             "0", 'Winning Tickets Unclaimed'].std().mean()/totaltixremain)
            totalprizesremain = totalprizes.loc[totalprizes['prizeamount']
                                                != "0", 'Winning Tickets Unclaimed'].sum()

        print(oddsprizes)
        print(stddevs)
        print(stDevpct)
        print(gameprob)
        # probability plus std dev percent for max probability
        print(gameprob-(stDevpct*stddevs))
        # probability converted to decimal odds, expanding number of tickets by subtracting standard deviations so the 1 in X number grows with more Std Devs
        print(1/(gameprob-(stDevpct*stddevs)))

        # get a cluster size by taking the probability any prize then converting to decimal odds, expanding number of tickets by subtracting standard deviations so the 1 in X number grows with more Std Devs
        clustersize = int(np.round(1/(gameprob-(stDevpct*stddevs)), 0))
        print(clustersize)
        # description of the parameters to add to the file name
        description = prizetype+"-" + \
            str(stddevs)+"stDevs"+"-RiskCoeff"+str(riskCoeff)

        # Get the sample size of total game, and them a number of clusters in hte sample
        sampleSize = np.round(
            totaltixremain/(1+(totaltixremain*np.power(0.03, 2))))
        print(sampleSize)
        clusterSample = np.round(sampleSize/clustersize)
        print(clusterSample)

        # use above numbers to generate a randomly shuffled list of prizes, then select a set to form a roll of scratcher tickets
        weightedList = generateWeightedList(
            prizes, totalprizes, totaltixstarting, tixinRoll)
        startpos = random.randint(0, tixinRoll)
        endpos = startpos+tixinRoll
        roll = weightedList[startpos:endpos]
        print(roll)

        # use function to get the optimal bankroll amount from the probability, standard deviations, and risk factor
        # divide stDev by totaltixremain so that it is a percentage of total, like the game prize probability figure
        bankroll = optimalbankroll(
            price, stDevpct, gameprob, clustersize, riskCoeff)
        longlosingstreak = bankroll['Longest Losing Streak']
        bankroll = bankroll['Optimal Bankroll']
        print(longlosingstreak)
        print(bankroll)

        tally = 0
        clusterCount = 1

        # loop through each ticket in the cluster
        print(len(roll))
        simCluster = pd.DataFrame(columns=simTable.columns)

        # pull tickets from cluster until the total tally sucks up the bankroll amount or until it goes through sample of clusters
        # while (tally >= -(bankroll)) & (clusterCount <= clusterSample):

        # Maybe change this run until the ticket sample size instead of bankroll?
        while (clusterCount <= clusterSample):
            randnum = random.randint(0, tixinRoll)
            print(randnum)

            cluster = []
            clusterOutcome = 0

            # but first check if the number of tickets purchased will exceed number left in the roll
            if (len(roll) - randnum) < (clustersize):
                clustergroup = roll[(randnum):(tixinRoll)]
                print(clustergroup)

                # get new roll to get remainder of cluster
                startpos = randnum
                endpos = startpos+tixinRoll
                roll = weightedList[startpos:endpos]
                print(roll)

                # get new cluster starting at first ticket in roll
                startpos = 0
                endpos = clustersize-len(clustergroup)
                clustergroup.extend(roll[startpos:endpos])
                print(clustergroup)

            else:
                # get the cluster as it is from the same roll
                clustergroup = roll[(randnum):(randnum+clustersize)]
            print(clustergroup)
            print(len(clustergroup))

            tic = 1

            # loop through each possible ticket up to the possible tickets in cluster
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

                # add cluster to dataframe of each cluster for this gameid, so it can be used for stats
                simCluster.loc[len(simCluster)] = cluster

                # add cluster outcome to a dataframe for all clusters of all games
                simTable.loc[len(simTable)] = cluster
                print(simTable.shape)

                # advance the ticket count by one
                tic = tic + 1

            print(simCluster.shape)
            # advance the cluster count by one
            clusterCount = clusterCount + 1

        # compile stats for comparison table
        probAnyPrize = ratingstable.loc[ratingstable['gameNumber']
                                        == gameid, 'Probability of Winning Any Prize'].values[0]
        probProfitPrize = ratingstable.loc[ratingstable['gameNumber']
                                           == gameid, 'Probability of Winning Profit Prize'].values[0]
        numTickets = simCluster['Ticket Number'].count()
        numClusters = clusterCount-1
        numPrizes = simCluster.loc[simCluster['Ticket Outcome']
                                   > 0, 'Ticket Outcome'].count()
        numProfitPrizes = simCluster.loc[simCluster['Ticket Outcome']
                                         > price, 'Ticket Outcome'].count()
        prizeFreq = numPrizes/numTickets
        profitprizeFreq = numProfitPrizes/numTickets
        avgClusterOutcome = simCluster['Cluster Outcome'].mean()
        medianClusterOutcome = simCluster['Cluster Outcome'].median()
        stdevTicketOutcome = simCluster['Ticket Outcome'].std()
        stdevAnyprizes = simCluster.loc[simCluster['Ticket Outcome']
                                        > 0, 'Ticket Outcome'].std()
        stdevAnyprizes = simCluster.loc[simCluster['Ticket Outcome']
                                        > price, 'Ticket Outcome'].std()
        finalTally = simCluster['Cluster Outcome'].sum()
        prizesstats = simCluster.loc[simCluster['Ticket Outcome']
                                     > 0, 'Ticket Outcome'].describe()

        result = [gameid, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize, probAnyPrize, probProfitPrize,
                  numTickets, numClusters, numPrizes, numProfitPrizes, prizeFreq, profitprizeFreq, avgClusterOutcome, medianClusterOutcome,
                  stdevTicketOutcome, stdevAnyprizes, stdevAnyprizes, finalTally, prizesstats]
        print(result)
        # add stats to a dataframe of stats for each game
        simOutcomes.loc[len(simOutcomes)] = result

        print(simTable)
        print(simOutcomes)

    allSimOutcomes.concat(simOutcomes, ignore_index=False)
    allSimTables.concat(simTable, ignore_index=False)

    simTable.to_csv("/Users/michaeljames/Documents/scratchersdatapull/simTable_" +
                    description+"2.csv", encoding='utf-8')
    simOutcomes.to_csv("/Users/michaeljames/Documents/scratchersdatapull/simOutcomes_" +
                       description+"2.csv", encoding='utf-8')

    allSimTables.to_csv(
        "/Users/michaeljames/Documents/scratchersdatapull/simTable_0-3StDevs.csv", encoding='utf-8')
    allSimOutcomes.to_csv(
        "/Users/michaeljames/Documents/scratchersdatapull/simOutcomes_0-3StDevs.csv", encoding='utf-8')

    return simTable, simOutcomes


prizetypes = ['any', 'profit']
stdeviations = [0, 3]
'''
#loop through each number of std devs and for whether any prize probability and then profit prizes
for t in prizetypes:
    for std in stdeviations:
        clusterloop(ratingstable, scratchertables, t, std, 2)        
'''




exportVAScratcherRecs()
exportAZScratcherRecs()
exportMOScratcherRecs()
exportOKScratcherRecs()
exportCAScratcherRecs()
exportNMScratcherRecs()
exportNYScratcherRecs()
exportDCScratcherRecs()
exportNCScratcherRecs()
exportTXScratcherRecs()
exportKSScratcherRecs()
exportWAScratcherRecs()    
#exportILScratcherRecs()
#exportFLScratcherRecs()
#exportOHScratcherRecs()

#exportKYScratcherRecs()
exportORScratcherRecs()
#exportMSScratcherRecs()
exportMDScratcherRecs()

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
logger.info(f'Finishing lotteryscrape.py at: {now}')
'''
scheduler = BlockingScheduler()
scheduler.add_job(exportVAScratcherRecs, 'cron', hour=0, minute=30)
scheduler.add_job(exportAZScratcherRecs, 'cron', hour=0, minute=30)
scheduler.start()
'''
