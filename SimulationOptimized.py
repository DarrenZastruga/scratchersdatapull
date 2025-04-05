#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 31 20:19:23 2025

@author: michaeljames
"""

import io
import json
import logging
import logging.handlers
import os
import re
import random
import requests
import urllib.parse
import urllib.request
from datetime import date, datetime
from dateutil.tz import tzlocal
from itertools import repeat

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import gspread
from gspread_dataframe import set_with_dataframe
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import psycopg2
from psycopg2.extensions import register_adapter, AsIs

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)


# Constants
LOG_FILE = "status.log"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
POWERS = {'B': 10 ** 9, 'K': 10 ** 3, 'M': 10 ** 6, 'T': 10 ** 12}
GSHEET_KEY = '1vAgFDVBit4C6H2HUnOd90imbtkCjOl1ekKychN2uc4o'
IMAGE_PATH = './gameimages/'

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)


def generateWeightedList(prizes, prizeNumbers):
    """
    Creates a weighted list of prizes based on the number of unclaimed tickets.
    Optimized for speed by using NumPy and vectorized operations.
    """
    prize_list = []
    for prize, count in zip(prizes, prizeNumbers):
        prize_list.extend(repeat(prize, count))

    random.shuffle(prize_list)  # In-place shuffle
    return prize_list


def optimalbankroll(cost, stDev, probability, odds, riskCoeff):
    """Calculates the optimal bankroll."""
    negProbability = 1 - (probability + stDev * 3)
    LonglosingStreak = round(abs(np.log(odds) / np.log(negProbability)))
    bankroll = cost * LonglosingStreak * riskCoeff
    return {'Longest Losing Streak': LonglosingStreak, 'Optimal Bankroll': bankroll}


def clusterloop(ratingstable, scratchertables, prizetype, stddevs, riskCoeff):
    """
        Runs a simulation of buying scratcher tickets in clusters to evaluate
        profitability, risk, and other metrics.
    """

    def rollsize(price):
        """Determines roll size based on ticket price."""
        switcher = {
            0: 100, 1: 200, 2: 100, 3: 100, 5: 40, 10: 40, 20: 20, 30: 20}
        return switcher.get(price, "Invalid game price")

    simTable = []  # Use a list to accumulate data efficiently
    simOutcomes = []

    for gameid in ratingstable['gameNumber']:
        price = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'price'].iloc[0]
        gamename = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'gameName'].iloc[0]

        # Extract data *once* for each game ID
        totalprizes = scratchertables.loc[
            (scratchertables['gameNumber'] == gameid) &
            (scratchertables['prizeamount'] != "Total"),
            ['prizeamount', 'Winning Tickets Unclaimed']
        ]

        totaltixremain = scratchertables.loc[
            (scratchertables['gameNumber'] == gameid) &
            (scratchertables['prizeamount'] == "Total"), 'Winning Tickets Unclaimed'].iloc[0]

        prizes = totalprizes['prizeamount'].to_list()
        prize_counts = totalprizes['Winning Tickets Unclaimed'].to_list() # Use for faster zip in generateWeightedList


        tixinRoll = rollsize(price)

        # Settings for profit/any prize
        if prizetype == "profit":
            gameprob = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Probability of Winning Profit Prize'].iloc[0]
            oddsprizes = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Odds of Profit Prize'].iloc[0]
            stDevpct = totalprizes.loc[(totalprizes['prizeamount'] != price) & (totalprizes['prizeamount'] != "0"), 'Winning Tickets Unclaimed'].std(skipna=True) / totaltixremain
        else: #Any
            gameprob = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Probability of Winning Any Prize'].iloc[0]
            oddsprizes = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Current Odds of Any Prize'].iloc[0]
            stDevpct = totalprizes.loc[totalprizes['prizeamount'] != "0", 'Winning Tickets Unclaimed'].std(skipna=True) / totaltixremain

        clustersize = int(round(1 / (gameprob - (stDevpct * stddevs)), 0))  # Optimized calc

        description = f"{prizetype}-{stddevs}stDevs-RiskCoeff{riskCoeff}"
        sampleSize = int(round(totaltixremain / (1 + (totaltixremain * (0.03 ** 2)))))
        clusterSample = int(round(sampleSize / clustersize))

        weightedList = generateWeightedList(prizes, prize_counts)
        tixinRoll = min(len(weightedList), tixinRoll)  #Prevent index errors in roll calls
        startpos = random.randint(0, tixinRoll-1) #Adjusted bounds for same reason
        roll = weightedList[startpos:] + weightedList[:startpos]  # Circular roll

        bankroll_data = optimalbankroll(price, stDevpct, gameprob, oddsprizes, riskCoeff)
        longlosingstreak = bankroll_data['Longest Losing Streak']
        bankroll = bankroll_data['Optimal Bankroll']


        tally = 0
        numPrizes = 0
        numProfitPrizes = 0
        tic_outcomes = [] #Store ticket outcomes in list

        for clusterCount in range(1, int(clusterSample) + 1):
            cluster = []
            clusterOutcome = 0
            clustergroup = roll[clusterCount:clusterCount + clustersize]
            
            if len(clustergroup)< clustersize:
                clustergroup.extend(roll[0:clustersize-len(clustergroup)])

            for tic in range(len(clustergroup)):
                ticOutcome = clustergroup[tic]
                tally += ticOutcome - price
                clusterOutcome += ticOutcome - price
                tic_outcomes.append(ticOutcome)
                simTable.append([gameid, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize,
                                  tic + 1, ticOutcome, clusterCount, clusterOutcome, tally])

                if ticOutcome > 0:
                    numPrizes += 1  #Faster than DataFrame-based counting

                if ticOutcome > price:
                    numProfitPrizes += 1
        
        
        tic_outcomes = np.array(tic_outcomes)

        # Compile stats (using NumPy for speed)
        probAnyPrize = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Probability of Winning Any Prize'].iloc[0]
        probProfitPrize = ratingstable.loc[ratingstable['gameNumber'] == gameid, 'Probability of Winning Profit Prize'].iloc[0]
        numTickets = len(tic_outcomes)  # or (clusterCount-1)*clustersize if not limited by available tix
        numClusters = clusterSample
        prizeFreq = numPrizes / numTickets if numTickets>0 else 0 # safe division
        profitprizeFreq = numProfitPrizes / numTickets if numTickets > 0 else 0
        
        #DescriptiveStats
        if len(tic_outcomes) > 0: #Make sure that array has more than 0 outcomes.
            avgClusterOutcome = np.mean(tic_outcomes)
            medianClusterOutcome = np.median(tic_outcomes)
            stdevTicketOutcome = np.std(tic_outcomes)
            
            prizesstats = pd.Series(tic_outcomes[tic_outcomes>0]).describe().to_dict()
            print(prizesstats)
            stdevAnyprizes = np.std(tic_outcomes[tic_outcomes>0]) if numPrizes > 1 else 0 #Avoid std on single or no elements
            stdevProfitprizes = np.std(tic_outcomes[tic_outcomes>price]) if numProfitPrizes >1 else 0 #ditto.
            
        else: 
            avgClusterOutcome = 0
            medianClusterOutcome = 0
            stdevTicketOutcome = 0
            stdevAnyprizes = 0
            stdevProfitprizes = 0
            prizesstats = {} #Empty dict.


        finalTally = np.sum(tic_outcomes - price)
        

        result = [gameid, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize, probAnyPrize, probProfitPrize,
                  numTickets, numClusters, numPrizes, numProfitPrizes, prizeFreq, profitprizeFreq, avgClusterOutcome, medianClusterOutcome,
                  stdevTicketOutcome, stdevAnyprizes, stdevAnyprizes, finalTally, prizesstats]
        simOutcomes.append(result)

    simTable_df = pd.DataFrame(simTable, columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size',
                                     'Ticket Number', 'Ticket Outcome', 'Cluster Number', 'Cluster Outcome', 'Total Tally'])
    simOutcomes_df = pd.DataFrame(simOutcomes, columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size', 'Any Prize Probability',
                                        'Profit Prize Probability', 'Number of Tickets', 'Number of Clusters', 'Number of Prizes', 'Number of Profit Prizes', 'Observed Prize Frequency', 'Observed Profit Prize Frequency',
                                        'Average Cluster Outcome', 'Median Cluster Outcome', 'StdDev Ticket Outcome', 'StdDev Any Prizes', 'StdDev Profit Prizes', 'Final Tally', 'Prizes Descriptive Stats'])
    
    simTable_df.to_csv(f"./simTable_{description}2.csv", encoding='utf-8')
    simOutcomes_df.to_csv(f"./simOutcomes_{description}2.csv", encoding='utf-8')
    return simTable_df, simOutcomes_df