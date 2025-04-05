#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 30 09:45:57 2025

@author: michaeljames
"""

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


def clusterloop(state, ratingstable, scratchertables, prizetype, stddevs, riskCoeff):

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

