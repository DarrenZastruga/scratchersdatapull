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
import urllib.parse
import urllib.request
from datetime import date, datetime
from dateutil.tz import tzlocal
from itertools import repeat
import numpy as np
import pandas as pd
# from bs4 import BeautifulSoup # Not used
from google.oauth2.service_account import Credentials
import gspread
# from gspread_dataframe import set_with_dataframe # Not used directly
# from pydrive.auth import GoogleAuth # Not used
# from pydrive.drive import GoogleDrive # Not used
# import psycopg2 # Not used
# from psycopg2.extensions import register_adapter, AsIs # Not used
import sys 
from dotenv import load_dotenv

# psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs) # Not used

# Ensure the script's directory is in the Python path for module imports
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
    print(f"Added '{script_dir}' to sys.path")


# Constants
LOG_FILE = "status.log"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
# POWERS = {'B': 10 ** 9, 'K': 10 ** 3, 'M': 10 ** 6, 'T': 10 ** 12} # Not actively used
GSHEET_KEY = '1vAgFDVBit4C6H2HUnOd90imbtkCjOl1ekKychN2uc4o'
# IMAGE_PATH = './gameimages/' # Not actively used

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if not logger.handlers: 
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
    console_handler = logging.StreamHandler(sys.stdout) # Log to console
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO) # Console logs INFO and above
    logger.addHandler(console_handler)


def authorize_gspread_from_path():
    script_abs_dir = os.path.dirname(os.path.abspath(__file__)) 
    dotenv_path = os.path.join(script_abs_dir, '.env')
    load_dotenv(dotenv_path=dotenv_path, override=True, verbose=True)
    env_var_for_path = 'GOOGLE_APPLICATION_CREDENTIALS_JSON' 
    creds_path = os.getenv(env_var_for_path) 
    logger.info(f"Trying to use credentials path from env var '{env_var_for_path}': {creds_path}")
    if not creds_path:
        logging.error(f"{env_var_for_path} not found or empty.")
        return None
    if not os.path.exists(creds_path):
        logging.error(f"Credentials file not found: {creds_path}. CWD: {os.getcwd()}")
        return None
    try:
        credentials = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
        client = gspread.authorize(credentials)
        logging.info(f"Gspread authorized using file: {creds_path}")
        return client
    except Exception as e:
        logging.error(f"Error authorizing gspread from file: {e}", exc_info=True)
        return None

def get_pack_sizes(gspread_client, state_name=None):
    try:
        if not gspread_client:
            logger.error("gspread_client not provided to get_pack_sizes.")
            return {}
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        worksheet_name = 'PackSizes' 
        try:
            worksheet = gsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"Worksheet '{worksheet_name}' not found.")
            return {}
        data = worksheet.get_all_values()
        if len(data) <= 1: 
            logger.warning(f"No data in sheet: {worksheet_name}.")
            return {}
        header = data[0]
        pack_sizes_df = pd.DataFrame(data[1:], columns=header)
        if 'State' not in pack_sizes_df.columns:
            logger.warning(f"Missing 'State' column in '{worksheet_name}'.")
            return {}
        pack_sizes_df.set_index('State', inplace=True)
        if state_name not in pack_sizes_df.index:
            logger.warning(f"State '{state_name}' not in '{worksheet_name}' index. Avail: {pack_sizes_df.index.tolist()}")
            return {}
        state_series = pack_sizes_df.loc[state_name]
        pack_sizes_dict = {}
        for price_col, val_str in state_series.items():
            if isinstance(price_col, str) and price_col.startswith('$'):
                try:
                    price_f = float(price_col.replace('$', '').strip())
                    if val_str and str(val_str).strip(): 
                        pack_sizes_dict[price_f] = int(str(val_str).strip())
                except ValueError:
                    logger.warning(f"Conv error for price '{price_col}' or pack_size '{val_str}' for state '{state_name}'.")
        if not pack_sizes_dict:
            logger.warning(f"No valid pack sizes for state '{state_name}' from '{worksheet_name}'.")
        else:
            logger.info(f"Pack sizes for '{state_name}': {pack_sizes_dict}")
        return pack_sizes_dict
    except Exception as e:
        logger.error(f"Error in get_pack_sizes for '{worksheet_name}': {e}", exc_info=True)
        return {}

def generateWeightedList(prizes, prizeNumbers):
    prize_list = []
    # Ensure prizeNumbers are integers and prizes are numeric
    for prize, count_val in zip(prizes, prizeNumbers):
        try:
            count = int(count_val)
            if isinstance(prize, (int, float)) and count > 0: # Only add if count is positive
                 prize_list.extend(repeat(prize, count))
            elif count <= 0:
                 logger.debug(f"Skipping prize {prize} in generateWeightedList due to count <= 0 ({count}).")
            else:
                 logger.debug(f"Skipping prize {prize} in generateWeightedList, not numeric or count issue.")
        except ValueError:
            logger.warning(f"Could not convert count '{count_val}' to int for prize {prize} in generateWeightedList.")

    if not prize_list: # If all counts were zero or prizes non-numeric
        logger.warning("generateWeightedList: prize_list is empty after processing inputs.")
        
    random.shuffle(prize_list)
    return prize_list

def optimalbankroll(cost, stDev, probability, odds, riskCoeff):
    negProbability = 1 - (probability + stDev * 3)
    if negProbability <= 0 or negProbability >=1: 
        LonglosingStreak = float('inf') if negProbability <=0 else 0 
    else:
        LonglosingStreak = round(abs(np.log(odds) / np.log(negProbability)))
    bankroll = cost * LonglosingStreak * riskCoeff
    return {'Longest Losing Streak': LonglosingStreak, 'Optimal Bankroll': bankroll}


def clusterloop(gspread_client, state, ratingstable_all_states, scratchertables_all_data, prizetype, stddevs, riskCoeff):
    logger.info(f"Clusterloop: State={state}, PrizeType={prizetype}, StdDevs={stddevs}, RiskCoeff={riskCoeff}")

    if 'State' not in ratingstable_all_states.columns:
        logger.error("'State' col missing in ratingstable. Cannot filter.")
        return pd.DataFrame(), pd.DataFrame()
    current_state_ratingstable = ratingstable_all_states[ratingstable_all_states['State'] == state].copy()
    if current_state_ratingstable.empty:
        logger.warning(f"No rating data for state '{state}'.")
        return pd.DataFrame(), pd.DataFrame()
    logger.info(f"Ratingstable for '{state}': {len(current_state_ratingstable)} games.")

    has_state_col_in_scratchers = 'State' in scratchertables_all_data.columns
    if has_state_col_in_scratchers:
        logger.info("Scratchertables has 'State' column, will filter.")
    else:
        logger.warning("Scratchertables LACKS 'State' column. Game lookups might be incorrect if gameIDs are not globally unique.")

    simTable, simOutcomes = [], []
    pack_sizes_for_state = get_pack_sizes(gspread_client, state_name=state)
    if not pack_sizes_for_state:
        logger.error(f"No pack sizes for '{state}'. Aborting clusterloop for this state.")
        return pd.DataFrame(), pd.DataFrame()

    for gameid_from_ratings in current_state_ratingstable['gameNumber']:
        game_row_rt = current_state_ratingstable[current_state_ratingstable['gameNumber'] == gameid_from_ratings]
        price = game_row_rt['price'].iloc[0] 
        gamename = game_row_rt['gameName'].iloc[0]
        logger.debug(f"Processing Game: {gamename} (ID: {gameid_from_ratings}, State: {state}, Price: ${price:.2f})")

        conditions_st = (scratchertables_all_data['gameNumber'] == gameid_from_ratings)
        if has_state_col_in_scratchers:
            conditions_st &= (scratchertables_all_data['State'] == state)
        
        game_specific_scratchertable_subset = scratchertables_all_data[conditions_st]
        if game_specific_scratchertable_subset.empty:
            logger.warning(f"No rows in scratchertables for game ID '{gameid_from_ratings}' (State: '{state if has_state_col_in_scratchers else 'N/A'}'). Skipping.")
            continue

        if 'prizeamount' not in game_specific_scratchertable_subset.columns:
            logger.error(f"'prizeamount' col missing for {gamename}. Cols: {game_specific_scratchertable_subset.columns}. Skip.")
            continue
        
        unique_prizeamounts_for_game = game_specific_scratchertable_subset['prizeamount'].unique()
        logger.debug(f"Unique 'prizeamount' for {gamename} (ID: {gameid_from_ratings}, State: {state}): {unique_prizeamounts_for_game}")
        
        if "Total" not in unique_prizeamounts_for_game:
            logger.warning(f"'Total' NOT among prizeamounts for {gamename}. Cannot find total tix. Unique: {unique_prizeamounts_for_game}. Skip.")
            continue

        totaltixremain_series = game_specific_scratchertable_subset.loc[
            game_specific_scratchertable_subset['prizeamount'] == "Total",
            'Winning Tickets Unclaimed' # This column should exist and be numeric (int) from main()
        ]
        
        if totaltixremain_series.empty:
            logger.error(f"UNEXPECTED: 'Total' row not found for {gamename} (ID:{gameid_from_ratings}) despite 'Total' being in unique values. Logic flaw. Skip.")
            continue
        
        # Ensure 'Winning Tickets Unclaimed' is correctly accessed and is a single value
        if len(totaltixremain_series) > 1:
            logger.warning(f"Multiple 'Total' rows found for {gamename} (ID:{gameid_from_ratings}). Using first. Data: {totaltixremain_series.tolist()}")
        
        totaltixremain = totaltixremain_series.iloc[0] 
        if pd.isna(totaltixremain) or totaltixremain == 0: # totaltixremain is already int from main(), so pd.isna is unlikely unless something went wrong.
            logger.warning(f"Total tickets for {gamename} is {totaltixremain}. Skipping.")
            continue
        logger.debug(f"Total tickets for {gamename}: {totaltixremain}")

        totalprizes_df = game_specific_scratchertable_subset.loc[
            game_specific_scratchertable_subset['prizeamount'] != "Total"
        ].copy()

        if totalprizes_df.empty:
            logger.warning(f"No prize tier data (non-'Total') for {gamename}. Skipping.")
            continue

        totalprizes_df['prizeamount'] = pd.to_numeric(totalprizes_df['prizeamount'], errors='coerce')
        # Winning Tickets Unclaimed should already be int from main()
        totalprizes_df.dropna(subset=['prizeamount', 'Winning Tickets Unclaimed'], inplace=True)
        
        if totalprizes_df.empty:
            logger.warning(f"All prize tiers NaN or missing Winning Tickets for {gamename}. Skipping.")
            continue

        prizes = totalprizes_df['prizeamount'].to_list()
        prize_counts = totalprizes_df['Winning Tickets Unclaimed'].to_list() 

        tixinRoll = pack_sizes_for_state.get(price) 
        if tixinRoll is None:
            tixinRoll = 30 
            logger.warning(f"No pack size for price ${price:.2f}, game {gamename}. Default: {tixinRoll}.")
        else:
            tixinRoll = int(tixinRoll)

        if prizetype == "profit":
            prob_col, odds_col = 'Probability of Winning Profit Prize', 'Odds of Profit Prize'
        else: 
            prob_col, odds_col = 'Probability of Winning Any Prize', 'Current Odds of Any Prize'
        
        if prob_col not in game_row_rt.columns or odds_col not in game_row_rt.columns :
            logger.error(f"Missing '{prob_col}' or '{odds_col}' for {gamename}. Skipping.")
            continue
        gameprob = game_row_rt[prob_col].iloc[0] 
        oddsprizes = game_row_rt[odds_col].iloc[0] 
        if pd.isna(gameprob) or pd.isna(oddsprizes):
            logger.warning(f"Prob/Odds NaN for {gamename} ({prizetype}). Skipping.")
            continue

        if prizetype == "profit":
             relevant_prizes_for_std = totalprizes_df.loc[
                (totalprizes_df['prizeamount'] != price) & (totalprizes_df['prizeamount'] != 0.0), 
                'Winning Tickets Unclaimed'
            ]
        else: # Any
            relevant_prizes_for_std = totalprizes_df.loc[
                totalprizes_df['prizeamount'] != 0.0, 
                'Winning Tickets Unclaimed'
            ]

        stDevpct = relevant_prizes_for_std.std(skipna=True) / totaltixremain if totaltixremain > 0 and not relevant_prizes_for_std.empty else 0.0
        if pd.isna(stDevpct): stDevpct = 0.0 

        denominator = gameprob - (stDevpct * stddevs)
        if denominator <= 1e-9:  
            clustersize = min(int(totaltixremain), 500) 
            if clustersize == 0 and totaltixremain > 0: clustersize = int(totaltixremain)
            elif clustersize == 0: logger.warning(f"Clustersize 0 for {gamename} after default. Skip."); continue 
        else:
            clustersize = int(round(1 / denominator, 0))

        if clustersize <= 0: clustersize = 1
        clustersize = min(clustersize, int(totaltixremain)) 
        if clustersize == 0: logger.warning(f"Final clustersize 0 for {gamename}. Skip."); continue 
        
        run_description_for_file = f"{state}-{prizetype}-{stddevs}stDevs-RiskCoeff{riskCoeff}" # Defined for filenames
        
        if totaltixremain > 0 : # totaltixremain is already confirmed > 0
            sampleSize = max(1, int(round(totaltixremain / (1 + (totaltixremain * (0.03 ** 2))))))
            clusterSample = max(1, int(round(sampleSize / clustersize))) if clustersize > 0 else 0
            if clusterSample == 0: logger.warning(f"ClusterSample 0 for {gamename}. Skip."); continue 
        else: logger.error(f"Unexpected: totaltixremain not > 0 for {gamename} at sampling. Skip."); continue

        valid_prizes_for_list = []
        valid_counts_for_list = []
        for p, c in zip(prizes, prize_counts): # prizes are float, prize_counts are int
            if c > 0: 
                valid_prizes_for_list.append(p)
                valid_counts_for_list.append(c)
        
        if not valid_prizes_for_list:
            logger.warning(f"No prizes with counts > 0 for {gamename}. WeightedList will be empty. Skip.")
            continue
            
        weightedList = generateWeightedList(valid_prizes_for_list, valid_counts_for_list)
        if not weightedList: 
            logger.warning(f"Weighted list empty for {gamename} after generation. Skip.")
            continue

        current_tixinRoll = min(len(weightedList), tixinRoll) 
        if current_tixinRoll == 0: 
            logger.warning(f"tixinRoll 0 for {gamename}. Skip.")
            continue

        startpos = random.randint(0, current_tixinRoll -1) 
        roll = weightedList[startpos:] + weightedList[:startpos]

        bankroll_data = optimalbankroll(price, stDevpct, gameprob, oddsprizes, riskCoeff)
        longlosingstreak = bankroll_data['Longest Losing Streak']
        bankroll = bankroll_data['Optimal Bankroll']

        tally, numPrizes, numProfitPrizes = 0,0,0
        tic_outcomes = []

        for cl_num_idx in range(int(clusterSample)): 
            clusterOutcome = 0
            current_cluster_group = []
            start_idx_in_roll = (cl_num_idx * clustersize) % len(roll) 
            for i in range(clustersize):
                current_cluster_group.append(roll[(start_idx_in_roll + i) % len(roll)])
            for tic_idx, ticOutcome_val in enumerate(current_cluster_group): 
                tally += ticOutcome_val - price
                clusterOutcome += ticOutcome_val - price
                tic_outcomes.append(ticOutcome_val)
                simTable.append([gameid_from_ratings, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize,
                                  tic_idx + 1, ticOutcome_val, cl_num_idx + 1, clusterOutcome, tally])
                if ticOutcome_val > 0: numPrizes += 1
                if ticOutcome_val > price: numProfitPrizes += 1
        
        if not tic_outcomes: 
            logger.warning(f"No ticket outcomes for {gamename}. Skipping stats.")
            continue
            
        tic_outcomes_np = np.array(tic_outcomes)
        probAnyPrize_game = game_row_rt['Probability of Winning Any Prize'].iloc[0]
        probProfitPrize_game = game_row_rt['Probability of Winning Profit Prize'].iloc[0]
        numTickets = len(tic_outcomes_np)
        numClusters = clusterSample
        prizeFreq = numPrizes / numTickets if numTickets > 0 else 0
        profitprizeFreq = numProfitPrizes / numTickets if numTickets > 0 else 0
        
        avgTicketOutcome, medianTicketOutcome, stdevTicketOutcome = 0,0,0
        stdevAnyprizes, stdevProfitprizes, prizesstats = 0,0,{}
        if numTickets > 0:
            avgTicketOutcome, medianTicketOutcome, stdevTicketOutcome = np.mean(tic_outcomes_np), np.median(tic_outcomes_np), np.std(tic_outcomes_np)
            positive_outcomes = tic_outcomes_np[tic_outcomes_np > 0]
            profit_outcomes = tic_outcomes_np[tic_outcomes_np > price]
            if len(positive_outcomes) > 0: prizesstats = pd.Series(positive_outcomes).describe().to_dict()
            if len(positive_outcomes) > 1: stdevAnyprizes = np.std(positive_outcomes) 
            if len(profit_outcomes) > 1: stdevProfitprizes = np.std(profit_outcomes)
        finalTally = np.sum(tic_outcomes_np - price) if numTickets > 0 else 0
        
        result = [gameid_from_ratings, gamename, price, longlosingstreak, bankroll, riskCoeff, prizetype, stddevs, clustersize, 
                  probAnyPrize_game, probProfitPrize_game, numTickets, numClusters, numPrizes, numProfitPrizes, 
                  prizeFreq, profitprizeFreq, avgTicketOutcome, medianTicketOutcome, stdevTicketOutcome, 
                  stdevAnyprizes, stdevProfitprizes, finalTally, prizesstats]
        simOutcomes.append(result)

    simTable_df = pd.DataFrame(simTable, columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size',
                                     'Ticket Number in Cluster', 'Ticket Outcome', 'Cluster Number', 'Cluster Net Outcome', 'Running Total Tally'])
    simOutcomes_df = pd.DataFrame(simOutcomes, columns=['Game Number', 'Game Name', 'Cost', 'Longest Losing Streak', 'Optimal Bankroll', 'Risk Coefficient', 'Prize Types', 'Standard Deviations', 'Cluster Size', 'Expected Any Prize Probability',
                                        'Expected Profit Prize Probability', 'Number of Tickets Simulated', 'Number of Clusters Simulated', 'Number of Prizes Won', 'Number of Profit Prizes Won', 'Observed Prize Frequency', 'Observed Profit Prize Frequency',
                                        'Average Ticket Outcome', 'Median Ticket Outcome', 'StdDev Ticket Outcome', 'StdDev Value of Prizes Won', 'StdDev Value of Profit Prizes Won', 'Final Tally', 'Descriptive Stats of Prizes Won'])
    
    output_dir = "./" 
    os.makedirs(output_dir, exist_ok=True)
    
    simTable_filename = os.path.join(output_dir, f"simTable_{run_description_for_file}.csv")
    simOutcomes_filename = os.path.join(output_dir, f"simOutcomes_{run_description_for_file}.csv")

    if not simTable_df.empty:
        simTable_df.to_csv(simTable_filename, encoding='utf-8', index=False)
        logger.info(f"Saved simTable to {simTable_filename}")
    else:
        logger.warning(f"simTable_df is empty for {run_description_for_file}. Not saving CSV.")

    if not simOutcomes_df.empty:
        simOutcomes_df.to_csv(simOutcomes_filename, encoding='utf-8', index=False)
        logger.info(f"Saved simOutcomes to {simOutcomes_filename}")
    else:
        logger.warning(f"simOutcomes_df is empty for {run_description_for_file}. Not saving CSV.")
        
    return simTable_df, simOutcomes_df

def main():
    now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f'Starting SimulationOptimized.py at: {now}')
    try:
        gspread_client = authorize_gspread_from_path()
        if not gspread_client: return
        logger.info("Gspread authorization successful.")
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        logger.info(f"Opened Google Sheet: {GSHEET_KEY}")

        ws_name = 'AllStatesRatings'
        try: ws_ratings = gsheet.worksheet(ws_name)
        except gspread.exceptions.WorksheetNotFound: logger.error(f"'{ws_name}' not found."); return
        data_r = ws_ratings.get_all_values()
        ratingstable = pd.DataFrame()
        if len(data_r) > 1:
            ratingstable = pd.DataFrame(data_r[1:], columns=data_r[0])
            logger.info(f"Loaded '{ws_name}': {len(ratingstable)} rows. Cols: {ratingstable.columns.tolist()}")
            if 'State' not in ratingstable.columns: logger.error(f"'State' col missing in '{ws_name}'."); return
            ratingstable.rename(columns={'Game #': 'gameNumber', 'Scratcher Cost': 'price', 'Game Name': 'gameName'}, inplace=True, errors='ignore')
            if 'gameNumber' in ratingstable.columns:
                ratingstable['gameNumber'] = ratingstable['gameNumber'].astype(str).str.strip()
            else: logger.error(f"'gameNumber'/'Game #' col missing in '{ws_name}'."); return
            ratingstable['State'] = ratingstable['State'].astype(str).str.strip()
            cols_convert_r = {'price': float, 'Probability of Winning Profit Prize': float, 'Odds of Profit Prize': float, 
                              'Probability of Winning Any Prize': float, 'Current Odds of Any Prize': float}
            pct_cols_r = ['Probability of Winning Profit Prize', 'Probability of Winning Any Prize']
            for col, typ in cols_convert_r.items():
                if col in ratingstable.columns:
                    ratingstable[col] = ratingstable[col].replace('',np.nan)
                    if ratingstable[col].dtype=='object':
                        ratingstable[col] = ratingstable[col].astype(str).str.replace('$','',regex=False).str.replace(',','',regex=False)
                        if col in pct_cols_r: ratingstable[col] = ratingstable[col].str.replace('%','',regex=False)
                    ratingstable[col] = pd.to_numeric(ratingstable[col], errors='coerce')
                    if col in pct_cols_r and typ==float and ratingstable[col].notna().any():
                        ratingstable[col] /= 100.0
            essential_num_cols_r = ['price'] + list(cols_convert_r.keys())
            ratingstable.dropna(subset=['gameNumber', 'State'] + [c for c in essential_num_cols_r if c in ratingstable.columns], inplace=True)
            logger.info(f"Ratingstable after clean: {len(ratingstable)} rows.")
        else: logger.error(f"'{ws_name}' empty."); return

        ws_name_st = 'ScratcherTables'
        try: ws_scratchers = gsheet.worksheet(ws_name_st)
        except gspread.exceptions.WorksheetNotFound: logger.error(f"'{ws_name_st}' not found."); return
        data_st = ws_scratchers.get_all_values()
        scratchertables = pd.DataFrame()
        if len(data_st) > 1:
            scratchertables = pd.DataFrame(data_st[1:], columns=data_st[0])
            logger.info(f"Loaded '{ws_name_st}': {len(scratchertables)} rows. Cols: {scratchertables.columns.tolist()}")
            has_state_col_st = 'State' in scratchertables.columns
            if has_state_col_st:
                scratchertables['State'] = scratchertables['State'].astype(str).str.strip()
            else:
                logger.warning(f"'State' col missing in '{ws_name_st}'. State-specific checks for data presence will be skipped.")

            if 'gameNumber' not in scratchertables.columns and 'Game #' in scratchertables.columns:
                scratchertables.rename(columns={'Game #': 'gameNumber'}, inplace=True)
            if 'gameNumber' in scratchertables.columns:
                scratchertables['gameNumber'] = scratchertables['gameNumber'].astype(str).str.strip()
            else: logger.error(f"'gameNumber'/'Game #' col missing in '{ws_name_st}'."); return

            if 'Winning Tickets Unclaimed' in scratchertables.columns:
                scratchertables['Winning Tickets Unclaimed'] = scratchertables['Winning Tickets Unclaimed'].replace('',np.nan)
                if scratchertables['Winning Tickets Unclaimed'].dtype=='object':
                    scratchertables['Winning Tickets Unclaimed'] = scratchertables['Winning Tickets Unclaimed'].astype(str).str.replace(',','',regex=False)
                # Ensure it's int after to_numeric, fillna with 0 before int conversion
                scratchertables['Winning Tickets Unclaimed'] = pd.to_numeric(scratchertables['Winning Tickets Unclaimed'], errors='coerce').fillna(0).astype(int)
            else:
                logger.error(f"'Winning Tickets Unclaimed' column missing in '{ws_name_st}'. Cannot proceed."); return


            def convert_prizeamount(val): 
                if isinstance(val, str):
                    cleaned_val = val.strip()
                    if cleaned_val.lower() == 'total': return "Total" 
                    if not cleaned_val: return np.nan
                    num_val = cleaned_val.replace('$','').replace(',','')
                    try: return float(num_val)
                    except ValueError: return np.nan
                return float(val) if isinstance(val, (int,float)) else np.nan

            if 'prizeamount' in scratchertables.columns:
                scratchertables['prizeamount'] = scratchertables['prizeamount'].apply(convert_prizeamount)
                logger.debug(f"Unique 'prizeamount' in '{ws_name_st}' after convert: {scratchertables['prizeamount'].unique()[:20]}")
                if "Total" not in scratchertables['prizeamount'].unique():
                     logger.warning(f"CRITICAL: 'Total' string NOT FOUND in 'prizeamount' col of FULL '{ws_name_st}' AFTER conversion.")
                else:
                     logger.info(f"CRITICAL: 'Total' string WAS FOUND in 'prizeamount' col of FULL '{ws_name_st}' after conversion.")
            else:
                logger.error(f"'prizeamount' column missing in '{ws_name_st}'. Cannot proceed."); return


            scratchertables.dropna(subset=['gameNumber'], inplace=True) # gameNumber must exist
            # Drop rows where prizeamount (numeric part) is NaN AND Winning Tickets Unclaimed is 0 (or was NaN)
            # This avoids issues if a prize tier has no prize amount and no tickets.
            condition_to_drop = (scratchertables['prizeamount'] != "Total") & \
                                scratchertables['prizeamount'].isna() & \
                                (scratchertables['Winning Tickets Unclaimed'] == 0)
            scratchertables = scratchertables[~condition_to_drop]
            logger.info(f"Scratchertables after clean: {len(scratchertables)} rows.")
        else: logger.error(f"'{ws_name_st}' empty."); return
            
        if ratingstable.empty or scratchertables.empty:
            logger.error("Core DataFrames empty. Cannot proceed."); return

        target_states = ["VA", "MD", "TX"] # Example: add more states to test
        for sim_state in target_states:
            logger.info(f"--- Evaluating data for state: {sim_state} ---")

            # Check 1: Does ratingstable have data for this state?
            if not ratingstable[ratingstable['State'] == sim_state].empty:
                logger.info(f"Rating data found for state {sim_state}.")
            else:
                logger.warning(f"No rating data found for state {sim_state} in 'AllStatesRatings'. Skipping simulation for this state.")
                continue # Skip to the next state

            # Check 2: Does scratchertables have data for this state?
            # This check is only meaningful if scratchertables has a 'State' column.
            if 'State' in scratchertables.columns:
                if not scratchertables[scratchertables['State'] == sim_state].empty:
                    logger.info(f"Scratcher data found for state {sim_state}.")
                else:
                    logger.warning(f"No scratcher data found for state {sim_state} in 'ScratcherTables'. Skipping simulation for this state.")
                    continue # Skip to the next state
            else:
                # If no 'State' column in scratchertables, we assume it's either global or pre-filtered.
                # The internal checks in clusterloop will then determine if game data exists.
                logger.info(f"Scratchertables does not have a 'State' column. Proceeding with {sim_state} assuming gameNumbers are distinct or data is relevant.")


            logger.info(f"--- Running simulation for state: {sim_state} ---")
            sim_table_df, sim_outcomes_df = clusterloop(
                gspread_client, 
                sim_state, 
                ratingstable, 
                scratchertables, 
                'profit', 2, 1.5
            )
            if sim_table_df is not None and not sim_table_df.empty:
                 logger.info(f"Clusterloop for {sim_state} completed successfully.")
            else:
                 logger.warning(f"Clusterloop for {sim_state} did not produce data or failed. Check logs.")

    except Exception as e:
        logger.exception(f"Critical error in main: {e}")
    finally:
        now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
        logger.info(f'Finishing SimulationOptimized.py at: {now}')

if __name__ == "__main__":
    main()