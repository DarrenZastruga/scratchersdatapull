#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 20:21:34 2025
Refactored for dynamic state processing.
@author: michaeljames
"""

from psycopg2.extensions import register_adapter, AsIs
import psycopg2
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from gspread_dataframe import set_with_dataframe
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from itertools import repeat
from dateutil.tz import tzlocal
from datetime import date, datetime
import urllib.request
import urllib.parse
import requests
import random
import re
import io
import json
import logging
import logging.handlers
import os
import sys
import importlib  # Added for dynamic imports

# Ensure the script's directory is in the Python path for module imports
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
    print(f"Added '{script_dir}' to sys.path")

psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

# Constants
LOG_FILE = "status.log"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
POWERS = {'B': 10 ** 9, 'K': 10 ** 3, 'M': 10 ** 6, 'T': 10 ** 12}
GSHEET_KEY = '1vAgFDVBit4C6H2HUnOd90imbtkCjOl1ekKychN2uc4o'
IMAGE_PATH = './gameimages/'

# Configuration: List of states to process
# To disable a state, comment it out. To add one, ensure the filename matches {state}lotteryscrape.py
STATES_TO_PROCESS = [
    'AR', 'AZ', 'CA', 'CO', 'DC', 'IL', 'KS', 'KY', 'MA', 'MD', 'MO', 'MN', 'MS',
    'NC', 'NM', 'NY', 'OH', 'OK', 'OR', 'SC', 'TX', 'VA', 'WA', 'WV'
]

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


def authorize_gspread():
    """Authorizes gspread client using service account credentials."""
    try:
        creds_json_string = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')

        if not creds_json_string:
            logging.error("GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable not found.")
            return None

        try:
            service_account_info = json.loads(creds_json_string)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse credentials JSON: {e}", exc_info=True)
            return None

        if service_account_info:
             credentials = Credentials.from_service_account_info(
                 service_account_info, scopes=SCOPES)
             return gspread.authorize(credentials)
        return None

    except Exception as e:
        logging.error(f"An unexpected error occurred during gspread authorization: {e}", exc_info=True)
        return None

def save_dataframe_to_gsheet(dataframe, worksheet_name, gspread_client):
    """Saves a Pandas DataFrame to a Google Sheet, overwriting existing content."""
    try:
        if dataframe is None or dataframe.empty:
             logger.warning(f"Attempted to save an empty or None DataFrame to {worksheet_name}. Skipping.")
             return

        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        try:
            worksheet = gsheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found. Creating it.")
            worksheet = gsheet.add_worksheet(title=worksheet_name, rows=1, cols=1)

        # Preprocessing
        df_to_save = dataframe.copy()
        df_to_save.replace([np.inf, -np.inf], None, inplace=True)
        df_to_save.fillna('', inplace=True)

        logger.info(f"Saving DataFrame to worksheet '{worksheet_name}'...")
        set_with_dataframe(worksheet=worksheet, dataframe=df_to_save,
                           include_index=False, include_column_header=True, resize=True)
        logger.info(f"DataFrame successfully saved to Google Sheet worksheet: {worksheet_name}")

    except Exception as e:
        logger.error(f"Failed to save DataFrame to Google Sheet worksheet {worksheet_name}: {e}", exc_info=True)

def save_dataframe_starting_at_row(dataframe, worksheet_name, start_row, gspread_client):
    """Clears content from a specified row downwards and saves a DataFrame starting at that row."""
    try:
        if dataframe is None or dataframe.empty:
            return
        
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        try:
            worksheet = gsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            logger.error(f"Worksheet '{worksheet_name}' not found.")
            return

        # Clear existing data
        try:
            max_rows = worksheet.row_count
            max_cols = worksheet.col_count
            if max_rows >= start_row:
                end_col_letter = gspread.utils.rowcol_to_a1(1, max(max_cols, len(dataframe.columns))).rstrip('1')
                clear_range = f'A{start_row}:{end_col_letter}{max_rows}'
                worksheet.batch_clear([clear_range])
        except Exception as e:
             logger.error(f"Error clearing range in {worksheet_name}: {e}", exc_info=True)

        # Save data
        df_to_save = dataframe.copy()
        df_to_save.replace([np.inf, -np.inf], None, inplace=True)
        df_to_save = df_to_save.astype(object)
        df_to_save.fillna('', inplace=True)

        logger.info(f"Saving combined data to '{worksheet_name}' starting at row {start_row}...")
        set_with_dataframe(worksheet=worksheet, dataframe=df_to_save,
                           row=start_row, col=1,
                           include_index=False,
                           include_column_header=False,
                           resize=False)
        logger.info(f"DataFrame successfully saved to '{worksheet_name}'.")

    except Exception as e:
        logger.error(f"Failed to save DataFrame to '{worksheet_name}': {e}", exc_info=True)

# --- DYNAMIC PROCESSOR ---

def process_state_module(state_code, gspread_client):
    """
    Dynamically imports the scraper module for the given state code,
    runs the scrape function, saves individual sheets, and returns dataframes.
    """
    module_name = f"{state_code.lower()}lotteryscrape"
    logger.info(f"--- Processing State: {state_code} (Module: {module_name}) ---")
    
    try:
        # Dynamically import the module (e.g., import azlotteryscrape)
        module = importlib.import_module(module_name)
        
        # Identify the correct function name
        # Standard: exportScratcherRecs
        # Edge case: OH uses exportOHScratcherRecs
        if hasattr(module, 'exportScratcherRecs'):
            scrape_func = module.exportScratcherRecs
        elif hasattr(module, f'export{state_code}ScratcherRecs'):
            scrape_func = getattr(module, f'export{state_code}ScratcherRecs')
        else:
            logger.error(f"No valid export function found in {module_name}.")
            return None, None

        # Run the scrape
        ratingstable, scratchertables = scrape_func()

        # Save Individual State Sheet
        if ratingstable is not None:
            save_dataframe_to_gsheet(ratingstable, f'{state_code}RatingsTable', gspread_client)
        
        # Prepare scratchertables for aggregation
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}.")
            return ratingstable, scratchertables
        else:
            logger.warning(f"No scratchertables data returned from {state_code} scrape.")
            return None, None

    except ImportError:
        logger.error(f"Could not import module {module_name}. Check if file exists.", exc_info=True)
        return None, None
    except Exception as e:
        logger.exception(f"Critical error processing {state_code}: {e}")
        return None, None


def main():
    """Main function to orchestrate the scratcher scraping process."""
    start_time = datetime.now(tzlocal())
    logger.info(f'Starting run at: {start_time.strftime("%Y-%m-%d %H:%M:%S %Z")}')

    gspread_client = authorize_gspread()
    if not gspread_client:
        print("Authorization failed. Exiting.")
        return

    print("Authorization successful!")
    
    # Target Columns for Combined Rating Table
    target_columns = [
        'price', 'gameName','gameNumber', 'topprize', 'topprizeremain',
        'topprizeavail','extrachances', 'secondChance','startDate',
        'Days Since Start', 'lastdatetoclaim', 'topprizeodds', 'overallodds',
        'Current Odds of Top Prize','Change in Current Odds of Top Prize',
        'Current Odds of Any Prize','Change in Current Odds of Any Prize',
        'Odds of Profit Prize','Change in Odds of Profit Prize',
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
        'Rank by Best Change in Probabilities', 'Rank Average', 'Overall Rank',
        'Rank by Cost', 'Photo','FAQ', 'About', 'Directory', 'Data Date',
        'Stats Page', 'gameURL', 'State'
    ]

    all_scratchertables_list = []
    all_ratingstables_list = []

    # --- Main Loop: Process each state ---
    for state in STATES_TO_PROCESS:
        ratingstable, scratchertables = process_state_module(state, gspread_client)
        
        # Collect Tables
        if scratchertables is not None and not scratchertables.empty:
            if 'State' not in scratchertables.columns:
                scratchertables['State'] = state
            all_scratchertables_list.append(scratchertables)

        # Collect Ratings
        if ratingstable is not None and not ratingstable.empty:
            ratingstable_processed = ratingstable.copy()
            ratingstable_processed['State'] = state
            
            # Handle duplicates
            ratingstable_processed = ratingstable_processed.loc[:, ~ratingstable_processed.columns.duplicated(keep='first')]
            
            # Ensure target columns exist
            for col in target_columns:
                if col not in ratingstable_processed.columns:
                    ratingstable_processed[col] = None
            
            # Reindex / Order columns
            final_cols = [col for col in target_columns if col in ratingstable_processed.columns]
            ratingstable_processed = ratingstable_processed[final_cols]
            
            all_ratingstables_list.append(ratingstable_processed)

    # --- Combine and Upload ScratcherTables ---
    if all_scratchertables_list:
        logger.info(f"Combining scratchertables data from {len(all_scratchertables_list)} states.")
        combined_scratchertables = pd.concat(all_scratchertables_list, ignore_index=True, join='outer')
        
        # Numeric Conversion Logic
        for col in combined_scratchertables.columns:
             is_potentially_numeric = pd.api.types.is_numeric_dtype(combined_scratchertables[col]) or combined_scratchertables[col].dtype == 'object'
             if is_potentially_numeric:
                 original_dtype = combined_scratchertables[col].dtype
                 try:
                     col_copy = combined_scratchertables[col].copy()
                     combined_scratchertables[col] = col_copy.apply(
                         lambda x: int(x) if isinstance(x, np.integer) else float(x) if isinstance(x, np.floating) else (None if pd.isna(x) else x)
                     )
                 except Exception:
                      if combined_scratchertables[col].dtype != 'object':
                         combined_scratchertables[col] = combined_scratchertables[col].astype(object)

        logger.info("Saving combined 'ScratcherTables'.")
        save_dataframe_to_gsheet(combined_scratchertables, 'ScratcherTables', gspread_client)
    else:
        logger.warning("No scratchertables data collected.")

    # --- Combine and Upload RatingsTables ---
    if all_ratingstables_list:
        logger.info(f"Combining ratingstable data from {len(all_ratingstables_list)} states.")
        combined_ratingstable = pd.concat(all_ratingstables_list, ignore_index=True, join='outer', sort=False)
        
        # Enforce column order and completeness
        for col in target_columns:
            if col not in combined_ratingstable.columns:
                combined_ratingstable[col] = None
        
        combined_ratingstable = combined_ratingstable.reindex(columns=target_columns)

        # Numeric Conversion Logic
        for col in combined_ratingstable.columns:
             is_potentially_numeric = pd.api.types.is_numeric_dtype(combined_ratingstable[col]) or combined_ratingstable[col].dtype == 'object'
             if is_potentially_numeric:
                 try:
                     col_copy = combined_ratingstable[col].copy()
                     combined_ratingstable[col] = col_copy.apply(
                         lambda x: int(x) if isinstance(x, np.integer) else float(x) if isinstance(x, np.floating) else (None if pd.isna(x) else x)
                     )
                 except Exception:
                      if combined_ratingstable[col].dtype != 'object':
                         combined_ratingstable[col] = combined_ratingstable[col].astype(object)

        logger.info("Saving combined 'AllStatesRatings'.")
        save_dataframe_starting_at_row(combined_ratingstable, 'AllStatesRatings', 3, gspread_client)
    else:
        logger.warning("No ratingstable data collected.")

    # Finish
    end_time = datetime.now(tzlocal())
    duration = end_time - start_time
    logger.info(f'Total execution time: {duration}')
    print(f'Total execution time: {duration}')

if __name__ == "__main__":
    main()