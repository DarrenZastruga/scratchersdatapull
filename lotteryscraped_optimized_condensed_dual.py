#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dual-write version: saves scratcher data to BOTH Google Sheets AND Supabase.
Drop-in replacement for lotteryscraped_optimized_condensed.py

New environment variables required:
  - SUPABASE_URL: Your Supabase project URL
  - SUPABASE_SERVICE_ROLE_KEY: Service role key for database writes

Existing environment variables still needed:
  - GOOGLE_APPLICATION_CREDENTIALS_JSON: Google service account credentials
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
import importlib

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

# Supabase configuration
SUPABASE_URL = os.environ.get('SUPABASE_URL', '')
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

# Configuration: List of states to process
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

# Column mapping from DataFrame columns to Supabase column names
COLUMN_MAPPING = {
    'price': 'price',
    'gameName': 'game_name',
    'gameNumber': 'game_number',
    'topprize': 'top_prize',
    'topprizeremain': 'top_prize_remain',
    'topprizeavail': 'top_prize_avail',
    'extrachances': 'extra_chances',
    'secondChance': 'second_chance',
    'startDate': 'start_date',
    'Days Since Start': 'days_since_start',
    'lastdatetoclaim': 'last_date_to_claim',
    'topprizeodds': 'top_prize_odds',
    'overallodds': 'overall_odds',
    'Current Odds of Top Prize': 'current_odds_top_prize',
    'Change in Current Odds of Top Prize': 'change_current_odds_top_prize',
    'Current Odds of Any Prize': 'current_odds_any_prize',
    'Change in Current Odds of Any Prize': 'change_current_odds_any_prize',
    'Odds of Profit Prize': 'odds_profit_prize',
    'Change in Odds of Profit Prize': 'change_odds_profit_prize',
    'Probability of Winning Any Prize': 'probability_any_prize',
    'Change in Probability of Any Prize': 'change_probability_any_prize',
    'Probability of Winning Profit Prize': 'probability_profit_prize',
    'Change in Probability of Profit Prize': 'change_probability_profit_prize',
    'StdDev of All Prizes': 'std_dev_all_prizes',
    'StdDev of Profit Prizes': 'std_dev_profit_prizes',
    'Odds of Any Prize + 3 StdDevs': 'odds_any_prize_3_std_devs',
    'Odds of Profit Prize + 3 StdDevs': 'odds_profit_prize_3_std_devs',
    'Max Tickets to Buy': 'max_tickets_to_buy',
    'Expected Value of Any Prize (as % of cost)': 'expected_value_any_prize',
    'Change in Expected Value of Any Prize': 'change_expected_value_any_prize',
    'Expected Value of Profit Prize (as % of cost)': 'expected_value_profit_prize',
    'Change in Expected Value of Profit Prize': 'change_expected_value_profit_prize',
    'Percent of Prizes Remaining': 'percent_prizes_remaining',
    'Percent of Profit Prizes Remaining': 'percent_profit_prizes_remaining',
    'Ratio of Decline in Prizes to Decline in Losing Ticket': 'ratio_decline_prizes_losing_ticket',
    'Rank by Best Probability of Winning Any Prize': 'rank_any_prize',
    'Rank by Best Probability of Winning Profit Prize': 'rank_profit_prize',
    'Rank by Least Expected Losses': 'rank_expected_losses',
    'Rank by Most Available Prizes': 'rank_available_prizes',
    'Rank by Best Change in Probabilities': 'rank_change_probabilities',
    'Rank Average': 'rank_average',
    'Overall Rank': 'overall_rank',
    'Rank by Cost': 'rank_by_cost',
    'Photo': 'photo_url',
    'FAQ': 'faq_url',
    'About': 'about_url',
    'Directory': 'directory_url',
    'Data Date': 'data_date',
    'Stats Page': 'stats_page_url',
    'gameURL': 'game_url',
    'State': 'state',
}


def supabase_request(endpoint, method='GET', data=None, params=None):
    """Make an authenticated request to the Supabase REST API."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.warning("Supabase credentials not configured. Skipping Supabase write.")
        return None

    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        'apikey': SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
        'Content-Type': 'application/json',
        'Prefer': 'resolution=merge-duplicates',  # UPSERT on conflict
    }

    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, params=params)
        elif method == 'POST':
            response = requests.post(url, headers=headers, json=data)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, params=params)
        else:
            return None

        if response.status_code >= 400:
            logger.error(f"Supabase API error ({response.status_code}): {response.text}")
            return None

        if response.text:
            return response.json()
        return True

    except Exception as e:
        logger.error(f"Supabase request failed: {e}", exc_info=True)
        return None


def save_ratings_to_supabase(combined_ratingstable):
    """Save the combined ratings table to Supabase scratcher_ratings table."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        logger.warning("Supabase not configured. Skipping Supabase save.")
        return

    logger.info("Saving ratings data to Supabase...")

    try:
        # Convert DataFrame to list of dicts with Supabase column names
        records = []
        for _, row in combined_ratingstable.iterrows():
            record = {}
            for df_col, sb_col in COLUMN_MAPPING.items():
                if df_col in row.index:
                    val = row[df_col]
                    # Handle NaN/None/inf
                    if pd.isna(val) or val == '' or (isinstance(val, float) and np.isinf(val)):
                        record[sb_col] = None
                    else:
                        record[sb_col] = val
                else:
                    record[sb_col] = None
            
            # Only include records with required fields
            if record.get('game_number') and record.get('state') and record.get('game_name'):
                records.append(record)

        if not records:
            logger.warning("No valid records to save to Supabase.")
            return
        
        # Before upsert, convert date objects to strings
        for record in records:
            for key, value in record.items():
                if isinstance(value, (datetime.date, datetime.datetime)):
                    record[key] = value.isoformat()
                    
        # Upsert in batches of 100
        batch_size = 100
        total_saved = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = supabase_request('scratcher_ratings', method='POST', data=batch)
            if result is not None:
                total_saved += len(batch)
                logger.info(f"Supabase: upserted batch {i}-{i + len(batch)}")
            else:
                logger.error(f"Supabase: failed to upsert batch {i}-{i + len(batch)}")

        logger.info(f"Supabase: successfully saved {total_saved}/{len(records)} records.")

    except Exception as e:
        logger.error(f"Failed to save to Supabase: {e}", exc_info=True)


# ============================================================
# Original Google Sheets functions (unchanged)
# ============================================================

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
        df_to_save = dataframe.copy()
        df_to_save.replace([np.inf, -np.inf], None, inplace=True)
        df_to_save = df_to_save.astype(object).fillna('')
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
        try:
            max_rows = worksheet.row_count
            max_cols = worksheet.col_count
            if max_rows >= start_row:
                end_col_letter = gspread.utils.rowcol_to_a1(1, max(max_cols, len(dataframe.columns))).rstrip('1')
                clear_range = f'A{start_row}:{end_col_letter}{max_rows}'
                worksheet.batch_clear([clear_range])
        except Exception as e:
            logger.error(f"Error clearing range in {worksheet_name}: {e}", exc_info=True)
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
        module = importlib.import_module(module_name)

        if hasattr(module, 'exportScratcherRecs'):
            scrape_func = module.exportScratcherRecs
        elif hasattr(module, f'export{state_code}ScratcherRecs'):
            scrape_func = getattr(module, f'export{state_code}ScratcherRecs')
        else:
            logger.error(f"No valid export function found in {module_name}.")
            return None, None

        ratingstable, scratchertables = scrape_func()

        if ratingstable is not None:
            save_dataframe_to_gsheet(ratingstable, f'{state_code}RatingsTable', gspread_client)

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

    # Check Supabase configuration
    if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
        print("Supabase configured - will dual-write to Google Sheets AND Supabase.")
    else:
        print("Supabase not configured - writing to Google Sheets only.")

    # Target Columns for Combined Rating Table
    target_columns = [
        'price', 'gameName', 'gameNumber', 'topprize', 'topprizeremain',
        'topprizeavail', 'extrachances', 'secondChance', 'startDate',
        'Days Since Start', 'lastdatetoclaim', 'topprizeodds', 'overallodds',
        'Current Odds of Top Prize', 'Change in Current Odds of Top Prize',
        'Current Odds of Any Prize', 'Change in Current Odds of Any Prize',
        'Odds of Profit Prize', 'Change in Odds of Profit Prize',
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
        'Rank by Best Change in Probabilities', 'Rank Average', 'Overall Rank',
        'Rank by Cost', 'Photo', 'FAQ', 'About', 'Directory', 'Data Date',
        'Stats Page', 'gameURL', 'State'
    ]

    all_scratchertables_list = []
    all_ratingstables_list = []

    # --- Main Loop: Process each state with per-state error recovery ---
    succeeded_states = []
    failed_states = []

    for state in STATES_TO_PROCESS:
        try:
            ratingstable, scratchertables = process_state_module(state, gspread_client)

            if scratchertables is not None and not scratchertables.empty:
                if 'State' not in scratchertables.columns:
                    scratchertables['State'] = state
                all_scratchertables_list.append(scratchertables)

            if ratingstable is not None and not ratingstable.empty:
                ratingstable_processed = ratingstable.copy()
                ratingstable_processed['State'] = state
                ratingstable_processed = ratingstable_processed.loc[:, ~ratingstable_processed.columns.duplicated(keep='first')]
                for col in target_columns:
                    if col not in ratingstable_processed.columns:
                        ratingstable_processed[col] = None
                final_cols = [col for col in target_columns if col in ratingstable_processed.columns]
                ratingstable_processed = ratingstable_processed[final_cols]
                all_ratingstables_list.append(ratingstable_processed)

            succeeded_states.append(state)
            logger.info(f"✅ {state} completed successfully.")

        except Exception as e:
            failed_states.append(state)
            logger.error(f"❌ {state} failed — skipping and continuing. Error: {e}", exc_info=True)
            continue

    # --- Summary ---
    logger.info(f"States succeeded ({len(succeeded_states)}/{len(STATES_TO_PROCESS)}): {succeeded_states}")
    if failed_states:
        logger.warning(f"States FAILED ({len(failed_states)}/{len(STATES_TO_PROCESS)}): {failed_states}")
        print(f"⚠️  {len(failed_states)} state(s) failed: {failed_states}")

    # --- Combine and Upload ScratcherTables ---
    if all_scratchertables_list:
        logger.info(f"Combining scratchertables data from {len(all_scratchertables_list)} states.")
        combined_scratchertables = pd.concat(all_scratchertables_list, ignore_index=True, join='outer')
        for col in combined_scratchertables.columns:
            is_potentially_numeric = pd.api.types.is_numeric_dtype(combined_scratchertables[col]) or combined_scratchertables[col].dtype == 'object'
            if is_potentially_numeric:
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
        for col in target_columns:
            if col not in combined_ratingstable.columns:
                combined_ratingstable[col] = None
        combined_ratingstable = combined_ratingstable.reindex(columns=target_columns)
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

        # DUAL WRITE: Save to Google Sheets
        logger.info("Saving combined 'AllStatesRatings' to Google Sheets.")
        save_dataframe_starting_at_row(combined_ratingstable, 'AllStatesRatings', 3, gspread_client)

        # DUAL WRITE: Save to Supabase
        save_ratings_to_supabase(combined_ratingstable)
    else:
        logger.warning("No ratingstable data collected.")

    # Finish
    end_time = datetime.now(tzlocal())
    duration = end_time - start_time
    logger.info(f'Total execution time: {duration}')
    print(f'Total execution time: {duration}')

    if failed_states:
        print(f"⚠️  Completed with errors. {len(succeeded_states)} succeeded, {len(failed_states)} failed: {failed_states}")
        sys.exit(1)  # Signal partial failure to CI
    else:
        print(f"✅ All {len(succeeded_states)} states completed successfully.")


if __name__ == '__main__':
    main()
