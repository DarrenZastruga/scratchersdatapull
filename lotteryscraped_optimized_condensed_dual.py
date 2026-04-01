#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dual-write version: saves scratcher data to BOTH Google Sheets AND Supabase.
Drop-in replacement for lotteryscraped_optimized_condensed.py

Memory-hardened: forces gc.collect() after each state, ensures driver.quit()
in finally blocks, and saves ScratcherTables/AllStatesRatings incrementally
so data is preserved even if the runner is OOM-killed.

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
import gc
import signal   
import traceback

class StateTimeoutError(Exception):
    """Raised when a state scrape exceeds the allowed time."""
    pass

def _timeout_handler(signum, frame):
    raise StateTimeoutError("State processing exceeded time limit")

# Per-state timeout in seconds (5 minutes max per state)
STATE_TIMEOUT_SECONDS = 300

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
    'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'IL', 'KS', 'KY', 'MA', 'MD',
    'MN', 'MO', 'MS', 'NC', 'NH', 'NM', 'NY', 'OH', 'OK', 'OR',
    'RI', 'SC', 'TX', 'VA', 'WA', 'WV'
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


# Supabase columns that are text type and should NOT have non-numeric strings stripped
SUPABASE_TEXT_COLUMNS = {
    'game_name', 'game_number', 'state', 'extra_chances', 'second_chance',
    'start_date', 'last_date_to_claim', 'about_url', 'directory_url',
    'data_date', 'stats_page_url', 'game_url', 'photo_url', 'faq_url',
    'top_prize_avail',  # Keeps "Top Prize Claimed" etc.
}


def _safe_numeric(value):
    """Try to convert a value to a number. Return None if it's a non-numeric string."""
    if value is None or value == '':
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
            return None
        return value
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) or np.isinf(value) else float(value)
    # It's a string — try to parse as number
    if isinstance(value, str):
        try:
            return float(value) if '.' in value else int(value)
        except (ValueError, TypeError):
            return None  # Non-numeric string like "Available", "TBD", "N/A"
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

            # Sanitize numeric columns: convert non-numeric strings to None
            for sb_col, val in record.items():
                if sb_col not in SUPABASE_TEXT_COLUMNS and val is not None:
                    record[sb_col] = _safe_numeric(val)

            # Only include records with required fields
            if record.get('game_number') and record.get('state') and record.get('game_name'):
                records.append(record)

        if not records:
            logger.warning("No valid records to save to Supabase.")
            return
        
        # Before upsert, convert date objects to strings and deduplicate
        records_dict = {}
        for record in records:
            for key, value in record.items():
                if isinstance(value, (date, datetime)):
                    record[key] = value.isoformat()
                # Convert numpy types to native Python types
                elif isinstance(value, np.integer):
                    record[key] = int(value)
                elif isinstance(value, np.floating):
                    record[key] = float(value) if not np.isnan(value) else None
            
            # Use (state, game_number) as deduplication key, keeping last occurrence
            composite_key = (record.get('state'), record.get('game_number'))
            records_dict[composite_key] = record
        
        # Convert back to list
        records = list(records_dict.values())
        
        # Upsert in batches of 100
        batch_size = 100
        total_saved = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            result = supabase_request('scratcher_ratings?on_conflict=state,game_number', method='POST', data=batch)
            if result is not None:
                total_saved += len(batch)
                logger.info(f"Supabase: upserted batch {i}-{i + len(batch)}")
            else:
                logger.error(f"Supabase: failed to upsert batch {i}-{i + len(batch)}")

        logger.info(f"Supabase: successfully saved {total_saved}/{len(records)} records (after deduplication).")

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


def initialize_gsheet_worksheet(worksheet_name, gspread_client, header_columns=None):
    """Clear a worksheet once at the start of a run. Optionally write header row."""
    try:
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        try:
            worksheet = gsheet.worksheet(worksheet_name)
            worksheet.clear()
        except gspread.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found. Creating it.")
            worksheet = gsheet.add_worksheet(title=worksheet_name, rows=1, cols=1)
        if header_columns:
            worksheet.update('A1', [header_columns])
        logger.info(f"Initialized worksheet '{worksheet_name}' (cleared + header written).")
        return worksheet
    except Exception as e:
        logger.error(f"Failed to initialize worksheet '{worksheet_name}': {e}", exc_info=True)
        return None


def append_dataframe_to_gsheet(dataframe, worksheet_name, gspread_client):
    """Appends a DataFrame to an existing Google Sheet worksheet without clearing it."""
    try:
        if dataframe is None or dataframe.empty:
            logger.warning(f"Attempted to append an empty or None DataFrame to {worksheet_name}. Skipping.")
            return
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        try:
            worksheet = gsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found. Creating it.")
            worksheet = gsheet.add_worksheet(title=worksheet_name, rows=1, cols=1)

        df_to_save = dataframe.copy()
        
        # Log column details for debugging
        logger.debug(f"📊 DataFrame columns for {worksheet_name}: {list(df_to_save.columns)}")
        logger.debug(f"   Total columns: {len(df_to_save.columns)}")
        
        # Check for required deduplication columns
        required_dedup_cols = ['State', 'gameNumber']  # Changed from lowercase
        missing_cols = [col for col in required_dedup_cols if col not in df_to_save.columns]
        
        if missing_cols:
            logger.error(f"❌ {worksheet_name}: Missing required columns for deduplication: {missing_cols}")
            logger.error(f"   Expected: {required_dedup_cols}")
            logger.error(f"   Actual: {list(df_to_save.columns)}")
            raise KeyError(f"Missing columns {missing_cols} in {worksheet_name} DataFrame. Expected columns: {required_dedup_cols}")
        
        df_to_save.replace([np.inf, -np.inf], None, inplace=True)
        df_to_save = df_to_save.astype(object).fillna('')
        
        # Before append, remove duplicate (State, gameNumber) rows
        df_to_save.drop_duplicates(subset=['State', 'gameNumber'], keep='last', inplace=True)  # Changed from lowercase
        
        # Convert date/datetime objects to ISO format strings
        for col in df_to_save.columns:
            df_to_save[col] = df_to_save[col].apply(
                lambda x: x.isoformat() if isinstance(x, (date, datetime)) else x
            )

        # Convert to list of lists (values only, no header)
        rows = df_to_save.values.tolist()
        if rows:
            worksheet.append_rows(rows, value_input_option='RAW')
            logger.info(f"Appended {len(rows)} rows to worksheet '{worksheet_name}'.")
    except KeyError as ke:
        logger.error(f"KeyError in append_dataframe_to_gsheet for {worksheet_name}: {ke}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to append DataFrame to Google Sheet worksheet {worksheet_name}: {e}", exc_info=True)
        raise
        
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


def sanitize_dataframe_types(df):
    """Convert numpy types to native Python types for serialization safety."""
    for col in df.columns:
        is_potentially_numeric = (
            pd.api.types.is_numeric_dtype(df[col]) or df[col].dtype == 'object'
        )
        if is_potentially_numeric:
            try:
                col_copy = df[col].copy()
                df[col] = col_copy.apply(
                    lambda x: int(x) if isinstance(x, np.integer)
                    else float(x) if isinstance(x, np.floating)
                    else (None if pd.isna(x) else x)
                )
            except Exception:
                if df[col].dtype != 'object':
                    df[col] = df[col].astype(object)
    return df


def log_memory_usage():
    """Log current process memory usage (Linux only)."""
    try:
        import resource
        mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB → MB on Linux
        logger.info(f"📊 Peak memory usage: {mem_mb:.0f} MB")
        print(f"📊 Peak memory usage: {mem_mb:.0f} MB")
    except Exception:
        pass


# --- STDOUT SUPPRESSION ---

class SuppressStdout:
    """Context manager to suppress stdout from state scraper modules.
    
    State modules often print() entire DataFrames, which can generate
    100K+ lines of output and cause GitHub Actions runners to be killed
    when the stdout buffer overflows (~64 MB).
    
    stderr is NOT suppressed so errors/warnings remain visible in logs.
    """
    def __enter__(self):
        self._original_stdout = sys.stdout
        self._devnull = open(os.devnull, 'w')
        sys.stdout = self._devnull
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout = self._original_stdout
        self._devnull.close()




def validate_state_modules():
    """Validate that all required state module files exist before processing."""
    logger.info("🔍 Validating state module files...")
    missing_modules = []
    found_modules = []
    
    for state in STATES_TO_PROCESS:
        module_name = f"{state.lower()}lotteryscrape"
        module_path = os.path.join(script_dir, f"{module_name}.py")
        
        if os.path.exists(module_path):
            found_modules.append(state)
            logger.debug(f"   ✅ {state}: {module_path}")
        else:
            missing_modules.append(state)
            logger.error(f"   ❌ {state}: {module_path} NOT FOUND")
    
    logger.info(f"📊 Module validation: {len(found_modules)}/{len(STATES_TO_PROCESS)} found")
    if missing_modules:
        logger.warning(f"⚠️  Missing modules for states: {missing_modules}")
    
    return len(missing_modules) == 0

# --- DYNAMIC PROCESSOR ---

def process_state_module(state_code, gspread_client):
    """
    Dynamically imports the scraper module for the given state code,
    runs the scrape function, saves individual sheets, and returns dataframes.
    """
    module_name = f"{state_code.lower()}lotteryscrape"
    logger.info(f"--- Processing State: {state_code} (Module: {module_name}) ---")

    try:
        # **NEW: Log the import attempt**
        logger.info(f"📦 Attempting to import module: {module_name}")
        logger.debug(f"   Python path includes: {sys.path[:3]}")  # Show first 3 paths
        
        try:
            module = importlib.import_module(module_name)
            logger.info(f"✅ Successfully imported {module_name}")
        except ModuleNotFoundError as mnf:
            logger.error(f"❌ ModuleNotFoundError: {module_name} not found in Python path")
            logger.error(f"   Available files in current directory: {os.listdir('.')[:20]}")  # Show available files
            raise
        except ImportError as ie:
            logger.error(f"❌ ImportError while importing {module_name}: {ie}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected error importing {module_name}: {type(e).__name__}: {e}", exc_info=True)
            raise

        # **NEW: Check function existence with detailed logging**
        if hasattr(module, 'exportScratcherRecs'):
            scrape_func = module.exportScratcherRecs
            logger.info(f"✅ Found exportScratcherRecs() in {module_name}")
        elif hasattr(module, f'export{state_code}ScratcherRecs'):
            scrape_func = getattr(module, f'export{state_code}ScratcherRecs')
            logger.info(f"✅ Found export{state_code}ScratcherRecs() in {module_name}")
        else:
            # **NEW: Enhanced error with available functions**
            available_funcs = [name for name in dir(module) if name.startswith('export')]
            logger.error(f"❌ No valid export function found in {module_name}")
            logger.error(f"   Expected: exportScratcherRecs or export{state_code}ScratcherRecs")
            logger.error(f"   Available export* functions: {available_funcs if available_funcs else 'NONE'}")
            logger.error(f"   All public functions: {[name for name in dir(module) if not name.startswith('_')][:10]}")
            return None, None

        # Suppress stdout during scrape to prevent massive DataFrame prints
        # Set per-state timeout to prevent hangs (e.g. Selenium stuck on a page)
        old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
        signal.alarm(STATE_TIMEOUT_SECONDS)
        try:
            with SuppressStdout():
                result = scrape_func()
            # **CRITICAL: Log the raw result IMMEDIATELY after the function returns**
            logger.info(f"🔍 {state_code}: scrape_func() returned raw result:")
            logger.info(f"   Type: {type(result).__name__}")
            logger.info(f"   Is None: {result is None}")
            if result is not None:
                if isinstance(result, (tuple, list)):
                    logger.info(f"   Length: {len(result)}")
                    for i, item in enumerate(result):
                        logger.info(f"   [{i}]: {type(item).__name__} (empty: {(isinstance(item, pd.DataFrame) and item.empty) if isinstance(item, pd.DataFrame) else 'N/A'})")
                else:
                    logger.info(f"   Unexpected type (not tuple/list): {result}")
        finally:
            signal.alarm(0)  # Cancel the alarm
            signal.signal(signal.SIGALRM, old_handler)  # Restore handler

        # --- Detailed diagnostics on scrape result ---
        if result is None:
            logger.error(f"⚠️  {state_code}: scrape_func() returned None (not a tuple).")
            return None, None

        if not isinstance(result, (tuple, list)):
            logger.error(f"⚠️  {state_code}: scrape_func() returned unexpected type: {type(result).__name__}.")
            return None, None

        if len(result) < 2:
            logger.error(f"⚠️  {state_code}: scrape_func() returned tuple with {len(result)} element(s), expected 2.")
            return None, None

        ratingstable, scratchertables = result[0], result[1]

        # Log ratingstable status
        if ratingstable is None:
            logger.warning(f"📋 {state_code}: ratingstable is None.")
        elif isinstance(ratingstable, pd.DataFrame):
            if ratingstable.empty:
                logger.warning(f"📋 {state_code}: ratingstable is empty DataFrame (0 rows).")
            else:
                logger.info(f"📋 {state_code}: ratingstable has {len(ratingstable)} rows, {len(ratingstable.columns)} cols.")
        else:
            logger.warning(f"📋 {state_code}: ratingstable is unexpected type: {type(ratingstable).__name__}.")

        # Log scratchertables status
        if scratchertables is None:
            logger.warning(f"🎰 {state_code}: scratchertables is None.")
        elif isinstance(scratchertables, pd.DataFrame):
            if scratchertables.empty:
                logger.warning(f"🎰 {state_code}: scratchertables is empty DataFrame (0 rows, cols={list(scratchertables.columns)[:5]}).")
            else:
                logger.info(f"🎰 {state_code}: scratchertables has {len(scratchertables)} rows, {len(scratchertables.columns)} cols.")
                scratchertables['State'] = state_code
        else:
            logger.warning(f"🎰 {state_code}: scratchertables is unexpected type: {type(scratchertables).__name__}.")
            scratchertables = None

        # Normalize empty DataFrames to None for consistent downstream handling
        if isinstance(ratingstable, pd.DataFrame) and ratingstable.empty:
            ratingstable = None
        if isinstance(scratchertables, pd.DataFrame) and scratchertables.empty:
            scratchertables = None

        return ratingstable, scratchertables

    except ImportError as ie:
        logger.error(f"❌ Could not import module {module_name}: {ie}", exc_info=True)
        logger.error(f"   File path checked: {os.path.join(script_dir, f'{module_name}.py')}")
        logger.error(f"   File exists: {os.path.exists(os.path.join(script_dir, f'{module_name}.py'))}")
        return None, None
    except StateTimeoutError:
        logger.error(f"⏰ {state_code} timed out after {STATE_TIMEOUT_SECONDS}s — skipping.")
        return None, None
    except Exception as e:
        logger.exception(f"❌ Critical error processing {state_code}: {type(e).__name__}: {e}")
        return None, None


def cast_rank_columns_to_int(df):
    """Cast rank columns to nullable integers for Supabase compatibility.
    
    Supabase integer columns reject float values like '55.0'.
    This converts rank columns from float to Int64 (nullable integer),
    and ensures NaN becomes None (JSON null).
    """
    int_columns = [
        'Rank by Best Probability of Winning Any Prize',
        'Rank by Best Probability of Winning Profit Prize',
        'Rank by Least Expected Losses',
        'Rank by Most Available Prizes',
        'Rank by Best Change in Probabilities',
        'Overall Rank',
        'Rank by Cost',
    ]
    for col in int_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # Convert to nullable Int64 so NaN stays as <NA> not float NaN
            try:
                df[col] = df[col].round(0).astype('Int64')
            except (ValueError, TypeError):
                # If conversion fails, leave as-is (will be handled by sanitize)
                pass
    return df


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

    # **NEW: Add module validation**
    if not validate_state_modules():
        logger.warning("⚠️  Some state modules are missing. Proceeding anyway but some states will fail.")
        
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

    # ─── INITIALIZE GOOGLE SHEETS ONCE ───
    # Clear worksheets at the start so incremental appends don't mix with stale data.
    # If the runner crashes mid-run, all states appended so far are preserved.
    logger.info("Initializing Google Sheets worksheets (clearing old data)...")
    initialize_gsheet_worksheet('ScratcherTables', gspread_client)
    initialize_gsheet_worksheet('AllStatesRatings', gspread_client, header_columns=target_columns)
    logger.info("Worksheets initialized.")

    all_ratingstables_list = []

    # --- Main Loop: Process each state with per-state error recovery ---
    succeeded_states = []
    failed_states = []
    failed_states_detail = {}  # **NEW: Track failure reasons**

    for idx, state in enumerate(STATES_TO_PROCESS):
        state_start = datetime.now(tzlocal())
        logger.info(f"[{idx+1}/{len(STATES_TO_PROCESS)}] Starting {state}...")
        print(f"[{idx+1}/{len(STATES_TO_PROCESS)}] Processing {state}...")

        try:
            ratingstable, scratchertables = process_state_module(state, gspread_client)

            # ─── INCREMENTAL APPEND: ScratcherTables ───
            if scratchertables is not None and not scratchertables.empty:
                if 'State' not in scratchertables.columns:
                    scratchertables['State'] = state
                scratchertables_sanitized = sanitize_dataframe_types(scratchertables.copy())
                try:
                    append_dataframe_to_gsheet(scratchertables_sanitized, 'ScratcherTables', gspread_client)
                    logger.info(f"🎰 Appended {state} scratchertables ({len(scratchertables)} rows).")
                except Exception as e:
                    logger.error(f"⚠️  Failed to append {state} ScratcherTables: {e}", exc_info=True)
            else:
                logger.warning(f"🎰 {state}: NO scratchertables to append.")

            # ─── ACCUMULATE ratings for combined Supabase upsert ───
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

                # Append this state's ratings to Google Sheets immediately
                try:
                    rt_sanitized = sanitize_dataframe_types(ratingstable_processed.copy())
                    append_dataframe_to_gsheet(rt_sanitized, 'AllStatesRatings', gspread_client)
                    logger.info(f"📋 Appended {state} ratings ({len(ratingstable_processed)} rows) to AllStatesRatings.")
                except Exception as e:
                    logger.error(f"⚠️  Failed to append {state} ratings to GSheets: {e}", exc_info=True)

            # Only count as succeeded if at least one dataframe has data
            if ratingstable is not None or (scratchertables is not None and not scratchertables.empty):
                succeeded_states.append(state)
                state_duration = datetime.now(tzlocal()) - state_start
                logger.info(f"✅ {state} completed successfully in {state_duration}.")
            else:
                failed_states.append(state)
                state_duration = datetime.now(tzlocal()) - state_start
                logger.warning(f"⚠️ {state} returned no data in {state_duration}.")

            # ─── INCREMENTAL SUPABASE SAVE ───
            # Upsert ALL accumulated ratings to Supabase after each state.
            # This is idempotent (upsert), so re-sending previous states is safe.
            try:
                if all_ratingstables_list:
                    combined_rt = pd.concat(all_ratingstables_list, ignore_index=True, join='outer', sort=False)
                    for col in target_columns:
                        if col not in combined_rt.columns:
                            combined_rt[col] = None
                    combined_rt = combined_rt.reindex(columns=target_columns)
                    # Cast rank columns to integers BEFORE sanitization/Supabase save
                    combined_rt = cast_rank_columns_to_int(combined_rt)
                    combined_rt = sanitize_dataframe_types(combined_rt)
                    save_ratings_to_supabase(combined_rt)
                    logger.info(f"📝 Incrementally saved to Supabase ({len(combined_rt)} rows, {len(succeeded_states)} states so far).")
            except Exception as e:
                logger.error(f"⚠️  Incremental Supabase save after {state} failed (non-fatal): {e}", exc_info=True)

        except Exception as e:
            # **NEW: Capture detailed failure info**
            error_type = type(e).__name__
            error_msg = str(e)
            failed_states.append(state)
            failed_states_detail[state] = {
                'error_type': error_type,
                'error_msg': error_msg,
                'traceback': traceback.format_exc()
            }
            logger.error(f"❌ {state} failed with {error_type}: {error_msg}", exc_info=True)
            
        finally:
            # ─── MEMORY CLEANUP after every state (success or failure) ───
            collected = gc.collect()
            logger.info(f"🧹 gc.collect() freed {collected} objects after {state}.")
            log_memory_usage()

       # --- Final Summary ---
    logger.info(f"States succeeded ({len(succeeded_states)}/{len(STATES_TO_PROCESS)}): {succeeded_states}")
    
    if failed_states:
        logger.warning(f"States FAILED ({len(failed_states)}/{len(STATES_TO_PROCESS)}): {failed_states}")
        
        # **NEW: Detailed failure analysis**
        logger.warning("=" * 60)
        logger.warning("FAILURE ANALYSIS:")
        logger.warning("=" * 60)
        for state, details in failed_states_detail.items():
            logger.warning(f"\n{state}:")
            logger.warning(f"  Error Type: {details['error_type']}")
            logger.warning(f"  Error Msg: {details['error_msg']}")
        logger.warning("=" * 60)
        
        print(f"⚠️  {len(failed_states)} state(s) failed: {failed_states}")

    # Finish
    end_time = datetime.now(tzlocal())
    duration = end_time - start_time
    logger.info(f'Total execution time: {duration}')
    print(f'Total execution time: {duration}')
    log_memory_usage()

    if failed_states:
        print(f"⚠️  Completed with errors. {len(succeeded_states)} succeeded, {len(failed_states)} failed: {failed_states}")
        sys.exit(1)
    else:
        print(f"✅ All {len(succeeded_states)} states completed successfully.")


if __name__ == '__main__':
    main()
