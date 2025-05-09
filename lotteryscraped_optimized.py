#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 29 20:21:34 2025

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

# Ensure the script's directory is in the Python path for module imports
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)
    print(f"Added '{script_dir}' to sys.path") # Optional: for confirmation in logs
    
#from dotenv import load_dotenv
#load_dotenv()
# Verify path is loaded
#print(
 #   f"Attempting to use credentials file: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")


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


# use this for running the script locally
# Load .env - make sure this is at the top
#script_dir = os.path.dirname(__file__)
#dotenv_path = os.path.join(script_dir, '.env')
#load_dotenv(dotenv_path=dotenv_path, override=True, verbose=True)


def authorize_gspread_from_path():
    """Authorizes using the file path specified in GOOGLE_APPLICATION_CREDENTIALS."""
    creds_path = os.environ.get(
        'GOOGLE_APPLICATION_CREDENTIALS_JSON')  # Use the PATH variable
    print(f"Trying to use credentials path: {creds_path}")  # Debug

    if not creds_path:
        logging.error(
            "GOOGLE_APPLICATION_CREDENTIALS environment variable (path) not found.")
        return None

    # Optional: Construct full path if creds_path is just the filename
    # if not os.path.isabs(creds_path):
    #    script_dir = os.path.dirname(__file__) # Get script directory
    #    creds_path = os.path.join(script_dir, creds_path) # Join with filename
    #    print(f"Constructed full path: {creds_path}") # Debug

    if not os.path.exists(creds_path):
        logging.error(f"Credentials file not found at path: {creds_path}")
        # Show where Python is looking
        print(f"Current working directory is: {os.getcwd()}")
        return None

    try:
        scopes = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_file(
            creds_path, scopes=scopes
        )
        client = gspread.authorize(credentials)
        logging.info(
            f"Successfully authorized gspread using file: {creds_path}")
        # Debug
        print(f"Successfully authorized gspread using file: {creds_path}")
        return client
    except Exception as e:
        logging.error(
            f"Error during gspread authorization from file: {e}", exc_info=True)
        print(f"Error authorizing from file: {e}")  # Debug
        return None

# save this for when run on GitHub


def authorize_gspread():
    """Authorizes gspread client using service account credentials."""
    service_account_info = None # Initialize to None
    try:
        # --- Get Credentials String ---
        creds_json_string = os.environ.get(
            'GOOGLE_APPLICATION_CREDENTIALS_JSON')

        # --- Verification and Parsing ---
        print("-" * 20)
        print(f"Type of retrieved value: {type(creds_json_string)}")

        if creds_json_string is None:
            print(
                "ERROR: GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is NOT SET or accessible.")
            logging.error(
                "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable not found.")
            return None  # Exit if not set

        elif isinstance(creds_json_string, str):
            # Print details
            print(
                f"Retrieved value (first 100 chars): {creds_json_string[:100]}...")
            print(f"Length of retrieved string: {len(creds_json_string)}")

            # --- Parse the JSON string (moved here) ---
            try:
                service_account_info = json.loads(creds_json_string) # Assign here
                logging.info("Successfully parsed credentials JSON string.")
                # Basic structure check (optional but good)
                if not isinstance(service_account_info, dict) or 'client_email' not in service_account_info:
                     print("WARNING: Parsed JSON does not look like a valid service account credential dictionary.")
                     logging.warning("Parsed JSON does not look like a valid service account credential dictionary.")
                     # You might want to return None here too if structure is invalid
                     # return None

            except json.JSONDecodeError as e:
                print(f"ERROR: Failed to parse credentials JSON: {e}. Check the environment variable content.")
                logging.error(
                    f"Failed to parse credentials JSON: {e}. Check if the content copied into the environment variable is the complete and valid JSON.", exc_info=True)
                return None # Exit if parsing fails
            # --- End of JSON Parsing ---

        else: # Handle cases where it's retrieved but not None or str
            print(
                f"WARNING: Retrieved GOOGLE_APPLICATION_CREDENTIALS_JSON is not a string or None. Value: {creds_json_string}")
            logging.warning(f"Retrieved GOOGLE_APPLICATION_CREDENTIALS_JSON is not a string or None. Value: {creds_json_string}")
            return None # Exit for unexpected type

        print("-" * 20)

        # --- Create Credentials (only if parsing succeeded) ---
        if service_account_info: # Check if parsing was successful and assigned the dict
             credentials = Credentials.from_service_account_info(
                 service_account_info, scopes=SCOPES)
             return gspread.authorize(credentials)
        else:
             # This case should theoretically be caught by earlier returns, but added for safety
             logging.error("Could not create credentials because service_account_info was not properly set.")
             return None

    except Exception as e: # Catch any other unexpected errors during the process
        logging.error(
            f"An unexpected error occurred during gspread authorization: {e}", exc_info=True)
        print(f"An unexpected critical error occurred during authorization: {e}")
        return None


def authorize_pydrive():
    """Authorizes PyDrive client."""
    gauth = GoogleAuth()
    return GoogleDrive(gauth)

def formatstr(s):
    """Formats a string, handling possible power suffixes (K, M, B, T)."""
    try:
        power = s[-1]
        if power.isdigit():
            return s
        else:
            return float(s[:-1]) * POWERS[power]
    except TypeError:
        return s


def download_image(url, file_path, file_name):
    """Downloads an image from the given URL and saves it to the specified path."""
    full_path = os.path.join(file_path, file_name) # Use os.path.join for robustness
    os.makedirs(file_path, exist_ok=True) # Ensure directory exists
    try:
        urllib.request.urlretrieve(url, full_path)
        logger.info(f"Downloaded image from {url} to {full_path}")
    except Exception as e:
        logger.error(f"Failed to download image from {url}: {e}")


def save_dataframe_to_gsheet(dataframe, worksheet_name, gspread_client):
    """Saves a Pandas DataFrame to a Google Sheet, overwriting existing content."""
    try:
        if dataframe is None or dataframe.empty:
             logger.warning(f"Attempted to save an empty or None DataFrame to {worksheet_name}. Skipping.")
             return

        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        try:
            worksheet = gsheet.worksheet(worksheet_name)
            logger.info(f"Clearing worksheet '{worksheet_name}' before saving new data.")
            worksheet.clear()
        except gspread.WorksheetNotFound:
            logger.info(f"Worksheet '{worksheet_name}' not found. Creating it.")
            # Decide on initial size or let it resize automatically
            worksheet = gsheet.add_worksheet(title=worksheet_name, rows=1, cols=1)

        # --- PREPROCESSING BEFORE SAVING ---
        # Important: Make a copy if you don't want to modify the original df
        df_to_save = dataframe.copy()
        # Replace infinite values with None or an empty string
        df_to_save.replace([np.inf, -np.inf], None, inplace=True)
        # Fill NaN values with empty string for Sheets compatibility
        df_to_save.fillna('', inplace=True)
        # --- END PREPROCESSING ---

        logger.info(f"Saving DataFrame to worksheet '{worksheet_name}'...")
        set_with_dataframe(worksheet=worksheet, dataframe=df_to_save,
                           include_index=False, include_column_header=True, resize=True)
        logger.info(
            f"DataFrame successfully saved to Google Sheet worksheet: {worksheet_name}")

    except gspread.exceptions.APIError as e:
         logger.error(f"gspread API Error saving to {worksheet_name}: {e}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Failed to save DataFrame to Google Sheet worksheet {worksheet_name}: {e}", exc_info=True)


# --- REMOVED append_all_data_to_scratcher_tables function ---
# This function is no longer needed as we will collect data and upload it all at once.


def generic_scratcher_scrape(url, state_name, payload=None, headers=None, data=None):
    """
        Generic function to scrape scratcher data from a given URL,
        intended to be called from within functions specific to each state.
    """
    try:
        logger.info(f"Attempting to scrape {state_name} data from {url}")
        if payload is None and headers is None and data is None:
            r = requests.get(url, timeout=30) # Added timeout
        else:
            r = requests.request("POST", url, headers=headers, data=payload, timeout=30) # Added timeout

        r.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        logger.info(
            f"Successfully scraped {state_name} Scratcher data from {url}")
        return soup

    except requests.exceptions.RequestException as e:
         logger.error(f"Request failed for {state_name} at {url}: {e}", exc_info=True)
         return None
    except Exception as e:
        logger.exception(
            f"Failed to scrape {state_name} Scratcher data from {url}: {e}")
        return None



# --- NEW FUNCTION ---
def save_dataframe_starting_at_row(dataframe, worksheet_name, start_row, gspread_client):
    """
    Clears content from a specified row downwards and saves a DataFrame starting at that row.
    Assumes headers ALREADY EXIST in the sheet rows ABOVE start_row.
    """
    try:
        if dataframe is None or dataframe.empty:
            logger.warning(f"Attempted to save an empty or None DataFrame to {worksheet_name} at row {start_row}. Skipping.")
            return
        if start_row < 1:
            logger.error(f"Invalid start_row ({start_row}). Must be 1 or greater.")
            return

        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        try:
            worksheet = gsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            logger.error(f"Worksheet '{worksheet_name}' not found. Cannot save data starting at row {start_row}.")
            return

        # --- Clear existing data from start_row downwards ---
        logger.info(f"Clearing data in worksheet '{worksheet_name}' from row {start_row} onwards.")
        try:
            max_rows = worksheet.row_count
            max_cols = worksheet.col_count # Use current actual max columns

            if max_rows >= start_row:
                # Use A1 notation for the clear range
                # Calculate end column letter based on DataFrame width if worksheet is narrower
                # Or just use a reasonably wide fixed range like 'ZZ' if simpler
                end_col_letter = gspread.utils.convert_to_a1_notation(1, max(max_cols, len(dataframe.columns))).rstrip('1')
                clear_range = f'A{start_row}:{end_col_letter}{max_rows}'
                logger.debug(f"Calculated clear range: {clear_range}")
                worksheet.batch_clear([clear_range])
                logger.info(f"Cleared range {clear_range} in '{worksheet_name}'.")
            else:
                logger.info(f"Worksheet '{worksheet_name}' has fewer than {start_row} rows. No clearing needed below start row.")

        except gspread.exceptions.APIError as api_err:
             logger.error(f"gspread API Error during clearing range below row {start_row} in {worksheet_name}: {api_err}", exc_info=True)
        except Exception as clear_err:
             logger.error(f"Unexpected error during clearing range below row {start_row} in {worksheet_name}: {clear_err}", exc_info=True)


        # --- PREPROCESSING BEFORE SAVING ---
        df_to_save = dataframe.copy()
        df_to_save.replace([np.inf, -np.inf], None, inplace=True)
        # Ensure fillna('') happens *after* potential conversions to object
        df_to_save = df_to_save.astype(object) # Ensure object type before fillna
        df_to_save.fillna('', inplace=True)

        logger.info(f"Saving DataFrame to worksheet '{worksheet_name}' starting at row {start_row} (headers excluded)...")
        # Upload data starting at the specified row, EXCLUDING the header
        set_with_dataframe(worksheet=worksheet, dataframe=df_to_save,
                           row=start_row, col=1, # Start writing data at cell A{start_row}
                           include_index=False,
                           include_column_header=False, # Set header to False
                           resize=False) # Do NOT resize the sheet
        logger.info(
            f"DataFrame successfully saved to '{worksheet_name}' starting at row {start_row}.")

    except gspread.exceptions.APIError as e:
         logger.error(f"gspread API Error saving to {worksheet_name} at row {start_row}: {e}", exc_info=True)
    except Exception as e:
        logger.error(
            f"Failed to save DataFrame to '{worksheet_name}' at row {start_row}: {e}", exc_info=True)
# --- MODIFIED run_XX_scratcher_recs functions ---
# Each function will now return the scratchertables DataFrame or None if an error occurs



def run_az_scratcher_recs(gspread_client):
    """Scrapes AZ data, saves ratingstable, returns scratchertables."""
    state_code = 'AZ'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from azlotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return ratingstable, scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None


def run_ca_scratcher_recs(gspread_client):
    """Scrapes CA data, saves ratingstable, returns scratchertables."""
    state_code = 'CA'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from calotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None





def run_dc_scratcher_recs(gspread_client):
    """Scrapes DC data, saves ratingstable, returns scratchertables."""
    state_code = 'DC'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from dclotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None

def run_ks_scratcher_recs(gspread_client):
    """Scrapes KS data, saves ratingstable, returns scratchertables."""
    state_code = 'KS'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from kslotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None 
    
def run_ky_scratcher_recs(gspread_client):
    """Scrapes KY data, saves ratingstable, returns scratchertables."""
    state_code = 'KY'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from kylotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None
    

def run_mo_scratcher_recs(gspread_client):
    """Scrapes MO data, saves ratingstable, returns scratchertables."""
    state_code = 'MO'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from molotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None
    

    
def run_nc_scratcher_recs(gspread_client):
    """Scrapes NC data, saves ratingstable, returns scratchertables."""
    state_code = 'NC'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from nclotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None

def run_nm_scratcher_recs(gspread_client):
    """Scrapes NM data, saves ratingstable, returns scratchertables."""
    state_code = 'NM'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from nmlotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None
    
def run_ny_scratcher_recs(gspread_client):
    """Scrapes NY data, saves ratingstable, returns scratchertables."""
    state_code = 'NY'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from nylotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None

def run_ok_scratcher_recs(gspread_client):
    """Scrapes OK data, saves ratingstable, returns scratchertables."""
    state_code = 'OK'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from oklotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None
    
def run_or_scratcher_recs(gspread_client):
    """Scrapes OR data, saves ratingstable, returns scratchertables."""
    state_code = 'OR'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from orlotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None
    
def run_tx_scratcher_recs(gspread_client):
    """Scrapes TX data, saves ratingstable, returns scratchertables."""
    state_code = 'TX'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from txlotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None

def run_va_scratcher_recs(gspread_client):
    """Scrapes VA data, saves ratingstable, returns scratchertables."""
    state_code = 'VA'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from valotteryscrape import exportScratcherRecs
         # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None
    
def run_wa_scratcher_recs(gspread_client):
    """Scrapes WA data, saves ratingstable, returns scratchertables."""
    state_code = 'WA'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        # --- Keep the original import style ---
        from walotteryscrape import exportScratcherRecs
        # --- Run the function ---
        ratingstable, scratchertables = exportScratcherRecs()

        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            print(scratchertables)
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        # Log the error WITH traceback using exc_info=True
        logger.error(f"Could not import {state_code}lotteryscrape. Skipping {state_code}.", exc_info=True)
        return None, None
    except Exception as e:
        # Catch other potential errors during execution
        logger.exception(f"Error occurred during {state_code} processing (after import): {e}")
        return None, None


# --- Add similar wrappers for il, fl, oh, ms if you enable them ---
# Example:
# def run_il_scratcher_recs(gspread_client):
#     """Scrapes IL data, saves ratingstable, returns scratchertables."""
#     state_code = 'IL'
#     logger.info(f"--- Processing State: {state_code} ---")
#     try:
#         from illotteryscrape import exportScratcherRecs
#         ratingstable, scratchertables = exportScratcherRecs()
#         save_dataframe_to_gsheet(
#             ratingstable, f'{state_code}RatingsTable', gspread_client)
#         if scratchertables is not None and not scratchertables.empty:
#             scratchertables['State'] = state_code
#             logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
#             return scratchertables
#         else:
#              logger.warning(f"No scratchertables data returned from {state_code} scrape.")
#              return None
#     except ImportError:
#         logger.error(f"Could not import illotteryscrape. Skipping {state_code}.")
#         return None
#     except Exception as e:
#         logger.exception(f"Error occurred during {state_code} processing: {e}")
#         return None   ###


def main():
    """Main function to orchestrate the scratcher scraping process."""
    start_time = datetime.now(tzlocal())
    logger.info(f'Starting lotteryscrape_optimized.py run at: {start_time.strftime("%Y-%m-%d %H:%M:%S %Z")}')

    gspread_client = None # Initialize client to None
    try:
        #gspread_client = authorize_gspread_from_path() #from local
        gspread_client = authorize_gspread() #from github
        if not gspread_client:
            logger.error("Gspread authorization failed. Exiting.")
            print("Authorization failed. Cannot proceed.")
            return # Exit if authorization fails

        print("Authorization successful!")
        
        # --- Define Target Columns for Combined Rating Table ---
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
            'Stats Page', # Assuming this column might exist in some scrapers
            'State' # Add State column here
        ]


        # --- Data Collection ---
        all_scratchertables_list = [] # Initialize list to hold DataFrames from each state
        all_ratingstables_list = []   # NEW: For combined ratings tables

        # List of functions to run
        # Add or remove state functions here as needed
        state_scrape_functions = [
            run_az_scratcher_recs,
            run_ca_scratcher_recs,
            run_dc_scratcher_recs,
            run_ks_scratcher_recs,
            run_ky_scratcher_recs,
            run_mo_scratcher_recs,
            run_nc_scratcher_recs,
            run_nm_scratcher_recs,
            run_ny_scratcher_recs,
            run_ok_scratcher_recs,
            run_or_scratcher_recs,
            run_tx_scratcher_recs,
            run_va_scratcher_recs,
            run_wa_scratcher_recs
            
            # Add other state functions here if enabled:
            # run_ms_scratcher_recs,
            # run_il_scratcher_recs,
            # run_fl_scratcher_recs,
            # run_oh_scratcher_recs,
        ]

        for scrape_func in state_scrape_functions:
             if callable(scrape_func):
                 # Get both tables from the state function
                 ratingstable, scratchertables = scrape_func(gspread_client)
                 state_code = scrape_func.__name__.split('_')[1].upper() # Infer state code

                 # Process scratchertables for combined sheet (as before)
                 if scratchertables is not None and not scratchertables.empty:
                     if 'State' not in scratchertables.columns: # Add state if not already added
                          scratchertables['State'] = state_code
                     all_scratchertables_list.append(scratchertables)
                 else:
                     logger.warning(f"Function {scrape_func.__name__} did not return valid scratchertables data.")

                 # --- NEW: Process ratingstable for combined sheet ---
                 if ratingstable is not None and not ratingstable.empty:
                     logger.info(f"Processing ratingstable for {state_code}...")
                     ratingstable_processed = ratingstable.copy()
                     ratingstable_processed['State'] = state_code # Add State column

                     # Ensure all target columns exist, add if missing
                     for col in target_columns:
                         if col not in ratingstable_processed.columns:
                             logger.warning(f"Column '{col}' missing in {state_code} ratingstable. Adding with None.")
                             ratingstable_processed[col] = None # Or np.nan

                     # Reindex to ensure correct column order and drop extras
                     try:
                        # Filter target_columns to only those present in the df + State + added ones
                        final_cols_for_state = [col for col in target_columns if col in ratingstable_processed.columns]
                        ratingstable_processed = ratingstable_processed[final_cols_for_state]
                        all_ratingstables_list.append(ratingstable_processed)
                        logger.info(f"Added processed {state_code} ratingstable to combined list.")
                     except KeyError as ke:
                         logger.error(f"KeyError re-indexing {state_code} ratingstable. Columns: {ratingstable_processed.columns}. Target: {target_columns}. Error: {ke}", exc_info=True)
                     except Exception as e:
                         logger.error(f"Error processing/re-indexing {state_code} ratingstable: {e}", exc_info=True)

                 else:
                     logger.warning(f"Function {scrape_func.__name__} did not return valid ratingstable data.")
                 # --- END NEW ---

             else:
                 logger.error(f"Item in state_scrape_functions is not callable: {scrape_func} (type: {type(scrape_func)})")

        # --- Combine and Upload ScratcherTables (as before) ---
        if all_scratchertables_list:
            logger.info(f"Combining scratchertables data from {len(all_scratchertables_list)} states.")
            combined_scratchertables = pd.concat(all_scratchertables_list, ignore_index=True, join='outer')
            logger.info(f"Combined scratchertables shape: {combined_scratchertables.shape}")

            # Aggressive final conversion (keep as before)
            logger.info("Performing final type conversion for JSON compatibility on combined scratchertables...")
            # ... (keep the aggressive conversion loop for combined_scratchertables) ...
            for col in combined_scratchertables.columns:
                 is_potentially_numeric = pd.api.types.is_numeric_dtype(combined_scratchertables[col]) or combined_scratchertables[col].dtype == 'object'
                 if is_potentially_numeric:
                     original_dtype = combined_scratchertables[col].dtype
                     try:
                         col_copy = combined_scratchertables[col].copy()
                         combined_scratchertables[col] = col_copy.apply(
                             lambda x: int(x) if isinstance(x, np.integer) else float(x) if isinstance(x, np.floating) else (None if pd.isna(x) else x)
                         )
                         if original_dtype != combined_scratchertables[col].dtype or original_dtype == 'object':
                              logger.info(f"  - Converted/Checked scratchertables column '{col}' (Original dtype: {original_dtype}, New dtype: {combined_scratchertables[col].dtype})")
                     except Exception as e:
                          logger.warning(f"  - Could not apply numeric conversion to scratchertables column '{col}' (dtype: {original_dtype}): {e}. Ensuring it's object type.")
                          if combined_scratchertables[col].dtype != 'object':
                             combined_scratchertables[col] = combined_scratchertables[col].astype(object)


            logger.info("Attempting to save combined data to 'ScratcherTables' sheet.")
            save_dataframe_to_gsheet(combined_scratchertables, 'ScratcherTables', gspread_client)
        else:
            logger.warning("No scratchertables data collected. 'ScratcherTables' sheet will not be updated/cleared.")

        # --- NEW: Combine and Upload RatingsTables ---
        if all_ratingstables_list:
            logger.info(f"Combining ratingstable data from {len(all_ratingstables_list)} states.")
            # Use outer join to handle potentially missing columns gracefully during concat
            combined_ratingstable = pd.concat(all_ratingstables_list, ignore_index=True, join='outer', sort=False)
            # Reindex one last time with the final target columns to ensure order and presence
            combined_ratingstable = combined_ratingstable.reindex(columns=target_columns)
            logger.info(f"Combined ratingstable shape: {combined_ratingstable.shape}")

            # Aggressive final conversion for the combined ratings table
            logger.info("Performing final type conversion for JSON compatibility on combined ratingstable...")
            # ... (Apply the same aggressive conversion loop as for scratchertables, but on combined_ratingstable) ...
            for col in combined_ratingstable.columns:
                 is_potentially_numeric = pd.api.types.is_numeric_dtype(combined_ratingstable[col]) or combined_ratingstable[col].dtype == 'object'
                 if is_potentially_numeric:
                     original_dtype = combined_ratingstable[col].dtype
                     try:
                         col_copy = combined_ratingstable[col].copy()
                         combined_ratingstable[col] = col_copy.apply(
                             lambda x: int(x) if isinstance(x, np.integer) else float(x) if isinstance(x, np.floating) else (None if pd.isna(x) else x)
                         )
                         if original_dtype != combined_ratingstable[col].dtype or original_dtype == 'object':
                              logger.info(f"  - Converted/Checked ratingstable column '{col}' (Original dtype: {original_dtype}, New dtype: {combined_ratingstable[col].dtype})")
                     except Exception as e:
                          logger.warning(f"  - Could not apply numeric conversion to ratingstable column '{col}' (dtype: {original_dtype}): {e}. Ensuring it's object type.")
                          if combined_ratingstable[col].dtype != 'object':
                             combined_ratingstable[col] = combined_ratingstable[col].astype(object)

            logger.info("Attempting to save combined data to 'AllStatesRankings' sheet starting at row 3.")
            # Use the NEW save function
            save_dataframe_starting_at_row(combined_ratingstable, 'AllStatesRankings', 3, gspread_client)
        else:
            logger.warning("No ratingstable data collected. 'AllStatesRankings' sheet will not be updated.")


    except Exception as e:
        logger.exception(f"A critical error occurred in the main execution block: {e}")
        print(f"A critical error occurred: {e}")

    finally:
        end_time = datetime.now(tzlocal())
        duration = end_time - start_time
        logger.info(f'Finishing lotteryscrape_optimized.py run at: {end_time.strftime("%Y-%m-%d %H:%M:%S %Z")}')
        logger.info(f'Total execution time: {duration}')
        print(f'Total execution time: {duration}')
  

if __name__ == "__main__":
    main()