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


# --- MODIFIED run_XX_scratcher_recs functions ---
# Each function will now return the scratchertables DataFrame or None if an error occurs

def run_va_scratcher_recs(gspread_client):
    """Scrapes VA data, saves ratingstable, returns scratchertables."""
    state_code = 'VA'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from valotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client) # Save ratings table immediately
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import valotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None # Return None on error

def run_az_scratcher_recs(gspread_client):
    """Scrapes AZ data, saves ratingstable, returns scratchertables."""
    state_code = 'AZ'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from azlotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import azlotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_mo_scratcher_recs(gspread_client):
    """Scrapes MO data, saves ratingstable, returns scratchertables."""
    state_code = 'MO'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from molotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import molotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

# --- Repeat the pattern for ALL other run_XX_scratcher_recs functions ---

def run_ok_scratcher_recs(gspread_client):
    """Scrapes OK data, saves ratingstable, returns scratchertables."""
    state_code = 'OK'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from oklotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import oklotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_ca_scratcher_recs(gspread_client):
    """Scrapes CA data, saves ratingstable, returns scratchertables."""
    state_code = 'CA'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from calotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import calotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_nm_scratcher_recs(gspread_client):
    """Scrapes NM data, saves ratingstable, returns scratchertables."""
    state_code = 'NM'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from nmlotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import nmlotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_ny_scratcher_recs(gspread_client):
    """Scrapes NY data, saves ratingstable, returns scratchertables."""
    state_code = 'NY'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from nylotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import nylotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_dc_scratcher_recs(gspread_client):
    """Scrapes DC data, saves ratingstable, returns scratchertables."""
    state_code = 'DC'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from dclotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import dclotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_nc_scratcher_recs(gspread_client):
    """Scrapes NC data, saves ratingstable, returns scratchertables."""
    state_code = 'NC'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from nclotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import nclotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_tx_scratcher_recs(gspread_client):
    """Scrapes TX data, saves ratingstable, returns scratchertables."""
    state_code = 'TX'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from txlotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import txlotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_ks_scratcher_recs(gspread_client):
    """Scrapes KS data, saves ratingstable, returns scratchertables."""
    state_code = 'KS'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from kslotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import kslotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_wa_scratcher_recs(gspread_client):
    """Scrapes WA data, saves ratingstable, returns scratchertables."""
    state_code = 'WA'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from walotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import walotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_or_scratcher_recs(gspread_client):
    """Scrapes OR data, saves ratingstable, returns scratchertables."""
    state_code = 'OR'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from orlotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import orlotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

def run_ky_scratcher_recs(gspread_client):
    """Scrapes KY data, saves ratingstable, returns scratchertables."""
    state_code = 'KY'
    logger.info(f"--- Processing State: {state_code} ---")
    try:
        from kylotteryscrape import exportScratcherRecs
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, f'{state_code}RatingsTable', gspread_client)
        if scratchertables is not None and not scratchertables.empty:
            scratchertables['State'] = state_code
            logger.info(f"Successfully processed {state_code}. Returning scratchertables.")
            return scratchertables
        else:
             logger.warning(f"No scratchertables data returned from {state_code} scrape.")
             return None
    except ImportError:
        logger.error(f"Could not import kylotteryscrape. Skipping {state_code}.")
        return None
    except Exception as e:
        logger.exception(f"Error occurred during {state_code} processing: {e}")
        return None

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
        gspread_client = authorize_gspread_from_path() #from local
        #gspread_client = authorize_gspread() #from github
        if not gspread_client:
            logger.error("Gspread authorization failed. Exiting.")
            print("Authorization failed. Cannot proceed.")
            return # Exit if authorization fails

        print("Authorization successful!")

        # --- Data Collection ---
        all_scratchertables_list = [] # Initialize list to hold DataFrames from each state

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
            run_wa_scratcher_recs,
            # Add other state functions here if enabled:
            # run_ms_scratcher_recs,
            # run_il_scratcher_recs,
            # run_fl_scratcher_recs,
            # run_oh_scratcher_recs,
        ]

        for scrape_func in state_scrape_functions:
             tables = scrape_func(gspread_client)
             if tables is not None and not tables.empty:
                 all_scratchertables_list.append(tables)
             else:
                 logger.warning(f"Function {scrape_func.__name__} did not return valid data.")


        # --- Combine and Upload ---
        if all_scratchertables_list:
            logger.info(f"Combining scratchertables data from {len(all_scratchertables_list)} states.")
            # Combine DataFrames, trying to preserve object dtype where possible
            combined_scratchertables = pd.concat(all_scratchertables_list, ignore_index=True, join='outer') # Use outer join just in case columns differ slightly
            logger.info(f"Combined DataFrame shape: {combined_scratchertables.shape}")
            logger.info(f"Dtypes immediately after concat:\n{combined_scratchertables.dtypes}")


            # --- AGGRESSIVE FINAL CONVERSION FOR JSON SERIALIZATION ---
            logger.info("Performing AGGRESSIVE final type conversion for JSON compatibility...")
            for col in combined_scratchertables.columns:
                # Check if column has a numeric-like dtype OR is object (could contain mixed types)
                is_potentially_numeric = pd.api.types.is_numeric_dtype(combined_scratchertables[col]) \
                                        or combined_scratchertables[col].dtype == 'object'

                if is_potentially_numeric:
                    # Apply a lambda function to convert numpy ints/floats to Python natives
                    # This needs to handle potential errors if non-numeric data exists in an object column
                    original_dtype = combined_scratchertables[col].dtype
                    try:
                        # Make a copy of the column before applying changes
                        col_copy = combined_scratchertables[col].copy()
                        # Apply the corrected conversion
                        combined_scratchertables[col] = col_copy.apply(
                            lambda x: int(x) if isinstance(x, np.integer) else       # Convert numpy int -> python int
                                      float(x) if isinstance(x, np.floating) else   # Convert numpy float -> python float
                                      (None if pd.isna(x) else x)                    # Keep None/NaN as None, preserve others
                        )
                        # Check if dtype actually changed or if object column was processed
                        if original_dtype != combined_scratchertables[col].dtype or original_dtype == 'object':
                             logger.info(f"  - Converted/Checked column '{col}' (Original dtype: {original_dtype}, New dtype: {combined_scratchertables[col].dtype})")

                    except (TypeError, ValueError) as e:
                         # This might happen if an object column has truly non-numeric strings mixed in (like 'Total')
                         logger.warning(f"  - Could not apply numeric conversion to column '{col}' (dtype: {original_dtype}): {e}. Ensuring it's object type.")
                         # Fallback: Ensure the column is object type if conversion fails
                         if combined_scratchertables[col].dtype != 'object':
                            combined_scratchertables[col] = combined_scratchertables[col].astype(object)
                    except Exception as e:
                        logger.error(f"  - Unexpected error during final conversion of column '{col}': {e}", exc_info=True)
                        # Fallback: Ensure the column is object type
                        if combined_scratchertables[col].dtype != 'object':
                             combined_scratchertables[col] = combined_scratchertables[col].astype(object)

            logger.info("Final check of dtypes before saving:")
            print(combined_scratchertables.dtypes)
            # --- END OF AGGRESSIVE CONVERSION ---
            
            # Optionally clear the sheet
            # (Keep existing commented-out code for clearing if desired)
            # Optionally clear the sheet even if no data was collected
            # try:
            #     gsheet = gspread_client.open_by_key(GSHEET_KEY)
            #     worksheet = gsheet.worksheet('ScratcherTables')
            #     worksheet.clear()
            #     logger.info("Cleared 'ScratcherTables' sheet as no data was collected.")
            # except Exception as e:
            #     logger.error(f"Failed to clear 'ScratcherTables' sheet after collecting no data: {e}")

            logger.info("Attempting to save combined data to 'ScratcherTables' sheet.")
            # Use the save function which handles clearing and uploading
            save_dataframe_to_gsheet(combined_scratchertables, 'ScratcherTables', gspread_client)
            
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