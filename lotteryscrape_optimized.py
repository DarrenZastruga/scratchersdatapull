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
from dotenv import load_dotenv
load_dotenv()
# Verify path is loaded
print(
    f"Attempting to use credentials file: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")


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
script_dir = os.path.dirname(__file__)
dotenv_path = os.path.join(script_dir, '.env')
load_dotenv(dotenv_path=dotenv_path, override=True, verbose=True)


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
    try:

        # --- Verification START ---
        creds_json_string = os.environ.get(
            'GOOGLE_APPLICATION_CREDENTIALS_JSON')

        print("-" * 20)  # Separator for clarity
        print(f"Type of retrieved value: {type(creds_json_string)}")

        if creds_json_string is None:
            print(
                "ERROR: GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable is NOT SET or accessible.")
            logging.error(
                "GOOGLE_APPLICATION_CREDENTIALS_JSON environment variable not found.")
            return None  # Or raise an error
        elif isinstance(creds_json_string, str):
            # Print only the start
            print(
                f"Retrieved value (first 100 chars): {creds_json_string[:100]}...")
            print(f"Length of retrieved string: {len(creds_json_string)}")
            # Optional: Basic check for JSON structure
            if creds_json_string.strip().startswith('{') and creds_json_string.strip().endswith('}'):
                print("Value appears to start and end like JSON.")
            else:
                print(
                    "WARNING: Value does NOT look like a complete JSON object (missing '{' or '}').")
        else:
            print(
                f"WARNING: Retrieved value is not a string or None. Value: {creds_json_string}")

        print("-" * 20)
        # --- Verification END ---

        # Now, proceed with the original logic, including the None check we added before
        if creds_json_string is None:
            # This logging might be redundant now but keeps the previous fix logic
            logging.error(
                "Failed to load Google Service Account credentials. Environment variable not set.")
            return None

            try:
                service_account_info = json.loads(creds_json_string)
                logging.info("Successfully parsed credentials JSON string.")

            except json.JSONDecodeError as e:
                logging.error(
                    f"Failed to parse credentials JSON: {e}. Check if the content copied into the environment variable is the complete and valid JSON.", exc_info=True)
                return None
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during gspread authorization: {e}", exc_info=True)
        return None
    credentials = Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES)
    return gspread.authorize(credentials)


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
    full_path = file_path + file_name
    try:
        urllib.request.urlretrieve(url, full_path)
        logger.info(f"Downloaded image from {url} to {full_path}")
    except Exception as e:
        logger.error(f"Failed to download image from {url}: {e}")


def save_dataframe_to_gsheet(dataframe, worksheet_name, gspread_client):
    """Saves a Pandas DataFrame to a Google Sheet."""
    try:
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        worksheet = gsheet.worksheet(worksheet_name)
        worksheet.clear()
        dataframe.replace([np.inf, -np.inf], 0, inplace=True)
        dataframe.fillna('', inplace=True)
        set_with_dataframe(worksheet=worksheet, dataframe=dataframe,
                           include_index=False, include_column_header=True, resize=True)
        logger.info(
            f"DataFrame successfully saved to Google Sheet worksheet: {worksheet_name}")
    except Exception as e:
        logger.error(
            f"Failed to save DataFrame to Google Sheet worksheet {worksheet_name}: {e}")


def append_all_data_to_scratcher_tables(gspread_client,
                                        worksheet_name, scratchertables):
    """Appends various scratcher data DataFrames to a single Google Sheet with handling for empty sheet."""
    try:
        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        worksheet = gsheet.worksheet(worksheet_name)

        # --- ADDED PREPROCESSING ---
        # Replace infinite values (not JSON serializable) with None
        # Important: Make a copy if you don't want to modify the original df outside this func
        df_to_append = scratchertables.copy()
        df_to_append.replace([np.inf, -np.inf], None, inplace=True)
        # Fill NaN values with empty string (or None) for Sheets compatibility
        df_to_append.fillna('', inplace=True)
        logger.info(f"Preprocessed DataFrame for appending to {worksheet_name}. Dtypes after processing:\n{df_to_append.dtypes}")
        # --- END OF ADDED PREPROCESSING ---

                # Check if the sheet is empty
        existing_data = worksheet.get_all_values()

        # Calculate the next empty row to append. Assign a default so it works even with an empty table
        # If header exists, start at len(existing_data) + 1. If empty, start at row 1.
        start_row = len(existing_data) + 1 if existing_data else 1
        include_header = not bool(existing_data) # Include header only if sheet is empty

        if not existing_data:  # If the sheet is empty, write the header along with the data
            logger.info(f"Worksheet '{worksheet_name}' is empty. Writing data with header.")
            set_with_dataframe(worksheet=worksheet, dataframe=df_to_append, # Use processed df
                               include_index=False, include_column_header=True, resize=False) # Let it resize if empty
        else:
            # Append the DataFrame to the worksheet
            logger.info(f"Appending data to '{worksheet_name}' starting at row {start_row}.")
            set_with_dataframe(worksheet=worksheet, dataframe=df_to_append, # Use processed df
                               include_index=False, include_column_header=False, row=start_row, resize=False)

        logger.info(
            f"Data successfully appended to {worksheet_name} starting at row: {start_row}!")

    except Exception as e:
        # Log the specific type that failed if possible (though the error msg already does)
        logger.error(
            f"Error appending data to {worksheet_name}: {e}", exc_info=True) # Add traceback
    except Exception as e:
        logger.error(
            f"Error appending data to  ScratcherTables: {e}")


def generic_scratcher_scrape(url, state_name, payload=None, headers=None, data=None):
    """
        Generic function to scrape scratcher data from a given URL,
        intended to be called from within functions specific to each state.
    """
    try:
        if payload is None and headers is None and data is None:
            r = requests.get(url)
        else:
            r = requests.request("POST", url, headers=headers, data=payload)

        response = r.text
        soup = BeautifulSoup(response, 'html.parser')
        logger.info(
            f"Successfully scraped {state_name} Scratcher data from {url}")
        return soup

    except Exception as e:
        logger.exception(
            f"Failed to scrape {state_name} Scratcher data from {url}: {e}")
        return None


def run_va_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Virginia."""
    from valotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'VARatingsTable', gspread_client)
        scratchertables['State'] = 'VA'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during VA scrape and save: {e}")


def run_az_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Arizona."""
    from azlotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'AZRatingsTable', gspread_client)
        scratchertables['State'] = 'AZ'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during AZ scrape and save: {e}")


def run_mo_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Missouri."""
    from molotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'MORatingsTable', gspread_client)
        scratchertables['State'] = 'MO'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during MO scrape and save: {e}")


def run_ok_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Oklahoma."""
    from oklotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'OKRatingsTable', gspread_client)
        scratchertables['State'] = 'OK'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during OK scrape and save: {e}")


def run_ca_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for California."""
    from calotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'CARatingsTable', gspread_client)
        scratchertables['State'] = 'CA'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during CA scrape and save: {e}")


def run_nm_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for New Mexico."""
    from nmlotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'NMRatingsTable', gspread_client)
        scratchertables['State'] = 'NM'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during NM scrape and save: {e}")


def run_ny_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for New York."""
    from nylotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'NYRatingsTable', gspread_client)
        scratchertables['State'] = 'NY'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during NY scrape and save: {e}")


def run_dc_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Washington DC."""
    from dclotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'DCRatingsTable', gspread_client)
        scratchertables['State'] = 'DC'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during DC scrape and save: {e}")


def run_nc_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for North Carolina."""
    from nclotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'NCRatingsTable', gspread_client)
        scratchertables['State'] = 'NC'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during NC scrape and save: {e}")


def run_tx_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Texas."""
    from txlotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'TXRatingsTable', gspread_client)
        scratchertables['State'] = 'TX'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during TX scrape and save: {e}")


def run_ks_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Kansas."""
    from kslotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'KSRatingsTable', gspread_client)
        scratchertables['State'] = 'KS'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during KS scrape and save: {e}")


def run_wa_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Washington."""
    from walotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'WARatingsTable', gspread_client)
        scratchertables['State'] = 'WA'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during WA scrape and save: {e}")


def run_or_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Oregon."""
    from orlotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'ORRatingsTable', gspread_client)
        scratchertables['State'] = 'OR'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during OR scrape and save: {e}")


def run_ky_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Kentucky."""
    from kylotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'KYRatingsTable', gspread_client)
        scratchertables['State'] = 'KY'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during KY scrape and save: {e}")


def run_il_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Illinois."""
    from illotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'ILRatingsTable', gspread_client)
        scratchertables['State'] = 'IL'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during IL scrape and save: {e}")


def run_fl_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Florida."""
    from fllotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'FLRatingsTable', gspread_client)
        scratchertables['State'] = 'FL'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during FL scrape and save: {e}")


def run_oh_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Ohio."""
    from ohlotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'OHRatingsTable', gspread_client)
        scratchertables['State'] = 'OH'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during OH scrape and save: {e}")


def run_ms_scratcher_recs(gspread_client):
    """Main function to execute the scratcher scraping and data processing for Mississippi."""
    from mslotteryscrape import exportScratcherRecs
    try:
        ratingstable, scratchertables = exportScratcherRecs()
        save_dataframe_to_gsheet(
            ratingstable, 'MSRatingsTable', gspread_client)
        scratchertables['State'] = 'MS'
        append_all_data_to_scratcher_tables(
            gspread_client, 'ScratcherTables', scratchertables)
    except Exception as e:
        logger.exception(f"Error occurred during MS scrape and save: {e}")


def main():
    """Main function to orchestrate the scratcher scraping process."""
    now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
    logger.info(f'Starting lotteryscrape.py at: {now}')

    try:
        gspread_client = authorize_gspread_from_path()
        if gspread_client:
            # Proceed with using the client
            print("Authorization successful!")
        else:
            print("Authorization failed.")

        # gspread_client = authorize_gspread() - use this for running on GitHub
        #pydrive_client = authorize_pydrive()

        gsheet = gspread_client.open_by_key(GSHEET_KEY)
        worksheet = gsheet.worksheet('ScratcherTables')
        worksheet.clear()

        run_az_scratcher_recs(gspread_client)
        run_ca_scratcher_recs(gspread_client)
        run_dc_scratcher_recs(gspread_client)
        run_ks_scratcher_recs(gspread_client)
        run_ky_scratcher_recs(gspread_client)
        run_mo_scratcher_recs(gspread_client)
        run_nc_scratcher_recs(gspread_client)
        run_nm_scratcher_recs(gspread_client)
        run_ny_scratcher_recs(gspread_client)
        run_ok_scratcher_recs(gspread_client)
        run_or_scratcher_recs(gspread_client)
        run_tx_scratcher_recs(gspread_client)
        run_va_scratcher_recs(gspread_client)
        run_wa_scratcher_recs(gspread_client)
        
        # run_ms_scratcher_recs(gspread_client)
        # run_il_scratcher_recs(gspread_client)
        # run_fl_scratcher_recs(gspread_client)
        # run_oh_scratcher_recs(gspread_client)

        now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')
        logger.info(f'Finishing lotteryscrape.py at: {now}')

    except Exception as e:
        logger.exception(f"A critical error occurred: {e}")


if __name__ == "__main__":
    main()
