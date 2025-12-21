import pandas as pd
import requests
import json
from datetime import date, datetime
from dateutil.tz import tzlocal
import numpy as np

now = datetime.now(tzlocal()).strftime('%Y-%m-%d %H:%M:%S %Z')

def exportMAScratcherRecs():
    """
    Scrapes the Massachusetts Lottery website for scratcher data by using its
    official API, with corrected logic based on user feedback.
    """
    # Define the API endpoints
    all_games_url = "https://www.masslottery.com/api/v1/games"
    
    # FIX #2: Corrected the base URL for fetching detailed prize info.
    game_detail_base_url = "https://www.masslottery.com/api/v1/instant-game-prizes?gameID="

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        print("Fetching list of all games from the API...")
        # Step 1: Get the list of all games
        r = requests.get(all_games_url, headers=headers)
        r.raise_for_status()
        all_games_data = r.json()

    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"Failed to fetch the main game list from the API: {e}")
        return None, None

    tixtables = pd.DataFrame()
    scratchersall_list = []

    # Loop through each game from the main list
    for game_summary in all_games_data:

        # FIX #1: Corrected the logic to properly identify active instant games.
        if game_summary.get('gameType') != 'Scratch':
            continue

        game_number = str(game_summary.get('id'))
        game_name = game_summary.get('name')
        price = float(game_summary.get('price', 0))
        overall_odds = game_summary.get('odds', '').replace('1 in ', '')
        game_photo = game_summary.get('icon').get('url')

        print(f"Processing Game #{game_number}: {game_name}")

        try:
            # Step 2: Get the detailed prize info for this specific game using the corrected URL
            detail_url = f"{game_detail_base_url}{game_number}"
            
            detail_r = requests.get(detail_url, headers=headers)
            detail_r.raise_for_status()
            game_detail_data = detail_r.json()
            # The prize tiers are in a nested list
            prize_tiers = game_detail_data.get('prizeTiers', [])
            if not prize_tiers:
                print(f"  No prize tier data found for game {game_number}. Skipping.")
                continue

            # Create a DataFrame from the prize tier data
            prize_table_df = pd.DataFrame(prize_tiers)
            prize_table_df.rename(columns={
                'prizeAmount': 'prizeamount',
                'totalPrizes': 'Winning Tickets At Start',
                'prizesRemaining': 'Winning Tickets Unclaimed',
                'odds': 'prizeodds'
                
            }, inplace=True)
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"  Could not fetch prize details for game {game_number}. Skipping. Error: {e}")
            continue

        # --- Data processing logic remains the same ---
        prize_table_df['gameNumber'] = game_number
        prize_table_df['gameName'] = game_name
        prize_table_df['gamePrice'] = price
        prize_table_df['gameURL'] = game_summary.get('url', f"https://www.masslottery.com/games/scratch/{game_number}")
        prize_table_df['gamePhoto'] = game_photo
        prize_table_df['dateexported'] = date.today()

        prize_table_df['prizeamount'] = pd.to_numeric(prize_table_df['prizeamount'], errors='coerce').fillna(price)

        # Find the highest prize amount to identify the top prize reliably
        topprize = prize_table_df['prizeamount'].max()
        
        # Get the entire row associated with that top prize
        top_prize_row = prize_table_df[prize_table_df['prizeamount'] == topprize]
        
        # Extract all top prize details from that row. Use .iloc[0] in case of a tie.
        topprizeodds = top_prize_row['prizeodds'].iloc[0].replace('1 in ', '').replace(',', '')
        topprizestarting = top_prize_row['Winning Tickets At Start'].iloc[0]
        topprizeremain = top_prize_row['Winning Tickets Unclaimed'].iloc[0]
        topprizeavail = 'Top Prize Claimed' if topprizeremain == 0 else np.nan

        tixtables = pd.concat([tixtables, prize_table_df], ignore_index=True)
        scratchersall_list.append({
            'price': price, 'gameName': game_name, 'gameNumber': game_number,
            'topprize': topprize, 'topprizeodds': topprizeodds, 'overallodds': overall_odds,
            'topprizestarting': topprizestarting, 'topprizeremain': topprizeremain,
            'topprizeavail': topprizeavail, 'extrachances': None, 'secondChance': None,
            'startDate': game_summary.get('startDate'), 'endDate': game_summary.get('expirationDate'), 
            'lastdatetoclaim': None,
            'dateexported': date.today(), 'gamePhoto': game_photo, 'gameURL': prize_table_df['gameURL'].iloc[0]
        })

    if not scratchersall_list:
        print("No game data was successfully processed. Exiting.")
        return None, None
        
    scratchersall = pd.DataFrame(scratchersall_list)
    scratchersall.to_csv("./MAscratcherslist.csv", encoding='utf-8', index=False)

    # --- The entire statistical calculation part of your script would follow here ---
    scratchersall = scratchersall.drop_duplicates(subset=['price','gameName', 'gameNumber','topprize', 'topprizeodds', 'overallodds', 'topprizeremain','topprizeavail', 'extrachances', 'secondChance', 'startDate', 'endDate', 'lastdatetoclaim', 'dateexported', 'gamePhoto', 'gameURL'])
    scratchersall = scratchersall.loc[scratchersall['gameNumber']!= "Coming Soon!", :]
    #scratchersall = scratchersall.drop_duplicates()
    # save scratchers list
    #scratchersall.to_sql('azscratcherlist', engine, if_exists='replace')
    scratchersall.to_csv("./mascratcherslist.csv", encoding='utf-8')

    # Create scratcherstables df, with calculations of total tix and total tix without prizes
    scratchertables = tixtables[['gameNumber', 'gameName', 'prizeamount','Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables = scratchertables.loc[scratchertables['gameNumber']!= "Coming Soon!", :]
    scratchertables = scratchertables.astype({'prizeamount': 'int32', 'Winning Tickets At Start': 'int32', 'Winning Tickets Unclaimed': 'int32'})
    
    # Get sum of tickets for all prizes by grouping by game number and then calculating with overall odds from scratchersall
    # Select columns first, then groupby and aggregate
    cols_to_sum = ['Winning Tickets At Start', 'Winning Tickets Unclaimed']
    gamesgrouped = scratchertables.groupby(
        by=['gameNumber', 'gameName', 'dateexported'], group_keys=False)[cols_to_sum].sum().reset_index() # reset_index() without levels works here
    gamesgrouped = gamesgrouped.merge(scratchersall[[
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

    # create new 'prize amounts' of "$0" for non-prize tickets and "Total" for the sum of all tickets, then append to scratcherstables
    nonprizetix = gamesgrouped[['gameNumber', 'gameName',
                                'Non-prize at start', 'Non-prize remaining', 'dateexported']].copy()
    nonprizetix.rename(columns={'Non-prize at start': 'Winning Tickets At Start',
                       'Non-prize remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    nonprizetix.loc[:, 'prizeamount'] = 0

    totals = gamesgrouped[['gameNumber', 'gameName',
                           'Total at start', 'Total remaining', 'dateexported']].copy()
    totals.rename(columns={'Total at start': 'Winning Tickets At Start',
                  'Total remaining': 'Winning Tickets Unclaimed'}, inplace=True)
    totals.loc[:, 'prizeamount'] = "Total"


    # loop through each scratcher game id number and add columns for each statistical calculation
    alltables = pd.DataFrame()
    currentodds = pd.DataFrame()
    for gameid in gamesgrouped['gameNumber']:
        gamerow = gamesgrouped.loc[(gamesgrouped['gameNumber'] == gameid), :].copy()
        startingtotal = int(gamerow.loc[:, 'Total at start'].values[0])
        tixtotal = int(gamerow.loc[:, 'Total remaining'].values[0])
        totalremain = scratchertables.loc[(scratchertables['gameNumber'] == gameid), [
            'gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        price = int(gamerow['price'].values[0])
        print(gameid)
        print(gamerow)
        print(gamerow.columns)

        prizes = totalremain.loc[:, 'prizeamount']
        
        startoddstopprize = tixtotal / totalremain.loc[totalremain['prizeamount']==totalremain['prizeamount'].max(), 'Winning Tickets At Start'].values[0]

        # add various columns for the scratcher stats that go into the ratings table
        gamerow.loc[:, 'Current Odds of Top Prize'] = float(gamerow['topprizeodds'].values[0])
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
        gamerow.loc[:, 'Max Tickets to Buy'] = tixtotal/(totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].sum()-totalremain.loc[totalremain['prizeamount']!=price,'Winning Tickets Unclaimed'].std().mean())

        totalremain[['prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']] = totalremain.loc[:, [
            'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed']].apply(pd.to_numeric)
        totalremain.loc[:, 'Starting Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[:, 'Expected Value'] = totalremain.apply(lambda row: (
            row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                   'Winning Tickets Unclaimed', 'Starting Expected Value', 'Expected Value', 'dateexported']].copy()

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
        gamerow.loc[:, 'Ratio of Decline in Prizes to Decline in Losing Ticket'] = chngLosingTix/chngAvailPrizes

        gamerow.loc[:, 'Photo'] = scratchersall.loc[scratchersall['gameNumber']==gameid,['gamePhoto']].values[0]
        gamerow.loc[:, 'FAQ'] = None
        gamerow.loc[:, 'About'] = None
        gamerow.loc[:, 'Directory'] = None
        gamerow.loc[:, 'Data Date'] = gamerow.loc[:, 'dateexported']

        currentodds = pd.concat([currentodds, gamerow], ignore_index=True)

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
        totalremain = totalremain[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                   'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']].copy()
        totalremain = pd.concat([totalremain, nonprizetix.loc[nonprizetix['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        totalremain = pd.concat([totalremain, totals.loc[totals['gameNumber'] == gameid, ['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start',
                                         'Winning Tickets Unclaimed', 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]], axis=0, ignore_index=True)
        print(totalremain.columns)

        # add expected values for final totals row
        allexcepttotal = totalremain.loc[totalremain['prizeamount'] != 'Total', :]

        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Starting Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets At Start']/startingtotal), axis=1)
        totalremain.loc[totalremain['prizeamount'] != 'Total', 'Expected Value'] = allexcepttotal.apply(
            lambda row: (row['prizeamount']-price)*(row['Winning Tickets Unclaimed']/tixtotal), axis=1)
        print(totalremain)
        alltables = pd.concat([alltables, totalremain], axis=0)

    scratchertables = alltables[['gameNumber', 'gameName', 'prizeamount', 'Winning Tickets At Start', 'Winning Tickets Unclaimed',
                                 'Prize Probability', 'Percent Tix Remaining', 'Starting Expected Value', 'Expected Value', 'dateexported']]


    # --- REVISED CONVERSION FOR JSON SERIALIZATION (Simpler) ---
    print("Converting numeric types in scratchertables to JSON-compatible types (using .astype(object))...")
    numeric_cols = scratchertables.select_dtypes(include=np.number).columns
    print(f"Numeric columns identified for conversion: {numeric_cols.tolist()}")
    
    for col in numeric_cols:
        # Force conversion to object dtype, which stores Python native types
        # This handles NaN by converting them to None within the object array.
        try:
            scratchertables[col] = scratchertables[col].astype(object)
            print(f"Converted column '{col}' using astype(object).")
        except Exception as e:
            # Added error handling just in case astype fails for some reason
            print(f"ERROR: Failed to convert column '{col}' using astype(object): {e}")
    
    # Ensure columns that might contain non-numeric strings like 'Total' are object type
    if 'prizeamount' in scratchertables.columns and scratchertables['prizeamount'].dtype != 'object':
        scratchertables['prizeamount'] = scratchertables['prizeamount'].astype(object)
    if 'gameNumber' in scratchertables.columns and scratchertables['gameNumber'].dtype != 'object':
         scratchertables['gameNumber'] = scratchertables['gameNumber'].astype(object) # Game number can be string
    
    print("Final scratchertables dtypes before returning:")
    print(scratchertables.dtypes)
# --- Corrected Diagnostic Check ---
    # Check for problematic NumPy types *within* object columns after conversion attempt
    print("\nDetailed check of types within potentially converted columns...")
    conversion_issue_found = False
    for col in numeric_cols: # Iterate over columns that *should* have been converted
        if col in scratchertables.columns:
            col_dtype = scratchertables[col].dtype
            print(f"  Column '{col}': Reported dtype = {col_dtype}")
            if col_dtype == 'object':
                # Sample the first few non-null values to check their actual Python type
                try:
                    # Using unique types found in a sample is more informative
                    unique_types_in_sample = scratchertables[col].dropna().head(20).apply(type).unique()
                    print(f"    Sampled value types: {unique_types_in_sample}")
                    # Explicitly check for numpy types within the object column
                    numpy_types_present = [t for t in unique_types_in_sample if 'numpy' in str(t)]
                    if numpy_types_present:
                         print(f"    WARNING: NumPy types {numpy_types_present} still present in object column '{col}'!")
                         conversion_issue_found = True
                except Exception as e:
                    print(f"    - Could not inspect types within column '{col}': {e}")
            elif col_dtype == np.int64:
                print(f"    ERROR: Column '{col}' is still {np.int64} despite conversion attempt!")
                conversion_issue_found = True
            elif col_dtype == np.float64:
                 print(f"    WARNING: Column '{col}' is still {np.float64}. This might be acceptable, but was expected to be object.")
                 # Decide if this is truly an error or just a warning
                 # conversion_issue_found = True # Uncomment if float64 is also problematic

    if conversion_issue_found:
        print("-----> WARNING: Potential type conversion issues detected. Review column types above. <-----")
    else:
        print("-----> Type conversion check passed (object columns inspected where applicable). <-----")
    # --- End of Corrected Diagnostic Check ---    
        print(scratchertables.dtypes)

    # save scratchers tables
    #scratchertables.to_sql('AZscratcherstables', engine, if_exists='replace')
    scratchertables.to_csv("./mascratchertables.csv", encoding='utf-8')

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
    
    # Also convert key columns in ratingstable if they might be numpy types
    # Although less likely to cause issues if not directly serialized as granularly.
    numeric_cols_ratings = [ # Add relevant numeric columns from ratingstable
        'price', 'topprizeremain', 'Days Since Start', # Integers
        'topprizeodds', 'overallodds', 'Current Odds of Top Prize', # Floats that might be np.float64
        # ... include all other numeric columns from ratingstable ...
        'Rank Average', 'Overall Rank', 'Rank by Cost'
    ]
    for col in numeric_cols_ratings:
        if col in ratingstable.columns:
             ratingstable[col] = ratingstable[col].astype(object) # Convert to object/python types
             
    print(scratchertables.columns)
    print(scratchertables)
    print(scratchertables.dtypes)
    # save ratingstable
    print(ratingstable)
    print(ratingstable.columns)
    #ratingstable.to_sql('AZratingstable', engine, if_exists='replace')

    ratingstable['Stats Page'] = "/arizona-statistics-for-each-scratcher-game"
    ratingstable = ratingstable[['price', 'gameName','gameNumber', 'topprize', 'topprizeremain','topprizeavail','extrachances', 'secondChance',
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
    ratingstable.to_csv("./MAratingstable.csv", encoding='utf-8')
    
    # Placeholder for the detailed table output
    scratchertables_output = tixtables.loc[:,['gameNumber', 'gameName', 'prizeamount',
                                 'Winning Tickets At Start', 'Winning Tickets Unclaimed', 'dateexported']]
    scratchertables_output.to_csv("./MAscratchertables.csv", encoding='utf-8', index=False)

    print("Output files generated: MAscratcherslist.csv, MAscratchertables.csv, MAratingstable.csv")
    
    # The function would return the fully processed dataframes after running the statistical analysis
    return scratchersall, scratchertables_output

if __name__ == '__main__':
    ratingstable, scratchertables = exportMAScratcherRecs()