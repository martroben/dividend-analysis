"""
Script to download fund price data from Nasdaq Baltic website.
Used to get all available instruments that have been active on market throughout the years.
"""

# standard
import datetime
from io import BytesIO
from pathlib import Path
import random
import time
# external
import openpyxl
import requests
import tqdm


##########
# Inputs #
##########

URL = "https://nasdaqbaltic.com/statistics/en/funds"
RAW_DATA_DIR = Path() / "data" / "raw"
RAW_FILE_NAME_HANDLE = "fund-prices"
DATA_START_YEAR = 2005


######################
# Maps and constants #
######################

DATE_PATTERN = "%Y-%m-%d"


#########################
# Classes and functions #
#########################

def random_delay():
    """
    Random delay between requests to avoid bot detection.
    """
    lambda_parameter = random.uniform(5, 30)
    delay = random.expovariate(1/lambda_parameter)
    delay = min(120, delay)
    time.sleep(delay)


def is_good_response(response: requests.Response) -> bool:
    """
    Check if request was successful and if it contains an xlsx with more than just header row.
    """
    if not response:
        return False

    workbook = openpyxl.load_workbook(filename=BytesIO(response.content))
    if workbook.active.max_row < 2:
        return False
    
    return True


###################
# Prepare session #
###################

session = requests.Session()

session_headers = {
    # Browser-style headers to mimic a human user
    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}
session.headers.update(session_headers)

# "Warm up" the session
_ = session.get(URL)
random_delay()


#############
# Load data #
#############

current_year = datetime.datetime.now().year
max_requests = 5

for year in tqdm.tqdm(range(DATA_START_YEAR, current_year + 1)):
    # Start trying from 2nd of January (potentially the first business day)
    query_day = datetime.date(year, 1, 2)
    i_request = 0
    while True:
        # Skip year if maximum requests is reached
        if i_request == max_requests:
            print(f'Maximum requests ({max_requests}) reached for year {year}. Skipping year.')
            break

        query_day_string = query_day.strftime(DATE_PATTERN)
        save_path = RAW_DATA_DIR / f'{RAW_FILE_NAME_HANDLE}_{query_day_string}.xlsx'
        # Skip if same data is already downloaded
        if save_path.exists():
            print(f'Raw data for {query_day} already exists. Skipping download.')
            break

        # Make request
        random_delay()
        params = {
            "download": "1",
            "date": query_day_string
        }
        response = session.get(
            URL,
            params=params
        )
        if is_good_response(response):
            # Save response
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as file:
                _ = file.write(response.content)
            break
        
        # Try next day if no good response
        i_request += 1
        query_day += datetime.timedelta(days=1)
