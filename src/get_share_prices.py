# standard
import csv
import datetime
from pathlib import Path
import random
import time
# external
import requests
import tqdm


##########
# Inputs #
##########

URL = "https://nasdaqbaltic.com/statistics/en/shares"
DIVIDENDS_AND_PAYOUTS_DATA_PATH = Path() / "data" / "dividends_and_payouts.csv"
RAW_DATA_DIR = Path() / "data" / "raw"
EVENTS_OF_INTEREST_TYPES = [
    "dividend ex-date"
]


######################
# Maps and constants #
######################

REQUEST_DATE_PATTERN = "%Y-%m-%d"
CSV_DATETIME_PATTERN ="%Y-%m-%d %H:%M:%S"


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


#############
# Load data #
#############

with open(DIVIDENDS_AND_PAYOUTS_DATA_PATH, "r", newline="") as file:
    reader = csv.DictReader(file)
    dividends_and_payouts_data = list(reader)

events_of_interest = [event for event in dividends_and_payouts_data if event["EVENT_TYPE"].lower() in EVENTS_OF_INTEREST_TYPES]


####################
# Prepare requests #
####################

events = []
for event in events_of_interest:
    date = datetime.datetime.strptime(event["PREVIOUS_BUSINESS_DAY"], CSV_DATETIME_PATTERN)
    params = {
        "download": "1",
        "date": date.strftime(REQUEST_DATE_PATTERN)
    }
    prepared_request = requests.Request("GET", URL, params=params).prepare()

    event["REQUEST"] = prepared_request
    event["REQUEST_URL"] = prepared_request.url
    event["RAW_DATA_PATH"] = RAW_DATA_DIR / f'share_prices_{date.strftime(REQUEST_DATE_PATTERN)}.xlsx'
    event["RESPONSE"] = None

    events += [event]


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
session.get("https://nasdaqbaltic.com/statistics/en/shares")
random_delay()


################
# Request data #
################

for event in tqdm.tqdm(events, desc="Requesting share price data"):
    # Skip if same data is already downloaded
    if event["RAW_DATA_PATH"].exists():
        print(f'Raw data for {event["DATE"]} already exists. Skipping download.')
        continue
    if event["RESPONSE"] is not None:
        print(f'Response for {event["DATE"]} already exists. Skipping request.')
        continue

    # Make request
    response = session.send(event["REQUEST"])
    event["RESPONSE"] = response
    response.raise_for_status()

    # Write raw data to file
    with open(event["RAW_DATA_PATH"], "wb") as file:
        _ = file.write(response.content)

    # Delay before next request to avoid bot detection
    random_delay()
