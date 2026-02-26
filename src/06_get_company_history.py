"""

"""

# standard
import csv
import datetime
from pathlib import Path
import random
import time
import tqdm
# external
import requests


##########
# Inputs #
##########

URL = "https://nasdaqbaltic.com/statistics/en/instrument"
TICKER_ISIN_PATH = Path() / "data" / "ticker_isin.csv"
RAW_DATA_DIR = Path() / "data" / "raw"


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
    lambda_parameter = random.uniform(8, 25)
    delay = random.expovariate(1/lambda_parameter)
    delay = min(120, delay)
    time.sleep(delay)



#############
# Read data #
#############

with open(TICKER_ISIN_PATH, "r", newline="") as file:
    reader = csv.DictReader(file)
    companies= list(reader)


for company in companies:
    # Get raw data path
    raw_data_path = RAW_DATA_DIR / f'company_history_{company["TICKER"]}.xlsx'
    company["RAW_DATA_PATH"] = raw_data_path

    # Prepare request
    url = f'{URL}/{company["ISIN"]}/trading/chart_price_download'
    params = {
        "start": "2005-01-01",
        "end": datetime.datetime.now().strftime(DATE_PATTERN)
    }
    prepared_request = requests.Request("GET", url, params=params).prepare()
    company["REQUEST"] = prepared_request
    company["REQUEST_URL"] = prepared_request.url
    company["RESPONSE"] = None


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
_ = session.get("https://nasdaqbaltic.com/statistics/en/capitalization")
random_delay()
random_delay()


################
# Request data #
################

for company in tqdm.tqdm(companies, desc="Requesting company history data"):
    # Skip if same data is already downloaded
    if company["RAW_DATA_PATH"].exists():
        print(f'Raw data for {company["TICKER"]} already exists. Skipping download.')
        continue
    if company["RESPONSE"] is not None:
        print(f'Response for {company["TICKER"]} already exists. Skipping request.')
        continue

    # Make request
    response = session.send(company["REQUEST"])
    company["RESPONSE"] = response
    response.raise_for_status()

    # Write raw data to file
    path = company["RAW_DATA_PATH"]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as file:
        _ = file.write(response.content)

    # Delay before next request to avoid bot detection
    random_delay()

