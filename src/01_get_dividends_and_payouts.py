"""
Script to download dividends and payouts data from Nasdaq Baltic website.
"""

# standard
import datetime
from pathlib import Path
# external
import requests


##########
# Inputs #
##########

URL = "https://nasdaqbaltic.com/statistics/en/dividends"
RAW_DATA_PATH = Path() / "data" / "raw" / "dividends-and-payouts.xlsx"


######################
# Maps and constants #
######################

DATE_PATTERN = "%Y-%m-%d"


######################
# Load data from web #
######################

# Request dividends and payouts data
params = {
    "download": "1",
    "filter": "1",
    "from": "2015-01-02",
    "to": datetime.datetime.now().strftime(DATE_PATTERN)
}
response = requests.get(URL, params=params)
response.raise_for_status()


#################
# Save raw data #
#################

# Save the raw Excel file
RAW_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(RAW_DATA_PATH, "wb") as file:
    file.write(response.content)
