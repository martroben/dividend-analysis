"""
Script to get a mapping of ticker code to ISIN from the share price data files.
ISIN = International Securities Identification Number
"""

# standard
import csv
from pathlib import Path


##########
# Inputs #
##########

SHARE_PRICES_PATH = Path() / "data" / "share-prices.csv"
FUND_PRICES_PATH = Path() / "data" / "fund-prices.csv"
SAVE_PATH = Path() / "data" / "ticker-isin.csv"


#############
# Read data #
#############

with open(SHARE_PRICES_PATH, "r", newline="") as file:
    reader = csv.DictReader(file)
    share_prices = list(reader)

with open(FUND_PRICES_PATH, "r", newline="") as file:
    reader = csv.DictReader(file)
    fund_prices = list(reader)

#######################
# Get ticker-ISIN map #
#######################

ticker_isin_map = {}

for row in share_prices:
    if row["TICKER"] in ticker_isin_map and ticker_isin_map[row["TICKER"]] != row["ISIN"]:
        raise ValueError(f'Warning: TICKER {row['TICKER']} has multiple ISINs: {ticker_isin_map[row['TICKER']]} and {row['ISIN']}')
    ticker_isin_map[row["TICKER"]] = row["ISIN"]

for row in fund_prices:
    if row["TICKER"] in ticker_isin_map and ticker_isin_map[row["TICKER"]] != row["ISIN"]:
        raise ValueError(f'Warning: TICKER {row['TICKER']} has multiple ISINs: {ticker_isin_map[row['TICKER']]} and {row['ISIN']}')
    ticker_isin_map[row["TICKER"]] = row["ISIN"]

ticker_isin_data = []
for ticker, isin in ticker_isin_map.items():
    ticker_isin_data += [{
        "TICKER": ticker,
        "ISIN": isin
    }]


#############
# Save data #
#############

with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=ticker_isin_data[0].keys())
    writer.writeheader()
    writer.writerows(ticker_isin_data)
