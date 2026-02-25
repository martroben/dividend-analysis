"""
Script to clean share prices xlsx data from Nasdaq Baltic website and save it as csv.
"""

# standard
import csv
from pathlib import Path
import re
# external
import openpyxl
import tqdm


######################
# Maps and constants #
######################

SHARE_PRICE_NAME_MAP = {
    "Ticker": "TICKER",
    "Name": "COMPANY_NAME",
    "ISIN": "ISIN",
    "Currency": "CURRENCY",
    "MarketPlace": "MARKET",
    "List/segment": "MARKET_LIST",
    "Average Price": "AVERAGE_PRICE",
    "Open Price": "OPENING_PRICE",
    "High Price": "HIGHEST_PRICE",
    "Low Price": "LOWEST_PRICE",
    "Last close Price": "LAST_CLOSE_PRICE",
    "Last Price": "LAST_PRICE",
    "Price Change(%)": "PRICE_CHANGE_PERCENTAGE",
    "Best bid": "BEST_BID",
    "Best ask": "BEST_ASK",
    "Trades": "N_TRADES",
    "Volume": "N_SHARES_TRADED",
    "Turnover": "TURNOVER",
    "Industry": "INDUSTRY_TYPE",
    "Supersector": "SUPERSECTOR_TYPE"
}

DATE_REGEX_PATTERN = r'(\d{4}-\d{2}-\d{2})'


##########
# Inputs #
##########

RAW_DATA_DIR = Path() / "data" / "raw"
SAVE_PATH = Path() / "data" / "share_prices.csv"


################
# Process data #
################

raw_paths_all = RAW_DATA_DIR.glob("*")
raw_paths = [path for path in raw_paths_all if "share_prices" in path.name.lower()]

date_pattern = re.compile(DATE_REGEX_PATTERN)

data = []
# Process each share price file
for path in tqdm.tqdm(raw_paths, desc="Processing raw share price files"):
    with open(path, "rb") as file:
        workbook = openpyxl.load_workbook(file, data_only=True)
        sheet = workbook["Shares"]

    header_row = sheet[1]
    column_names = [SHARE_PRICE_NAME_MAP[cell.value] for cell in header_row] + ["DATE"]
 
    # Get date from file name
    date = date_pattern.search(path.name).group(1)

    # Convert rows to dicts and add date
    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_with_date = row + (date,)
        row_data = dict(zip(column_names, row_with_date))
        data += [row_data]

# Check if all rows have the same columns
column_names_set = set(tuple(row.keys()) for row in data)
if len(column_names_set) > 1:
    raise ValueError(f'Not all files have the same columns. Different sets: {column_names_set}')

# Sort by date
data.sort(key=lambda x: x["DATE"])


#############
# Save data #
#############

with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=column_names)
    writer.writeheader()
    writer.writerows(data)
