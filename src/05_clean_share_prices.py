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

DATE_REGEX_PATTERN = r'(\d{4}-\d{2}-\d{2})'
DATE_COLUMN_NAME = "DATE"

SELECT_MAP = {
    "Ticker": "TICKER",
    "Name": "NAME",
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
    "Volume": "N_UNITS_TRADED",
    "Turnover": "TURNOVER",
    "Industry": "INDUSTRY_TYPE",
    "Supersector": "SUPERSECTOR_TYPE",
    DATE_COLUMN_NAME: DATE_COLUMN_NAME
}


##########
# Inputs #
##########

RAW_DATA_DIR = Path() / "data" / "raw"
SAVE_PATH = Path() / "data" / "share-prices.csv"
RAW_FILE_NAME_HANDLE = "share-prices"


################
# Process data #
################

raw_paths = RAW_DATA_DIR.glob(f'{RAW_FILE_NAME_HANDLE}*')
date_pattern = re.compile(DATE_REGEX_PATTERN)

data_raw = []
for path in tqdm.tqdm(raw_paths, desc="Processing raw price files"):
    with open(path, "rb") as file:
        workbook = openpyxl.load_workbook(file, data_only=True)
        sheet = workbook.active

    column_names = [cell.value for cell in sheet[1]]
 
    # Get date from file name
    date = date_pattern.search(path.name).group(1)

    # Convert rows to dicts and add date
    for row in sheet.iter_rows(min_row=2, values_only=True):
        column_names_with_date = column_names + [DATE_COLUMN_NAME]
        row_with_date = row + (date,)
        row_data = dict(zip(column_names_with_date, row_with_date))
        data_raw += [row_data]

# Select relevant columns
data = []
for raw_row in data_raw:
    row = {}
    for key, value in raw_row.items():
        if key not in SELECT_MAP.keys() and key != DATE_COLUMN_NAME:
            continue
        row[SELECT_MAP[key]] = value
    data += [row]

# Check if all rows have the same columns
column_names_set = set(tuple(row.keys()) for row in data)
if len(column_names_set) > 1:
    raise ValueError(f'Not all files have the same columns. Different sets: {column_names_set}')

# Sort by date
data.sort(key=lambda x: x[DATE_COLUMN_NAME])


#############
# Save data #
#############

with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
