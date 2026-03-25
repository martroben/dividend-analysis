"""
Script to clean instrument history xlsx data from Nasdaq Baltic website and save it as csv.
"""

# standard
import csv
from pathlib import Path
import re
# external
import openpyxl
import tqdm


##########
# Inputs #
##########

RAW_DATA_DIR = Path() / "data" / "raw"
SAVE_PATH = Path() / "data" / "history.csv"
RAW_FILE_NAME_HANDLE = "history"


######################
# Maps and constants #
######################

TICKER_REGEX_PATTERN = rf'{RAW_FILE_NAME_HANDLE}_(.+)\.xlsx'
DATE_PATTERN = "%Y-%m-%d"

TICKER_COLUMN_NAME = "TICKER"
DATE_COLUMN_NAME = "DATE"

SELECT_MAP = {
    "Date": DATE_COLUMN_NAME,
    # Last price adjusted for splits
    "Last price adjusted ": "LAST_PRICE_ADJUSTED",
    "Trades": "N_TRADES",
    "Volume":"N_UNITS_TRADED",
    "Turnover": "TURNOVER",
    "Currency": "CURRENCY",
    "ISIN": "ISIN",
    "TICKER": "TICKER"
}


################
# Process data #
################

raw_paths = RAW_DATA_DIR.glob(f'{RAW_FILE_NAME_HANDLE}*')
ticker_pattern = re.compile(TICKER_REGEX_PATTERN)

data_raw = []
for path in tqdm.tqdm(raw_paths, desc="Processing raw history files"):
    with open(path, "rb") as file:
        workbook = openpyxl.load_workbook(file, data_only=True)
        sheet = workbook.active

    column_names = [cell.value for cell in sheet[1]]

    # Get ticker from file name
    ticker = ticker_pattern.search(path.name).group(1)

    # Convert rows to dicts
    for row in sheet.iter_rows(min_row=2, values_only=True):
        column_names_with_ticker = column_names + [TICKER_COLUMN_NAME]
        row_with_ticker = row + (ticker,)
        row_data = dict(zip(column_names_with_ticker, row_with_ticker))
        data_raw += [row_data]

# Select and format relevant columns
data = []
for row_raw in data_raw:
    row = {}
    for key, value in row_raw.items():
        if key not in SELECT_MAP.keys():
            continue
        row[SELECT_MAP[key]] = value
        # Make sure date is YYYY-mm-dd
    row[DATE_COLUMN_NAME] = row[DATE_COLUMN_NAME].strftime(DATE_PATTERN)
    data += [row]

# Check if all rows have the same columns
column_names_set = set(tuple(row.keys()) for row in data)
if len(column_names_set) > 1:
    raise ValueError(f'Not all files have the same columns. Different sets: {column_names_set}')

# Sort by ticker code and date
data.sort(key=lambda x: (x[TICKER_COLUMN_NAME], x[DATE_COLUMN_NAME]))

#############
# Save data #
#############

SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
