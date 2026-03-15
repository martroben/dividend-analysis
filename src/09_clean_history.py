"""
Script to clean instrument history xlsx data from Nasdaq Baltic website and save it as csv.
"""

# standard
import csv
from pathlib import Path
# external
import openpyxl
import tqdm


######################
# Maps and constants #
######################

SELECT_MAP = {
    'Date': "DATE",
    # Last price adjusted for splits
    'Last price adjusted ': "LAST_PRICE_ADJUSTED",
    'Trades': "N_TRADES",
    'Volume':"N_UNITS_TRADED",
    'Turnover': "TURNOVER",
    'Currency': "CURRENCY",
    'ISIN': "ISIN"
}


##########
# Inputs #
##########

RAW_DATA_DIR = Path() / "data" / "raw"
SAVE_PATH = Path() / "data" / "history.csv"
RAW_FILE_NAME_HANDLE = "history"


################
# Process data #
################

raw_paths = RAW_DATA_DIR.glob(f'{RAW_FILE_NAME_HANDLE}*')

data_raw = []
for path in tqdm.tqdm(raw_paths, desc="Processing raw history files"):
    with open(path, "rb") as file:
        workbook = openpyxl.load_workbook(file, data_only=True)
        sheet = workbook.active

    column_names = [cell.value for cell in sheet[1]]
    # Convert rows to dicts
    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_data = dict(zip(column_names, row))
        data_raw += [row_data]

# Select relevant columns
data = []
for raw_row in data_raw:
    row = {}
    for key, value in raw_row.items():
        if key not in SELECT_MAP.keys():
            continue
        row[SELECT_MAP[key]] = value
    data += [row]

# Check if all rows have the same columns
column_names_set = set(tuple(row.keys()) for row in data)
if len(column_names_set) > 1:
    raise ValueError(f'Not all files have the same columns. Different sets: {column_names_set}')

# Sort by ISIN code and date
data.sort(key=lambda x: (x["ISIN"], x["DATE"]))

#############
# Save data #
#############

with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
