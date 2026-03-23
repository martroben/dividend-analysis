"""
Script to clean dividends and payouts xlsx data from Nasdaq Baltic website and save it as csv.
"""

# standard
import csv
import datetime
from pathlib import Path
# external
import openpyxl


##########
# Inputs #
##########

URL = "https://nasdaqbaltic.com/statistics/en/dividends"
RAW_DATA_PATH = Path() / "data" / "raw" / "dividends-and-payouts.xlsx"
SAVE_PATH = Path() / "data" / "dividends-and-payouts.csv"


######################
# Maps and constants #
######################

DIVIDENDS_AND_PAYOUTS_NAME_MAP = {
    "Issuer": "NAME",
    "Ticker": "TICKER",
    "Market": "MARKET",
    "Date": "DATE",
    'Event': "EVENT_TYPE",
    "Amount per share": "AMOUNT_PER_SHARE_EUR"
}

MARKET_COUNTRY_MAP = {
    "RIG": "LV",
    "VLN": "LT",
    "TLN": "EE"
}


#########################
# Classes and functions #
#########################

def try_fixing_date(date: float | int) -> datetime.datetime:
    """
    Some dates in input data are not formatted as dates, but given as numeric values.
    This happens because Excel stores dates as the number of days since 1899-12-30. This function tries to convert such numeric values to actual dates.
    """
    fixed_date = datetime.date(1899, 12, 30) + datetime.timedelta(days=date)
    if fixed_date.year >= 2015 and fixed_date.year <= datetime.datetime.now().year:
        return fixed_date
    raise ValueError(f'Could not fix date: {date}')


#################
# Load raw data #
#################

with open(RAW_DATA_PATH, "rb") as file:
    workbook = openpyxl.load_workbook(file, data_only=True)
    sheet = workbook["Worksheet"]


####################
# Process raw data #
####################

header_row = sheet[1]
column_names = [DIVIDENDS_AND_PAYOUTS_NAME_MAP[cell.value] for cell in header_row]

# Get data as a list of dicts
data_raw = []
for row in sheet.iter_rows(min_row=2, values_only=True):
    row_data = dict(zip(column_names, row))
    data_raw += [row_data]

# Remove rows where all values are empty (e.g. due to formatting issues in the original Excel file)
dividends_and_payouts_data = [event for event in data_raw if any(tuple(event.values()))]

# Try fixing event dates
for event in dividends_and_payouts_data:
    if not isinstance(event["DATE"], datetime.datetime):
        event["DATE"] = try_fixing_date(event["DATE"])

# Convert all date fields to date, not datetime
for event in dividends_and_payouts_data:
    event["DATE"] = datetime.date(event["DATE"].year, event["DATE"].month, event["DATE"].day)

# Fix Ignitis gamyba and Telia Lietuva missing ticker
know_missing_ticker_values = {
    "Ignitis gamyba": "LNR1L",
    "Telia Lietuva": "TEL1L"
}
for event in dividends_and_payouts_data:
    if event["TICKER"]:
        continue
    if ticker := know_missing_ticker_values.get(event["NAME"]):
        event["TICKER"] = ticker
    else:
        raise ValueError(f'found event with no ticker value given: {event}')


###############
# Save as csv #
###############

SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=column_names)
    writer.writeheader()
    writer.writerows(dividends_and_payouts_data)
