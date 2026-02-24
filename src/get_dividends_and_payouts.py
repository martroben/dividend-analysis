"""
Script to download dividends and payouts data from Nasdaq Baltic website and save it as csv file.
"""

# standard
import csv
import datetime
from pathlib import Path
# external
import openpyxl
import requests


##########
# Inputs #
##########

URL = "https://nasdaqbaltic.com/statistics/en/dividends"
RAW_DATA_PATH = Path() / "data" / "raw" / "dividends_and_payouts.xlsx"
SAVE_PATH = Path() / "data" / "dividends_and_payouts.csv"


######################
# Maps and constants #
######################

DATE_PATTERN = "%Y-%m-%d"

DIVIDENDS_AND_PAYOUTS_NAME_MAP = {
    "Issuer": "COMPANY_NAME",
    "Ticker": "TICKER",
    "Market": "MARKET",
    "Date": "DATE",
    'Event': "EVENT_TYPE",
    "Amount per share": "AMOUNT_PER_SHARE_EUR"
}

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
    "Turnover": "TURNOVER_EUR",
    "Industry": "INDUSTRY_TYPE",
    "Supersector": "SUPERSECTOR_TYPE"
}

MARKET_COUNTRY_MAP = {
    "RIG": "LV",
    "VLN": "LT",
    "TLN": "EE"
}


#########################
# Classes and functions #
#########################

def is_ee_holiday(date: datetime.date) -> bool:
    """
    Can't detect the following:
        - suur reede
        - ülestõusmispühade 1. püha
        - nelipühade 1. püha
    """
    if date.weekday() >= 5:
        return True
    
    public_holidays = [
        datetime.date(date.year, 1, 1),   # 1. jaanuar – uusaasta
        datetime.date(date.year, 2, 24),  # 24. veebruar – iseseisvuspäev
        datetime.date(date.year, 5, 1),   # 1. mai – kevadpüha
        datetime.date(date.year, 6, 23),  # 23. juuni – võidupüha
        datetime.date(date.year, 6, 24),  # 24. juuni – jaanipäev
        datetime.date(date.year, 8, 20),  # 20. august – taasiseseisvumispäev
        datetime.date(date.year, 12, 24), # 24. detsember – jõululaupäev
        datetime.date(date.year, 12, 25), # 25. detsember – esimene jõulupüha
        datetime.date(date.year, 12, 26)  # 26. detsember – teine jõulupüha
    ]
    if date in public_holidays:
        return True
    return False


def is_lv_holiday(date: datetime.date) -> bool:
    """
    Can't detect some of the moving holidays.
    """
    if date.weekday() >= 5:
        return True
    
    public_holidays = [
        datetime.date(date.year, 1, 1),     # 1 January New Year's Day
        datetime.date(date.year, 5, 1),     # 1 May Labour Day
        datetime.date(date.year, 5, 4),     # 4 May Restoration of Independence Day
        datetime.date(date.year, 6, 23),    # 23 June Līgo Day
        datetime.date(date.year, 6, 24),    # 24 June Jāņi Day
        datetime.date(date.year, 11, 18),   # 18 November Proclamation Day of the Republic of Latvia
        datetime.date(date.year, 12, 24),   # 24 December Christmas Eve
        datetime.date(date.year, 12, 25),   # 25 December Christmas Day
        datetime.date(date.year, 12, 26),   # 26 December Second Day of Christmas
        datetime.date(date.year, 12, 31)    # 31 December New Year's Eve
    ]
    if date in public_holidays:
        return True
    return False


def is_lt_holiday(date: datetime.date) -> bool:
    """
    Can't detect some of the moving holidays.
    """
    if date.weekday() >= 5:
        return True
    
    public_holidays = [
        datetime.date(date.year, 1, 1),   # 1 January New Year's Day
        datetime.date(date.year, 2, 16),  # 16 February Day of Restoration of the State of Lithuania (1918)
        datetime.date(date.year, 3, 11),  # 11 March Day of Restoration of Independence of Lithuania (1990)
        datetime.date(date.year, 5, 1),   # 1 May International Workers' Day
        datetime.date(date.year, 6, 24),  # 24 June St. John's Day / Day of Dew
        datetime.date(date.year, 7, 6),   # 6 July Statehood Day
        datetime.date(date.year, 8, 15),  # 15 August Assumption Day
        datetime.date(date.year, 11, 1),  # 1 November All Saints' Day
        datetime.date(date.year, 11, 2),  # 2 November All Souls' Day
        datetime.date(date.year, 12, 24), # 24 December Christmas Eve
        datetime.date(date.year, 12, 25), # 25 December Christmas
        datetime.date(date.year, 12, 26)  # 26 December Christmas
    ]
    if date in public_holidays:
        return True
    return False


def get_previous_business_day(date: datetime.date, country: str) -> datetime.date:
    """
    Given a date, return the previous business day for the given country.
    """
    max_offset = 5

    offset = 0
    while offset < max_offset:
        date -= datetime.timedelta(days=1)
        offset += 1
        if country.lower() == "ee" and not is_ee_holiday(date):
            return date
        elif country.lower() == "lv" and not is_lv_holiday(date):
            return date
        elif country.lower() == "lt" and not is_lt_holiday(date):
            return date

    raise ValueError(f'Could not find a previous business day for {date} in country {country} within {max_offset} days.')


def try_fixing_date(date: float | int) -> datetime.datetime:
    """
    Some dates in input data are not formatted as dates, but given as numeric values.
    This happens because Excel stores dates as the number of days since 1899-12-30. This function tries to convert such numeric values to actual dates.
    """
    fixed_date = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=date)
    if fixed_date.year >= 2015 and fixed_date.year <= datetime.datetime.now().year:
        return fixed_date
    raise ValueError(f'Could not fix date: {date}')


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


####################
# Process raw data #
####################

# Reload data from the Excel file
with open(RAW_DATA_PATH, "rb") as file:
    workbook = openpyxl.load_workbook(file, data_only=True)
    sheet = workbook["Worksheet"]

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

# Add previous business day for each event
for event in dividends_and_payouts_data:
    country = MARKET_COUNTRY_MAP[event["MARKET"]]
    previous_business_day = get_previous_business_day(
        event["DATE"],
        country=country
    )
    event["PREVIOUS_BUSINESS_DAY"] = previous_business_day

column_names += ["PREVIOUS_BUSINESS_DAY"]


###############
# Save as csv #
###############

with open(SAVE_PATH, "w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=column_names)
    writer.writeheader()
    writer.writerows(dividends_and_payouts_data)
