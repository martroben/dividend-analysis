"""
Script to download Estonian consumer price index changes data from the national statistics API.
Adds a column for cumulative change since given year.
"""

# standard
import json
from pathlib import Path
# external
import polars as pl
from polars import col
import requests


##########
# Inputs #
##########

INFLATION_API_URL = "https://andmed.stat.ee/api/v1/en/stat/IA021"
SAVE_PATH = Path() / "data" / "consumer-price-index-change.csv"

API_QUERY_CONSUMER_PRICE_INDEX = {
    "code": "Näitaja",
    "selection": {
        "filter": "item",
        # Select consumer price index change from same month last year
        "values": [
            "1"
        ]
    }
}
API_QUERY_MONTH = {
    "code": "Kuu",
    "selection": {
        "filter": "item",
        # Filter values for December of each year
        "values": [
            "12"
        ]
    }
}
API_QUERY_RESPONSE_FORMAT = {
    "format": "json-stat2"
}


################
# Request data #
################

api_query_data = {
    "query": [
        API_QUERY_CONSUMER_PRICE_INDEX,
        API_QUERY_MONTH
    ],
    "response": API_QUERY_RESPONSE_FORMAT
}

response = requests.post(
    INFLATION_API_URL,
    data=json.dumps(api_query_data)
)

response.raise_for_status()


################
# Process data #
################

years_raw = list(response.json()["dimension"]["Aasta"]["category"]["label"].values())
changes_raw = response.json()["value"]

years = [int(year) for year, change in zip(years_raw, changes_raw) if change]
changes = [change / 100 for change in changes_raw if change]

consumer_price_index_change = pl.DataFrame(
    {
        "YEAR": years,
        "CONSUMER_PRICE_INDEX_CHANGE": changes
    }
)

save_data = (
    consumer_price_index_change
    .with_columns(
        CHANGE_MULTIPLIER=1 + col("CONSUMER_PRICE_INDEX_CHANGE")
    )
    .with_columns(
        CONSUMER_PRICE_INDEX_CHANGE_STARTING_FROM_YEAR=col("CHANGE_MULTIPLIER").cum_prod(reverse=True) - 1
    )
    .drop(col("CHANGE_MULTIPLIER"))
)


#############
# Save data #
#############

SAVE_PATH.parent.mkdir(exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    save_data.write_csv(file)
