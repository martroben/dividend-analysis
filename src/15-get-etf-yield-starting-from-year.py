"""
Script to get information about cumulative ETF price yields.
We assume ETF was bought in the beginning of some year.
We want to know what is the cumulative yield in price increase starting from this year until the most recent price info.
Cumulative yield is the increase per euro spent.
"""

# standard
from pathlib import Path
# external
import polars as pl
from polars import col


##########
# Inputs #
##########

ETF_PRICES_PATH = Path() / "data" / "etf-prices.csv"
SAVE_PATH = Path() / "data" / "etf-yield-starting-from-year.csv"


#############
# Load data #
#############

with open(ETF_PRICES_PATH) as file:
    etf_prices = pl.read_csv(file)


################
# Process data #
################

save_data = (
    etf_prices
    .sort(
        col("TICKER"),
        col("YEAR")
    )
    .with_columns(
        LATEST_PRICE=col("PRICE").last().over(col("TICKER"))
    )
    .with_columns(
        PRICE_INCREASE_STARTING_FROM_YEAR=col("LATEST_PRICE") - col("PRICE")
    )
    .with_columns(
        PRICE_YIELD_STARTING_FROM_YEAR=col("PRICE_INCREASE_STARTING_FROM_YEAR") / col("PRICE")
    )
    .rename({
        "YEAR": "START_YEAR"
    })
    .drop(
        col("LATEST_PRICE"),
        col("NAME")
    )
)


#############
# save data #
#############

SAVE_PATH.parent.mkdir(exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    save_data.write_csv(file)
