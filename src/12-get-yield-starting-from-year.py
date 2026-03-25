"""
Script to get information about cumulative price and dividend yields.
We assume stock was bought in the beginning of some year.
We want to know what is the cumulative yield in price increase and dividends starting from this year until the most recent price info.
Cumulative yield is the increase per euro spent.
"""

# standard
import datetime
from pathlib import Path
# external
import polars as pl
from polars import col

##########
# Inputs #
##########

ANNUAL_PRICES_PATH = Path() / "data" / "annual-prices.csv"
DIVIDENDS_PATH = Path() / "data" / "dividends.csv"

PRICE_YIELD_SAVE_PATH = Path() / "data" / "price-yield-starting-from-year.csv"
DIVIDEND_YIELD_SAVE_PATH = Path() / "data" / "dividend-yield-starting-from-year.csv"


#############
# Load data #
#############

with open(ANNUAL_PRICES_PATH) as file:
    annual_prices = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )

with open(DIVIDENDS_PATH) as file:
    dividends = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )


######################
# Process price data #
######################

price_yield_starting_from_year = (
    annual_prices
    # Get increase of price from this year to the latest year with available data
    # Current year increase is part of the increase, because the price used is from the first business day of the year
    .sort(
        col("TICKER"),
        col("YEAR")
    )
    .with_columns(
        LATEST_PRICE_EUR=col("PRICE_EUR").last().over(col("TICKER"), col("LISTING_EPISODE"))
    )
    .with_columns(
        PRICE_INCREASE_STARTING_FROM_YEAR_EUR=col("LATEST_PRICE_EUR") - col("PRICE_EUR")
    )
    .with_columns(
        PRICE_YIELD_STARTING_FROM_YEAR=col("PRICE_INCREASE_STARTING_FROM_YEAR_EUR") / col("PRICE_EUR")
    )
    .rename({
        "YEAR": "START_YEAR"
    })
    .drop(
        col("LATEST_PRICE_EUR")
    )
)


#########################
# Process dividend data #
#########################

year_ticker_combinations = (
    annual_prices
    .select(
        col("TICKER"),
        col("YEAR"),
        col("LISTING_EPISODE")
    )
)

current_year = datetime.datetime.now().year

# For each year, calculate how much future dividend per stock is received starting from this year
cumulative_dividend_starting_from_year = (
    dividends
    # Aggregate by year
    .with_columns(
        YEAR=col("DATE").dt.year()
    )
    # Set current year dividend to 0 (because some companies might have not yet paid current year's dividend)
    .with_columns(
        DIVIDEND_PER_UNIT_EUR=pl.when(
            col("YEAR") == current_year
        )
        .then(0)
        .otherwise(col("DIVIDEND_PER_UNIT_EUR"))
    )
    # Add years with zero dividends
    .join(
        year_ticker_combinations,
        on=[col("YEAR"), col("TICKER")],
        how="right"
    )
    .group_by(
        col("TICKER"),
        col("YEAR"),
        col("LISTING_EPISODE")
    )
    .agg(
        DIVIDEND_PER_UNIT_EUR=col("DIVIDEND_PER_UNIT_EUR").sum().fill_null(0)
    )
    # Get cumulative future dividend if stock was bought in time to include all current year dividend payouts.
    .sort(
        col("TICKER"),
        col("YEAR")
    )
    .with_columns(
        CUMULATIVE_DIVIDEND_PER_UNIT_STARTING_FROM_YEAR_EUR=col("DIVIDEND_PER_UNIT_EUR").cum_sum(reverse=True).over(col("TICKER"), col("LISTING_EPISODE"))
    )
)

dividend_yield_starting_from_year = (
    cumulative_dividend_starting_from_year
    .join(
        annual_prices,
        on=[col("TICKER"), col("YEAR"), col("LISTING_EPISODE")],
        how="left"
    )
    .rename({
        "YEAR": "START_YEAR"
    })
    .with_columns(
        DIVIDEND_YIELD_STARTING_FROM_YEAR=col("CUMULATIVE_DIVIDEND_PER_UNIT_STARTING_FROM_YEAR_EUR") / col("PRICE_EUR")
    )
)


#############
# Save data #
#############

PRICE_YIELD_SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(PRICE_YIELD_SAVE_PATH, "w", newline="") as file:
    price_yield_starting_from_year.write_csv(file)

DIVIDEND_YIELD_SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(DIVIDEND_YIELD_SAVE_PATH, "w", newline="") as file:
    dividend_yield_starting_from_year.write_csv(file)
