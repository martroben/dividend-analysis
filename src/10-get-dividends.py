"""
Script to analyse dividend returns.
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

DIVIDENDS_AND_PAYOUTS_PATH = Path() / "data" / "dividends-and-payouts.csv"
HISTORY_PATH = Path() / "data" / "history.csv"
SAVE_PATH = Path() / "data" / "dividends.csv"


######################
# Maps and constants #
######################

RELEVANT_EVENTS = ["Dividend ex-date"]


#############
# Load data #
#############

with open(DIVIDENDS_AND_PAYOUTS_PATH) as file:
    dividends_and_payouts_raw = pl.read_csv(
        file,
        schema_overrides={"DATE": pl.Date}
    )

with open(HISTORY_PATH) as file:
    history_raw = pl.read_csv(
        file,
        schema_overrides={"DATE": pl.Date}
    )


################
# Process data #
################

# Keep only events of interest for dividend calculations.
# I.e. skip increasing stock capital and other events that are already reflected in stock prices.
dividends_and_payouts = (
    dividends_and_payouts_raw
    .filter(
        col("EVENT_TYPE").is_in(RELEVANT_EVENTS),
        col("DATE") < datetime.datetime.now()
    )
)

# Get dates for which price data is available immediately before dividend payment
# This is used to calculate dividend yield
dividend_dates = (
    dividends_and_payouts
    .select(
        col("TICKER"),
        DIVIDEND_DATE=col("DATE")
    )
)

price_dates = (
    history_raw
    .select(
        col("TICKER"),
        PRICE_DATE=col("DATE")
    )
)

last_price_dates = (
    dividend_dates
    .join(
        # Cross all prices per TICKER
        price_dates,
        on=col("TICKER"),
        how="left"
    )
    .filter(
        # Keep only crosses where PRICE_DATE is before DIVIDEND_DATE
        col("PRICE_DATE") < col("DIVIDEND_DATE")
    )
    .group_by(
        # For each dividend date...
        col("TICKER"),
        col("DIVIDEND_DATE")
    )
    .agg(
        # ...take the latest valid PRICE_DATE
        LAST_PRICE_DATE=col("PRICE_DATE").max()
    )
)

# Get dividends with corresponding instrument prices
dividends_raw = (
    dividends_and_payouts

    .join(
        last_price_dates,
        left_on=[col("TICKER"), col("DATE")],
        right_on=[col("TICKER"), col("DIVIDEND_DATE")],
        how="left"
    )
    .join(
        history_raw,
        left_on=[col("TICKER"), col("LAST_PRICE_DATE")],
        right_on=[col("TICKER"), col("DATE")],
        how="left"
    )
    .select(
        col("TICKER"),
        col("DATE"),
        col("AMOUNT_PER_SHARE_EUR"),
        col("LAST_PRICE_DATE"),
        col("CURRENCY"),
        LAST_PRICE=col("LAST_PRICE_ADJUSTED")
    )
)

# Check for non-eur prices
non_eur_dividends = (
    dividends_raw
    .filter(col("CURRENCY") != "EUR")
)

if non_eur_dividends.height:
    raise ValueError("There are instruments with prices given in other currency than Euros. Please adjust before using them in the proceeding calculations.")

# Add dividend yields
dividends = (
    dividends_raw
    .drop(col("CURRENCY"))
    .rename(
        {
            "LAST_PRICE": "LAST_PRICE_EUR",
            "AMOUNT_PER_SHARE_EUR": "DIVIDEND_PER_UNIT_EUR"
        }
    )
    .with_columns(
        YIELD=col("DIVIDEND_PER_UNIT_EUR") / col("LAST_PRICE_EUR")
    )
    .sort(
        col("TICKER"),
        col("DATE")
    )
)


#############
# Save data #
#############

SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    dividends.write_csv(file)
