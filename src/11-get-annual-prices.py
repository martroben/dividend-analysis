"""
Get annual prices from history data.
Add episode numbers to distinguish between periods when the same instrument has been on market and then disappeared.
"""

# standard
from pathlib import Path
# external
import polars as pl
from polars import col


##########
# Inputs #
##########

HISTORY_PATH = Path() / "data" / "history.csv"
SAVE_PATH = Path() / "data" / "annual-prices.csv"


#############
# Load data #
#############

with open(HISTORY_PATH) as file:
    history_raw = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )


################
# Process data #
################

non_eur_prices = (
    history_raw
    .filter(col("CURRENCY") != "EUR")
)

if non_eur_prices.height:
    raise ValueError("There are instruments with prices given in other currency than Euros. Please adjust before using them in the proceeding calculations.")

history = (
    history_raw
    .with_columns(
        YEAR=col("DATE").dt.year()
    )
    .filter(
        col("LAST_PRICE_ADJUSTED").is_not_null()
    )
)

year_ticker_combinations = (
    history
    .group_by(
        col("TICKER")
    )
    .agg(
        MIN_YEAR=col("YEAR").min(),
        MAX_YEAR=col("YEAR").max()
    )
    .with_columns(
        YEAR=pl.int_ranges(col("MIN_YEAR"), col("MAX_YEAR") + 1)
    )
    .explode(col("YEAR"))
    .select(
        col("YEAR"),
        col("TICKER")
    )
)

annual_prices = (
    history
    .sort(
        col("TICKER"),
        col("DATE")
    )
    .group_by(
        col("TICKER"),
        col("YEAR")
    )
    .agg(
        # Set annual price as the price of the first day of the year
        PRICE_EUR=col("LAST_PRICE_ADJUSTED").first()
    )
    .join(
        year_ticker_combinations,
        on=[col("YEAR"), col("TICKER")],
        how="right"
    )
)

# Some instruments disappear from market for some years and then re-appear
# We add "episode" numbers so that the longitudional data could be compared
annual_prices_with_episodes = (
    annual_prices
    .sort(
        col("TICKER"),
        col("YEAR")
    )
    .with_columns(
        IS_START_OF_STREAK=(
            # start of streak = row where price is not null, but previous year price was null
            col("PRICE_EUR").is_not_null() &
            col("PRICE_EUR").shift().is_null()
        ).over("TICKER")
    )
    .with_columns(
        LISTING_EPISODE=col("IS_START_OF_STREAK").cum_sum().over("TICKER")
    )
    .filter(col("PRICE_EUR").is_not_null())
    .drop(col("IS_START_OF_STREAK"))
)


#############
# Save data #
#############

SAVE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    annual_prices_with_episodes.write_csv(file)
