"""
Clean annual prices for both shares and funds.
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

DIVIDENDS_PATH = Path() / "data" / "dividends.csv"
SHARE_PRICES_PATH = Path() / "data" / "share-prices.csv"
FUND_PRICES_PATH = Path() / "data" / "fund-prices.csv"
SAVE_PATH = Path() / "data" / "annual-prices.csv"


######################
# Maps and constants #
######################

SHARE_PRICE_COLUMNS = [
    "AVERAGE_PRICE",
    "OPENING_PRICE",
    "HIGHEST_PRICE",
    "LOWEST_PRICE",
    "LAST_CLOSE_PRICE",
    "LAST_PRICE"
]


#############
# Load data #
#############

with open(DIVIDENDS_PATH) as file:
    dividends_raw = pl.read_csv(file)

with open(SHARE_PRICES_PATH) as file:
    share_prices_raw = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )

with open(FUND_PRICES_PATH) as file:
    fund_prices_raw = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )


#######################
# Process shares data #
#######################

non_eur_share_prices = (
    share_prices_raw
    .filter(col("CURRENCY") != "EUR")
)

if non_eur_share_prices.height:
    raise ValueError("There are shares with prices given in other currency than Euros. Please adjust before using them in the proceeding calculations.")

share_prices = (
    share_prices_raw
    # Drop rows where all price column values are null
    .filter(
        ~pl.all_horizontal([col(column_name).is_null() for column_name in SHARE_PRICE_COLUMNS])
    )
    .with_columns(
        YEAR=col("DATE").dt.year()
    )
)

share_year_ticker_combinations = (
    share_prices
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

share_annual_prices = (
    share_prices
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
        PRICE_EUR=pl.coalesce(col("LAST_CLOSE_PRICE").first(), col("LAST_PRICE").first())
    )
    .join(
        share_year_ticker_combinations,
        on=[col("YEAR"), col("TICKER")],
        how="right"
    )
)

# Some instruments disappear from market for some years and then re-appear
# We add "episode" numbers so that the longitudional data could be compared
share_annual_prices_with_episodes = (
    share_annual_prices
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
        I_LISTING_EPISODE=col("IS_START_OF_STREAK").cum_sum().over("TICKER")
    )
    .filter(col("PRICE_EUR").is_not_null())
    .drop(col("IS_START_OF_STREAK"))
)


#####################
# Process fund data #
#####################

non_eur_fund_prices = (
    fund_prices_raw
    .filter(col("CURRENCY") != "EUR")
)

if non_eur_share_prices.height:
    raise ValueError("There are funds with prices given in other currency than Euros. Please adjust before using them in the proceeding calculations.")

fund_prices = (
    fund_prices_raw
    # Drop rows where all price column values are null
    .filter(
        col("LAST_PRICE").is_not_null()
    )
    .with_columns(
        YEAR=col("DATE").dt.year()
    )
)

fund_year_ticker_combinations = (
    fund_prices
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

fund_annual_prices = (
    fund_prices
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
        PRICE_EUR=col("LAST_PRICE").first()
    )
    .join(
        fund_year_ticker_combinations,
        on=[col("YEAR"), col("TICKER")],
        how="right"
    )
)

# Some instruments disappear from market for some years and then re-appear
# We add "episode" numbers so that the longitudional data could be compared
fund_annual_prices_with_episodes = (
    fund_annual_prices
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
        I_LISTING_EPISODE=col("IS_START_OF_STREAK").cum_sum().over("TICKER")
    )
    .filter(col("PRICE_EUR").is_not_null())
    .drop(col("IS_START_OF_STREAK"))
)


#################################
# Combine shares and funds data #
#################################

annual_prices = pl.concat([
    share_annual_prices_with_episodes,
    fund_annual_prices_with_episodes
])


#############
# Save data #
#############

with open(SAVE_PATH, "w", newline="") as file:
    annual_prices.write_csv(file)
