"""
Script to prepare data for plotting.
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

PRICE_YIELD_STARTING_FROM_YEAR_PATH = Path() / "data" / "price-yield-starting-from-year.csv"
DIVIDEND_YIELD_STARTING_FROM_YEAR_PATH = Path() / "data" / "dividend-yield-starting-from-year.csv"
ETF_YIELD_STARTING_FROM_YEAR_PATH = Path() / "data" / "etf-yield-starting-from-year.csv"
CONSUMER_PRICE_INDEX_CHANGE_PATH = Path() / "data" / "consumer-price-index-change.csv"

PRICE_AND_DIVIDEND_DATA_SAVE_PATH = Path() / "data" / "plot-data-price-and-dividend.csv"
INFLATION_DATA_SAVE_PATH = Path() / "data" / "plot-data-inflation.csv"
# Durations of years (from starting year) to compare on the plots
RELEVANT_INVESTMENT_DURATIONS_YEARS = [3, 5, 7]
# Number of instruments to plot in addition to the comparison ETF-s
N_INSTRUMENTS_TO_PLOT = 10


#############
# Load data #
#############

with open(PRICE_YIELD_STARTING_FROM_YEAR_PATH) as file:
    price_yield_starting_from_year = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )

with open(DIVIDEND_YIELD_STARTING_FROM_YEAR_PATH) as file:
    dividend_yield_starting_from_year = pl.read_csv(
        file,
        schema_overrides={
            "DATE": pl.Date
        }
    )

with open(ETF_YIELD_STARTING_FROM_YEAR_PATH) as file:
    etf_yield_starting_from_year = pl.read_csv(file)


with open(CONSUMER_PRICE_INDEX_CHANGE_PATH) as file:
    consumer_price_index_change = pl.read_csv(file)


####################################
# Select / filter data of interest #
####################################

current_year = datetime.datetime.now().year
relevant_start_years = [current_year - n_years for n_years in RELEVANT_INVESTMENT_DURATIONS_YEARS]

currently_active_instruments = (
    price_yield_starting_from_year
    .group_by(
        col("TICKER"),
        col("LISTING_EPISODE")
    )
    .agg(
        LATEST_YEAR_WITH_DATA=col("START_YEAR").max()
    )
    .filter(
        col("LATEST_YEAR_WITH_DATA") == current_year
    )
    .select(
        col("TICKER"),
        col("LISTING_EPISODE")
    )
)

dividend_paying_instruments = (
    dividend_yield_starting_from_year
    .group_by(
        col("TICKER"),
        col("LISTING_EPISODE")
    )
    .agg(
        TOTAL_DIVIDEND_EUR=col("DIVIDEND_PER_UNIT_EUR").sum()
    )
    .filter(
        col("TOTAL_DIVIDEND_EUR") > 0
    )
    .select(
        col("TICKER"),
        col("LISTING_EPISODE")
    )
)

relevant_data = (
    price_yield_starting_from_year
    .join(
        dividend_yield_starting_from_year,
        on=[col("TICKER"), col("START_YEAR")],
        how="left"
    )
    # Filter only instruments that have price data on current year
    .join(
        currently_active_instruments,
        on=[col("TICKER"), col("LISTING_EPISODE")],
        how="semi"
    )
    # Filter only instruments that have ever paid dividends
    .join(
        dividend_paying_instruments,
        on=[col("TICKER"), col("LISTING_EPISODE")],
        how="semi"
    )
    # Add listing episode to ticker name if the instrument has more than one listing episode
    .with_columns(
        TICKER=pl.when(
            col("LISTING_EPISODE") == 1
        ).then(
            col("TICKER")
        ).otherwise(
            pl.concat_str([col("TICKER"), col("LISTING_EPISODE")], separator="_")
        )
    )
    # Select relevant years
    .filter(
        col("START_YEAR").is_in(relevant_start_years)
    )
    .rename({
        "PRICE_YIELD_STARTING_FROM_YEAR": "PRICE_YIELD",
        "DIVIDEND_YIELD_STARTING_FROM_YEAR": "DIVIDEND_YIELD"
    })
    .select(
        col("TICKER"),
        col("START_YEAR"),
        col("PRICE_YIELD"),
        col("DIVIDEND_YIELD")
    )
)


###################################################
# Select / filter comparison ETF data of interest #
###################################################

comparison_etf_relevant_data = (
    etf_yield_starting_from_year
    .filter(
        col("START_YEAR").is_in(relevant_start_years)
    )
    .rename({
        "PRICE_YIELD_STARTING_FROM_YEAR": "PRICE_YIELD"
    })
    # Add dividend yield column of type float64 to concatenate with other data later
    .with_columns(
        DIVIDEND_YIELD=pl.lit(0).cast(pl.Float64)
    )
    .select(
        col("TICKER"),
        col("START_YEAR"),
        col("PRICE_YIELD"),
        col("DIVIDEND_YIELD")
    )
)


###############
# Get ranking #
###############

# Set rank by mean rank of the total yield across all start years of interest
rank = (
    relevant_data
    .sort(
        col("START_YEAR"),
        # Sort by total yield
        col("PRICE_YIELD") + col("DIVIDEND_YIELD"),
        descending=[False, True]
    )
    .with_columns(
        RANK=pl.row_index().over(col("START_YEAR"))
    )
    .group_by(col("TICKER"))
    .agg(
        MEAN_RANK=col("RANK").mean()
    )
    .sort(col("MEAN_RANK"))
    .with_columns(
        # Cast to signed integer, so the data can be concatenated with ETF conmparison data with negative ranks
        RANK=pl.row_index().cast(pl.Int8) + 1
    )
    .drop(col("MEAN_RANK"))
)

data_with_rank = (
    relevant_data
    .join(
        rank,
        on=col("TICKER"),
        how="left"
    )
    .filter(
        col("RANK") <= N_INSTRUMENTS_TO_PLOT
    )
    .sort(col("RANK"))
)


###################################
# Get ranking of comparison ETF-s #
###################################

n_comparison_etfs = comparison_etf_relevant_data["TICKER"].unique().count()

etf_rank = (
    comparison_etf_relevant_data
    .sort(
        col("START_YEAR"),
        col("PRICE_YIELD"),
        descending=[False, True]
    )
    .with_columns(
        RANK=pl.row_index().over(col("START_YEAR"))
    )
    .group_by(col("TICKER"))
    .agg(
        MEAN_RANK=col("RANK").mean()
    )
    .sort(col("MEAN_RANK"))
    .with_columns(
        # Cast to signed int, because row_index() returns an uint
        # Make ETF ranks negative numbers so that lowest ETF rank is -1
        RANK=pl.row_index().cast(pl.Int8) - n_comparison_etfs
    )
    .drop(col("MEAN_RANK"))
)

comparison_etf_data_with_rank = (
    comparison_etf_relevant_data
    .join(
        etf_rank,
        on=col("TICKER"),
        how="left"
    )
    .sort(col("RANK"))
)


##############################
# Add bar relative positions #
##############################

price_and_dividend_save_data = (
    pl.concat([
        data_with_rank,
        comparison_etf_data_with_rank
    ])
    .with_columns(
        # base = y position where bar starts
        # top = y position where bar ends
        PRICE_YIELD_BASE=col("DIVIDEND_YIELD"),
        PRICE_YIELD_TOP=col("PRICE_YIELD") + col("DIVIDEND_YIELD")
    )
    .drop(col("PRICE_YIELD"))
)


###########################
# Get inflation plot data #
###########################

inflation_save_data = (
    consumer_price_index_change
    .rename({
        "YEAR": "START_YEAR",
        "CONSUMER_PRICE_INDEX_CHANGE_STARTING_FROM_YEAR": "CUMULATIVE_INFLATION"
    })
    .filter(col("START_YEAR").is_in(relevant_start_years))
    .select(
        col("START_YEAR"),
        col("CUMULATIVE_INFLATION")
    )
)


#############
# Save data #
#############

PRICE_AND_DIVIDEND_DATA_SAVE_PATH.parent.mkdir(exist_ok=True)
with open(PRICE_AND_DIVIDEND_DATA_SAVE_PATH, "w", newline="") as file:
    price_and_dividend_save_data.write_csv(file)

INFLATION_DATA_SAVE_PATH.parent.mkdir(exist_ok=True)
with open(INFLATION_DATA_SAVE_PATH, "w", newline="") as file:
    inflation_save_data.write_csv(file)
