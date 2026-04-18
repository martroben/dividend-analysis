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
ETF_DATA_SAVE_PATH = Path() / "data" / "plot-data-etf.csv"
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


#####################
# Add missing years #
#####################

# For instruments that have not been on market for all durations of interest, the earliest start year values are inserted.
# This assumes that the cumulative growth is zero for years before the instrument came to the market.
start_year_ticker_combinations = (
    relevant_data
    .select(col("START_YEAR"))
    .unique()
    .join(
        relevant_data
        .select(col("TICKER"))
        .unique(),
        how="cross"
    )
)

missing_start_years = (
    relevant_data
    .sort(col("START_YEAR"))
    # Select values with earliest available start year
    .group_by(col("TICKER"))
    .agg(
        PRICE_YIELD=col("PRICE_YIELD").first(),
        DIVIDEND_YIELD=col("DIVIDEND_YIELD").first()
    )
    .join(
        start_year_ticker_combinations,
        on=col("TICKER"),
        how="right"
    )
    .join(
        relevant_data,
        on=[col("TICKER"), col("START_YEAR")],
        how="anti"
    )
    # Set correct column positions for concatenation
    .select(
        col("TICKER"),
        col("START_YEAR"),
        col("PRICE_YIELD"),
        col("DIVIDEND_YIELD")
    )
)

relevant_data_nulls_added = pl.concat([
    relevant_data,
    missing_start_years
])


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
    relevant_data_nulls_added
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
        RANK=pl.row_index() + 1
    )
    .drop(col("MEAN_RANK"))
)

data_with_rank = (
    relevant_data_nulls_added
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

dividend_rank = (
    data_with_rank
    .group_by(col("TICKER"))
    .agg(
        MEAN_DIVIDEND_YIELD=col("DIVIDEND_YIELD").mean()
    )
    .sort(
        col("MEAN_DIVIDEND_YIELD"),
        descending=True
    )
    .with_columns(
        DIVIDEND_RANK=pl.row_index() + 1
    )
    .drop(col("MEAN_DIVIDEND_YIELD"))
)

data_with_dividend_rank = (
    data_with_rank
    .join(
        dividend_rank,
        on=col("TICKER"),
        how="left"
    )
)


###################################
# Get ranking of comparison ETF-s #
###################################

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
        RANK=pl.row_index() + 1
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
)


##############################
# Add bar relative positions #
##############################

plot_data = (
    data_with_dividend_rank
    .with_columns(
        # base = y position where bar starts
        PRICE_YIELD_BASE=col("DIVIDEND_YIELD"),
        MEAN_ANNUAL_YIELD=(col("PRICE_YIELD") + col("DIVIDEND_YIELD")) / (current_year - col("START_YEAR")),
        MEAN_ANNUAL_DIVIDEND_YIELD=col("DIVIDEND_YIELD") / (current_year - col("START_YEAR"))
    )
    .sort(
        [col("DIVIDEND_RANK"), col("START_YEAR")],
        descending=[True, False]
    )
)

plot_data_etf = (
    comparison_etf_data_with_rank
    .with_columns(
        # base = y position where bar starts
        PRICE_YIELD_BASE=col("DIVIDEND_YIELD"),
        MEAN_ANNUAL_YIELD=(col("PRICE_YIELD") + col("DIVIDEND_YIELD")) / (current_year - col("START_YEAR")),
        MEAN_ANNUAL_DIVIDEND_YIELD=col("DIVIDEND_YIELD") / (current_year - col("START_YEAR"))
    )
    .sort(col("RANK"), descending=True)
)


###########################
# Get inflation plot data #
###########################

plot_data_inflation = (
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
    plot_data.write_csv(file)

ETF_DATA_SAVE_PATH.parent.mkdir(exist_ok=True)
with open(ETF_DATA_SAVE_PATH, "w", newline="") as file:
    plot_data_etf.write_csv(file)

INFLATION_DATA_SAVE_PATH.parent.mkdir(exist_ok=True)
with open(INFLATION_DATA_SAVE_PATH, "w", newline="") as file:
    plot_data_inflation.write_csv(file)
