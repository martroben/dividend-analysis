"""
Script to download historical ETF prices. Gets the January 1st price for each year.
Script does not include dividend data. Use only for instruments with accumulating dividend strategy.
Uses the yfinance library: https://ranaroussi.github.io/yfinance/
"""

# standard
from pathlib import Path
# external
import polars as pl
from polars import col
import yfinance


##########
# Inputs #
##########

TICKERS = [
    # iShares Core S&P 500 UCITS ETF USD (Acc) (CSPX.AS)
    # https://finance.yahoo.com/quote/CSPX.AS/
    "CSPX.AS",
    # Xtrackers Stoxx Europe 600 UCITS ETF 1C (XSX6.DE)
    # https://finance.yahoo.com/quote/XSX6.DE/
    "XSX6.DE"
]
SAVE_PATH = Path() / "data" / "etf-prices.csv"


#############
# Load data #
#############

data_raw = yfinance.download(
    tickers=TICKERS,
    period="max",
    interval="1mo"
)

metadata_raw = {}
for ticker in TICKERS:
    metadata_raw[ticker] = yfinance.Ticker(ticker).get_history_metadata()


####################
# Process metadata #
####################

metadata = []
for ticker, data in metadata_raw.items():
    ticker_metadata = {
        "TICKER": ticker,
        "NAME": data["longName"],
        "CURRENCY": data["currency"]
    }
    metadata += [ticker_metadata]


################
# Process data #
################

data = (
    # The input Pandas data frame uses multi-index.
    # This results in columns named as ('Close', 'CSPX.AS') for close price etc.
    pl.DataFrame(data_raw.reset_index())
    # Unpivot. Keep ('Date', '') as an index colum and put all the other nasty names to the "variable" column
    .unpivot(
        index="('Date', '')"
    )
    .with_columns(
        # Parse the first part of ('Close', 'CSPX.AS') to the PRICE_TYPE column
        PRICE_TYPE=col("variable")
            .str.replace("^\\(", "")
            .str.replace("\\)$", "")
            .str.split(",")
            .list.get(0)
            .str.strip_chars("' "),
        # Parse the second part of ('Close', 'CSPX.AS') to the TICKER column
        TICKER=col("variable")
            .str.replace("^\\(", "")
            .str.replace("\\)$", "")
            .str.split(",")
            .list.get(1)
            .str.strip_chars("' "),
        YEAR=col("('Date', '')").dt.year(),
        MONTH=col("('Date', '')").dt.month()
    )
    .filter(
        # Filter January close prices
        col("MONTH") == 1,
        col("PRICE_TYPE").str.to_lowercase() == "close",
        col("value").is_not_null()
    )
    .rename({
        "value": "PRICE"
    })
    .join(
        # Add currency and instrument full name from metadata
        pl.DataFrame(metadata),
        on=col("TICKER"),
        how="left"
    )
    .select(
        col("TICKER"),
        col("YEAR"),
        col("PRICE"),
        col("CURRENCY"),
        col("NAME")
    )
    .sort(
        col("TICKER"),
        col("YEAR")
    )
)


#############
# Save data #
#############

SAVE_PATH.parent.mkdir(exist_ok=True)
with open(SAVE_PATH, "w", newline="") as file:
    data.write_csv(file)
