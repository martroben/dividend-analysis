# standard
from pathlib import Path
# external
import plotly
import polars as pl
from polars import col


##########
# Inputs #
##########

PRICE_AND_DIVIDEND_DATA_PATH = Path() / "data" / "plot-data-price-and-dividend.csv"
ETF_DATA_PATH = Path() / "data" / "plot-data-etf.csv"
INFLATION_DATA_PATH = Path() / "data" / "plot-data-inflation.csv"
PLOT_SAVE_PATH = Path() / "result" / "plot.png"

PLOT_TITLE = "Return on 1 euro invested on 1st of January of the given start year"

START_YEARS_TO_PLOT = [2019, 2021, 2023]

ANNOTATIONS = (
    "<u>github.com/martroben/dividend-analysis/</u>"
    "<br>"
    "CC-BY license: Mart Roben"
)

######################
# Maps and constants #
######################

DIVIDENT_BAR_COLOUR = "#628395"
PRICE_BAR_COLOUR = "#cf995f"
INFLATION_LINE_COLOUR = "#96897b"

ANNOTATION_FONT_SIZE = 30


#############
# Load data #
#############

with open(PRICE_AND_DIVIDEND_DATA_PATH) as file:
    price_and_dividend_data_raw = pl.read_csv(file)

with open(ETF_DATA_PATH) as file:
    etf_data_raw = pl.read_csv(file)

with open(INFLATION_DATA_PATH) as file:
    inflation_data_raw = pl.read_csv(file)


############################
# Convert to dict of lists #
############################

# Get a dictionary in the following format:
# {
#     # A dict of lists for each year
#     2019: {
#         "TICKER": ["ticker1", "ticker2", "ticker3" ...],
#         "DIVIDENT_YIELD": [123, 223, 323, ...],
#         ...
#     },
#     2021: {
#         ...
#     }
# }

price_and_dividend_data_raw.columns
etf_data_raw.columns

price_and_dividend_data_combined = pl.concat([
    price_and_dividend_data_raw,
    etf_data_raw]
)

price_and_dividend_data = {}
for start_year in price_and_dividend_data_combined["START_YEAR"].unique():
    single_start_year_data = (
        price_and_dividend_data_combined
        .filter(col("START_YEAR") == start_year)
    )
    price_and_dividend_data[start_year] = single_start_year_data.to_dict(as_series=False)

inflation_data = {}
for start_year in inflation_data_raw["START_YEAR"].unique():
    single_start_year_data = (
        inflation_data_raw
        .filter(col("START_YEAR") == start_year)
    )
    inflation_data[start_year] = single_start_year_data.to_dict(as_series=False)


###################
# Get plot traces #
###################

# Calculate y-axis ranges for each year
year_ranges = {}
for year in START_YEARS_TO_PLOT:
    year_data = price_and_dividend_data[year]

    year_y_max = max(year_data["PRICE_YIELD"] + [1])        # Make sure the max is at least 1
    year_y_top = year_y_max + max(1, year_y_max * 0.30)     # Add 30% padding
    year_ranges[year] = year_y_top

subplot_heights = [year_ranges[year] / max(year_ranges.values()) for year in START_YEARS_TO_PLOT]

figure = plotly.subplots.make_subplots(
    rows=len(START_YEARS_TO_PLOT),
    cols=1,
    shared_xaxes=False,
    vertical_spacing=0.15,
    subplot_titles=[f"start year {year}" for year in START_YEARS_TO_PLOT],
    row_heights=subplot_heights
    # figure=figure
)

traces = {}
for i_row, start_year in enumerate(START_YEARS_TO_PLOT, start=1):
    year_data = price_and_dividend_data[start_year]
    year_inflation_data = inflation_data[start_year]

    if (i_row, 1) not in traces:
        traces[(i_row, 1)] = []
    
    # Add dividend bars
    figure.add_trace(
        plotly.graph_objects.Bar(
            x=year_data["TICKER"],
            y=year_data["DIVIDEND_YIELD"],
            name=f"{start_year} dividend yield",
            marker_color=DIVIDENT_BAR_COLOUR,
            showlegend=False
        ),
        row=i_row,
        col=1
    )

    # Add price yield bars
    figure.add_trace(
        plotly.graph_objects.Bar(
            x=year_data["TICKER"],
            y=year_data["PRICE_YIELD"],
            base=year_data["PRICE_YIELD_BASE"],
            name=f"{year} price yield",
            marker_color=PRICE_BAR_COLOUR,
            # Add text with divident yield and total yield
            text=[
                f"({round(dividend_yield, 3)} | {round(total_yield, 3)})" if is_active else ""
                for dividend_yield, total_yield, is_active in zip(
                    year_data["MEAN_ANNUAL_DIVIDEND_YIELD"],
                    year_data["MEAN_ANNUAL_YIELD"],
                    year_data["IS_ACTIVE"]
                )
            ],
            textposition="outside",
            showlegend=False
        ),
        row=i_row,
        col=1
    )

    # Add inflation line across the entire subplot
    figure.add_shape(
        type="line",
        # From first to last ticker
        x0=year_data["TICKER"][0],
        x1=year_data["TICKER"][-1],
        # Horizontal line
        y0=year_inflation_data["CUMULATIVE_INFLATION"][0],
        y1=year_inflation_data["CUMULATIVE_INFLATION"][0],
        # Subplot references
        xref=f"x{i_row}",
        yref=f"y{i_row}",
        # Overlap in the beginning and end
        x0shift=-1,
        x1shift=1,
        line={
            "color": INFLATION_LINE_COLOUR,
            "width": 4,
            "dash": "dash"
        },
        label={
            # Add the inflation value as label
            "text": round(year_inflation_data["CUMULATIVE_INFLATION"][0], 3),
            "font": {"size": 20},
            "textposition": "end",
            "yanchor": "bottom"
        }
    )

# Add dummy traces for legend
figure.add_trace(
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="markers",
        name="dividend yield" + 10 * " ",
        marker=dict(color=DIVIDENT_BAR_COLOUR, size=30),
        showlegend=True
    ),
    row=1,
    col=1
)

figure.add_trace(
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="markers",
        name="price yield" + 10 * " ",
        marker=dict(color=PRICE_BAR_COLOUR, size=36),
        showlegend=True
    ),
    row=1,
    col=1
)

figure.add_trace(
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="markers",
        name="<span style='font-size: 18pt'>(0.012 | 0.123)</span>  (dividend | total) average annual yield" + 10 * " ",
        marker=dict(color="rgba(0,0,0,0)", size=1),
        showlegend=True
    ),
    row=1,
    col=1
)

figure.add_trace(
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="lines",
        name="inflation",
        line=dict(color=INFLATION_LINE_COLOUR, dash="dash", width=4),
        showlegend=True
    ),
    row=1,
    col=1
)


###################
# Get plot layout #
###################

for row, year in enumerate(START_YEARS_TO_PLOT, start=1):
    figure.update_yaxes(
        range=[0, year_ranges[year]],
        tickmode="linear",
        tick0=0,
        ticksuffix=" EUR",
        gridcolor="gray",
        tickfont={"size": 24},
        dtick=1,
        zeroline=True,
        zerolinecolor="black",
        zerolinewidth=2,
        row=row,
        col=1
    )

figure.update_layout(
    uniformtext={
        # Bar label text size
        "minsize": 20,
        "mode": "show"
    },
    height=800 * len(START_YEARS_TO_PLOT),
    width=2200,
    plot_bgcolor="white",
    bargap=0.4,
    title={
        "text": ANNOTATIONS,
        "font": {"size": 24},
        "yanchor": "top",
        "xanchor": "left",
        "x": 0.01,
        "y": 0.03,
    },
    # title={
    #     "text": PLOT_TITLE,
    #     "font": {"size": 50},
    #     "x": 0.5,
    #     "xanchor": "center"
    # },
    margin={"pad": 20, "t": 300, "l": 200, "b": 300, "r": 200},
    legend={
        "orientation": "h",
        "x": 0.5,
        "y": 1.04,
        "xanchor": "center",
        "yanchor": "bottom",
        "font": {"size": 32}
    }
)

# figure.add_annotation(
#     x=-0.07,
#     y=0.5,
#     xref="paper",
#     yref="paper",
#     text=Y_AXIS_TITLE,
#     showarrow=False,
#     textangle=-90,
#     font={"size": 40},
#     xanchor="center",
#     yanchor="middle"
# )

for annotation in figure.layout.annotations:
    annotation.x = 0
    annotation.xanchor = "left"
    annotation.font.size = ANNOTATION_FONT_SIZE

figure.update_xaxes(
    tickfont={"size": 20},
    type="category"
)


###############
# Save figure #
###############

PLOT_SAVE_PATH.parent.mkdir(exist_ok=True)
plotly.io.write_image(
    figure,
    PLOT_SAVE_PATH,
    format="png"
)

# TODO: arrange by some better logic
