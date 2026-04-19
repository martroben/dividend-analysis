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
PLOT_SAVE_PATH = Path() / "result" / "sample-plot.png"

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

DIVIDEND_BAR_COLOUR = "#628395"
PRICE_BAR_COLOUR = "#cf995f"
INFLATION_LINE_COLOUR = "#96897b"

TITLE_FONT_SIZE = 50
LEGEND_FONT_SIZE = 32
SMALL_LABEL_FONT_SIZE = 20
BIG_LABEL_FONT_SIZE = 24


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

# Add dividend and price yield bars

i_col = 1
traces = {}
for i_row, start_year in enumerate(START_YEARS_TO_PLOT, start=1):
    if (i_row, i_col) not in traces:
        traces[(i_row, i_col)] = []

    year_data = price_and_dividend_data[start_year]
    
    # Add dividend bars
    traces[(i_row, i_col)] += [
        plotly.graph_objects.Bar(
            x=year_data["TICKER"],
            y=year_data["DIVIDEND_YIELD"],
            name=f'{start_year} dividend yield',
            marker_color=DIVIDEND_BAR_COLOUR,
            showlegend=False
        )
    ]
    # Get annual average yield text to add to bars
    annual_average_texts = []
    for dividend_yield, total_yield, is_active in zip(year_data["MEAN_ANNUAL_DIVIDEND_YIELD"], year_data["MEAN_ANNUAL_YIELD"], year_data["IS_ACTIVE"]):
        # Don't add label if the instrument had not been created yet in the current start_year
        if not is_active:
            annual_average_texts += [""]
            continue
        
        text = f'({round(dividend_yield, 3)} | {round(total_yield, 3)})'
        annual_average_texts += [text]

    # Add price yield bars
    traces[(i_row, i_col)] += [
        plotly.graph_objects.Bar(
            x=year_data["TICKER"],
            y=year_data["PRICE_YIELD"],
            base=year_data["PRICE_YIELD_BASE"],
            name=f"{start_year} price yield",
            marker_color=PRICE_BAR_COLOUR,
            # Add text with divident yield and total yield
            text=annual_average_texts,
            textposition="outside",
            showlegend=False
        )
    ]

# Add inflation line across each subplot
i_col = 1
shapes = {}
for i_row, start_year in enumerate(START_YEARS_TO_PLOT, start=1):
    if not (i_row, i_col) in shapes:
        shapes[(i_row, i_col)] = []

    year_inflation_data = inflation_data[start_year]
    inflation_line = {
        "type": "line",
        # From first to last ticker
        "x0": year_data["TICKER"][0],
        "x1": year_data["TICKER"][-1],
        # Horizontal line
        "y0": year_inflation_data["CUMULATIVE_INFLATION"][0],
        "y1": year_inflation_data["CUMULATIVE_INFLATION"][0],
        # Subplot axis references
        "xref": f'x{i_row}',
        "yref": f'y{i_row}',
        # Overlap in the beginning and end
        "x0shift": -1,
        "x1shift": 1,
        "line": {
            "color": INFLATION_LINE_COLOUR,
            "width": 4,
            "dash": "dash"
        },
        "label": {
            # Add the inflation value as label
            "text": round(year_inflation_data["CUMULATIVE_INFLATION"][0], 3),
            "font": {"size": SMALL_LABEL_FONT_SIZE},
            "textposition": "end",
            "yanchor": "bottom"
        }
    }
    shapes[(i_row, i_col)] += [inflation_line]


#####################
# Get legend traces #
#####################

# Use dummy traces for legend

padding_between_legend_items = 10

# Add legend to only the first subplot to avoid duplicate legend items
traces[(1, 1)] += [
    # Dividend yield bar legend
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="markers",
        name="dividend yield" + padding_between_legend_items * " ",
        marker={
            "color": DIVIDEND_BAR_COLOUR,
            "size": LEGEND_FONT_SIZE
        },
        showlegend=True
    ),
    # Price yield bar legend
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="markers",
        name="price yield" + padding_between_legend_items * " ",
        marker={
            "color": PRICE_BAR_COLOUR,
            "size": LEGEND_FONT_SIZE
        },
        showlegend=True
    ),
    # Annual average yield legend
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="markers",
        name="<span style='font-size: 18pt'>(0.012 | 0.123)</span>  (dividend | total) average annual yield" + padding_between_legend_items * " ",
        marker={
            "color": "rgba(0,0,0,0)",
            "size": 1
        },
        showlegend=True
    ),
    # Inflation line legend
    plotly.graph_objects.Scatter(
        x=[None],
        y=[None],
        mode="lines",
        name="inflation",
        line={
            "color": INFLATION_LINE_COLOUR,
            "dash": "dash",
            "width": 4
        },
        showlegend=True
    )
]

###################
# Get plot layout #
###################

layout = plotly.graph_objects.Layout(
    uniformtext={
        # Bar label text size
        "minsize": SMALL_LABEL_FONT_SIZE,
        "mode": "show"
    },
    # Dynamic size based on number of subplots
    height=800 * len(START_YEARS_TO_PLOT),
    width=2200,
    plot_bgcolor="white",
    bargap=0.4,
    # Use title to annotate source instead
    title={
        "text": ANNOTATIONS,
        "font": {"size": BIG_LABEL_FONT_SIZE},
        "yanchor": "top",
        "xanchor": "left",
        "x": 0.01,
        "y": 0.02,
    },
    margin={
        "pad": 20,      # Axis tick label padding
        "t": 400,
        "l": 200,
        "b": 300,
        "r": 200
    },
    legend={
        "orientation": "h",
        "x": 0.5,
        "y": 1.06,
        "xanchor": "center",
        "yanchor": "bottom",
        "font": {"size": LEGEND_FONT_SIZE}
    }
)

base_layout_figure = plotly.graph_objects.Figure(
    layout=layout
)


################
# Get subplots #
################

# Calculate y-axis ranges for each subplot
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
    subplot_titles=[f'<b>START YEAR {year}:</b>' for year in START_YEARS_TO_PLOT],
    row_heights=subplot_heights,
    figure=base_layout_figure
)

# Add traces and shapes
i_col = 1
for i_row, year in enumerate(START_YEARS_TO_PLOT, start=1):
    # Bar traces
    for trace in traces[(i_row, i_col)]:
        figure.add_trace(
            trace,
            row=i_row,
            col=i_col
        )
    # Inflation line shapes
    for shape in shapes[(i_row, i_col)]:
        figure.add_shape(
            shape,
            row=i_row,
            col=i_col
        )

# Update axes for each subplot
i_col = 1
for i_row, year in enumerate(START_YEARS_TO_PLOT, start=1):
    figure.update_yaxes(
        range=[0, year_ranges[year]],
        tickmode="linear",
        tick0=0,
        ticksuffix=" EUR",
        gridcolor="gray",
        tickfont={"size": BIG_LABEL_FONT_SIZE},
        dtick=1,
        zeroline=True,
        zerolinecolor="black",
        zerolinewidth=2,
        row=i_row,
        col=i_col
    )
    figure.update_xaxes(
        tickfont={"size": BIG_LABEL_FONT_SIZE},
        type="category"
    )

# Add annotations
# Set subplot title x postion to left
for annotation in figure.layout.annotations:
    annotation.x = 0
    annotation.xanchor = "left"
    annotation.font.size = BIG_LABEL_FONT_SIZE

# Add plot title as annotation to have more control over its position and styling
title_annotation = {
    "text": PLOT_TITLE,
    "xref": "paper",
    "yref": "paper",
    "xanchor": "center",
    "yanchor": "bottom",
    "x": 0.5,
    "y": 1.14,
    "showarrow": False,
    "font": {"size": TITLE_FONT_SIZE},
    "align": "center"
}
figure.add_annotation(title_annotation)


###############
# Save figure #
###############

PLOT_SAVE_PATH.parent.mkdir(exist_ok=True)
plotly.io.write_image(
    figure,
    PLOT_SAVE_PATH,
    format="png"
)
