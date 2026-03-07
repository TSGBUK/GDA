import pandas as pd
import pyarrow.dataset as ds
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "BalancingServices" / "Parquet"

# --- Data Loader ------------------------------------------------------------
def list_years():
    years = []
    for name in os.listdir(PARQUET_DIR):
        if name.startswith("year="):
            try:
                years.append(int(name.split("=")[1]))
            except ValueError:
                pass
    return sorted(years)


def load_data(year=None):
    dataset = ds.dataset(PARQUET_DIR, format="parquet", partitioning="hive")
    if year is not None:
        table = dataset.to_table(filter=(ds.field("year") == int(year)))
    else:
        table = dataset.to_table()
    df = table.to_pandas()
    if "DatetimeUTC" in df.columns:
        df["DatetimeUTC"] = pd.to_datetime(df["DatetimeUTC"], utc=True)
    return df


def cost_columns(df: pd.DataFrame):
    return [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])
            and c not in ["SETT_DATE", "SETT_PERIOD", "DatetimeUTC", "year"]]


def plot_yearly_totals():
    """Stacked bar chart of yearly total costs by category."""
    years = list_years()
    if not years:
        print("No data to plot.")
        return
    records = []
    for y in years:
        df = load_data(y)
        if df.empty:
            continue
        rec = {"Year": y}
        for c in cost_columns(df):
            rec[c] = df[c].sum()
        records.append(rec)
    if not records:
        print("No data available.")
        return
    df_plot = pd.DataFrame(records).set_index("Year")
    fig = go.Figure()
    for col in df_plot.columns:
        fig.add_trace(go.Bar(name=col, x=df_plot.index, y=df_plot[col].values))
    fig.update_layout(
        title="Yearly total balancing service costs",
        barmode='stack',
        xaxis_title='Year',
        yaxis_title='Cost (currency units)',
        legend_title='Category'
    )
    fig.show()


def plot_cost_timeseries(year=None, resample_freq='M'):
    """Plot time series of cost categories for a given year (default monthly).

    If year is None, all data is concatenated.
    """
    df = load_data(year)
    if df.empty:
        print("No data for requested period.")
        return
    df.set_index("DatetimeUTC", inplace=True)
    df_res = df[cost_columns(df)].resample(resample_freq).sum()
    fig = go.Figure()
    for col in df_res.columns:
        fig.add_trace(go.Scatter(x=df_res.index, y=df_res[col], mode='lines', name=col))
    fig.update_layout(
        title=f"Balancing services cost timeseries ({resample_freq} sums)",
        xaxis_title='Date',
        yaxis_title='Cost',
        hovermode='x unified'
    )
    fig.show()


# --- Main -------------------------------------------------------------------
if __name__ == "__main__":
    # simple demo when run directly
    plot_yearly_totals()
