import pandas as pd
import pyarrow.dataset as ds
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "DemandData" / "Parquet"

# --- Data Loader ------------------------------------------------------------
def list_years():
    years = []
    if not os.path.isdir(PARQUET_DIR):
        return years
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
        df.set_index("DatetimeUTC", inplace=True)
    return df


def plot_nd_tsd(year=None, resample_freq='1H'):
    """Plot ND and TSD time series for a given year or all data."""
    df = load_data(year)
    if df.empty:
        print("No data for requested period.")
        return
    cols = [c for c in ['ND', 'TSD'] if c in df.columns]
    if not cols:
        print("No ND/TSD columns found.")
        return
    df_res = df[cols].resample(resample_freq).mean()
    fig = go.Figure()
    for c in df_res.columns:
        fig.add_trace(go.Scatter(x=df_res.index, y=df_res[c], mode='lines', name=c))
    fig.update_layout(title=f"ND & TSD ({resample_freq} averages)", xaxis_title='Date', yaxis_title='MW')
    fig.show()


def plot_monthly_demand(year):
    """Bar chart showing average ND/TSD by month for a given year."""
    df = load_data(year)
    if df.empty:
        print("No data for year", year)
        return
    df['Month'] = df.index.month
    cols = [c for c in ['ND', 'TSD'] if c in df.columns]
    monthly = df.groupby('Month')[cols].mean()
    fig = go.Figure()
    for c in monthly.columns:
        fig.add_trace(go.Bar(x=monthly.index, y=monthly[c], name=c))
    fig.update_layout(title=f"Monthly avg demand for {year}", xaxis_title='Month', yaxis_title='MW', barmode='group')
    fig.show()


# --- Main -------------------------------------------------------------------
if __name__ == "__main__":
    yrs = list_years()
    if yrs:
        plot_nd_tsd(year=yrs[-1])
