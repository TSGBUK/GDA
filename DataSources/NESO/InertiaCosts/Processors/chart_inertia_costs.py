import os
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pyarrow.dataset as ds


ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "InertiaCosts" / "Parquet"


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
    return df


def plot_daily_timeseries(year=None, resample_freq="1D"):
    df = load_data(year)
    if df.empty:
        print("No data for requested period.")
        return

    df = df.sort_values("DatetimeUTC")
    df = df.set_index("DatetimeUTC")

    if resample_freq:
        series = df["Cost_per_GVAs"].resample(resample_freq).mean()
        title = f"Inertia cost time series ({resample_freq} average)"
    else:
        series = df["Cost_per_GVAs"]
        title = "Inertia cost time series"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=series.index,
            y=series.values,
            mode="lines",
            name="Cost_per_GVAs",
        )
    )
    fig.update_layout(
        title=title,
        xaxis_title="Date",
        yaxis_title="Cost per GVAs",
        hovermode="x unified",
    )
    fig.show()


def plot_yearly_summary():
    years = list_years()
    if not years:
        print("No data to plot.")
        return

    rows = []
    for year in years:
        df = load_data(year)
        if df.empty:
            continue
        costs = df["Cost_per_GVAs"]
        rows.append(
            {
                "Year": year,
                "Average": costs.mean(),
                "Maximum": costs.max(),
                "Total": costs.sum(),
            }
        )

    if not rows:
        print("No data to plot.")
        return

    summary = pd.DataFrame(rows)

    fig = go.Figure()
    fig.add_trace(go.Bar(name="Average", x=summary["Year"], y=summary["Average"]))
    fig.add_trace(go.Bar(name="Maximum", x=summary["Year"], y=summary["Maximum"]))
    fig.add_trace(go.Bar(name="Total", x=summary["Year"], y=summary["Total"]))
    fig.update_layout(
        title="Inertia cost yearly summary",
        barmode="group",
        xaxis_title="Year",
        yaxis_title="Cost per GVAs",
    )
    fig.show()


if __name__ == "__main__":
    years = list_years()
    if not years:
        print("No parquet data found. Run parquet_data_conversion.py first.")
    else:
        plot_yearly_summary()
        plot_daily_timeseries(year=years[-1], resample_freq="1D")
