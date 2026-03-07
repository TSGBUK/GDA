import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "Inertia" / "Parquet"
F0_HZ = 50.0  # Nominal system frequency
GRID_CAPACITY_GW = 50.0  # Assumed UK system size

# --- Data Loader ------------------------------------------------------------
def list_years():
    """Find available years in the parquet dataset (Hive partitioned)."""
    years = []
    for name in os.listdir(PARQUET_DIR):
        if name.startswith("year="):
            years.append(int(name.split("=")[1]))
    return sorted(years)

def load_data(year=None, start_date=None, end_date=None):
    """Load parquet data using pyarrow.dataset, optionally filtered by year or date range."""
    dataset = ds.dataset(PARQUET_DIR, format="parquet", partitioning="hive")
    filters = []
    if year is not None:
        filters.append(ds.field("year") == int(year))
    if start_date or end_date:
        # Assuming DatetimeUTC column is in the data
        if start_date:
            filters.append(ds.field("DatetimeUTC") >= pd.Timestamp(start_date, tz='UTC'))
        if end_date:
            filters.append(ds.field("DatetimeUTC") <= pd.Timestamp(end_date, tz='UTC'))
    if filters:
        # Combine multiple filters using & operator
        combined_filter = filters[0]
        for f in filters[1:]:
            combined_filter = combined_filter & f
        table = dataset.to_table(filter=combined_filter)
    else:
        table = dataset.to_table()
    df = table.to_pandas()
    df["Date"] = pd.to_datetime(df["DatetimeUTC"])
    return df

# --- Conversions ------------------------------------------------------------
def inertia_to_units(gvas):
    """Return inertia expressed in multiple units."""
    if pd.isna(gvas):
        return {"GVAs": None, "MWs": None, "MW_equiv": None, "PctCapacity": None}

    # 1 GVA·s = 1000 MW·s
    mw_s = gvas * 1000.0

    # MW imbalance for 1 Hz/s RoCoF
    mw_equiv = (2 * mw_s / F0_HZ)

    # Fraction of system capacity
    pct_capacity = 100.0 * mw_equiv / (GRID_CAPACITY_GW * 1000)

    return {
        "GVAs": round(gvas, 2),
        "MWs": round(mw_s, 0),
        "MW_equiv": round(mw_equiv, 0),
        "PctCapacity": round(pct_capacity, 2)
    }

# --- Charting Function ------------------------------------------------------
def plot_inertia_chart(year=None, start_date=None, end_date=None, resample_freq='1H', unit='GVAs'):
    """
    Plot interactive inertia charts for the specified year or date range.
    Resamples data to reduce size for plotting.
    Unit can be 'GVAs', 'MWs', 'MW_equiv', 'PctCapacity'
    """
    df = load_data(year=year, start_date=start_date, end_date=end_date)
    if df.empty:
        print("No data found for the specified period.")
        return
    
    # Set Date as index for resampling
    df.set_index("Date", inplace=True)

    # Resample to reduce data points
    df_resampled_outturn = df["Outturn Inertia"].resample(resample_freq).mean()
    df_resampled_market = df["Market Provided Inertia"].resample(resample_freq).mean()

    # Calculate difference
    df_resampled_diff = df_resampled_outturn - df_resampled_market

    # Convert to selected unit
    if unit == 'MWs':
        df_resampled_outturn = df_resampled_outturn * 1000.0
        df_resampled_market = df_resampled_market * 1000.0
        df_resampled_diff = df_resampled_diff * 1000.0
    elif unit == 'MW_equiv':
        df_resampled_outturn = (2 * df_resampled_outturn * 1000.0) / F0_HZ
        df_resampled_market = (2 * df_resampled_market * 1000.0) / F0_HZ
        df_resampled_diff = (2 * df_resampled_diff * 1000.0) / F0_HZ
    elif unit == 'PctCapacity':
        mw_equiv_outturn = (2 * df_resampled_outturn * 1000.0) / F0_HZ
        mw_equiv_market = (2 * df_resampled_market * 1000.0) / F0_HZ
        mw_equiv_diff = (2 * df_resampled_diff * 1000.0) / F0_HZ
        df_resampled_outturn = 100.0 * mw_equiv_outturn / (GRID_CAPACITY_GW * 1000)
        df_resampled_market = 100.0 * mw_equiv_market / (GRID_CAPACITY_GW * 1000)
        df_resampled_diff = 100.0 * mw_equiv_diff / (GRID_CAPACITY_GW * 1000)

    # Create subplot for outturn and market inertia
    fig = make_subplots(
        rows=1, cols=1,
        subplot_titles=(f'Grid Inertia ({unit})',)
    )
    
    # Add outturn inertia trace
    fig.add_trace(
        go.Scatter(x=df_resampled_outturn.index, y=df_resampled_outturn.values,
                   mode='lines', name=f'Outturn Inertia ({unit})', line=dict(color='blue')),
        row=1, col=1
    )

    # Add market provided inertia trace
    fig.add_trace(
        go.Scatter(x=df_resampled_market.index, y=df_resampled_market.values,
                   mode='lines', name=f'Market Provided Inertia ({unit})', line=dict(color='green')),
        row=1, col=1
    )

    # Add difference trace
    fig.add_trace(
        go.Scatter(x=df_resampled_diff.index, y=df_resampled_diff.values,
                   mode='lines', name=f'Inertia Difference (Outturn - Market, {unit})', line=dict(color='red')),
        row=1, col=1
    )

    # Update layout for interactivity
    fig.update_layout(
        title=f"Grid Inertia Over Time ({resample_freq} averages, {unit})",
        hovermode="x unified",
        height=600
    )
    
    # Update y-axes labels
    fig.update_yaxes(title_text=f"Inertia ({unit})", row=1, col=1)
    fig.update_xaxes(title_text="Date", row=1, col=1)

    # Show the plot (opens in browser)
    fig.show()

# --- Main -------------------------------------------------------------------
if __name__ == "__main__":
    years = list_years()
    if not years:
        print("No parquet data found.")
        exit()

    print(f"Available years: {years}")
    year_input = input("Enter year to plot (or press Enter for all): ").strip()
    year = int(year_input) if year_input else None

    start_date = input("Enter start date (YYYY-MM-DD) or press Enter: ").strip()
    end_date = input("Enter end date (YYYY-MM-DD) or press Enter: ").strip()
    start_date = start_date if start_date else None
    end_date = end_date if end_date else None

    resample_freq = input("Enter resample frequency (e.g., '1H', '1D', default '1H'): ").strip()
    resample_freq = resample_freq if resample_freq else '1H'

    unit = input("Enter unit ('GVAs', 'MWs', 'MW_equiv', 'PctCapacity', default 'GVAs'): ").strip()
    unit = unit if unit in ['GVAs', 'MWs', 'MW_equiv', 'PctCapacity'] else 'GVAs'

    plot_inertia_chart(year=year, start_date=start_date, end_date=end_date, resample_freq=resample_freq, unit=unit)