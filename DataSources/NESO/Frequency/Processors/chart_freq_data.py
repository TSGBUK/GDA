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
PARQUET_DIR = ROOT / "DataSources" / "NESO" / "Frequency" / "Parquet"
F0_HZ = 50.0  # Nominal system frequency

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
        # Assuming Date column is in the data
        if start_date:
            filters.append(ds.field("Date") >= pd.Timestamp(start_date, tz='UTC'))
        if end_date:
            filters.append(ds.field("Date") <= pd.Timestamp(end_date, tz='UTC'))
    if filters:
        # Combine multiple filters using & operator
        combined_filter = filters[0]
        for f in filters[1:]:
            combined_filter = combined_filter & f
        table = dataset.to_table(filter=combined_filter)
    else:
        table = dataset.to_table()
    df = table.to_pandas()
    df["Date"] = pd.to_datetime(df["Date"])
    return df

# --- Charting Function ------------------------------------------------------
def _time_deltas(df: pd.DataFrame) -> pd.Series:
    """Compute interval lengths between successive samples (seconds)."""
    return df["Date"].diff().dt.total_seconds().fillna(0)


def duration_below(df: pd.DataFrame, threshold: float) -> float:
    """Total seconds where frequency is strictly below *threshold*."""
    deltas = _time_deltas(df)
    mask = df["Value"] < threshold
    return float(deltas[mask].sum())


def duration_above(df: pd.DataFrame, threshold: float) -> float:
    """Total seconds where frequency is strictly above *threshold*."""
    deltas = _time_deltas(df)
    mask = df["Value"] > threshold
    return float(deltas[mask].sum())


def duration_between(df: pd.DataFrame, low: float, high: float) -> float:
    """Seconds where frequency is between low and high inclusive."""
    deltas = _time_deltas(df)
    mask = (df["Value"] >= low) & (df["Value"] <= high)
    return float(deltas[mask].sum())


def plot_frequency_chart(year=None, start_date=None, end_date=None, resample_freq='1min'):
    """
    Plot interactive frequency and RoCoF charts for the specified year or date range.
    Resamples data to reduce size for plotting.
    """
    df = load_data(year=year, start_date=start_date, end_date=end_date)
    if df.empty:
        print("No data found for the specified period.")
        return
    
    # Set Date as index for resampling
    df.set_index("Date", inplace=True)

    # Resample to reduce data points
    df_resampled = df["Value"].resample(resample_freq).mean()

    # Calculate RoCoF (Rate of Change of Frequency) in Hz/s
    # Use the time difference between samples
    time_diff = df_resampled.index.to_series().diff().dt.total_seconds()
    rocof = df_resampled.diff() / time_diff  # Hz/s
    
    # Create subplots: frequency on top, RoCoF on bottom
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=('Grid Frequency', 'Rate of Change of Frequency (RoCoF)')
    )
    
    # Add frequency trace
    fig.add_trace(
        go.Scatter(x=df_resampled.index, y=df_resampled.values,
                   mode='lines', name='Frequency (Hz)', line=dict(color='blue')),
        row=1, col=1
    )

    # Add RoCoF trace
    fig.add_trace(
        go.Scatter(x=rocof.index, y=rocof.values,
                   mode='lines', name='RoCoF (Hz/s)', line=dict(color='red')),
        row=2, col=1
    )

    # Add horizontal lines for frequency thresholds
    fig.add_hline(y=F0_HZ, line_dash="dash", line_color="green", 
                  annotation_text="Nominal 50 Hz", row=1, col=1)
    fig.add_hline(y=49.90, line_dash="dot", line_color="red", 
                  annotation_text="Lower Limit 49.90 Hz", row=1, col=1)
    fig.add_hline(y=50.10, line_dash="dot", line_color="red", 
                  annotation_text="Upper Limit 50.10 Hz", row=1, col=1)
    
    # Add additional frequency thresholds
    fig.add_hline(y=49.80, line_dash="dash", line_color="purple", 
                  annotation_text="Market Response", row=1, col=1)
    fig.add_hline(y=50.20, line_dash="dash", line_color="purple", 
                  annotation_text="Market Response", row=1, col=1)

    # Add RoCoF threshold lines (typical limits are around 0.125-0.5 Hz/s)
    fig.add_hline(y=0.125, line_dash="dot", line_color="orange", 
                  annotation_text="RoCoF Limit 0.125 Hz/s", row=2, col=1)
    fig.add_hline(y=-0.125, line_dash="dot", line_color="orange", 
                  annotation_text="RoCoF Limit -0.125 Hz/s", row=2, col=1)

    # Update layout for interactivity
    fig.update_layout(
        title=f"Grid Frequency and RoCoF Over Time ({resample_freq} averages)",
        hovermode="x unified",
        height=800
    )
    
    # Update y-axes labels
    fig.update_yaxes(title_text="Frequency (Hz)", row=1, col=1)
    fig.update_yaxes(title_text="RoCoF (Hz/s)", row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=1)

    # Show the plot (opens in browser)
    fig.show()


def plot_time_in_band_chart(year=None, start_date=None, end_date=None):
    """Compute and chart how long the grid spends in several frequency bands per year.

    When a specific year is provided we show just that year; otherwise all available
    years are processed.  The result is a grouped bar chart (seconds converted to hours)
    with one bar per band for each year.
    """
    years = [year] if year is not None else list_years()
    if not years:
        print("No parquet data found.")
        return

    thresholds = [49.95, 49.90, 49.80, 49.50, 49.00]
    records = []
    for y in years:
        df = load_data(year=y, start_date=start_date, end_date=end_date)
        if df.empty:
            continue
        rec = {"Year": y}
        for thr in thresholds:
            rec[f"< {thr}"] = duration_below(df, thr) / 3600.0
        rec["> 50"] = duration_above(df, 50.0) / 3600.0
        rec["On target"] = duration_between(df, 49.95, 50.0) / 3600.0
        records.append(rec)
    if not records:
        print("No data found for the requested period.")
        return

    df_plot = pd.DataFrame(records).set_index("Year")
    # transpose for easier plotting: columns become bands
    df_plot = df_plot.transpose()

    fig = go.Figure()
    for band in df_plot.index:
        fig.add_trace(go.Bar(name=band, x=df_plot.columns, y=df_plot.loc[band].values))

    fig.update_layout(
        title="Time-in-band per year (hours)",
        barmode='group',
        xaxis_title='Year',
        yaxis_title='Hours',
        legend_title='Band'
    )
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

    resample_freq = input("Enter resample frequency (e.g., '1min', '10s', default '1min'): ").strip()
    resample_freq = resample_freq if resample_freq else '1min'

    # choose chart type before plotting
    print("\nChart types:")
    print("1) Frequency & RoCoF (default)")
    print("2) Time-in-band durations per year (hours)")
    chart_choice = input("Select chart type [1/2]: ").strip() or "1"
    if chart_choice == "2":
        plot_time_in_band_chart(year=year, start_date=start_date, end_date=end_date)
    else:
        plot_frequency_chart(year=year, start_date=start_date, end_date=end_date, resample_freq=resample_freq)