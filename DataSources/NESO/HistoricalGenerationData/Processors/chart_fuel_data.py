import pandas as pd
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
from pathlib import Path

# --- Constants --------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
CSV_FILE = ROOT / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"

# --- Data Loader ------------------------------------------------------------
def load_data(start_date=None, end_date=None):
    """Load CSV data, optionally filtered by date range."""
    df = pd.read_csv(CSV_FILE, parse_dates=['DATETIME'])
    df.set_index('DATETIME', inplace=True)
    if start_date:
        df = df[df.index >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df.index <= pd.Timestamp(end_date)]
    return df

# --- Charting Function ------------------------------------------------------
def plot_fuel_chart(start_date=None, end_date=None, fuels=None, resample_freq=None):
    """
    Plot interactive fuel generation charts for the specified date range.
    Fuels is a list of column names to plot, e.g., ['GAS', 'COAL', 'NUCLEAR']
    Resample_freq to reduce data points, e.g., '1H', '1D'
    """
    df = load_data(start_date=start_date, end_date=end_date)
    if df.empty:
        print("No data found for the specified period.")
        return
    
    if resample_freq:
        df = df.resample(resample_freq).mean()

    # If no fuels specified, plot key ones
    if not fuels:
        fuels = ['GAS', 'COAL', 'NUCLEAR', 'WIND', 'HYDRO', 'BIOMASS', 'SOLAR']

    # Create subplots: stacked fuels on top, fossil % middle, total generation bottom
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('Fuel Generation (GW)', 'Fossil Generation (% of Total)', 'Total Generation (GW)')
    )
    
    colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'cyan']
    for i, fuel in enumerate(fuels):
        if fuel in df.columns:
            color = colors[i % len(colors)]
            fig.add_trace(
                go.Scatter(x=df.index, y=df[fuel]/1000,  # Convert MW to GW
                           mode='lines', name=f'{fuel} (GW)', line=dict(color=color),
                           fill='tonexty', stackgroup='fuels'),
                row=1, col=1
            )
        else:
            print(f"Column {fuel} not found in data.")

    # Add fossil % trace
    if 'FOSSIL_perc' in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df['FOSSIL_perc'],
                       mode='lines', name='Fossil %', line=dict(color='black')),
            row=2, col=1
        )
    else:
        print("FOSSIL_perc column not found.")

    # Add total generation trace
    if 'GENERATION' in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df['GENERATION']/1000,  # Convert MW to GW
                       mode='lines', name='Total Generation (GW)', line=dict(color='darkblue')),
            row=3, col=1
        )
    else:
        print("GENERATION column not found.")

    # Update layout for interactivity
    fig.update_layout(
        title=f"Fuel Generation Over Time ({'Native' if not resample_freq else resample_freq} resolution)",
        hovermode="x unified",
        height=1000
    )
    
    # Update y-axes labels
    fig.update_yaxes(title_text="Generation (GW)", row=1, col=1)
    fig.update_yaxes(title_text="Fossil %", row=2, col=1)
    fig.update_yaxes(title_text="Total Generation (GW)", row=3, col=1)
    fig.update_xaxes(title_text="DateTime", row=3, col=1)

    # Show the plot (opens in browser)
    fig.show()

# --- Main -------------------------------------------------------------------
if __name__ == "__main__":
    print("Available fuel columns: GAS, COAL, NUCLEAR, WIND, WIND_EMB, HYDRO, IMPORTS, BIOMASS, OTHER, SOLAR, STORAGE, GENERATION, etc.")
    fuels_input = input("Enter fuel types to plot (comma-separated, default key ones): ").strip()
    fuels = [f.strip() for f in fuels_input.split(',')] if fuels_input else None

    start_date = input("Enter start date (YYYY-MM-DD) or press Enter: ").strip()
    end_date = input("Enter end date (YYYY-MM-DD) or press Enter: ").strip()
    start_date = start_date if start_date else None
    end_date = end_date if end_date else None

    resample_freq = input("Enter resample frequency (e.g., '1H', '1D', or press Enter for native): ").strip()
    resample_freq = resample_freq if resample_freq else None

    plot_fuel_chart(start_date=start_date, end_date=end_date, fuels=fuels, resample_freq=resample_freq)