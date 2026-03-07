import pandas as pd
import pyarrow.dataset as ds
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
import os
import warnings
from pathlib import Path
warnings.filterwarnings('ignore')

# --- Paths ------------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
WEATHER_PARQUET_DIR = ROOT / "DataSources" / "Weather" / "Parquet"
GENERATION_CSV = ROOT / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"

# --- Data Loading -----------------------------------------------------------
def list_years():
    """Find available years in the weather parquet dataset."""
    years = []
    for name in os.listdir(WEATHER_PARQUET_DIR):
        if name.startswith("year="):
            years.append(int(name.split("=")[1]))
    return sorted(years)

def load_weather_data(years=None, start_date=None, end_date=None):
    """Load weather data from parquet, optionally filtered by years or date range."""
    dataset = ds.dataset(WEATHER_PARQUET_DIR, format="parquet", partitioning="hive")

    filters = []
    if years:
        # Filter by multiple years
        year_filters = [ds.field("year") == int(year) for year in years]
        combined_filter = year_filters[0]
        for f in year_filters[1:]:
            combined_filter = combined_filter | f
        filters.append(combined_filter)

    if start_date or end_date:
        # Assuming Date column is in the data
        if start_date:
            filters.append(ds.field("Date") >= pd.Timestamp(start_date))
        if end_date:
            filters.append(ds.field("Date") <= pd.Timestamp(end_date))

    if filters:
        # Combine multiple filters using & operator
        combined_filter = filters[0]
        for f in filters[1:]:
            combined_filter = combined_filter & f
        table = dataset.to_table(filter=combined_filter)
    else:
        table = dataset.to_table()

    df = table.to_pandas()
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date').sort_index()
    return df[['Solar_Radiation_W_m2']]

def load_generation_data(start_date=None, end_date=None, columns=['SOLAR']):
    """Load generation data from CSV, optionally filtered by date range and columns."""
    df = pd.read_csv(GENERATION_CSV, parse_dates=['DATETIME'])
    df = df.set_index('DATETIME').sort_index()

    # Filter date range if specified
    if start_date:
        df = df[df.index >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df.index <= pd.Timestamp(end_date)]

    return df[columns]

def merge_datasets(weather_df, generation_df):
    """Merge weather and generation data on datetime index."""
    # Both datasets should have datetime index
    merged = pd.merge_asof(
        weather_df.sort_index(),
        generation_df.sort_index(),
        left_index=True,
        right_index=True,
        tolerance=pd.Timedelta('1h')  # Allow 1 hour tolerance for matching
    )

    # Drop rows with missing data
    merged = merged.dropna()

    return merged

# --- Analysis Functions -----------------------------------------------------
def correlation_analysis(df):
    """Calculate correlation between solar radiation and generation."""
    corr = df['Solar_Radiation_W_m2'].corr(df['SOLAR'])
    print(".4f")

    return corr

def solar_radiation_bins_analysis(df, bins=20):
    """Analyze generation by solar radiation bins."""
    df_copy = df.copy()
    df_copy['Solar_Bin'] = pd.cut(df_copy['Solar_Radiation_W_m2'], bins=bins)

    bin_stats = df_copy.groupby('Solar_Bin').agg({
        'SOLAR': ['count', 'mean', 'std', 'min', 'max'],
        'Solar_Radiation_W_m2': ['mean', 'min', 'max']
    }).round(2)

    print("\n=== Solar Radiation Bins Analysis ===")
    print(bin_stats)

    return bin_stats

def create_scatter_plot(df, sample_size=None):
    """Create scatter plot of solar radiation vs generation."""
    plot_df = df.copy()
    if sample_size and len(plot_df) > sample_size:
        plot_df = plot_df.sample(n=sample_size, random_state=42)

    # Calculate trend line
    z = np.polyfit(plot_df['Solar_Radiation_W_m2'], plot_df['SOLAR'], 2)
    p = np.poly1d(z)
    x_trend = np.linspace(plot_df['Solar_Radiation_W_m2'].min(),
                          plot_df['Solar_Radiation_W_m2'].max(), 100)
    y_trend = p(x_trend)

    # Create figure
    fig = go.Figure()

    # Add scatter plot
    fig.add_trace(go.Scatter(
        x=plot_df['Solar_Radiation_W_m2'],
        y=plot_df['SOLAR'],
        mode='markers',
        name='Data Points',
        marker=dict(color='orange', size=2, opacity=0.6),
        hovertemplate='Solar Radiation: %{x:.1f} W/m²<br>Generation: %{y:.0f} MW'
    ))

    # Add trend line
    fig.add_trace(go.Scatter(
        x=x_trend,
        y=y_trend,
        mode='lines',
        name='.2f',
        line=dict(color='red', dash='dash', width=3)
    ))

    # Update layout
    fig.update_layout(
        title='Solar Radiation vs Solar Generation (Native Resolution, No Smoothing)',
        xaxis_title='Solar Radiation (W/m²)',
        yaxis_title='Solar Generation (MW)',
        hovermode='closest',
        showlegend=True
    )

    # Show the plot
    fig.show()

def create_density_plot(df):
    """Create density plot for solar radiation vs generation."""
    # Create 2D histogram data
    x = df['Solar_Radiation_W_m2']
    y = df['SOLAR']

    # Create figure with subplots
    fig = make_subplots(
        rows=1, cols=1,
        subplot_titles=('Solar Radiation vs Generation Density',)
    )

    # Add 2D histogram
    fig.add_trace(go.Histogram2d(
        x=x,
        y=y,
        colorscale='Oranges',
        nbinsx=50,
        nbinsy=50,
        hovertemplate='Solar Radiation: %{x:.1f} W/m²<br>Generation: %{y:.0f} MW<br>Count: %{z}'
    ))

    # Update layout
    fig.update_layout(
        title='Solar Radiation vs Solar Generation Density',
        xaxis_title='Solar Radiation (W/m²)',
        yaxis_title='Solar Generation (MW)',
        hovermode='closest'
    )

    # Show the plot
    fig.show()

def time_series_analysis(df, resample_freq='1D', start_date=None, end_date=None):
    """Analyze time series relationship across the full date range."""
    if df.empty:
        print("No data available for time series analysis")
        return

    # Load GAS generation data for the same period (as comparison to solar)
    gas_df = load_generation_data(start_date=start_date, end_date=end_date, columns=['GAS', 'GENERATION'])

    # Resample to reduce data points for better visualization
    df_resampled = df.resample(resample_freq).mean()
    gas_resampled = gas_df.resample(resample_freq).mean()
    generation_resampled = gas_df[['GENERATION']].resample(resample_freq).mean()

    # Create subplots - now 4 rows
    fig = make_subplots(
        rows=4, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=('Solar Radiation (W/m²)', 'Solar Generation (MW)', 'GAS Generation (MW)', 'Total Generation (MW)')
    )

    # Add solar radiation trace
    fig.add_trace(go.Scatter(
        x=df_resampled.index,
        y=df_resampled['Solar_Radiation_W_m2'],
        mode='lines',
        name='Solar Radiation (W/m²)',
        line=dict(color='orange', width=2),
        hovertemplate='Time: %{x}<br>Solar Radiation: %{y:.1f} W/m²'
    ), row=1, col=1)

    # Add solar generation trace
    fig.add_trace(go.Scatter(
        x=df_resampled.index,
        y=df_resampled['SOLAR'],
        mode='lines',
        name='Solar Generation (MW)',
        line=dict(color='yellow', width=2),
        hovertemplate='Time: %{x}<br>Generation: %{y:.0f} MW'
    ), row=2, col=1)

    # Add GAS generation trace
    fig.add_trace(go.Scatter(
        x=gas_resampled.index,
        y=gas_resampled['GAS'],
        mode='lines',
        name='GAS Generation (MW)',
        line=dict(color='black', width=2),
        hovertemplate='Time: %{x}<br>GAS Generation: %{y:.0f} MW'
    ), row=3, col=1)

    # Add total generation trace
    fig.add_trace(go.Scatter(
        x=generation_resampled.index,
        y=generation_resampled['GENERATION'],
        mode='lines',
        name='Total Generation (MW)',
        line=dict(color='purple', width=2),
        hovertemplate='Time: %{x}<br>Total Generation: %{y:.0f} MW'
    ), row=4, col=1)

    # Update layout
    date_range = f"{df.index.min().strftime('%Y-%m-%d')} to {df.index.max().strftime('%Y-%m-%d')}"
    fig.update_layout(
        title=f'Solar Radiation, Solar Generation, GAS Generation & Total Generation - {date_range} ({resample_freq} resolution)',
        hovermode='x unified',
        height=1000
    )

    # Update y-axes labels
    fig.update_yaxes(title_text="Solar Radiation (W/m²)", row=1, col=1)
    fig.update_yaxes(title_text="Solar Generation (MW)", row=2, col=1)
    fig.update_yaxes(title_text="GAS Generation (MW)", row=3, col=1)
    fig.update_yaxes(title_text="Total Generation (MW)", row=4, col=1)
    fig.update_xaxes(title_text="DateTime", row=4, col=1)

    # Show the plot
    fig.show()

def statistical_summary(df):
    """Provide detailed statistical summary."""
    print("\n=== Statistical Summary ===")
    print("\nSolar Radiation (W/m²):")
    print(df['Solar_Radiation_W_m2'].describe())

    print("\nSolar Generation (MW):")
    print(df['SOLAR'].describe())

    print("\nJoint Statistics:")
    print(f"Data points: {len(df)}")
    print(f"Date range: {df.index.min()} to {df.index.max()}")

    # Calculate some derived metrics
    df_copy = df.copy()
    df_copy['Solar_Efficiency'] = df_copy['SOLAR'] / df_copy['Solar_Radiation_W_m2']
    print("\nSolar Efficiency (MW/W/m²):")
    print(df_copy['Solar_Efficiency'].describe())

# --- Main Analysis ----------------------------------------------------------
def main(years=None, start_date=None, end_date=None, resample_freq='1D'):
    print("=== Solar Radiation vs Solar Generation Analysis ===")

    # Load data
    print("Loading weather data...")
    weather_df = load_weather_data(years=years, start_date=start_date, end_date=end_date)

    print("Loading generation data...")
    generation_df = load_generation_data(start_date=start_date, end_date=end_date, columns=['SOLAR'])

    print(f"Weather data: {len(weather_df)} points")
    print(f"Generation data: {len(generation_df)} points")

    # Merge datasets
    print("Merging datasets...")
    merged_df = merge_datasets(weather_df, generation_df)
    print(f"Merged data: {len(merged_df)} points")

    if merged_df.empty:
        print("No overlapping data found!")
        return

    # Perform analyses
    correlation_analysis(merged_df)
    solar_radiation_bins_analysis(merged_df)
    statistical_summary(merged_df)

    # Create visualizations
    print("\nCreating scatter plot...")
    create_scatter_plot(merged_df, sample_size=50000)  # Sample for scatter plot

    print("Creating density plot...")
    create_density_plot(merged_df)

    print("Creating time series analysis...")
    time_series_analysis(merged_df, resample_freq=resample_freq, start_date=start_date, end_date=end_date)

    print("\nAnalysis complete! Interactive plots displayed in browser.")

if __name__ == "__main__":
    # Interactive version like other scripts
    years = list_years()
    if not years:
        print("No weather parquet data found.")
        exit()

    print(f"Available years: {years}")
    print("=== Solar Radiation vs Solar Generation Chart ===")
    print("This script creates interactive charts showing the relationship between solar radiation and solar generation.")
    print("Includes GAS generation and total generation comparison in time series.")
    print("Example: Use 2015-01-01 to 2025-01-01 for aligned 10-year analysis")

    # Get year input
    year_input = input("Enter year(s) to analyze (comma-separated, or press Enter for all): ").strip()
    years_list = None
    if year_input:
        try:
            years_list = [int(y.strip()) for y in year_input.split(',')]
            # Validate years
            invalid_years = [y for y in years_list if y not in years]
            if invalid_years:
                print(f"Warning: Years {invalid_years} not available. Using available years only.")
                years_list = [y for y in years_list if y in years]
        except ValueError:
            print("Invalid year format. Using all years.")
            years_list = None

    # Get date range input
    start_date = input("Enter start date (YYYY-MM-DD, e.g., 2015-01-01) or press Enter: ").strip()
    end_date = input("Enter end date (YYYY-MM-DD, e.g., 2025-01-01) or press Enter: ").strip()
    start_date = start_date if start_date else None
    end_date = end_date if end_date else None

    # Get resample frequency
    resample_freq = input("Enter resample frequency (e.g., '1H', '1D', default '1D'): ").strip()
    resample_freq = resample_freq if resample_freq else '1D'

    # Run analysis
    main(years=years_list, start_date=start_date, end_date=end_date, resample_freq=resample_freq)