import pandas as pd
import pyarrow.dataset as ds
import numpy as np
from pathlib import Path

# --- Paths ------------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
WEATHER_PARQUET_DIR = ROOT / "DataSources" / "Weather" / "Parquet"
GENERATION_CSV = ROOT / "DataSources" / "NESO" / "HistoricalGenerationData" / "df_fuel_ckan.csv"

# --- Data Loading -----------------------------------------------------------
def load_weather_data(years=None):
    """Load weather data from parquet, optionally filtered by years."""
    dataset = ds.dataset(WEATHER_PARQUET_DIR, format="parquet", partitioning="hive")

    if years:
        # Filter by multiple years
        year_filters = [ds.field("year") == int(year) for year in years]
        combined_filter = year_filters[0]
        for f in year_filters[1:]:
            combined_filter = combined_filter | f
        table = dataset.to_table(filter=combined_filter)
    else:
        table = dataset.to_table()

    df = table.to_pandas()
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date').sort_index()
    return df[['Wind_Speed_100m_kph']]

def load_generation_data(start_date=None, end_date=None):
    """Load generation data from CSV, optionally filtered by date range."""
    df = pd.read_csv(GENERATION_CSV, parse_dates=['DATETIME'])
    df = df.set_index('DATETIME').sort_index()

    # Filter date range if specified
    if start_date:
        df = df[df.index >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df.index <= pd.Timestamp(end_date)]

    return df[['WIND']]  # Wind generation in MW

def merge_datasets(weather_df, generation_df):
    """Merge weather and generation data on datetime index."""
    # Both datasets should have datetime index
    merged = pd.merge_asof(
        weather_df.sort_index(),
        generation_df.sort_index(),
        left_index=True,
        right_index=True,
        tolerance=pd.Timedelta('1H')  # Allow 1 hour tolerance for matching
    )

    # Drop rows with missing data
    merged = merged.dropna()

    return merged

# --- Analysis Functions -----------------------------------------------------
def correlation_analysis(df):
    """Calculate correlation between wind speed and generation."""
    corr = df['Wind_Speed_100m_kph'].corr(df['WIND'])
    print(".4f")

    # Pearson correlation (same as default)
    pearson_corr = df['Wind_Speed_100m_kph'].corr(df['WIND'], method='pearson')

    print(".4f")

    return corr, pearson_corr

def wind_speed_bins_analysis(df, bins=20):
    """Analyze generation by wind speed bins."""
    df_copy = df.copy()
    df_copy['Wind_Bin'] = pd.cut(df_copy['Wind_Speed_100m_kph'], bins=bins)

    bin_stats = df_copy.groupby('Wind_Bin').agg({
        'WIND': ['count', 'mean', 'std', 'min', 'max'],
        'Wind_Speed_100m_kph': ['mean', 'min', 'max']
    }).round(2)

    print("\n=== Wind Speed Bins Analysis ===")
    print(bin_stats.head(10))  # Show first 10 bins

    return bin_stats

def statistical_summary(df):
    """Provide detailed statistical summary."""
    print("\n=== Statistical Summary ===")
    print("\nWind Speed (100m, kph):")
    print(df['Wind_Speed_100m_kph'].describe())

    print("\nWind Generation (MW):")
    print(df['WIND'].describe())

    print("\nJoint Statistics:")
    print(f"Data points: {len(df)}")
    print(f"Date range: {df.index.min()} to {df.index.max()}")

    # Calculate some derived metrics
    df_copy = df.copy()
    df_copy['Wind_Efficiency'] = df_copy['WIND'] / df_copy['Wind_Speed_100m_kph']
    print("\nWind Efficiency (MW/kph):")
    print(df_copy['Wind_Efficiency'].describe())

# --- Main Analysis ----------------------------------------------------------
def main():
    print("=== Wind Speed vs Wind Generation Analysis ===")

    # Load data
    print("Loading weather data...")
    weather_df = load_weather_data()  # Load all years

    print("Loading generation data...")
    generation_df = load_generation_data()  # Load all data

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
    wind_speed_bins_analysis(merged_df)
    statistical_summary(merged_df)

    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()