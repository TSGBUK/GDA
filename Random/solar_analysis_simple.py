import pandas as pd
import pyarrow.dataset as ds
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
    print(f"{corr:.4f}")

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
def main(start_date='2015-01-01', end_date='2025-01-01'):
    print("=== Solar Radiation vs Solar Generation Analysis ===")

    # Load data
    print("Loading weather data...")
    weather_df = load_weather_data(start_date=start_date, end_date=end_date)

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

    print("\nAnalysis complete!")

if __name__ == "__main__":
    main()