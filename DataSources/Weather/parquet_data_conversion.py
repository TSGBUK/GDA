import pandas as pd
import os
from pathlib import Path
import concurrent.futures
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet

# --- paths ---
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
CSV_DIR = ROOT / "DataSources" / "Weather"
PARQUET_DIR = CSV_DIR / "Parquet"

# Ensure output exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def convert_csv_to_parquet():
    """
    Convert weather CSV to partitioned Parquet files.
    Partitioned by year from Date column.
    """
    csv_file = "uk_weather_data_2010-01-01_2025-12-31.csv"
    fpath = CSV_DIR / csv_file

    if not fpath.exists():
        print(f"[error] {csv_file} not found")
        return

    print(f"[load] Loading {csv_file}...")

    try:
        # Read the CSV
        df = pd.read_csv(
            fpath,
            dtype={
                "Date": "string",
                "Temperature_C": "float32",
                "Wind_Speed_100m_kph": "float32",
                "Solar_Radiation_W_m2": "float32"
            },
            engine="python"
        )

        # Convert Date to datetime for year extraction
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%dT%H:%M", errors="coerce")

        # Drop any rows with invalid dates
        before = len(df)
        df = df.dropna(subset=["Date"])
        dropped = before - len(df)
        if dropped:
            print(f"[clean] Dropped {dropped} rows with invalid dates")

        output_name = "uk_weather_data_2010-01-01_2025-12-31.csv"
        if has_fresh_partitioned_output(PARQUET_DIR, output_name, fpath.stat().st_mtime):
            print(f"[skip] {csv_file} already converted")
            return

        write_partitioned_parquet(
            df=df,
            parquet_dir=PARQUET_DIR,
            csv_file_name=output_name,
            parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
            timestamp_column="Date",
        )

        print("[done] Conversion complete")

    except Exception as e:
        print(f"[error] {csv_file}: {e}")


if __name__ == "__main__":
    convert_csv_to_parquet()