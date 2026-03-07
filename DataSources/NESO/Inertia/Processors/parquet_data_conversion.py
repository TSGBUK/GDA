import pandas as pd
import os
from pathlib import Path
import concurrent.futures
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet

# --- paths ---
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
CSV_DIR = ROOT / "DataSources" / "NESO" / "Inertia"
PARQUET_DIR = CSV_DIR / "Parquet"

# Ensure output exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def settlement_to_datetime(date_series: pd.Series, period_series: pd.Series) -> pd.Series:
    """
    Convert Settlement Date + Settlement Period to UTC datetime.
    UK settlement periods are half-hourly, 1 = 00:00–00:30, 2 = 00:30–01:00, ..., 48 = 23:30–00:00.
    """
    date = pd.to_datetime(date_series, format="%Y-%m-%d", errors="coerce", utc=True)
    # Each period is 30 mins offset from midnight
    offset = (period_series.astype(int) - 1) * 30
    return date + pd.to_timedelta(offset, unit="m")


def convert_csv_to_parquet():
    """
    Convert new-format CSVs to partitioned Parquet files.
    Partitioned by year of Settlement Date.
    """
    csv_files = [file_name for file_name in os.listdir(CSV_DIR) if file_name.lower().endswith(".csv")]

    def process_file(file_name: str) -> None:
        fpath = CSV_DIR / file_name

        try:
            preview = pd.read_csv(fpath, nrows=1)
            year = str(pd.to_datetime(preview["Settlement Date"].iloc[0]).year)
        except Exception:
            print(f"[skip] Could not determine year from {file_name}")
            return

        if has_fresh_partitioned_output(PARQUET_DIR, file_name, fpath.stat().st_mtime):
            print(f"[skip] {file_name} already converted")
            return

        print(f"[conv] {file_name} → {PARQUET_DIR}")

        try:
            df = pd.read_csv(
                fpath,
                usecols=["Settlement Date", "Settlement Period", "Outturn Inertia", "Market Provided Inertia"],
                dtype={"Settlement Date": "string", "Settlement Period": "int32"},
                engine="python"
            )

            df["DatetimeUTC"] = settlement_to_datetime(df["Settlement Date"], df["Settlement Period"])

            df["Outturn Inertia"] = pd.to_numeric(df["Outturn Inertia"], errors="coerce")
            df["Market Provided Inertia"] = pd.to_numeric(df["Market Provided Inertia"], errors="coerce")

            before = len(df)
            df = df.dropna()
            dropped = before - len(df)
            if dropped:
                print(f"[clean] Dropped {dropped} bad rows in {file_name}")

            write_partitioned_parquet(
                df=df,
                parquet_dir=PARQUET_DIR,
                csv_file_name=file_name,
                parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
                fallback_year=year,
                timestamp_column="DatetimeUTC",
            )

        except Exception as e:
            print(f"[error] {file_name}: {e}")

    max_workers = max(1, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, file_name) for file_name in csv_files]
        for future in concurrent.futures.as_completed(futures):
            future.result()


if __name__ == "__main__":
    convert_csv_to_parquet()
