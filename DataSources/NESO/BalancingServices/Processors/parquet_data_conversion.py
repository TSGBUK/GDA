import pandas as pd
import os
from pathlib import Path
import concurrent.futures
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet

# --- paths ---
ROOT = next(p for p in Path(__file__).resolve().parents if p.name.lower() == "gda")
CSV_DIR = ROOT / "DataSources" / "NESO" / "BalancingServices"
PARQUET_DIR = CSV_DIR / "Parquet"

# Ensure output exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def settlement_to_datetime(date_series: pd.Series, period_series: pd.Series) -> pd.Series:
    """
    Convert Settlement Date + Settlement Period to UTC datetime.
    UK settlement periods are half-hourly, 1 = 00:00â€“00:30, 2 = 00:30â€“01:00, ..., 48 = 23:30â€“00:00.

    The CSVs use mixed date formats: older files use "DD/MM/YYYY" while some later files
    are ISO "YYYY-MM-DD".  We try the UK style first and fall back to pandas' parser.
    """
    # try strict UK format first
    date = pd.to_datetime(date_series, format="%d/%m/%Y", errors="coerce", utc=True)
    # fallback where parsing failed
    mask = date.isna()
    if mask.any():
        date.loc[mask] = pd.to_datetime(
            date_series.loc[mask],
            errors="coerce",
            utc=True
        )
    offset = (period_series.astype(int) - 1) * 30
    return date + pd.to_timedelta(offset, unit="m")


def convert_csv_to_parquet():
    """
    Convert balancing services CSVs to partitioned Parquet files.
    Partition by year derived from settlement date.
    """
    csv_files = [file_name for file_name in os.listdir(CSV_DIR) if file_name.lower().endswith(".csv")]

    def process_file(file_name: str) -> None:
        fpath = CSV_DIR / file_name
        try:
            preview = pd.read_csv(fpath, nrows=1)
            year = str(pd.to_datetime(preview["SETT_DATE"].iloc[0], dayfirst=True).year)
        except Exception:
            print(f"[skip] Could not determine year from {file_name}")
            return

        if has_fresh_partitioned_output(PARQUET_DIR, file_name, fpath.stat().st_mtime):
            print(f"[skip] {file_name} already converted")
            return

        print(f"[conv] {file_name} â†’ {PARQUET_DIR}")
        try:
            df = pd.read_csv(
                fpath,
                dtype={"SETT_DATE": "string", "SETT_PERIOD": "int32"},
                engine="python"
            )
            df["DatetimeUTC"] = settlement_to_datetime(df["SETT_DATE"], df["SETT_PERIOD"])

            cost_cols = [c for c in df.columns if c not in ["SETT_DATE", "SETT_PERIOD", "DatetimeUTC"]]
            for c in cost_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce")

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
