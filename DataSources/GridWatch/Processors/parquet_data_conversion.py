import pandas as pd
import os
from pathlib import Path
import re
import concurrent.futures
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet


# --- paths ------------------------------------------------------------------
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
CSV_DIR = ROOT / "DataSources" / "GridWatch"
PARQUET_DIR = CSV_DIR / "Parquet"

# ensure output exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def convert_csv_to_parquet():
    """Convert Gridwatch CSV input(s) to partitioned parquet by year.

    Supports either:
    - legacy single file: ``gridwatch.csv``
    - chunked files: ``gridwatch_chunk_*.csv``

    Output goes to ``Parquet/year=YYYY/<source>.parquet``.
    """

    chunk_files = sorted(CSV_DIR.glob("gridwatch_chunk_*.csv"))
    legacy_file = CSV_DIR / "gridwatch.csv"
    if chunk_files:
        csv_files = chunk_files
    elif legacy_file.exists():
        csv_files = [legacy_file]
    else:
        print(f"[error] No Gridwatch CSV inputs found in: {CSV_DIR}")
        print("[hint] Expected gridwatch.csv or gridwatch_chunk_*.csv")
        return

    parquet_engine = os.getenv("PARQUET_ENGINE", "pyarrow")

    def convert_one_csv(csv_file: Path) -> None:
        try:
            df = pd.read_csv(csv_file, skipinitialspace=True, low_memory=False)
        except Exception as e:
            print(f"[error] failed to read CSV {csv_file.name}: {e}")
            return

        if "timestamp" not in df.columns:
            print(f"[error] missing 'timestamp' column in {csv_file.name}")
            return

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        before = len(df)
        df = df.dropna(subset=["timestamp"])
        dropped = before - len(df)
        if dropped:
            print(f"[clean] {csv_file.name}: dropped {dropped} rows with invalid timestamps")

        if df.empty:
            print(f"[skip] {csv_file.name}: no valid rows after cleaning")
            return

        numeric_cols = [col for col in df.columns if col != "timestamp"]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

        output_name = f"{csv_file.stem}.csv"
        if has_fresh_partitioned_output(PARQUET_DIR, output_name, csv_file.stat().st_mtime):
            print(f"[skip] {csv_file.name}: already converted")
            return

        write_partitioned_parquet(
            df=df,
            parquet_dir=PARQUET_DIR,
            csv_file_name=output_name,
            parquet_engine=parquet_engine,
            timestamp_column="timestamp",
        )
        print(f"[conv] writing {csv_file.name} -> {PARQUET_DIR}")

    for csv_file in csv_files:
        convert_one_csv(csv_file)


if __name__ == "__main__":
    convert_csv_to_parquet()
