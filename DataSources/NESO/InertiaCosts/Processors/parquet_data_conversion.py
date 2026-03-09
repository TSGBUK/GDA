import os
import concurrent.futures
from pathlib import Path

import pandas as pd
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet


ROOT = next(p for p in Path(__file__).resolve().parents if p.name.lower() == "gda")
CSV_DIR = ROOT / "DataSources" / "NESO" / "InertiaCosts"
PARQUET_DIR = CSV_DIR / "Parquet"

PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def parse_settlement_dates(date_series: pd.Series) -> pd.Series:
    """Parse mixed date formats used in inertia cost CSVs into UTC timestamps."""
    date_str = date_series.astype("string").str.strip()

    parsed = pd.to_datetime(date_str, format="%Y-%m-%d", errors="coerce", utc=True)
    missing = parsed.isna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(
            date_str.loc[missing],
            format="%d/%m/%Y",
            errors="coerce",
            utc=True,
        )

    missing = parsed.isna()
    if missing.any():
        parsed.loc[missing] = pd.to_datetime(
            date_str.loc[missing],
            errors="coerce",
            utc=True,
            dayfirst=True,
        )

    return parsed


def convert_csv_to_parquet() -> None:
    """Convert inertia cost CSVs into year-partitioned parquet files."""
    csv_files = [file_name for file_name in sorted(os.listdir(CSV_DIR)) if file_name.lower().endswith(".csv")]

    def process_file(file_name: str) -> None:
        csv_path = CSV_DIR / file_name
        print(f"[read] {file_name}")

        try:
            preview = pd.read_csv(csv_path, nrows=0)
            cols = list(preview.columns)

            date_col = "Settlement Date" if "Settlement Date" in cols else None
            if "Cost_per_GVAs" in cols:
                cost_col = "Cost_per_GVAs"
            elif "Cost" in cols:
                cost_col = "Cost"
            else:
                cost_col = None

            if not date_col or not cost_col:
                raise ValueError(f"Required columns not found. Columns present: {cols}")

            df = pd.read_csv(
                csv_path,
                usecols=[date_col, cost_col],
                dtype={date_col: "string", cost_col: "string"},
                engine="python",
            )
            df = df.rename(columns={date_col: "Settlement Date", cost_col: "Cost_per_GVAs"})
        except Exception as exc:
            print(f"[error] {file_name}: {exc}")
            return

        df["DatetimeUTC"] = parse_settlement_dates(df["Settlement Date"])
        df["Cost_per_GVAs"] = pd.to_numeric(df["Cost_per_GVAs"], errors="coerce")

        before = len(df)
        df = df.dropna(subset=["DatetimeUTC", "Cost_per_GVAs"]).copy()
        dropped = before - len(df)
        if dropped:
            print(f"[clean] Dropped {dropped} invalid rows in {file_name}")

        if df.empty:
            print(f"[skip] No valid rows in {file_name}")
            return

        file_stem = Path(file_name).stem.replace(" ", "_").replace("(", "").replace(")", "")

        output_name = f"{file_stem}.csv"
        if has_fresh_partitioned_output(PARQUET_DIR, output_name, csv_path.stat().st_mtime):
            print(f"[skip] {file_name} already up to date")
            return

        out_df = df[["Settlement Date", "DatetimeUTC", "Cost_per_GVAs"]].copy()
        out_df = out_df.sort_values("DatetimeUTC")

        write_partitioned_parquet(
            df=out_df,
            parquet_dir=PARQUET_DIR,
            csv_file_name=output_name,
            parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
            timestamp_column="DatetimeUTC",
        )
        print(f"[conv] {file_name} -> {PARQUET_DIR}")

    max_workers = max(1, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, file_name) for file_name in csv_files]
        for future in concurrent.futures.as_completed(futures):
            future.result()


if __name__ == "__main__":
    convert_csv_to_parquet()

