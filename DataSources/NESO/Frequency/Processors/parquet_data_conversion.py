import pandas as pd
import os
from pathlib import Path
import re
import concurrent.futures
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet


# --- paths ---
ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
CSV_DIR = ROOT / "DataSources" / "NESO" / "Frequency"
PARQUET_DIR = CSV_DIR / "Parquet"

# Ensure output exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# regex to detect ISO-style rows
ISO_PAT = re.compile(r"\d{4}-\d{2}-\d{2}")


def log(message: str) -> None:
    print(message, flush=True)

def parse_mixed_datetime(series: pd.Series) -> pd.Series:
    """
    Parse mixed datetime formats into UTC.
    Tries explicit formats first, then falls back to pandas' parser.
    """
    s = series.astype(str).str.strip()

    # Try strict parsing (UK + ISO with tz)
    out = pd.to_datetime(
        s,
        format="%d/%m/%Y %H:%M:%S",
        errors="coerce",
        utc=True
    )

    mask_fail = out.isna()
    if mask_fail.any():
        out.loc[mask_fail] = pd.to_datetime(
            s[mask_fail],
            format="%Y-%m-%d %H:%M:%S %z",
            errors="coerce",
            utc=True
        )

    # Final fallback: let pandas infer
    mask_fail = out.isna()
    if mask_fail.any():
        out.loc[mask_fail] = pd.to_datetime(
            s[mask_fail],
            errors="coerce",
            utc=True, 
        )

    return out

def convert_csv_to_parquet():
    """
    Convert CSVs in CSV_DIR to partitioned Parquet files in PARQUET_DIR/year=YYYY/.
    Skips files that already exist in Parquet unless the CSV is newer.
    """
    csv_files = [file_name for file_name in os.listdir(CSV_DIR) if file_name.lower().endswith(".csv")]

    def process_file(file_name: str) -> None:
        fpath = CSV_DIR / file_name

        parts = [p for p in file_name.replace(".csv", "").split("-") if p.isdigit()]
        if not parts:
            log(f"[skip] Could not determine year from {file_name}")
            return
        year = parts[0] if len(parts[0]) == 4 else parts[1]

        if has_fresh_partitioned_output(PARQUET_DIR, file_name, fpath.stat().st_mtime):
            log(f"[skip] {file_name} already converted")
            return

        log(f"[conv] {file_name} → {PARQUET_DIR}")

        try:
            log(f"[work] reading {file_name}")
            df = pd.read_csv(
                fpath,
                header=None,
                names=["Date", "Value"],
                usecols=[0, 1],
                dtype={0: "string", 1: "string"},
                on_bad_lines="skip",
                engine="python"
            )

            log(f"[work] parsing datetime {file_name}")
            df["Date"] = parse_mixed_datetime(df["Date"])
            log(f"[work] parsing numeric {file_name}")
            df["Value"] = pd.to_numeric(df["Value"].str.strip(), errors="coerce")
            before = len(df)
            df = df.dropna()
            dropped = before - len(df)
            if dropped:
                log(f"[clean] Dropped {dropped} bad rows in {file_name}")

            log(f"[work] writing {file_name}")
            write_partitioned_parquet(
                df=df,
                parquet_dir=PARQUET_DIR,
                csv_file_name=file_name,
                parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
                fallback_year=year,
                timestamp_column="Date",
            )
            log(f"[done] {file_name} rows={len(df)}")

        except Exception as e:
            log(f"[error] {file_name}: {e}")

    max_workers = max(1, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, file_name) for file_name in csv_files]
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == "__main__":
    convert_csv_to_parquet()
