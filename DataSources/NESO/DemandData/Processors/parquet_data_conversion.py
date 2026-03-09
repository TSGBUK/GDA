import pandas as pd
import os
import re
import json
import concurrent.futures
from pathlib import Path
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet

# --- paths ---
ROOT = next(p for p in Path(__file__).resolve().parents if p.name.lower() == "gda")
CSV_DIR = ROOT / "DataSources" / "NESO" / "DemandData"
PARQUET_DIR = CSV_DIR / "Parquet"

SCHEMA_PATH = ROOT / "DataSchema.json"


def load_dataset_schema(dataset_id: str) -> dict[str, dict[str, str]]:
    try:
        with SCHEMA_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        print(f"[warn] Could not read {SCHEMA_PATH}: {exc}")
        return {}

    datasets = payload.get("datasets", []) if isinstance(payload, dict) else []
    for dataset in datasets:
        if dataset.get("id") != dataset_id:
            continue
        raw = dataset.get("schema", {}).get("rawCsv", {})
        columns = raw.get("columns", []) if isinstance(raw, dict) else []
        schema: dict[str, dict[str, str]] = {}
        for column in columns:
            name = column.get("name")
            if isinstance(name, str) and name:
                schema[name] = {
                    "type": str(column.get("type", "number")).lower(),
                    "unit": str(column.get("unit", "")),
                }
        return schema

    print(f"[warn] Dataset schema '{dataset_id}' not found in {SCHEMA_PATH.name}")
    return {}


SCHEMA = load_dataset_schema("DemandData")


# Ensure output exists
PARQUET_DIR.mkdir(parents=True, exist_ok=True)


def has_fresh_parquet_output(csv_file_name: str, csv_path: Path) -> bool:
    return has_fresh_partitioned_output(PARQUET_DIR, csv_file_name, csv_path.stat().st_mtime)


def settlement_to_datetime(date_series: pd.Series, period_series: pd.Series) -> pd.Series:
    """
    Build UTC timestamp from settlement date (various formats) and half-hour period.
    """
    text = date_series.astype("string").str.strip()

    date = pd.to_datetime(text, format="%d-%b-%Y", errors="coerce", utc=True)

    mask = date.isna()
    if mask.any():
        date.loc[mask] = pd.to_datetime(
            text.loc[mask],
            format="%Y-%m-%d",
            errors="coerce",
            utc=True,
        )

    mask = date.isna()
    if mask.any():
        ddmmyyyy_re = re.compile(r"^\d{2}/\d{2}/\d{4}$")
        ddmmyyyy_vals = text.loc[mask]
        ddmmyyyy_mask = ddmmyyyy_vals.str.match(ddmmyyyy_re)
        if ddmmyyyy_mask.any():
            date.loc[ddmmyyyy_vals[ddmmyyyy_mask].index] = pd.to_datetime(
                ddmmyyyy_vals[ddmmyyyy_mask],
                format="%d/%m/%Y",
                errors="coerce",
                utc=True,
            )

    # Try additional explicit formats only (avoid inference fallback warnings)
    extra_formats = [
        "%Y/%m/%d",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%d-%b-%Y %H:%M:%S",
    ]
    for date_format in extra_formats:
        mask = date.isna()
        if not mask.any():
            break
        date.loc[mask] = pd.to_datetime(
            text.loc[mask],
            format=date_format,
            errors="coerce",
            utc=True,
        )

    offset = (period_series.astype(int) - 1) * 30
    return date + pd.to_timedelta(offset, unit="m")


def convert_csv_to_parquet():
    """
    Convert each demanddata CSV into a partitioned parquet file by year.
    """
    csv_files = [file_name for file_name in os.listdir(CSV_DIR) if file_name.lower().endswith(".csv")]

    def process_file(file_name: str) -> None:
        fpath = CSV_DIR / file_name

        if has_fresh_parquet_output(file_name, fpath):
            print(f"[skip] {file_name} already converted")
            return

        print(f"[conv] processing {file_name}")
        try:
            df = pd.read_csv(fpath, dtype=str, engine="python")
        except Exception as e:
            print(f"[error] could not read {file_name}: {e}")
            return

        if "SETTLEMENT_DATE" not in df.columns or "SETTLEMENT_PERIOD" not in df.columns:
            print(f"[skip] {file_name} missing date/period columns")
            return

        df["DatetimeUTC"] = settlement_to_datetime(df["SETTLEMENT_DATE"], df["SETTLEMENT_PERIOD"])
        for col in df.columns:
            if col in ("SETTLEMENT_DATE", "SETTLEMENT_PERIOD", "DatetimeUTC"):
                continue
            if col in SCHEMA:
                if SCHEMA[col]["type"] == "integer":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                else:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                print(f"[warn] unexpected column '{col}' found in {file_name}")
                df[col] = pd.to_numeric(df[col], errors="coerce")

        before = len(df)
        df = df.dropna(subset=["DatetimeUTC"])
        dropped = before - len(df)
        if dropped:
            print(f"[clean] dropped {dropped} rows with bad dates in {file_name}")

        if df["DatetimeUTC"].isna().all():
            print(f"[skip] no valid year in {file_name}")
            return

        write_partitioned_parquet(
            df=df,
            parquet_dir=PARQUET_DIR,
            csv_file_name=file_name,
            parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
            timestamp_column="DatetimeUTC",
        )

    max_workers = max(1, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, file_name) for file_name in csv_files]
        for future in concurrent.futures.as_completed(futures):
            future.result()
    print("conversion complete")


if __name__ == "__main__":
    convert_csv_to_parquet()
