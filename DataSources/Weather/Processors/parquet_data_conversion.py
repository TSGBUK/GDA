import os
import re
import concurrent.futures
from pathlib import Path
from typing import Optional

import pandas as pd
from Scripts.parquet_partitioning import has_fresh_partitioned_output, write_partitioned_parquet


ROOT = next(p for p in Path(__file__).resolve().parents if p.name.lower() == "gda")
CSV_DIR = ROOT / "DataSources" / "Weather"
PARQUET_DIR = CSV_DIR / "Parquet"

PARQUET_DIR.mkdir(parents=True, exist_ok=True)

YEAR_RE = re.compile(r"(?:19|20)\d{2}")
DATE_HINTS = ("date", "time", "datetime", "settlement", "start", "end", "period")
ISO_LIKE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T].*)?$")


def parse_datetime_smart(series: pd.Series) -> pd.Series:
    text = series.astype("string").str.strip()
    non_na = text.dropna()
    if non_na.empty:
        return pd.to_datetime(text, errors="coerce", utc=True)
    iso_ratio = non_na.str.match(ISO_LIKE_RE).mean()
    use_dayfirst = iso_ratio < 0.7
    return pd.to_datetime(text, errors="coerce", dayfirst=use_dayfirst, utc=True)


def infer_year_from_filename(file_name: str) -> Optional[str]:
    match = YEAR_RE.search(file_name)
    return match.group(0) if match else None


def infer_year_from_dataframe(df: pd.DataFrame) -> Optional[str]:
    if df.empty:
        return None

    for column in df.columns:
        col_name = str(column).lower()
        if any(token in col_name for token in DATE_HINTS):
            parsed = parse_datetime_smart(df[column])
            valid = parsed.dropna()
            if not valid.empty:
                return str(valid.iloc[0].year)

    sample = df.head(50)
    for column in sample.columns:
        parsed = parse_datetime_smart(sample[column])
        valid = parsed.dropna()
        if not valid.empty:
            return str(valid.iloc[0].year)
    return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        series = df[column]
        if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
            cleaned = series.astype("string").str.strip()

            if any(token in str(column).lower() for token in DATE_HINTS):
                parsed = parse_datetime_smart(cleaned)
                if parsed.notna().mean() >= 0.5:
                    df[column] = parsed
                    continue

            numeric = pd.to_numeric(cleaned.str.replace(",", "", regex=False), errors="coerce")
            if numeric.notna().mean() >= 0.8:
                df[column] = numeric
    return df


def convert_csv_to_parquet() -> None:
    csv_files = [file_name for file_name in os.listdir(CSV_DIR) if file_name.lower().endswith(".csv")]

    def process_file(file_name: str) -> None:
        csv_path = CSV_DIR / file_name
        if has_fresh_partitioned_output(PARQUET_DIR, file_name, csv_path.stat().st_mtime):
            print(f"[skip] {file_name} already converted")
            return

        try:
            df = pd.read_csv(csv_path, on_bad_lines="skip", engine="python")
        except UnicodeDecodeError:
            df = pd.read_csv(
                csv_path,
                on_bad_lines="skip",
                engine="python",
                encoding="latin1",
            )
        except Exception as exc:
            print(f"[error] Failed reading {file_name}: {exc}")
            return

        if df.empty:
            print(f"[skip] {file_name} has no rows")
            return

        year = (
            infer_year_from_dataframe(df)
            or infer_year_from_filename(file_name)
            or "unknown"
        )

        print(f"[conv] {file_name} â†’ {PARQUET_DIR}")
        try:
            df = normalize_columns(df)
            before = len(df)
            df = df.dropna(how="all")
            dropped = before - len(df)
            if dropped:
                print(f"[clean] Dropped {dropped} empty rows in {file_name}")

            write_partitioned_parquet(
                df=df,
                parquet_dir=PARQUET_DIR,
                csv_file_name=file_name,
                parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
                fallback_year=year,
            )
        except Exception as exc:
            print(f"[error] {file_name}: {exc}")

    max_workers = max(1, os.cpu_count() or 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, file_name) for file_name in csv_files]
        for future in concurrent.futures.as_completed(futures):
            future.result()


if __name__ == "__main__":
    convert_csv_to_parquet()

