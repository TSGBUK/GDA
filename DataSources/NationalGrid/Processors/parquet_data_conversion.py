import os
import re
import sys
from pathlib import Path

import pandas as pd


ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
DATASET_DIR = ROOT / "DataSources" / "NationalGrid"
CSV_ROOT = DATASET_DIR / "history"
PARQUET_ROOT = DATASET_DIR / "Parquet"

PARQUET_ROOT.mkdir(parents=True, exist_ok=True)

YEAR_PAT = re.compile(r"(19|20)\d{2}")

PRIMARY_PREFIX = "live-primary"
GSP_PREFIX = "live-gsp-data"
BSP_PREFIXES = ("bsp-", "bsp ", "bsp")

sys.path.insert(0, str(ROOT / "Scripts"))
from parquet_partitioning import write_partitioned_parquet  

def log(message: str) -> None:
    print(message, flush=True)


def detect_timestamp_column(columns: list[str]) -> str | None:
    preferred = [
        "timestamp",
        "datetime",
        "date_time",
        "date",
        "settlementdate",
        "settlement_date",
        "trading_period_start",
        "start_time",
        "time",
    ]

    lowered = {c.lower(): c for c in columns}
    for key in preferred:
        if key in lowered:
            return lowered[key]

    for col in columns:
        c = col.lower()
        if "date" in c or "time" in c or "timestamp" in c:
            return col

    return None


def parse_year_from_file_name(file_name: str) -> str:
    match = YEAR_PAT.search(file_name)
    return match.group(0) if match else "unknown"


def infer_group(section_slug: str) -> str | None:
    if section_slug.startswith(PRIMARY_PREFIX):
        return "live-primary-all"
    if section_slug.startswith(GSP_PREFIX):
        return "live-gsp-all"
    if any(section_slug.startswith(prefix) for prefix in BSP_PREFIXES):
        return "bsp-all"
    return None


def read_csv_safely(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(path, encoding=encoding, low_memory=False)
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    raise RuntimeError(f"Unable to read CSV {path}: {last_error}")


def extract_location_hint(file_name: str) -> str:
    stem = Path(file_name).stem
    if "__" in stem:
        stem = stem.split("__", 1)[1]

    for suffix in (
        "-primary-transformer-flows",
        "_primary-transformer-flows",
        "_swales",
        "_swest",
        "_wmids",
        "_emids",
        "-bsp",
    ):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]

    stem = stem.replace("_", "-")
    stem = re.sub(r"-+", "-", stem).strip("-")
    return stem or "unknown"


def normalize_dataframe(df: pd.DataFrame, file_name: str) -> tuple[pd.DataFrame, str, str]:
    ts_col = detect_timestamp_column([str(c) for c in df.columns])

    year = parse_year_from_file_name(file_name)
    timestamp_series = pd.Series([pd.NaT] * len(df), index=df.index, dtype="datetime64[ns, UTC]")

    if ts_col:
        parsed = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
        if parsed.notna().any():
            timestamp_series = parsed
            year = str(int(parsed.dropna().dt.year.mode().iloc[0]))

    df["timestamp_utc"] = timestamp_series
    df["location_hint"] = extract_location_hint(file_name)

    return df, year, ts_col or ""


def convert_one_csv(csv_path: Path) -> tuple[str | None, pd.DataFrame | None]:
    section_dir = csv_path.parent
    section_slug = section_dir.name
    group_name = infer_group(section_slug)

    if not group_name:
        return None, None

    try:
        df = read_csv_safely(csv_path)
        if df.empty:
            log(f"[skip] {csv_path.name} empty")
            return group_name, None

        df, year, source_timestamp_col = normalize_dataframe(df, csv_path.name)
        df["source_section"] = section_slug
        df["source_file"] = csv_path.name
        df["source_year_hint"] = year
        df["source_timestamp_col"] = source_timestamp_col

        log(f"[stage] {group_name}: {section_slug}/{csv_path.name} rows={len(df)}")
        return group_name, df

    except Exception as exc:  # noqa: BLE001
        log(f"[error] {section_slug}/{csv_path.name}: {exc}")
        return group_name, None


def convert_csv_to_parquet() -> None:
    if not CSV_ROOT.exists():
        log(f"[skip] No history folder found: {CSV_ROOT}")
        return

    csv_files = sorted(CSV_ROOT.glob("*/*.csv"))
    if not csv_files:
        log(f"[skip] No CSV files found under {CSV_ROOT}")
        return

    log(f"[info] Found {len(csv_files)} CSV files under {CSV_ROOT}")

    grouped_frames: dict[str, list[pd.DataFrame]] = {
        "live-primary-all": [],
        "live-gsp-all": [],
        "bsp-all": [],
    }

    for csv_path in csv_files:
        group_name, df = convert_one_csv(csv_path)
        if group_name and df is not None and not df.empty:
            grouped_frames[group_name].append(df)

    for group_name, frames in grouped_frames.items():
        if not frames:
            log(f"[skip] no rows for {group_name}")
            continue

        merged = pd.concat(frames, ignore_index=True, sort=False)

        dedupe_exclude = {
            "source_section",
            "source_file",
            "source_year_hint",
            "source_timestamp_col",
        }
        dedupe_columns = [col for col in merged.columns if col not in dedupe_exclude]

        if dedupe_columns:
            before = len(merged)
            dedupe_frame = merged[dedupe_columns].copy()
            dedupe_frame = dedupe_frame.where(pd.notna(dedupe_frame), "")

            for col in dedupe_frame.columns:
                dedupe_frame[col] = dedupe_frame[col].astype(str).str.strip()

            merged["_row_fingerprint"] = pd.util.hash_pandas_object(
                dedupe_frame,
                index=False,
            ).astype("uint64")

            merged = merged.drop_duplicates(subset=["_row_fingerprint"], keep="last")
            merged = merged.drop(columns=["_row_fingerprint"])

            dropped = before - len(merged)
            if dropped:
                log(f"[clean] dropped {dropped} duplicate rows in {group_name}")

        if "timestamp_utc" in merged.columns:
            merged = merged.sort_values(by=["timestamp_utc", "location_hint"], na_position="last")

        output_csv_name = group_name.replace("-", "_") + ".csv"
        log(f"[write] {group_name} -> {PARQUET_ROOT} rows={len(merged)}")
        write_partitioned_parquet(
            df=merged,
            parquet_dir=PARQUET_ROOT,
            csv_file_name=output_csv_name,
            parquet_engine=os.getenv("PARQUET_ENGINE", "pyarrow"),
            timestamp_column="timestamp_utc",
        )

    log("[info] DataSources/NationalGrid parquet conversion complete")


if __name__ == "__main__":
    convert_csv_to_parquet()
