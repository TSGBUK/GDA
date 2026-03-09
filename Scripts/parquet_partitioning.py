from __future__ import annotations

from pathlib import Path
from typing import Iterable
import re

import pandas as pd

_DATE_NAME_HINTS = (
    "datetime",
    "timestamp",
    "date",
    "time",
    "settlement",
    "period",
    "start",
    "end",
)


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _normalize_database_name(name: str) -> str:
    text = name.strip().lower()
    text = _NON_ALNUM_RE.sub("_", text)
    text = text.strip("_")
    return text or "unknown"


def determine_partition_columns(row_count: int) -> list[str]:
    _ = row_count
    return ["year", "week"]


def _find_timestamp_column(df: pd.DataFrame) -> str | None:
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            return col

    lowered = {str(col).lower(): col for col in df.columns}
    preferred = [
        "DatetimeUTC",
        "timestamp_utc",
        "timestamp",
        "Date",
        "date",
        "DATETIME",
        "datetime",
        "SETTLEMENT_DATE",
        "Settlement Date",
        "ValueDate",
        "time",
    ]

    for key in preferred:
        if key.lower() in lowered:
            return str(lowered[key.lower()])

    for col in df.columns:
        name = str(col).lower()
        if any(token in name for token in _DATE_NAME_HINTS):
            return str(col)

    return None


def _build_partition_frame(
    df: pd.DataFrame,
    partition_columns: Iterable[str],
    fallback_year: str | int | None = None,
    timestamp_column: str | None = None,
    database_name: str | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    out = df.copy()
    required = list(partition_columns)

    db_name = _normalize_database_name(database_name or "unknown")
    out["database"] = db_name

    timestamp_col = timestamp_column or _find_timestamp_column(out)
    parsed_ts = None
    if timestamp_col and timestamp_col in out.columns:
        parsed_ts = pd.to_datetime(out[timestamp_col], errors="coerce", utc=True)
        if parsed_ts.notna().sum() == 0:
            parsed_ts = None

    if parsed_ts is not None:
        out["year"] = parsed_ts.dt.year.astype("Int64")
        if "month" in required:
            out["month"] = parsed_ts.dt.month.astype("Int64")
        if "week" in required:
            out["week"] = parsed_ts.dt.isocalendar().week.astype("Int64")
    else:
        if fallback_year is None:
            fallback_year = "unknown"
        out["year"] = str(fallback_year)
        if "week" in required:
            out["week"] = "00"

    if "year" in out.columns:
        out["year"] = out["year"].astype("string").fillna("unknown")

    if "month" in out.columns:
        out["month"] = (
            pd.to_numeric(out["month"], errors="coerce")
            .astype("Int64")
            .astype("string")
            .str.zfill(2)
            .fillna("00")
        )

    if "week" in out.columns:
        out["week"] = (
            pd.to_numeric(out["week"], errors="coerce")
            .astype("Int64")
            .astype("string")
            .str.zfill(2)
            .fillna("00")
        )

    effective = [col for col in required if col in out.columns]
    return out, effective


def has_fresh_partitioned_output(
    parquet_dir: Path,
    csv_file_name: str,
    csv_mtime: float,
    database_name: str | None = None,
) -> bool:
    parquet_name = csv_file_name.replace(".csv", ".parquet")
    db_name = _normalize_database_name(database_name or parquet_dir.parent.name)
    matches = list((parquet_dir / db_name).glob(f"**/{parquet_name}"))
    if not matches:
        return False
    return all(path.stat().st_mtime >= csv_mtime for path in matches)


def write_partitioned_parquet(
    df: pd.DataFrame,
    parquet_dir: Path,
    csv_file_name: str,
    parquet_engine: str,
    fallback_year: str | int | None = None,
    timestamp_column: str | None = None,
    database_name: str | None = None,
) -> list[Path]:
    row_count = len(df)
    partition_cols = determine_partition_columns(row_count)
    db_name = _normalize_database_name(database_name or parquet_dir.parent.name)

    partitioned, effective_cols = _build_partition_frame(
        df,
        partition_columns=partition_cols,
        fallback_year=fallback_year,
        timestamp_column=timestamp_column,
        database_name=db_name,
    )

    if "week" in partition_cols and "week" not in effective_cols:
        print(f"[warn] {csv_file_name}: week partition requested but no timestamp; using available partitions")

    written: list[Path] = []
    for keys, sub in partitioned.groupby(effective_cols, dropna=False):
        key_values = (keys,) if not isinstance(keys, tuple) else keys
        outdir = parquet_dir / db_name
        for col, value in zip(effective_cols, key_values):
            if col == "database":
                continue
            outdir = outdir / f"{col}={value}"
        outdir.mkdir(parents=True, exist_ok=True)

        outpath = outdir / csv_file_name.replace(".csv", ".parquet")
        payload = sub.drop(columns=effective_cols, errors="ignore")
        payload.to_parquet(outpath, engine=parquet_engine, compression="snappy", index=False)
        written.append(outpath)

    return written
