#!/usr/bin/env python3
"""Shared helpers for UK Power Networks CSV -> Parquet conversion.

Goal: preserve CSV data like-for-like (all rows and all columns) while writing
Parquet outputs under DataSources/UkPowerNetworks/Parquet.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd


ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")
UKPN_ROOT = ROOT / "DataSources" / "UkPowerNetworks"
HISTORY_ROOT = UKPN_ROOT / "history"
PARQUET_ROOT = UKPN_ROOT / "Parquet"

PARQUET_ROOT.mkdir(parents=True, exist_ok=True)
CSV_ROWS_TO_KEEP = 10


def read_csv_safely(path: Path) -> pd.DataFrame:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return pd.read_csv(path, encoding=encoding, low_memory=False)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError(f"Unable to read CSV {path}: {last_error}")


def has_fresh_parquet_output(csv_path: Path, parquet_path: Path) -> bool:
    if not parquet_path.exists():
        return False
    return parquet_path.stat().st_mtime >= csv_path.stat().st_mtime


def discover_csvs_for_slug(dataset_slug: str) -> list[Path]:
    pattern = f"**/{dataset_slug}__export.csv"
    return sorted(path for path in HISTORY_ROOT.glob(pattern) if path.is_file())


def build_output_path(csv_path: Path) -> Path:
    rel = csv_path.relative_to(HISTORY_ROOT)
    return PARQUET_ROOT / rel.with_suffix(".parquet")


def validate_header_roundtrip(csv_path: Path, df: pd.DataFrame) -> None:
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.reader(handle)
        csv_header = next(reader, [])

    parquet_header = [str(col) for col in df.columns]
    if [str(c) for c in csv_header] != parquet_header:
        raise RuntimeError(
            f"Header mismatch while converting {csv_path.name}: "
            f"csv={len(csv_header)} columns parquet={len(parquet_header)} columns"
        )


def _read_csv_rows(csv_path: Path) -> list[list[str]]:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with csv_path.open("r", encoding=encoding, newline="") as handle:
                return list(csv.reader(handle))
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    raise RuntimeError(f"Unable to read rows for prune {csv_path}: {last_error}")


def prune_csv_keep_last_rows(csv_path: Path, keep_rows: int = CSV_ROWS_TO_KEEP) -> tuple[int, int]:
    """Keep header + last N data rows in CSV after successful conversion.

    Returns:
        (rows_removed, rows_remaining)
    """
    rows = _read_csv_rows(csv_path)
    if not rows:
        return (0, 0)

    header = rows[0]
    data_rows = rows[1:]

    if len(data_rows) <= keep_rows:
        return (0, len(data_rows))

    kept = data_rows[-keep_rows:]
    rows_removed = len(data_rows) - len(kept)

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(kept)

    return (rows_removed, len(kept))


def convert_one_csv(csv_path: Path, force: bool = False) -> tuple[str, int, int]:
    out_path = build_output_path(csv_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not force and has_fresh_parquet_output(csv_path, out_path):
        return ("skip", 0, 0)

    df = read_csv_safely(csv_path)
    validate_header_roundtrip(csv_path, df)

    rows = len(df)
    cols = len(df.columns)

    # Like-for-like conversion: no column drops, no row filtering.
    df.to_parquet(out_path, engine="pyarrow", compression="snappy", index=False)

    removed, remaining = prune_csv_keep_last_rows(csv_path, keep_rows=CSV_ROWS_TO_KEEP)
    if removed > 0:
        print(
            f"[prune] {csv_path.name}: removed={removed}, kept={remaining} data rows",
            flush=True,
        )

    return ("ok", rows, cols)


def convert_dataset_slug(dataset_slug: str, force: bool = False) -> int:
    csv_files = discover_csvs_for_slug(dataset_slug)
    if not csv_files:
        print(f"[skip] {dataset_slug}: no matching CSV files under {HISTORY_ROOT}")
        return 0

    ok = 0
    skip = 0
    fail = 0

    for csv_path in csv_files:
        rel = csv_path.relative_to(HISTORY_ROOT)
        try:
            status, rows, cols = convert_one_csv(csv_path, force=force)
            if status == "skip":
                skip += 1
                print(f"[skip] {dataset_slug}: {rel}")
            else:
                ok += 1
                print(f"[conv] {dataset_slug}: {rel} -> rows={rows}, cols={cols}")
        except Exception as exc:  # noqa: BLE001
            fail += 1
            print(f"[error] {dataset_slug}: {rel} -> {exc}")

    print(
        f"[done] {dataset_slug}: files={len(csv_files)} converted={ok} skipped={skip} failed={fail}"
    )
    return 1 if fail else 0
