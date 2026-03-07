#!/usr/bin/env python3
"""Remove duplicate CSV files matching the pattern '* copy.csv'.

By default this script performs a dry run and prints what would be deleted.
Use --run to actually remove files.
"""

from __future__ import annotations

import argparse
from pathlib import Path


ROOT = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")


def find_copy_csv_files(root_dir: Path) -> list[Path]:
    """Return all CSV files matching '* copy.csv' recursively."""
    matches = []
    for path in root_dir.rglob("*.csv"):
        if path.is_file() and path.name.lower().endswith(" copy.csv"):
            matches.append(path)
    return sorted(matches)


def dedupe(root_dir: Path, run: bool) -> int:
    """Delete matching files when run=True, otherwise print dry-run output."""
    targets = find_copy_csv_files(root_dir)

    if not targets:
        print("No files matching '* copy.csv' were found.")
        return 0

    print(f"Found {len(targets)} file(s) matching '* copy.csv':")
    for path in targets:
        print(f"  - {path}")

    if not run:
        print("\nDry run only. Re-run with --run to delete these files.")
        return 0

    deleted = 0
    failed = 0
    for path in targets:
        try:
            path.unlink()
            deleted += 1
            print(f"[deleted] {path}")
        except Exception as exc:
            failed += 1
            print(f"[error] {path}: {exc}")

    print("\nSummary")
    print(f"Deleted: {deleted}")
    print(f"Failed: {failed}")
    return 0 if failed == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Remove duplicate CSVs named '* copy.csv' recursively."
    )
    parser.add_argument(
        "--root",
        default=str(ROOT),
        help="Root directory to scan recursively (default: repository root)",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Actually delete files. Without this, script runs in dry-run mode.",
    )

    args = parser.parse_args()
    root_dir = Path(args.root).resolve()
    if not root_dir.exists():
        print(f"Error: root path does not exist: {root_dir}")
        return 1

    return dedupe(root_dir, run=args.run)


if __name__ == "__main__":
    raise SystemExit(main())
