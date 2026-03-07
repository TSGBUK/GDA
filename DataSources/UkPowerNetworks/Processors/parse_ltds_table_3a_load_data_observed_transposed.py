#!/usr/bin/env python3
"""Parser for dataset: ltds-table-3a-load-data-observed-transposed."""

from __future__ import annotations

import argparse

from ukpn_parquet_common import convert_dataset_slug


DATASET_SLUG = "ltds-table-3a-load-data-observed-transposed"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=f"Convert UKPN CSV exports for {DATASET_SLUG} to Parquet"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-convert even when parquet output is newer than CSV input.",
    )
    args = parser.parse_args()
    return convert_dataset_slug(DATASET_SLUG, force=args.force)


if __name__ == "__main__":
    raise SystemExit(main())
