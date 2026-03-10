#!/usr/bin/env python3
"""Wrapper entrypoint for CSV header mapping extraction script."""

from __future__ import annotations

import runpy
from pathlib import Path


if __name__ == "__main__":
    target = Path(__file__).resolve().parents[1] / "DataSources" / "NationalGrid" / "Processors" / "extract_csv_headers_mapping.py"
    runpy.run_path(str(target), run_name="__main__")
