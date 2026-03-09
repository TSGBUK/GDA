#!/usr/bin/env python3
"""Master UKPN parquet conversion runner.

Walks history CSV exports, maps each dataset slug to a dedicated parser script,
and executes each parser.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


ROOT = next(p for p in Path(__file__).resolve().parents if p.name.lower() == "gda")
UKPN_ROOT = ROOT / "DataSources" / "UkPowerNetworks"
HISTORY_ROOT = UKPN_ROOT / "history"
PROCESSORS_DIR = UKPN_ROOT / "Processors"


def to_parser_name(slug: str) -> str:
    safe = []
    for ch in slug:
        if ch.isalnum():
            safe.append(ch.lower())
        else:
            safe.append("_")
    normalized = "".join(safe)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return f"parse_{normalized.strip('_')}.py"


def discover_dataset_slugs() -> list[str]:
    slugs = {
        path.name[:-len("__export.csv")]
        for path in HISTORY_ROOT.rglob("*__export.csv")
        if path.is_file()
    }
    return sorted(slugs)


def run_parser(script_path: Path, force: bool) -> int:
    cmd = [sys.executable, str(script_path)]
    if force:
        cmd.append("--force")

    print(f"[run] {script_path.name}")
    completed = subprocess.run(cmd)
    return int(completed.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run UKPN per-dataset CSV->Parquet parsers discovered from history exports."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Forward --force to each dataset parser.",
    )
    args = parser.parse_args()

    if not HISTORY_ROOT.exists():
        print(f"[skip] History root not found: {HISTORY_ROOT}")
        return 0

    slugs = discover_dataset_slugs()
    if not slugs:
        print(f"[skip] No *__export.csv files found under: {HISTORY_ROOT}")
        return 0

    print(f"[info] Dataset slugs discovered: {len(slugs)}")

    ok = 0
    fail = 0
    missing = 0

    for slug in slugs:
        parser_name = to_parser_name(slug)
        parser_path = PROCESSORS_DIR / parser_name

        if not parser_path.exists():
            missing += 1
            print(f"[missing] parser not found for {slug}: {parser_name}")
            continue

        code = run_parser(parser_path, force=args.force)
        if code == 0:
            ok += 1
        else:
            fail += 1

    print("\n=== UKPN Parquet Conversion Summary ===")
    print(f"Datasets discovered: {len(slugs)}")
    print(f"Parser runs succeeded: {ok}")
    print(f"Parser runs failed: {fail}")
    print(f"Parsers missing: {missing}")

    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())

