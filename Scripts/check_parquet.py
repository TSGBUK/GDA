#!/usr/bin/env python3
"""Utility to scan parquet directories and optionally perform a full reset.

Walking the tree from a given root, the script will list every directory named
``Parquet`` (case-insensitive). By default the results are written to stdout;
you can send them to a file with ``--report`` or fully remove parquet outputs
with ``--cleanup``.

The intent is to help identify and reset parquet outputs during environment
rebuilds.
"""

import os
import argparse


SKIP_DIR_NAMES = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "grid",
    "node_modules",
}


def should_skip_path(path):
    lower = path.replace("\\", "/").lower()
    if "/site-packages/" in lower:
        return True
    if "/lib/" in lower and "/python" in lower:
        return True
    return False


def scan(root):
    """Return a list of paths for directories named ``Parquet`` under ``root``.

    The check is case-insensitive so both ``Parquet`` and ``parquet`` are
    detected.  The returned list includes the full path to each matching
    directory.  Removal is handled separately by the caller so this function
    only performs the discovery step.
    """
    matches = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]

        if should_skip_path(dirpath):
            continue

        # look for a folder named 'Parquet' amongst the current level
        # os.walk yields the parent directory in dirpath; we check the basename
        if os.path.basename(dirpath).lower() == "parquet":
            matches.append(dirpath)
    return matches


def cleanup_parquet_data(root, parquet_dirs):
    """Delete parquet folder trees and any loose *.parquet files under root."""
    import shutil

    removed_dirs = 0
    removed_files = 0
    failed = 0

    for d in parquet_dirs:
        try:
            shutil.rmtree(d)
            removed_dirs += 1
            print(f"removed parquet directory tree: {d}")
        except Exception as exc:  # pragma: no cover - best-effort cleanup
            failed += 1
            print(f"failed to remove {d}: {exc}")

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_NAMES]
        if should_skip_path(dirpath):
            continue

        for name in filenames:
            if not name.lower().endswith(".parquet"):
                continue
            file_path = os.path.join(dirpath, name)
            try:
                os.remove(file_path)
                removed_files += 1
                print(f"removed parquet file: {file_path}")
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                failed += 1
                print(f"failed to remove file {file_path}: {exc}")

    print(
        f"cleanup summary: removed_dirs={removed_dirs}, "
        f"removed_files={removed_files}, failed={failed}"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Scan parquet folders and optionally fully reset parquet outputs."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default="GDA",
        help="Root directory to scan (defaults to 'GDA' in the current cwd).",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Full reset: remove all Parquet/parquet directories and *.parquet files under root.",
    )
    parser.add_argument(
        "--report",
        help="Path to a file where the report of empty directories will be written.",
    )
    args = parser.parse_args()

    matches = scan(args.root)

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            for d in matches:
                f.write(d + "\n")
    else:
        for d in matches:
            print(d)

    if args.cleanup:
        cleanup_parquet_data(args.root, matches)


if __name__ == "__main__":
    main()
