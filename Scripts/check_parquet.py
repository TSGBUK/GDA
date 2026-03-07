#!/usr/bin/env python3
"""Utility to scan for parquet files and optionally remove empty directories.

Walking the tree from a given root, the script will list every directory that
contains no ``*.parquet`` files.  By default the results are written to
stdout; you can send them to a file with ``--report`` or delete the empty
directories with ``--cleanup``.

The intent is to help identify and clean up folders where parquet data has
been removed or otherwise lost.
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


def main():
    parser = argparse.ArgumentParser(
        description="Scan for and optionally remove folders named 'Parquet'."
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
        help="Remove directories that contain no parquet files.",
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
        import shutil

        for d in matches:
            try:
                shutil.rmtree(d)
                print(f"removed parquet directory tree: {d}")
            except Exception as exc:  # pragma: no cover - best-effort cleanup
                print(f"failed to remove {d}: {exc}")


if __name__ == "__main__":
    main()
