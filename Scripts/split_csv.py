#!/usr/bin/env python3
"""Recursively split large CSV files into smaller chunks.

This utility scans a root directory for CSV files and splits every file
larger than a threshold into smaller chunks.  Each chunk contains the
original header, and the source file is only deleted after successful,
verified chunk creation.
"""

import sys
import argparse
from pathlib import Path


def human_readable_size(size_bytes):
    """Convert bytes to human-readable format."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def is_generated_chunk(path: Path) -> bool:
    return "_chunk_" in path.stem


def split_one_csv(input_path: Path, chunk_size_mb: float) -> list[Path]:
    """Split one CSV file and return created chunk paths.

    The function writes to temporary files first and renames to final
    `*_chunk_XXX.csv` names only when complete.
    """
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir = input_path.parent
    prefix = input_path.stem
    chunk_size_bytes = int(chunk_size_mb * 1024 * 1024)

    print(f"Splitting: {input_path}")
    print(f"Target chunk size: {chunk_size_mb} MB")
    print(f"Original size: {human_readable_size(input_path.stat().st_size)}")

    temp_chunk_files: list[Path] = []
    final_chunk_files: list[Path] = []
    chunk_num = 1
    current_chunk_size = 0
    current_file = None
    current_temp_path = None
    header = ""
    rows_read = 0
    rows_written = 0
    lines_in_chunk = 0

    try:
        with open(input_path, "r", encoding="utf-8") as infile:
            header = infile.readline()
            if not header:
                raise ValueError(f"CSV has no header row: {input_path}")

            current_temp_path = output_dir / f".{prefix}_chunk_{chunk_num:03d}.csv.tmp"
            current_file = open(current_temp_path, "w", encoding="utf-8")
            current_file.write(header)
            current_chunk_size = len(header.encode("utf-8"))
            temp_chunk_files.append(current_temp_path)

            for line in infile:
                rows_read += 1
                line_size = len(line.encode("utf-8"))

                if current_chunk_size + line_size > chunk_size_bytes and lines_in_chunk > 0:
                    current_file.close()
                    print(
                        f"  ✓ Chunk {chunk_num}: {current_temp_path.name} "
                        f"({human_readable_size(current_temp_path.stat().st_size)}, {lines_in_chunk:,} rows)"
                    )

                    chunk_num += 1
                    current_temp_path = output_dir / f".{prefix}_chunk_{chunk_num:03d}.csv.tmp"
                    current_file = open(current_temp_path, "w", encoding="utf-8")
                    current_file.write(header)
                    current_chunk_size = len(header.encode("utf-8"))
                    temp_chunk_files.append(current_temp_path)
                    lines_in_chunk = 0

                current_file.write(line)
                rows_written += 1
                current_chunk_size += line_size
                lines_in_chunk += 1

            if current_file and not current_file.closed:
                current_file.close()
                print(
                    f"  ✓ Chunk {chunk_num}: {current_temp_path.name} "
                    f"({human_readable_size(current_temp_path.stat().st_size)}, {lines_in_chunk:,} rows)"
                )

        if rows_read != rows_written:
            raise RuntimeError(
                f"Row count mismatch for {input_path.name}: read {rows_read:,}, wrote {rows_written:,}"
            )

        for idx, temp_path in enumerate(temp_chunk_files, start=1):
            final_path = output_dir / f"{prefix}_chunk_{idx:03d}.csv"
            temp_path.replace(final_path)
            final_chunk_files.append(final_path)

    except Exception:
        if current_file and not current_file.closed:
            current_file.close()
        for path in temp_chunk_files:
            if path.exists():
                path.unlink()
        raise

    print(f"✓ Created {len(final_chunk_files)} chunk(s)")
    return final_chunk_files


def find_csvs_to_split(root_dir: Path, threshold_mb: float) -> list[Path]:
    """Find CSV files larger than threshold under root directory."""
    threshold_bytes = int(threshold_mb * 1024 * 1024)
    candidates: list[Path] = []
    for csv_path in root_dir.rglob("*.csv"):
        if not csv_path.is_file():
            continue
        if is_generated_chunk(csv_path):
            continue
        if csv_path.stat().st_size > threshold_bytes:
            candidates.append(csv_path)
    return sorted(candidates)


def auto_split_large_csvs(root_dir: Path, threshold_mb: float, chunk_mb: float, delete_original: bool) -> int:
    """Split all CSVs larger than threshold under root_dir."""
    candidates = find_csvs_to_split(root_dir, threshold_mb)

    print(f"Root: {root_dir}")
    print(f"Threshold: {threshold_mb} MB")
    print(f"Chunk size: {chunk_mb} MB")
    print(f"Delete original after success: {delete_original}")
    print(f"Large CSV files found: {len(candidates)}")
    print("-" * 60)

    processed = 0
    skipped = 0
    failed = 0

    for file_path in candidates:
        try:
            chunk_files = split_one_csv(file_path, chunk_mb)
            total_chunks_size = sum(path.stat().st_size for path in chunk_files)
            print(f"Total chunk size: {human_readable_size(total_chunks_size)}")

            if delete_original:
                file_path.unlink()
                print(f"✓ Removed original: {file_path}")
            else:
                print(f"[keep] Original retained: {file_path}")

            processed += 1
            print("-" * 60)
        except Exception as exc:
            failed += 1
            print(f"[error] {file_path}: {exc}", file=sys.stderr)
            print("-" * 60)

    if not candidates:
        skipped = 1

    print("Summary")
    print("-" * 60)
    print(f"Processed: {processed}")
    print(f"Failed: {failed}")
    if skipped:
        print("No files met the threshold.")

    return 0 if failed == 0 else 1


def main():
    default_root = next(p for p in Path(__file__).resolve().parents if p.name == "GDA")

    parser = argparse.ArgumentParser(
        description="Automatically split large CSV files recursively.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scan GDA and split every CSV > 100MB into 75MB chunks, then delete originals
  python Scripts/split_csv.py

  # Keep originals after splitting
  python Scripts/split_csv.py --keep-original

  # Custom root/threshold/chunk sizes
  python Scripts/split_csv.py --root . --threshold 120 --chunk-size 80
        """
    )

    parser.add_argument(
        "--root",
        default=str(default_root),
        help="Root directory to scan recursively (default: repository root)"
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=100,
        help="Split files larger than this size in megabytes (default: 100)"
    )
    parser.add_argument(
        "--chunk-size",
        type=float,
        default=75,
        help="Target chunk size in megabytes (default: 75)"
    )
    parser.add_argument(
        "--keep-original",
        action="store_true",
        help="Keep the original CSV after successful splitting"
    )

    args = parser.parse_args()

    try:
        root_dir = Path(args.root).resolve()
        if not root_dir.exists():
            raise FileNotFoundError(f"Root directory not found: {root_dir}")
        if args.threshold <= 0:
            raise ValueError("--threshold must be greater than 0")
        if args.chunk_size <= 0:
            raise ValueError("--chunk-size must be greater than 0")

        return auto_split_large_csvs(
            root_dir=root_dir,
            threshold_mb=args.threshold,
            chunk_mb=args.chunk_size,
            delete_original=not args.keep_original,
        )

    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error splitting file: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
