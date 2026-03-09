#!/usr/bin/env python3
"""Locate and optionally execute any ``parquet_data_conversion.py`` scripts.

This tool walks a directory tree looking for files literally named
``parquet_data_conversion.py``.  When invoked it will list each match and,
if requested, run the script with the same Python interpreter used for this
utility.

The behaviour mirrors the earlier ``check_parquet.py`` utility but operates on
script files rather than directories.
"""

import os
import argparse
import subprocess
import sys
import re
from pathlib import Path


def find_conversion_scripts(root, include_non_processors=False):
    """Yield full paths of files named ``parquet_data_conversion.py`` under
    ``root``."""
    for dirpath, dirnames, filenames in os.walk(root):
        dir_parts = Path(dirpath).parts
        if any(part in {".venv", "grid", "venv", "__pycache__", ".git"} for part in dir_parts):
            continue
        for fname in filenames:
            if fname == "parquet_data_conversion.py":
                full_path = Path(dirpath) / fname
                if include_non_processors or "Processors" in full_path.parts:
                    yield str(full_path)


def default_root():
    script_path = Path(__file__).resolve()

    # Prefer an ancestor that looks like the repository root.
    for parent in script_path.parents:
        if (parent / "DataSources").is_dir() and (parent / "Scripts").is_dir():
            return str(parent)

    # Back-compat fallback for older layouts that relied on the folder name.
    for parent in script_path.parents:
        if parent.name.lower() == "gda":
            return str(parent)

    return str(Path.cwd())


def _has_module(python_exe, module_name):
    try:
        check = subprocess.run(
            [python_exe, "-c", f"import {module_name}"],
            capture_output=True,
            text=True,
        )
        return check.returncode == 0
    except Exception:
        return False


def select_python_executable(root, explicit_python=None):
    """Pick Python interpreter for child converter scripts.

    Priority:
    1) --python path if provided
    2) current interpreter if it has pandas
    3) repo-local grid/.venv interpreters with pandas
    4) current interpreter (last resort)
    """
    if explicit_python:
        return explicit_python

    if _has_module(sys.executable, "pandas"):
        return sys.executable

    candidates = [
        Path(root) / "grid" / "Scripts" / "python.exe",
        Path(root) / ".venv" / "Scripts" / "python.exe",
        Path(root) / "venv" / "Scripts" / "python.exe",
    ]
    for candidate in candidates:
        if candidate.exists() and _has_module(str(candidate), "pandas"):
            return str(candidate)

    return sys.executable


def dataset_name_from_script(script_path):
    """Infer dataset folder from <dataset>/Processors/parquet_data_conversion.py."""
    path = Path(script_path)
    parts = path.parts
    if "Processors" in parts:
        idx = parts.index("Processors")
        if idx > 0:
            return parts[idx - 1]
    return path.parent.name


def normalize_log_line(dataset, line):
    """Normalize converter output to a consistent batch-friendly format."""
    text = line.strip()
    if not text:
        return None

    conv_match = re.match(r"^\[conv\]\s+(.+?)\s+(?:→|â†’|->)\s+(.+)$", text)
    if conv_match:
        csv_name = Path(conv_match.group(1).strip()).name
        return f"[{dataset}] [conv] {csv_name}"

    skip_match = re.match(r"^\[skip\]\s+(.+)$", text)
    if skip_match:
        body = skip_match.group(1).strip()
        return f"[{dataset}] [skip] {body}"

    clean_match = re.match(r"^\[clean\]\s+(.+)$", text)
    if clean_match:
        return f"[{dataset}] [clean] {clean_match.group(1).strip()}"

    err_match = re.match(r"^\[error\]\s+(.+)$", text)
    if err_match:
        return f"[{dataset}] [error] {err_match.group(1).strip()}"

    if "Traceback" in text or "ModuleNotFoundError" in text:
        return f"[{dataset}] [error] {text}"

    return f"[{dataset}] {text}"


def run_script_pretty(script, python_executable, parquet_engine=None):
    """Run one converter and print normalized output lines."""
    dataset = dataset_name_from_script(script)
    repo_root = str(Path(__file__).resolve().parents[1])
    print(f"\n=== {dataset} ===")
    child_env = os.environ.copy()
    child_env["PYTHONIOENCODING"] = "utf-8"
    existing_pythonpath = child_env.get("PYTHONPATH", "")
    child_env["PYTHONPATH"] = repo_root + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
    if parquet_engine:
        child_env["PARQUET_ENGINE"] = parquet_engine
    ret = subprocess.run(
        [python_executable, script],
        capture_output=True,
        text=True,
        env=child_env,
        cwd=repo_root,
    )

    stdout_lines = ret.stdout.splitlines() if ret.stdout else []
    stderr_lines = ret.stderr.splitlines() if ret.stderr else []

    emitted = False
    for line in stdout_lines + stderr_lines:
        normalized = normalize_log_line(dataset, line)
        if normalized:
            print(normalized)
            emitted = True

    if ret.returncode == 0:
        if not emitted:
            print(f"[{dataset}] [ok] no messages")
        else:
            print(f"[{dataset}] [ok] completed")
    else:
        print(f"[{dataset}] [failed] exit={ret.returncode}")

    return ret.returncode


def main():
    parser = argparse.ArgumentParser(
        description="Find and optionally run parquet_data_conversion scripts."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=default_root(),
        help="Directory to walk (default: detected GDA root).",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Execute each discovered script with the current Python interpreter.",
    )
    parser.add_argument(
        "--python",
        help="Python executable to use for converter scripts (default: auto-detect).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Include parquet_data_conversion.py outside Processors directories.",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="When used with --run, stream raw script output instead of normalized pretty logs.",
    )
    parser.add_argument(
        "--report",
        help="File to which the list of discovered scripts will be written.",
    )
    parser.add_argument(
        "--allow-missing-backend",
        action="store_true",
        help="When set, do not fail if no parquet backend is available; skip conversions.",
    )
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    matches = sorted(set(find_conversion_scripts(root, include_non_processors=args.all)))
    run_python = select_python_executable(root, args.python)

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            for m in matches:
                f.write(m + "\n")
    elif not args.run:
        for m in matches:
            print(m)

    if not matches:
        print(f"No parquet_data_conversion.py scripts found under: {root}")

    if args.run:
        print(f"Found {len(matches)} parquet conversion script(s).")
        print(f"Using Python: {run_python}")
        has_pandas = _has_module(run_python, "pandas")
        has_pyarrow_parquet = _has_module(run_python, "pyarrow.parquet")
        has_fastparquet = _has_module(run_python, "fastparquet")

        if not has_pandas:
            print("[error] Selected interpreter is missing required module: pandas")
            print(f"[hint] Install deps with: \"{run_python}\" -m pip install -r requirements.txt")
            return 1

        parquet_engine = None
        if has_pyarrow_parquet:
            parquet_engine = "pyarrow"
        elif has_fastparquet:
            parquet_engine = "fastparquet"
        else:
            message = "[warn] No parquet backend detected (missing pyarrow.parquet and fastparquet)."
            print(message)
            print(f"[hint] Use Python 3.10-3.12 and install one backend in that env.")
            print(f"[hint] Example: \"{run_python}\" -m pip install fastparquet")
            if args.allow_missing_backend:
                print("[skip] Skipping parquet conversion run because no backend is available.")
                return 0
            return 1

        print(f"Parquet engine: {parquet_engine}")
        failures = 0
        for script in matches:
            if args.raw:
                print(f"running {script}")
                repo_root = str(Path(__file__).resolve().parents[1])
                child_env = os.environ.copy()
                child_env["PYTHONIOENCODING"] = "utf-8"
                existing_pythonpath = child_env.get("PYTHONPATH", "")
                child_env["PYTHONPATH"] = repo_root + (os.pathsep + existing_pythonpath if existing_pythonpath else "")
                if parquet_engine:
                    child_env["PARQUET_ENGINE"] = parquet_engine
                ret = subprocess.run([run_python, script], env=child_env, cwd=repo_root)
                if ret.returncode != 0:
                    failures += 1
                    print(f"script {script} exited with {ret.returncode}")
            else:
                if run_script_pretty(script, run_python, parquet_engine=parquet_engine) != 0:
                    failures += 1

        if failures:
            print(f"\nDone with {failures} failed script(s).")
            return 1
        print("\nDone. All parquet conversion scripts completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
