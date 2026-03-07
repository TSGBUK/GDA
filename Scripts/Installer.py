#!/usr/bin/env python3
"""Linux-friendly installer/pipeline runner for GDA.

This script mirrors the behavior of Scripts/Installer.ps1 with:
- full pipeline mode
- validate-only mode
- resume support
- step-1 pip output filtering + conditional pip self-upgrade
- package presence checks + critical import health/repair
- step-3 output sanitization + colored formatting
- step-7 live output mode (running + [work] overwrite line)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


TICK = "✔"
CROSS = "✖"
ARROW = "➤"


class Colors:
    CYAN = "\033[36m"
    DARK_CYAN = "\033[96m"
    YELLOW = "\033[33m"
    DARK_YELLOW = "\033[93m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    GRAY = "\033[90m"
    RESET = "\033[0m"


def use_color() -> bool:
    return sys.stdout.isatty() and os.environ.get("NO_COLOR") is None


def color(text: str, code: str) -> str:
    if not use_color():
        return text
    return f"{code}{text}{Colors.RESET}"


@dataclass
class Step:
    number: int
    name: str
    runner: Callable[[], int]


class Installer:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.scripts_dir = Path(__file__).resolve().parent
        self.repo_root = self.scripts_dir.parent
        self.python_exe, self.venv_name, self.venv_exists = self._select_python()

        self.log_file = self.scripts_dir / f"pipeline_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        self.resume_state_file = self.scripts_dir / ".installer_resume.json"
        self.live_suppress_keywords = args.live_output_suppress_keywords or ["[skip]"]

        if args.reset_resume and self.resume_state_file.exists():
            self.resume_state_file.unlink(missing_ok=True)

        self.steps = self._build_steps()
        self.completed_steps: set[int] = set()
        if args.resume:
            self._load_resume_state()

        self.results: list[tuple[int, str, float]] = []

    def _select_python(self) -> tuple[str, str, bool]:
        active_venv = os.environ.get("VIRTUAL_ENV")
        candidates = []
        if active_venv:
            candidates.append((Path(active_venv) / "bin" / "python", Path(active_venv).name))
            candidates.append((Path(active_venv) / "Scripts" / "python.exe", Path(active_venv).name))

        candidates.extend([
            (self.repo_root / "grid" / "bin" / "python", "grid"),
            (self.repo_root / "grid" / "Scripts" / "python.exe", "grid"),
            (self.repo_root / ".venv" / "bin" / "python", ".venv"),
            (self.repo_root / ".venv" / "Scripts" / "python.exe", ".venv"),
        ])

        for path, name in candidates:
            if path.exists():
                return str(path), name, True

        return "python", "system", False

    def _build_steps(self) -> list[Step]:
        if self.args.validate:
            return [
                Step(1, "Validate parquet vs csv", self._run_validate_only),
            ]

        return [
            Step(1, "Install requirements", self._run_step_requirements),
            Step(2, "Check/repair parquet backend", self._run_step_parquet_backend),
            Step(3, "Verify setup", self._run_step_verify_setup),
            Step(4, "Check parquet dirs", self._run_step_check_parquet),
            Step(5, "Dedupe files", self._run_step_dedupe),
            Step(6, "Split large csv files", self._run_step_split),
            Step(7, "Run parquet conversions", self._run_step_run_conversions),
        ]

    def _step_signature(self) -> list[str]:
        return [f"{s.number}:{s.name}" for s in self.steps]

    def _load_resume_state(self) -> None:
        if not self.resume_state_file.exists():
            return
        try:
            data = json.loads(self.resume_state_file.read_text(encoding="utf-8"))
            if data.get("steps") == self._step_signature():
                self.completed_steps = set(int(i) for i in data.get("completed_steps", []))
            else:
                self._print(color("[resume] Step list changed; ignoring old resume state.", Colors.DARK_YELLOW))
        except Exception:
            self._print(color("[resume] Could not parse resume state; starting fresh.", Colors.DARK_YELLOW))

    def _save_resume_state(self, step_number: int) -> None:
        self.completed_steps.add(step_number)
        state = {
            "steps": self._step_signature(),
            "completed_steps": sorted(self.completed_steps),
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        self.resume_state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _log(self, line: str = "") -> None:
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def _print(self, line: str = "") -> None:
        print(line)

    def _header(self) -> None:
        self._print("")
        self._print(color("====================================================", Colors.DARK_CYAN))
        self._print(color("         PYTHON DATA PIPELINE CONTROLLER           ", Colors.CYAN))
        self._print(color("====================================================", Colors.DARK_CYAN))
        self._print(color(f"Venv Name: {self.venv_name}", Colors.GRAY))
        self._print(color(f"Venv Exists: {self.venv_exists}", Colors.GRAY))
        self._print(color(f"Python: {self.python_exe}", Colors.GRAY))
        self._print(color(f"Parquet Cleanup: {self.args.cleanup_parquet}", Colors.GRAY))
        self._print(color(f"Validate Only: {self.args.validate}", Colors.GRAY))
        self._print(color(f"Live Output Suppress Keywords: {', '.join(self.live_suppress_keywords)}", Colors.GRAY))
        self._print(color(f"Resume: {self.args.resume}", Colors.GRAY))
        self._print(color(f"Resume State File: {self.resume_state_file.name}", Colors.GRAY))
        self._print(color(f"Log File: {self.log_file.name}", Colors.GRAY))
        self._print("")

    def _run_process_capture(self, cmd: list[str], cwd: Path | None = None) -> tuple[int, list[str], list[str]]:
        proc = subprocess.Popen(
            cmd,
            cwd=str(cwd) if cwd else None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        out, err = proc.communicate()
        return proc.returncode, out.splitlines(), err.splitlines()

    def _sanitize_verify_line(self, line: str) -> str:
        return re.sub(r"[^\x09\x0A\x0D\x20-\x7E]", "", line)

    def _format_verify_line(self, line: str) -> str:
        txt = self._sanitize_verify_line(line)
        if not txt.strip():
            return txt
        if re.match(r"^=+", txt):
            return color(txt, Colors.DARK_CYAN)
        if re.match(r"^\[[0-9]+\]", txt) or txt.startswith("Summary:"):
            return color(txt, Colors.CYAN)
        if re.search(r"MISSING|No NVIDIA GPU|CUDA toolkit .* not found|\bfailed\b|Traceback", txt, flags=re.I):
            return color(txt, Colors.RED)
        if re.search(r"optional, not installed|CPU-only mode|may not be fully installed", txt, flags=re.I):
            return color(txt, Colors.YELLOW)
        if re.search(r"version|successful|ready for TSGB data processing|All core dependencies installed correctly", txt, flags=re.I):
            return color(txt, Colors.GREEN)
        return txt

    def _run_step_requirements(self) -> int:
        cmd = [self.python_exe, "-m", "pip", "install", "-r", "../requirements.txt"]
        code, out_lines, err_lines = self._run_process_capture(cmd, cwd=self.scripts_dir)

        pip_upgrade_notice = False
        for line in out_lines + err_lines:
            self._log(line)
            if "[notice] A new release of pip is available" in line:
                pip_upgrade_notice = True
            if "Requirement already satisfied:" in line:
                continue
            self._print(line)

        if code != 0:
            return code

        if pip_upgrade_notice:
            self._print(color("[pip] New pip release detected, upgrading pip...", Colors.GRAY))
            ucode, uout, uerr = self._run_process_capture(
                [self.python_exe, "-m", "pip", "install", "--upgrade", "pip"],
                cwd=self.scripts_dir,
            )
            for line in uout + uerr:
                self._log(line)
                if "Requirement already satisfied:" in line:
                    continue
                self._print(line)
            if ucode != 0:
                return ucode

        self._check_installed_packages()
        return self._check_and_repair_critical_imports()

    def _requirements_packages(self) -> list[str]:
        req = self.repo_root / "requirements.txt"
        if not req.exists():
            return []

        packages: list[str] = []
        for raw in req.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("-"):
                continue
            name = re.split(r"[<>=!~]", line, maxsplit=1)[0].strip()
            if "[" in name:
                name = name.split("[", 1)[0].strip()
            if name:
                packages.append(name)
        return sorted(set(packages))

    def _check_installed_packages(self) -> None:
        mapping = {
            "scikit-learn": "sklearn",
            "python-dateutil": "dateutil",
        }
        pkgs = self._requirements_packages()
        if not pkgs:
            return

        self._print("")
        self._print(color("[pkg-check] Verifying required packages", Colors.DARK_CYAN))
        self._log("[pkg-check] Verifying required packages")

        found = missing = unknown = 0
        for pkg in pkgs:
            module = mapping.get(pkg, pkg.replace("-", "_"))
            if not module:
                line = f"=  {pkg} (not detected)"
                self._print(color(line, Colors.YELLOW))
                self._log(line)
                unknown += 1
                continue

            pcode, out, _ = self._run_process_capture(
                [
                    self.python_exe,
                    "-c",
                    "import importlib.util,sys; m=sys.argv[1]; print('FOUND' if importlib.util.find_spec(m) else 'MISSING')",
                    module,
                ],
                cwd=self.scripts_dir,
            )
            probe = out[0].strip() if (pcode == 0 and out) else "UNKNOWN"

            if probe == "FOUND":
                line = f"{TICK} {pkg}"
                self._print(color(line, Colors.GREEN))
                self._log(line)
                found += 1
            elif probe == "MISSING":
                line = f"{CROSS} {pkg}"
                self._print(color(line, Colors.RED))
                self._log(line)
                missing += 1
            else:
                line = f"=  {pkg} (not detected)"
                self._print(color(line, Colors.YELLOW))
                self._log(line)
                unknown += 1

        summary = f"[pkg-check] found={found} missing={missing} not-detected={unknown}"
        self._print(color(summary, Colors.DARK_CYAN))
        self._print(color("[pkg-check] '=' usually means detection ambiguity, not necessarily missing.", Colors.DARK_YELLOW))
        self._log(summary)

    def _check_and_repair_critical_imports(self) -> int:
        checks = [
            ("numpy.rec", "numpy"),
            ("pandas", "pandas"),
            ("pyarrow.parquet", "pyarrow==22.0.0"),
            ("fastparquet", "fastparquet==2025.12.0"),
        ]

        self._print(color("[health] Checking critical imports...", Colors.DARK_CYAN))
        self._log("[health] Checking critical imports...")

        for module, repair in checks:
            ok = self._can_import(module)
            if ok:
                line = f"{TICK} import {module}"
                self._print(color(line, Colors.GREEN))
                self._log(line)
                continue

            warn = f"{CROSS} import {module} (attempting repair: {repair})"
            self._print(color(warn, Colors.RED))
            self._log(warn)

            rcode, rout, rerr = self._run_process_capture(
                [self.python_exe, "-m", "pip", "install", "--no-cache-dir", "--force-reinstall", repair],
                cwd=self.scripts_dir,
            )
            for line in rout + rerr:
                self._log(line)

            if rcode != 0:
                return rcode

            if self._can_import(module):
                line = f"{TICK} repaired {module}"
                self._print(color(line, Colors.GREEN))
                self._log(line)
            else:
                line = f"{CROSS} repair failed for {module}"
                self._print(color(line, Colors.RED))
                self._log(line)
                return 1

        return 0

    def _can_import(self, module: str) -> bool:
        code, out, _ = self._run_process_capture(
            [self.python_exe, "-c", "import importlib,sys; importlib.import_module(sys.argv[1]); print('OK')", module],
            cwd=self.scripts_dir,
        )
        return code == 0 and out and out[0].strip() == "OK"

    def _run_step_parquet_backend(self) -> int:
        if self._can_import("pyarrow.parquet") and self._can_import("fastparquet"):
            return 0
        code, out, err = self._run_process_capture(
            [
                self.python_exe,
                "-m",
                "pip",
                "install",
                "--no-cache-dir",
                "--force-reinstall",
                "pyarrow==22.0.0",
                "fastparquet==2025.12.0",
            ],
            cwd=self.scripts_dir,
        )
        for line in out + err:
            self._log(line)
            self._print(line)
        return code

    def _run_step_verify_setup(self) -> int:
        code, out, err = self._run_process_capture([self.python_exe, "verify_setup.py"], cwd=self.scripts_dir)
        for line in out + err:
            self._log(line)
            self._print(self._format_verify_line(line))
        return code

    def _run_step_check_parquet(self) -> int:
        cmd = [self.python_exe, "check_parquet.py", "..", "--report", "parq_clean.txt"]
        if self.args.cleanup_parquet:
            cmd.append("--cleanup")
        code, out, err = self._run_process_capture(cmd, cwd=self.scripts_dir)
        for line in out + err:
            self._log(line)
            self._print(line)
        return code

    def _run_step_dedupe(self) -> int:
        code, out, err = self._run_process_capture([self.python_exe, "dedupe.py"], cwd=self.scripts_dir)
        for line in out + err:
            self._log(line)
            self._print(line)
        return code

    def _run_step_split(self) -> int:
        code, out, err = self._run_process_capture([self.python_exe, "split_csv.py"], cwd=self.scripts_dir)
        for line in out + err:
            self._log(line)
            self._print(line)
        return code

    def _run_step_run_conversions(self) -> int:
        cmd = [
            self.python_exe,
            "-u",
            "run_parquet_conversions.py",
            "--report",
            "parq_run.txt",
            "--run",
            "--raw",
        ]

        proc = subprocess.Popen(
            cmd,
            cwd=str(self.scripts_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

        work_line_active = False
        work_width = 0
        last_activity = time.time()

        assert proc.stdout is not None
        while True:
            line = proc.stdout.readline()
            if line:
                text = line.rstrip("\n")
                self._log(text)
                last_activity = time.time()

                if any(k.lower() in text.lower() for k in self.live_suppress_keywords):
                    continue

                if text.lstrip().lower().startswith("running "):
                    if work_line_active:
                        print()
                        work_line_active = False
                        work_width = 0
                    self._print(text)
                    continue

                if "[work]" in text.lower():
                    pad = "" if work_width <= len(text) else " " * (work_width - len(text))
                    print(f"\r{text}{pad}", end="", flush=True)
                    work_line_active = True
                    work_width = max(work_width, len(text))
                    continue

                continue

            if proc.poll() is not None:
                break

            if time.time() - last_activity >= 5:
                hb = f"[work] still running... {dt.datetime.now().strftime('%H:%M:%S')}"
                self._log(hb)
                pad = "" if work_width <= len(hb) else " " * (work_width - len(hb))
                print(f"\r{hb}{pad}", end="", flush=True)
                work_line_active = True
                work_width = max(work_width, len(hb))
                last_activity = time.time()

            time.sleep(0.2)

        if work_line_active:
            print()

        return proc.returncode or 0

    def _run_validate_only(self) -> int:
        cmd = [self.python_exe, "validate_parquet_vs_csv.py", "--root", "..", "--report", "parquet_validation.txt"]
        code, out, err = self._run_process_capture(cmd, cwd=self.scripts_dir)
        for line in out + err:
            self._log(line)
            self._print(line)
        return code

    def _run_one_step(self, step: Step, total_steps: int) -> int:
        self._print(color(f"[{step.number}/{total_steps}] {ARROW} {step.name}", Colors.CYAN))
        self._print(color("----------------------------------------------------", Colors.GRAY))

        start = time.time()
        code = step.runner()
        duration = round(time.time() - start, 2)

        if code == 0:
            self._save_resume_state(step.number)
            self._print("")
            self._print(color(f"{TICK} Step completed in {duration} s", Colors.GREEN))
            self._print("")
            self.results.append((step.number, "Success", duration))
            return 0

        self._print("")
        self._print(color(f"{CROSS} Step failed (Exit Code: {code})", Colors.RED))
        self._print(color(f"See log file: {self.log_file.name}", Colors.RED))
        self.results.append((step.number, "Failed", duration))
        return code

    def _show_summary(self) -> None:
        self._print("")
        self._print(color("================== PIPELINE SUMMARY ==================", Colors.YELLOW))
        total = 0.0
        for step_num, status, duration in self.results:
            total += duration
            marker = TICK if status == "Success" else CROSS
            c = Colors.GREEN if status == "Success" else Colors.RED
            self._print(color(f" {marker} Step {step_num} - {duration}s ({status})", c))
        self._print(color("------------------------------------------------------", Colors.YELLOW))
        self._print(color(f" Total Runtime: {round(total, 2)} s", Colors.YELLOW))
        self._print(color("======================================================", Colors.YELLOW))
        self._print("")

    def run(self) -> int:
        self._header()

        total = len(self.steps)
        for step in self.steps:
            if self.args.resume and step.number in self.completed_steps:
                self._print(color(f"[{step.number}/{total}] {ARROW} skipped (resume): already completed", Colors.GRAY))
                self.results.append((step.number, "Resumed", 0.0))
                continue

            code = self._run_one_step(step, total)
            if code != 0:
                self._show_summary()
                return code

        self._show_summary()
        if self.resume_state_file.exists():
            self.resume_state_file.unlink(missing_ok=True)
        self._print(color(f"{TICK} PIPELINE FINISHED SUCCESSFULLY", Colors.GREEN))
        self._print("")
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="GDA installer/pipeline runner (Linux-friendly Python version).",
        epilog=(
            "Warnings:\n"
            "- Full conversion runs can take several hours on large/chunked datasets.\n"
            "- Use --cleanup-parquet only when a full parquet rebuild is intended.\n"
            "- --validate is read-only but can still take a long time on large trees."
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--cleanup-parquet", action="store_true", help="Remove Parquet trees during check step.")
    parser.add_argument("--resume", action="store_true", help="Resume from saved completed steps.")
    parser.add_argument("--reset-resume", action="store_true", help="Clear resume checkpoint before starting.")
    parser.add_argument("--validate", action="store_true", help="Run only CSV-vs-Parquet validation.")
    parser.add_argument(
        "--live-output-suppress-keywords",
        nargs="*",
        default=["[skip]"],
        help="Keywords to suppress from step-7 live output.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    installer = Installer(args)
    return installer.run()


if __name__ == "__main__":
    raise SystemExit(main())
