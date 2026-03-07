#!/usr/bin/env python3
"""
Daily National Grid data sync.

What it does:
- Parses datasources.txt where lines starting with # are section/group names.
- Downloads each CSV source (max 2 concurrent downloads).
- Expands any datapackage.json URL into CSV resource URLs.
- Appends new rows into per-source history CSV files.
- De-duplicates rows so reruns do not create duplicate entries.
- Maintains/extends headings safely if a source schema evolves.
- Cleans temp files as it goes.

Auth support:
- Optional auto-load cookies from Chrome (Windows) using browser_cookie3.
- Optional explicit Cookie header via env var NG_COOKIE_HEADER.

Install:
  pip install requests browser-cookie3

Run (from DataSources/NationalGrid folder):
  python daily_ng_sync.py

Run with Chrome cookies:
  python daily_ng_sync.py --use-chrome-cookies

Output:
  ./history/<section_slug>/<resource_id>__<filename>.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from collections import deque
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests


DATA_SOURCE_FILE = "datasources.txt"
DEFAULT_OUTPUT_DIR = "history"
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_MAX_FILES_PER_MINUTE = 30
DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_RETRIES = 3

USER_AGENT = (
    "GDA-NationalGrid-HistorySync/1.1 "
    "(respectful-open-data-sync; max-concurrency=2; max-files-per-min=30; contact=tsgbuk@gmail.com)"
    "(feel free to reach out if you have any questions or concerns about this sync script)"
)

logger = logging.getLogger("daily_ng_sync")


@dataclass(frozen=True)
class SourceEntry:
    section: str
    url: str
    line_number: int


@dataclass
class TaskResult:
    source: SourceEntry
    output_path: Path | None
    downloaded_rows: int
    appended_rows: int
    skipped: bool
    error: str | None


class GlobalRateLimiter:
    def __init__(self, max_calls: int, period_seconds: float) -> None:
        self.max_calls = max(1, int(max_calls))
        self.period_seconds = float(period_seconds)
        self.lock = threading.Lock()
        self.call_times: deque[float] = deque()

    def acquire(self) -> None:
        while True:
            wait_seconds = 0.0
            with self.lock:
                now = time.monotonic()
                while self.call_times and (now - self.call_times[0]) >= self.period_seconds:
                    self.call_times.popleft()

                if len(self.call_times) < self.max_calls:
                    self.call_times.append(now)
                    return

                oldest = self.call_times[0]
                wait_seconds = max(0.01, self.period_seconds - (now - oldest))

            time.sleep(wait_seconds)


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return cleaned or "uncategorized"


def parse_sources(datasource_path: Path) -> list[SourceEntry]:
    current_section = "uncategorized"
    entries: list[SourceEntry] = []

    with datasource_path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line:
                continue

            if line.startswith("#"):
                heading = line.lstrip("#").strip()
                if heading:
                    current_section = heading
                continue

            if line.startswith("http://") or line.startswith("https://"):
                entries.append(SourceEntry(section=current_section, url=line, line_number=line_no))

    return entries


def build_output_filename(url: str) -> str:
    parsed = urlparse(url)
    base_name = Path(parsed.path).name or "download.csv"
    if not base_name.lower().endswith(".csv"):
        base_name = f"{base_name}.csv"

    parts = [p for p in parsed.path.split("/") if p]
    resource_id = None
    for idx, part in enumerate(parts):
        if part == "resource" and idx + 1 < len(parts):
            resource_id = parts[idx + 1]
            break

    if resource_id:
        return f"{resource_id}__{base_name}"

    url_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    return f"{url_hash}__{base_name}"


def source_output_identity(section: str, url: str) -> tuple[str, str]:
    return slugify(section), build_output_filename(url)


def row_signature(row: dict[str, str], header: list[str]) -> str:
    raw = "\x1f".join((row.get(col, "") or "").strip() for col in header)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def open_csv_reader(path: Path):
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        handle = None
        try:
            handle = path.open("r", encoding=encoding, newline="")
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                handle.close()
                raise ValueError("CSV has no header")
            return handle, reader
        except Exception as exc:  # noqa: BLE001
            if handle is not None and not handle.closed:
                handle.close()
            last_error = exc

    raise RuntimeError(f"Unable to read CSV {path}: {last_error}")


def merge_headers(existing: list[str] | None, incoming: list[str]) -> list[str]:
    if not existing:
        return incoming
    merged = list(existing)
    for col in incoming:
        if col not in merged:
            merged.append(col)
    return merged


def rewrite_csv_with_header(path: Path, merged_header: list[str]) -> None:
    temp_fd, temp_name = tempfile.mkstemp(prefix="rewrite_", suffix=".csv", dir=str(path.parent))
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        in_handle, reader = open_csv_reader(path)
        with in_handle, temp_path.open("w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=merged_header, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                normalized = {col: row.get(col, "") for col in merged_header}
                writer.writerow(normalized)

        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def build_existing_hashes(path: Path, header: list[str]) -> set[str]:
    hashes: set[str] = set()
    if not path.exists():
        return hashes

    in_handle, reader = open_csv_reader(path)
    with in_handle:
        for row in reader:
            normalized = {col: row.get(col, "") for col in header}
            hashes.add(row_signature(normalized, header))

    return hashes


def compact_history_file(path: Path) -> tuple[int, int]:
    """
    Rewrites a history CSV in place, removing duplicate rows by full-row signature.
    Returns (rows_before, rows_after).
    """
    if not path.exists() or path.stat().st_size == 0:
        return 0, 0

    in_handle, reader = open_csv_reader(path)
    with in_handle:
        header = list(reader.fieldnames or [])
        if not header:
            return 0, 0

        unique_rows: list[dict[str, str]] = []
        seen: set[str] = set()

        for row in reader:
            normalized = {col: row.get(col, "") for col in header}
            sig = row_signature(normalized, header)
            if sig in seen:
                continue
            seen.add(sig)
            unique_rows.append(normalized)

    rows_before = len(unique_rows)
    in_handle2, reader2 = open_csv_reader(path)
    with in_handle2:
        rows_before = sum(1 for _ in reader2)

    rows_after = len(unique_rows)
    if rows_after == rows_before:
        return rows_before, rows_after

    temp_fd, temp_name = tempfile.mkstemp(prefix="compact_", suffix=".csv", dir=str(path.parent))
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        with temp_path.open("w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=header, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(unique_rows)

        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)

    return rows_before, rows_after


def compact_existing_history(output_root: Path) -> tuple[int, int, int]:
    """
    Compacts all existing history CSVs under output_root.
    Returns (files_compacted, rows_removed, files_scanned).
    """
    csv_files = sorted(output_root.glob("*/*.csv"))
    files_compacted = 0
    rows_removed = 0

    for csv_file in csv_files:
        try:
            before, after = compact_history_file(csv_file)
            removed = before - after
            if removed > 0:
                files_compacted += 1
                rows_removed += removed
                logger.info("[compact] %s removed=%d rows", csv_file, removed)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[compact] failed for %s: %s", csv_file, exc)

    return files_compacted, rows_removed, len(csv_files)


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        cookies[key.strip()] = value.strip()
    return cookies


def load_cookie_header_file(cookie_file: str | None) -> dict[str, str]:
    if not cookie_file:
        return {}

    path = Path(cookie_file).expanduser().resolve()
    if not path.exists():
        logger.warning("Cookie file not found: %s", path)
        return {}

    raw = path.read_bytes()
    if not raw:
        logger.warning("Cookie file is empty: %s", path)
        return {}

    if raw.startswith(b"SQLite format 3"):
        logger.warning(
            "Cookie file %s is a Chrome SQLite DB, not a Cookie header text file.",
            path,
        )
        logger.warning(
            "Use a text file containing: name=value; name2=value2 (or Netscape cookies.txt export)."
        )
        return {}

    content = None
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            content = raw.decode(encoding).strip()
            break
        except Exception:  # noqa: BLE001
            continue

    if not content:
        logger.warning("Cookie file could not be decoded as text: %s", path)
        return {}

    # Support Netscape cookies.txt export format.
    if "\t" in content and any(token in content for token in ("# Netscape", "TRUE\t", "FALSE\t")):
        parsed: dict[str, str] = {}
        for line in content.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            parts = stripped.split("\t")
            if len(parts) < 7:
                continue
            name = parts[5].strip()
            value = parts[6].strip()
            if name:
                parsed[name] = value
        if parsed:
            return parsed

    return parse_cookie_header(content)


def load_chrome_cookies_if_requested(use_chrome_cookies: bool) -> dict[str, str]:
    if not use_chrome_cookies:
        return {}

    try:
        import browser_cookie3  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "--use-chrome-cookies requested but browser_cookie3 is not installed. "
            "Run: pip install browser-cookie3"
        ) from exc

    try:
        jar = browser_cookie3.chrome(domain_name="connecteddata.nationalgrid.co.uk")
        cookies = {cookie.name: cookie.value for cookie in jar}
        return cookies
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Chrome cookie loading failed (continuing without Chrome cookies): %s",
            exc,
        )
        logger.warning(
            "If auth is required, use NG_COOKIE_HEADER env var or --cookie-file with a Cookie header string."
        )
        return {}


def make_session(base_cookies: dict[str, str], timeout_seconds: int) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    if base_cookies:
        session.cookies.update(base_cookies)
    session.request_timeout = timeout_seconds  # type: ignore[attr-defined]
    return session


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    retries: int,
    rate_limiter: GlobalRateLimiter | None = None,
):
    last_error: Exception | None = None
    timeout = getattr(session, "request_timeout", DEFAULT_TIMEOUT_SECONDS)

    for attempt in range(1, retries + 1):
        try:
            if rate_limiter is not None:
                rate_limiter.acquire()
            response = session.request(method, url, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries:
                time.sleep(min(5, attempt * 1.5))

    raise RuntimeError(f"Request failed after {retries} attempts for {url}: {last_error}")


def expand_datapackage_sources(
    entries: Iterable[SourceEntry],
    base_cookies: dict[str, str],
    timeout_seconds: int,
    retries: int,
    rate_limiter: GlobalRateLimiter,
) -> list[SourceEntry]:
    expanded: list[SourceEntry] = []
    seen_outputs: set[tuple[str, str]] = set()
    session = make_session(base_cookies, timeout_seconds)

    for entry in entries:
        url = entry.url
        if not url.lower().endswith("datapackage.json"):
            output_identity = source_output_identity(entry.section, url)
            if output_identity not in seen_outputs:
                expanded.append(entry)
                seen_outputs.add(output_identity)
            continue

        response = request_with_retries(session, "GET", url, retries, rate_limiter=rate_limiter)
        package = response.json()
        resources = package.get("resources", []) if isinstance(package, dict) else []

        for resource in resources:
            if not isinstance(resource, dict):
                continue
            resource_url = resource.get("url")
            if not resource_url:
                continue

            absolute_url = urljoin(url, str(resource_url))
            resource_format = str(resource.get("format", "")).lower()
            if not (
                absolute_url.lower().endswith(".csv")
                or resource_format == "csv"
            ):
                continue

            output_identity = source_output_identity(entry.section, absolute_url)
            if output_identity in seen_outputs:
                continue

            expanded.append(
                SourceEntry(
                    section=entry.section,
                    url=absolute_url,
                    line_number=entry.line_number,
                )
            )
            seen_outputs.add(output_identity)

    return expanded


def process_source(
    source: SourceEntry,
    output_root: Path,
    base_cookies: dict[str, str],
    timeout_seconds: int,
    retries: int,
    polite_delay_seconds: float,
    temp_cleanup_lock: threading.Lock,
    output_locks: dict[Path, threading.Lock],
    output_locks_lock: threading.Lock,
    rate_limiter: GlobalRateLimiter,
) -> TaskResult:
    parsed_path = urlparse(source.url).path.lower()
    if parsed_path and not (
        parsed_path.endswith(".csv")
        or parsed_path.endswith("datapackage.json")
        or "/download/" not in parsed_path
    ):
        return TaskResult(
            source=source,
            output_path=None,
            downloaded_rows=0,
            appended_rows=0,
            skipped=True,
            error=None,
        )

    section_dir = output_root / slugify(source.section)
    section_dir.mkdir(parents=True, exist_ok=True)
    output_path = section_dir / build_output_filename(source.url)

    with output_locks_lock:
        output_lock = output_locks.setdefault(output_path, threading.Lock())

    session = make_session(base_cookies, timeout_seconds)

    temp_fd, temp_name = tempfile.mkstemp(prefix="download_", suffix=".csv", dir=str(section_dir))
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        if polite_delay_seconds > 0:
            time.sleep(polite_delay_seconds)

        response = request_with_retries(
            session,
            "GET",
            source.url,
            retries,
            rate_limiter=rate_limiter,
        )
        with temp_path.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 64):
                if chunk:
                    out.write(chunk)

        in_handle, incoming_reader = open_csv_reader(temp_path)
        with in_handle:
            incoming_header = incoming_reader.fieldnames or []

        with output_lock:
            existing_header: list[str] | None = None
            if output_path.exists() and output_path.stat().st_size > 0:
                existing_handle, existing_reader = open_csv_reader(output_path)
                with existing_handle:
                    existing_header = list(existing_reader.fieldnames or [])

            merged_header = merge_headers(existing_header, incoming_header)

            if output_path.exists() and existing_header and merged_header != existing_header:
                rewrite_csv_with_header(output_path, merged_header)

            existing_hashes = build_existing_hashes(output_path, merged_header)

            appended_rows = 0
            downloaded_rows = 0

            write_header = not output_path.exists() or output_path.stat().st_size == 0
            with output_path.open("a", encoding="utf-8", newline="") as out_handle:
                writer = csv.DictWriter(out_handle, fieldnames=merged_header, extrasaction="ignore")
                if write_header:
                    writer.writeheader()

                in_handle2, incoming_reader2 = open_csv_reader(temp_path)
                with in_handle2:
                    for row in incoming_reader2:
                        downloaded_rows += 1
                        normalized = {col: row.get(col, "") for col in merged_header}
                        sig = row_signature(normalized, merged_header)
                        if sig in existing_hashes:
                            continue
                        writer.writerow(normalized)
                        existing_hashes.add(sig)
                        appended_rows += 1

        return TaskResult(
            source=source,
            output_path=output_path,
            downloaded_rows=downloaded_rows,
            appended_rows=appended_rows,
            skipped=False,
            error=None,
        )

    except Exception as exc:  # noqa: BLE001
        return TaskResult(
            source=source,
            output_path=output_path,
            downloaded_rows=0,
            appended_rows=0,
            skipped=True,
            error=str(exc),
        )
    finally:
        with temp_cleanup_lock:
            try:
                temp_path.unlink(missing_ok=True)
            except PermissionError:
                logger.warning("Temp file cleanup delayed (locked): %s", temp_path)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Temp file cleanup skipped for %s: %s", temp_path, exc)


def main() -> int:
    parser = argparse.ArgumentParser(description="Daily National Grid CSV history sync")
    parser.add_argument("--sources", default=DATA_SOURCE_FILE, help="Path to datasources.txt")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DIR, help="Output history folder")
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    parser.add_argument("--max-files-per-minute", type=int, default=DEFAULT_MAX_FILES_PER_MINUTE)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--polite-delay", type=float, default=0.15)
    parser.add_argument(
        "--compact-existing",
        action="store_true",
        help="Compact existing history CSV files by removing duplicate rows before sync",
    )
    parser.add_argument("--use-chrome-cookies", action="store_true")
    parser.add_argument(
        "--cookie-file",
        default="",
        help="Path to a text file containing full Cookie header value (name=value; name2=value2)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.max_concurrency < 1:
        raise SystemExit("--max-concurrency must be >= 1")

    if args.max_concurrency > 2:
        print("Requested concurrency > 2; capping at 2 to respect source.")
        args.max_concurrency = 2

    if args.max_files_per_minute < 1:
        raise SystemExit("--max-files-per-minute must be >= 1")

    if args.max_files_per_minute > 2:
        print("Requested file rate > 2 per minute; capping at 2 to respect source.")
        args.max_files_per_minute = 30

    source_path = Path(args.sources).resolve()
    if not source_path.exists():
        raise SystemExit(f"Sources file not found: {source_path}")

    output_root = Path(args.output).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    if args.compact_existing:
        print("Compacting existing history CSV files...")
        files_compacted, rows_removed, files_scanned = compact_existing_history(output_root)
        print(
            f"Compaction complete: scanned={files_scanned}, compacted={files_compacted}, rows_removed={rows_removed}"
        )

    entries = parse_sources(source_path)
    if not entries:
        raise SystemExit("No URLs found in datasources file.")

    env_cookie_header = os.getenv("NG_COOKIE_HEADER", "").strip()
    env_cookies = parse_cookie_header(env_cookie_header) if env_cookie_header else {}
    file_cookies = load_cookie_header_file(args.cookie_file)
    chrome_cookies = load_chrome_cookies_if_requested(args.use_chrome_cookies)
    base_cookies = {**chrome_cookies, **file_cookies, **env_cookies}

    print(f"Parsed {len(entries)} source lines from {source_path}")
    if base_cookies:
        print(f"Loaded {len(base_cookies)} auth cookies")
    else:
        print("No auth cookies loaded (public endpoints may still work)")

    print(f"Rate limit: {args.max_files_per_minute} requests per minute")

    rate_limiter = GlobalRateLimiter(max_calls=args.max_files_per_minute, period_seconds=60.0)

    expanded_entries = expand_datapackage_sources(
        entries=entries,
        base_cookies=base_cookies,
        timeout_seconds=args.timeout,
        retries=args.retries,
        rate_limiter=rate_limiter,
    )
    print(f"Expanded to {len(expanded_entries)} concrete downloadable URLs")

    temp_cleanup_lock = threading.Lock()
    output_locks: dict[Path, threading.Lock] = {}
    output_locks_lock = threading.Lock()
    results: list[TaskResult] = []

    with ThreadPoolExecutor(max_workers=min(2, args.max_concurrency)) as executor:
        futures = [
            executor.submit(
                process_source,
                source=entry,
                output_root=output_root,
                base_cookies=base_cookies,
                timeout_seconds=args.timeout,
                retries=args.retries,
                polite_delay_seconds=args.polite_delay,
                temp_cleanup_lock=temp_cleanup_lock,
                output_locks=output_locks,
                output_locks_lock=output_locks_lock,
                rate_limiter=rate_limiter,
            )
            for entry in expanded_entries
        ]

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if result.error:
                print(f"[ERROR] {result.source.url} -> {result.error}")
            elif result.skipped:
                print(f"[SKIP] {result.source.url} (non-CSV source)")
            else:
                s = result.source.url
                words = s.split("download/")
                print(
                    "[Complete - " 
                    f"{words[1]} was downloaded \n"
                    f"{result.downloaded_rows} were downloaded and {result.appended_rows} were appended - Total appened rows: {sum(r.appended_rows for r in results)}    "
                )

    success_count = sum(1 for r in results if not r.error)
    fail_count = sum(1 for r in results if r.error)
    total_downloaded = sum(r.downloaded_rows for r in results)
    total_appended = sum(r.appended_rows for r in results)

    print("\n=== Sync Summary ===")
    print(f"Sources processed: {len(results)}")
    print(f"Succeeded: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Rows downloaded: {total_downloaded}")
    print(f"Rows appended (new): {total_appended}")

    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
