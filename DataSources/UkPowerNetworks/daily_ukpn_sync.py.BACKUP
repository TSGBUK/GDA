#!/usr/bin/env python3
"""
Daily UK Power Networks portal data sync (public datasets).

What it does:
- Reads either:
    - a catalog CSV export from UKPN OpenDataSoft, or
    - a plain text file containing one export URL per line.
- Selects public datasets by default when using catalog CSV input.
- Builds CSV export URLs per dataset and downloads them.
- Appends only new rows into per-dataset history CSV files.
- De-duplicates rows so reruns do not create duplicate entries.
- Maintains/extends headings safely if source schema evolves.

Auth support:
- Optional auto-load cookies from Chrome (Windows) using browser_cookie3.
- Optional explicit Cookie header via env var UKPN_COOKIE_HEADER.

Install:
  pip install requests browser-cookie3

Run (from repository root):
    python DataSources/UkPowerNetworks/daily_ukpn_sync.py --catalog "DataSources/UkPowerNetworks/datasources.txt"

Output:
  ./DataSources/UkPowerNetworks/history/<section_slug>/<datasetid>__export.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import requests


DEFAULT_CATALOG_PATH = "datasources.txt"
DEFAULT_OUTPUT_DIR = "history"
DEFAULT_MAX_CONCURRENCY = 2
DEFAULT_MAX_FILES_PER_MINUTE = 30
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_RETRIES = 3
DEFAULT_STATE_JSON = "ukpn_sync_state.json"
UKPN_DOMAIN = "ukpowernetworks.opendatasoft.com"

USER_AGENT = (
    "GDA-UKPN-HistorySync/1.0 "
    "(respectful-open-data-sync; max-concurrency=2; max-files-per-min=30; contact=local-user)"
)

logger = logging.getLogger("daily_ukpn_sync")


@dataclass(frozen=True)
class DatasetSource:
    section: str
    datasetid: str
    access_rights: str
    export_url: str
    fallback_url: str


@dataclass
class TaskResult:
    source: DatasetSource
    output_path: Path | None
    downloaded_rows: int
    appended_rows: int
    skipped: bool
    skip_reason: str | None
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


def row_signature(row: dict[str, str], header: list[str]) -> str:
    raw = "\x1f".join((row.get(col, "") or "").strip() for col in header)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def format_bytes(size_bytes: int | None) -> str:
    if size_bytes is None or size_bytes < 0:
        return "unknown"

    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(size_bytes)
    idx = 0
    while value >= 1024.0 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1

    if idx == 0:
        return f"{int(value)} {units[idx]}"
    return f"{value:.2f} {units[idx]}"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_utc_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def determine_sync_interval(row_count: int) -> timedelta:
    if row_count < 50_000:
        return timedelta(hours=1)
    elif row_count < 250_000:
        return timedelta(hours=4)
    elif row_count < 1_000_000:
        return timedelta(days=1)
    elif row_count < 10_000_000:
        return timedelta(days=7)
    elif row_count < 20_000_000:
        return timedelta(days=14)
    else:
        return timedelta(days=30)


def count_data_rows(path: Path) -> int:
    if not path.exists() or path.stat().st_size == 0:
        return 0

    in_handle, reader = open_csv_reader(path)
    with in_handle:
        return sum(1 for _ in reader)


def extract_datasetid_from_output_path(path: Path) -> str:
    name = path.name
    suffix = "__export.csv"
    if name.endswith(suffix):
        return name[: -len(suffix)]
    return path.stem


def load_sync_state(path: Path) -> dict:
    if not path.exists():
        return {
            "schemaVersion": "1.0.0",
            "generatedAtUtc": utc_now_iso(),
            "files": {},
        }

    try:
        content = path.read_text(encoding="utf-8")
        loaded = json.loads(content)
        if not isinstance(loaded, dict):
            raise ValueError("state JSON root must be object")
        loaded.setdefault("schemaVersion", "1.0.0")
        loaded.setdefault("files", {})
        if not isinstance(loaded.get("files"), dict):
            loaded["files"] = {}
        return loaded
    except Exception as exc:  # noqa: BLE001
        logger.warning("State JSON could not be loaded (%s). Reinitializing.", exc)
        return {
            "schemaVersion": "1.0.0",
            "generatedAtUtc": utc_now_iso(),
            "files": {},
        }


def save_sync_state(path: Path, state: dict) -> None:
    state["generatedAtUtc"] = utc_now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def should_download_now(datasetid: str, files_state: dict, now_utc: datetime) -> tuple[bool, str | None]:
    file_state = files_state.get(datasetid)
    if not isinstance(file_state, dict):
        return True, None

    next_due_str = file_state.get("nextDownloadUtc")
    next_due_dt = parse_utc_iso(next_due_str)
    if next_due_dt is None:
        return True, None

    if now_utc >= next_due_dt:
        return True, None

    return False, f"next due at {next_due_dt.isoformat()}"


def build_history_metrics(output_root: Path) -> dict[str, dict[str, int | str]]:
    metrics: dict[str, dict[str, int | str]] = {}
    for csv_path in output_root.rglob("*__export.csv"):
        if not csv_path.is_file():
            continue

        datasetid = extract_datasetid_from_output_path(csv_path)
        try:
            row_count = count_data_rows(csv_path)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not count rows for %s: %s", csv_path, exc)
            row_count = 0

        metrics[datasetid] = {
            "path": str(csv_path),
            "sizeBytes": int(csv_path.stat().st_size),
            "rowCount": int(row_count),
        }

    return metrics


def update_single_dataset_state(
    state: dict,
    datasetid: str,
    output_path: Path,
    now_utc: datetime,
    was_downloaded: bool = False,
) -> None:
    """Update state for a single dataset based on its output file."""
    files_state = state.setdefault("files", {})
    if not isinstance(files_state, dict):
        files_state = {}
        state["files"] = files_state

    existing = files_state.get(datasetid)
    if not isinstance(existing, dict):
        existing = {}

    try:
        row_count = count_data_rows(output_path)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not count rows for %s: %s", output_path, exc)
        row_count = 0

    interval = determine_sync_interval(row_count)
    last_download = existing.get("lastDownloadedUtc")
    if was_downloaded or not last_download:
        last_download = now_utc.isoformat()

    last_download_dt = parse_utc_iso(last_download) or now_utc
    next_due = (last_download_dt + interval).isoformat()

    try:
        size_bytes = int(output_path.stat().st_size)
    except Exception:  # noqa: BLE001
        size_bytes = 0

    files_state[datasetid] = {
        "path": str(output_path),
        "sizeBytes": size_bytes,
        "rowCount": row_count,
        "lastDownloadedUtc": last_download_dt.isoformat(),
        "nextDownloadUtc": next_due,
        "intervalSeconds": int(interval.total_seconds()),
    }


def update_sync_state_from_history(
    state: dict,
    output_root: Path,
    downloaded_datasetids: set[str],
    now_utc: datetime,
) -> None:
    files_state = state.setdefault("files", {})
    if not isinstance(files_state, dict):
        files_state = {}
        state["files"] = files_state

    history_metrics = build_history_metrics(output_root)

    for datasetid, metric in history_metrics.items():
        existing = files_state.get(datasetid)
        if not isinstance(existing, dict):
            existing = {}

        row_count = int(metric["rowCount"])
        interval = determine_sync_interval(row_count)
        last_download = existing.get("lastDownloadedUtc")
        if datasetid in downloaded_datasetids or not last_download:
            last_download = now_utc.isoformat()

        last_download_dt = parse_utc_iso(last_download) or now_utc
        next_due = (last_download_dt + interval).isoformat()

        files_state[datasetid] = {
            "path": metric["path"],
            "sizeBytes": int(metric["sizeBytes"]),
            "rowCount": row_count,
            "lastDownloadedUtc": last_download_dt.isoformat(),
            "nextDownloadUtc": next_due,
            "intervalSeconds": int(interval.total_seconds()),
        }


def open_csv_reader(path: Path):
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
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


def parse_cookie_header(cookie_header: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    ignored_attrs = {
        "expires",
        "max-age",
        "domain",
        "path",
        "secure",
        "httponly",
        "samesite",
        "priority",
        "partitioned",
    }
    for part in cookie_header.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if key.lower() in ignored_attrs:
            continue
        cookies[key] = value.strip()
    return cookies


def parse_cookie_file_content(content: str) -> dict[str, str]:
    # Supports:
    # 1) Cookie header format: name=value; name2=value2
    # 2) Set-Cookie lines (with attributes), one per line
    # 3) Multi-line plain name=value pairs
    cookies: dict[str, str] = {}
    normalized = content.replace("\r", "\n")
    lines = [line.strip() for line in normalized.split("\n") if line.strip()]

    if len(lines) == 1 and "set-cookie:" not in lines[0].lower():
        return parse_cookie_header(lines[0])

    ignored_attrs = {
        "expires",
        "max-age",
        "domain",
        "path",
        "secure",
        "httponly",
        "samesite",
        "priority",
        "partitioned",
    }

    for line in lines:
        lower = line.lower()
        if lower.startswith("cookie:"):
            line = line.split(":", 1)[1].strip()
            cookies.update(parse_cookie_header(line))
            continue

        if lower.startswith("set-cookie:"):
            line = line.split(":", 1)[1].strip()

        first_segment = line.split(";", 1)[0].strip()
        if "=" in first_segment:
            name, value = first_segment.split("=", 1)
            name = name.strip()
            if name and name.lower() not in ignored_attrs:
                cookies[name] = value.strip()
            continue

        # Fallback for unusual multi-cookie lines without explicit prefix.
        cookies.update(parse_cookie_header(line))

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

    return parse_cookie_file_content(content)


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
        jar = browser_cookie3.chrome(domain_name=UKPN_DOMAIN)
        return {cookie.name: cookie.value for cookie in jar}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Chrome cookie loading failed (continuing without Chrome cookies): %s", exc)
        logger.warning(
            "If auth is required, use UKPN_COOKIE_HEADER env var or --cookie-file with a Cookie header string."
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
    url: str,
    retries: int,
    rate_limiter: GlobalRateLimiter,
) -> requests.Response:
    last_error: Exception | None = None
    timeout = getattr(session, "request_timeout", DEFAULT_TIMEOUT_SECONDS)

    for attempt in range(1, retries + 1):
        try:
            rate_limiter.acquire()
            response = session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries:
                time.sleep(min(5, attempt * 1.5))

    raise RuntimeError(f"Request failed after {retries} attempts for {url}: {last_error}")


def infer_section(row: dict[str, str]) -> str:
    for key in (
        "default.theme",
        "dublin-core.subject",
        "inspire.theme",
    ):
        value = (row.get(key, "") or "").strip()
        if value and value.lower() != "n/a":
            return value.split(",", 1)[0].strip()
    return "uncategorized"


def get_access_rights(row: dict[str, str]) -> str:
    for key in (
        "dublin-core.accessRights",
        "dcat.accessRights",
        "default.access_rights",
    ):
        value = (row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def build_export_urls(datasetid: str) -> tuple[str, str]:
    quoted = quote(datasetid, safe="")
    primary = (
        f"https://{UKPN_DOMAIN}/api/explore/v2.1/catalog/datasets/{quoted}/exports/csv"
        "?lang=en&timezone=UTC&use_labels=false"
    )
    fallback = f"https://{UKPN_DOMAIN}/explore/dataset/{quoted}/download/?format=csv"
    return primary, fallback


def extract_datasetid_from_url(url: str) -> str:
    # Supports URLs like: /catalog/datasets/<datasetid>/exports/csv
    match = re.search(r"/catalog/datasets/([^/]+)/exports/csv", url)
    if not match:
        return ""
    return match.group(1).strip()


def parse_sources_from_url_list(path: Path) -> list[DatasetSource]:
    sources: list[DatasetSource] = []
    seen_dataset_ids: set[str] = set()

    raw = path.read_text(encoding="utf-8", errors="ignore")
    for raw_line in raw.splitlines():
        # Treat lines that start with # as comments in URL list files.
        if raw_line.lstrip().startswith("#"):
            continue

        url = raw_line.strip()
        if not url:
            continue

        datasetid = extract_datasetid_from_url(url)
        if not datasetid or datasetid in seen_dataset_ids:
            continue

        _, fallback_url = build_export_urls(datasetid)
        sources.append(
            DatasetSource(
                section="",
                datasetid=datasetid,
                access_rights="",
                export_url=url,
                fallback_url=fallback_url,
            )
        )
        seen_dataset_ids.add(datasetid)

    return sources


def parse_catalog_sources(catalog_path: Path, public_only: bool = True) -> list[DatasetSource]:
    in_handle, reader = open_csv_reader(catalog_path)
    sources: list[DatasetSource] = []
    seen_dataset_ids: set[str] = set()

    with in_handle:
        for row in reader:
            datasetid = (row.get("datasetid", "") or "").strip()
            if not datasetid or datasetid in seen_dataset_ids:
                continue

            access_rights = get_access_rights(row)
            if public_only and access_rights.lower() != "open":
                continue

            section = infer_section(row)
            export_url, fallback_url = build_export_urls(datasetid)

            sources.append(
                DatasetSource(
                    section=section,
                    datasetid=datasetid,
                    access_rights=access_rights,
                    export_url=export_url,
                    fallback_url=fallback_url,
                )
            )
            seen_dataset_ids.add(datasetid)

    return sources


def parse_sources(catalog_path: Path, public_only: bool = True) -> list[DatasetSource]:
    # If file looks like a URL list, parse as manual datasource list.
    if catalog_path.suffix.lower() in {".txt", ".list"}:
        return parse_sources_from_url_list(catalog_path)

    # Fallback to catalog CSV behavior.
    return parse_catalog_sources(catalog_path, public_only=public_only)


def dataset_id_to_parser_name(datasetid: str) -> str:
    """Convert dataset ID to parser script filename.
    
    Examples:
        "ukpn-live-faults" -> "parse_ukpn_live_faults.py"
        "grid-and-primary-sites" -> "parse_grid_and_primary_sites.py"
    """
    safe = []
    for ch in datasetid:
        if ch.isalnum():
            safe.append(ch.lower())
        else:
            safe.append("_")
    normalized = "".join(safe)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return f"parse_{normalized.strip('_')}.py"


def run_parquet_conversion(datasetid: str, processors_dir: Path) -> bool:
    """Run parquet conversion for a specific dataset.
    
    Returns True if successful, False otherwise.
    """
    parser_name = dataset_id_to_parser_name(datasetid)
    parser_path = processors_dir / parser_name
    
    if not parser_path.exists():
        logger.warning("Parser not found for %s: %s (skipping parquet conversion)", datasetid, parser_path)
        return False
    
    try:
        cmd = [sys.executable, str(parser_path)]
        print(f"[PARQUET] {datasetid}: running {parser_name}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.warning("Parquet conversion failed for %s: %s", datasetid, result.stderr)
            print(f"[PARQUET-ERROR] {datasetid}: {result.stderr}")
            return False
        print(f"[PARQUET-OK] {datasetid}")
        return True
    except subprocess.TimeoutExpired:
        logger.warning("Parquet conversion timeout for %s", datasetid)
        print(f"[PARQUET-TIMEOUT] {datasetid}")
        return False
    except Exception as exc:  # noqa: BLE001
        logger.warning("Parquet conversion error for %s: %s", datasetid, exc)
        print(f"[PARQUET-ERROR] {datasetid}: {exc}")
        return False


def process_source(
    source: DatasetSource,
    output_root: Path,
    base_cookies: dict[str, str],
    timeout_seconds: int,
    retries: int,
    polite_delay_seconds: float,
    temp_cleanup_lock: threading.Lock,
    output_locks: dict[Path, threading.Lock],
    output_locks_lock: threading.Lock,
    rate_limiter: GlobalRateLimiter,
    state: dict,
    state_json_path: Path,
    state_lock: threading.Lock,
    now_utc: datetime,
    processors_dir: Path,
    skip_parquet: bool = False,
) -> TaskResult:
    section_dir = output_root / slugify(source.section)
    section_dir.mkdir(parents=True, exist_ok=True)
    output_path = section_dir / f"{source.datasetid}__export.csv"

    with output_locks_lock:
        output_lock = output_locks.setdefault(output_path, threading.Lock())

    session = make_session(base_cookies, timeout_seconds)
    temp_fd, temp_name = tempfile.mkstemp(prefix="download_", suffix=".csv", dir=str(section_dir))
    os.close(temp_fd)
    temp_path = Path(temp_name)

    try:
        if polite_delay_seconds > 0:
            time.sleep(polite_delay_seconds)

        download_error: Exception | None = None
        response: requests.Response | None = None
        for url in (source.export_url, source.fallback_url):
            try:
                response = request_with_retries(session, url, retries, rate_limiter)
                break
            except Exception as exc:  # noqa: BLE001
                download_error = exc

        if response is None:
            raise RuntimeError(f"All download endpoints failed for {source.datasetid}: {download_error}")

        size_header = response.headers.get("Content-Length", "").strip()
        expected_size: int | None = None
        if size_header.isdigit():
            expected_size = int(size_header)

        print(
            f"[START] {source.datasetid}: expected-size={format_bytes(expected_size)}"
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

        # Update state JSON immediately after successful download
        with state_lock:
            update_single_dataset_state(
                state=state,
                datasetid=source.datasetid,
                output_path=output_path,
                now_utc=now_utc,
                was_downloaded=True,
            )
            save_sync_state(state_json_path, state)

        # Run parquet conversion unless skipped
        parquet_ok = True
        if not skip_parquet:
            parquet_ok = run_parquet_conversion(source.datasetid, processors_dir)

        return TaskResult(
            source=source,
            output_path=output_path,
            downloaded_rows=downloaded_rows,
            appended_rows=appended_rows,
            skipped=False,
            skip_reason=None,
            error=None,
        )

    except Exception as exc:  # noqa: BLE001
        return TaskResult(
            source=source,
            output_path=output_path,
            downloaded_rows=0,
            appended_rows=0,
            skipped=True,
            skip_reason=None,
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
    script_dir = Path(__file__).resolve().parent

    parser = argparse.ArgumentParser(description="Daily UK Power Networks CSV history sync")
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG_PATH,
        help="Path to datasource input file (catalog CSV or URL list text file)",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT_DIR, help="Output history folder")
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    parser.add_argument("--max-files-per-minute", type=int, default=DEFAULT_MAX_FILES_PER_MINUTE)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--polite-delay", type=float, default=0.15)
    parser.add_argument(
        "--include-non-public",
        action="store_true",
        help="Include non-public datasets listed in the catalog (may still require auth and fail)",
    )
    parser.add_argument("--use-chrome-cookies", action="store_true")
    parser.add_argument(
        "--cookie-file",
        default="",
        help="Path to a text file containing full Cookie header value (name=value; name2=value2)",
    )
    parser.add_argument(
        "--state-json",
        default=DEFAULT_STATE_JSON,
        help="Path to sync state JSON (tracks file size, row count, and next scheduled download)",
    )
    parser.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Skip parquet conversion after downloads",
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

    catalog_path = Path(args.catalog).expanduser().resolve()
    if not catalog_path.exists():
        raise SystemExit(f"Catalog file not found: {catalog_path}")

    output_root = Path(args.output)
    if not output_root.is_absolute():
        output_root = (script_dir / output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    state_json_path = Path(args.state_json)
    if not state_json_path.is_absolute():
        state_json_path = (script_dir / state_json_path).resolve()

    processors_dir = script_dir / "Processors"
    if not processors_dir.exists():
        raise SystemExit(f"Processors directory not found: {processors_dir}")

    state = load_sync_state(state_json_path)
    now_utc = datetime.now(timezone.utc)

    public_only = not args.include_non_public
    all_sources = parse_sources(catalog_path, public_only=public_only)
    if not all_sources:
        raise SystemExit("No matching datasets found in catalog.")

    files_state = state.get("files", {}) if isinstance(state.get("files"), dict) else {}
    sources: list[DatasetSource] = []
    skipped_by_schedule: list[TaskResult] = []
    for source in all_sources:
        should_download, reason = should_download_now(source.datasetid, files_state, now_utc)
        if should_download:
            sources.append(source)
        else:
            skipped_by_schedule.append(
                TaskResult(
                    source=source,
                    output_path=None,
                    downloaded_rows=0,
                    appended_rows=0,
                    skipped=True,
                    skip_reason=reason,
                    error=None,
                )
            )

    env_cookie_header = os.getenv("UKPN_COOKIE_HEADER", "").strip()
    env_cookies = parse_cookie_header(env_cookie_header) if env_cookie_header else {}
    file_cookies = load_cookie_header_file(args.cookie_file)
    chrome_cookies = load_chrome_cookies_if_requested(args.use_chrome_cookies)
    base_cookies = {**chrome_cookies, **file_cookies, **env_cookies}

    print(f"Catalog: {catalog_path}")
    print(f"Matched datasets: {len(all_sources)} ({'public only' if public_only else 'all access levels'})")
    print(f"Due for download now: {len(sources)}")
    print(f"Skipped by schedule: {len(skipped_by_schedule)}")
    print(f"Output root: {output_root}")
    print(f"State JSON: {state_json_path}")
    if base_cookies:
        print(f"Loaded {len(base_cookies)} auth cookies")
    else:
        print("No auth cookies loaded (public endpoints may still work)")

    print(f"Rate limit: {args.max_files_per_minute} requests per minute")

    rate_limiter = GlobalRateLimiter(max_calls=args.max_files_per_minute, period_seconds=60.0)
    temp_cleanup_lock = threading.Lock()
    output_locks: dict[Path, threading.Lock] = {}
    output_locks_lock = threading.Lock()
    state_lock = threading.Lock()
    results: list[TaskResult] = []

    if sources:
        with ThreadPoolExecutor(max_workers=min(2, args.max_concurrency)) as executor:
            futures = [
                executor.submit(
                    process_source,
                    source=source,
                    output_root=output_root,
                    base_cookies=base_cookies,
                    timeout_seconds=args.timeout,
                    retries=args.retries,
                    polite_delay_seconds=args.polite_delay,
                    temp_cleanup_lock=temp_cleanup_lock,
                    output_locks=output_locks,
                    output_locks_lock=output_locks_lock,
                    rate_limiter=rate_limiter,
                    state=state,
                    state_json_path=state_json_path,
                    state_lock=state_lock,
                    now_utc=now_utc,
                    processors_dir=processors_dir,
                    skip_parquet=args.skip_parquet,
                )
                for source in sources
            ]

            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result.error:
                    print(f"[ERROR] {result.source.datasetid} -> {result.error}")
                else:
                    print(
                        f"[COMPLETE] {result.source.datasetid}: downloaded={result.downloaded_rows} "
                        f"appended={result.appended_rows}"
                    )

    for skipped in skipped_by_schedule:
        results.append(skipped)
        print(f"[SKIP] {skipped.source.datasetid}: {skipped.skip_reason}")

    success_count = sum(1 for r in results if not r.error)
    fail_count = sum(1 for r in results if r.error)
    skipped_count = sum(1 for r in results if r.skipped and not r.error)
    total_downloaded = sum(r.downloaded_rows for r in results)
    total_appended = sum(r.appended_rows for r in results)

    downloaded_datasetids = {
        r.source.datasetid
        for r in results
        if not r.error and not r.skipped
    }
    update_sync_state_from_history(state, output_root, downloaded_datasetids, datetime.now(timezone.utc))
    save_sync_state(state_json_path, state)

    print("\n=== Sync Summary ===")
    print(f"Datasets processed: {len(results)}")
    print(f"Succeeded: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Skipped: {skipped_count}")
    print(f"Rows downloaded: {total_downloaded}")
    print(f"Rows appended (new): {total_appended}")
    print(f"State JSON written: {state_json_path}")

    return 1 if fail_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
