#!/usr/bin/env python3
"""
Initial National Grid raw CSV acquisition.

What it does:
- Reads datasources.json (Sources[].Links[]).
- Downloads CSV links as raw files.
- Expands datapackage.json links into CSV resource URLs.
- Loads optional cookies from cookie.txt in key,value format.
- Writes DownloadResults.json with file size and last downloaded timestamp.

Default output folder is RAWData, but you can override it with --output-dir.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests


DEFAULT_SOURCES_FILE = "datasources.json"
DEFAULT_OUTPUT_DIR = "RAWData"
DEFAULT_RESULTS_FILENAME = "DownloadResults.json"
DEFAULT_COOKIE_FILE = "cookie.txt"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_RETRIES = 3
DEFAULT_MAX_CONCURRENCY = 2

USER_AGENT = "GDA-NationalGrid-InitAcquire/1.0"


@dataclass(frozen=True)
class SourceLink:
    dataset: str
    url: str


def safe_dataset_folder_name(dataset: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", dataset).strip().strip(".")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned or "unknown-dataset"


def load_sources(source_path: Path) -> list[SourceLink]:
    payload = json.loads(source_path.read_text(encoding="utf-8"))
    raw_sources = payload.get("Sources", [])

    links: list[SourceLink] = []
    for source in raw_sources:
        if not isinstance(source, dict):
            continue
        dataset = str(source.get("Dataset", "unknown")).strip() or "unknown"
        for raw_link in source.get("Links", []):
            if not isinstance(raw_link, str):
                continue
            url = raw_link.strip()
            if url.startswith("http://") or url.startswith("https://"):
                links.append(SourceLink(dataset=dataset, url=url))

    return links


def parse_cookie_file(cookie_file: Path) -> dict[str, str]:
    cookies: dict[str, str] = {}
    if not cookie_file.exists():
        return cookies

    content = cookie_file.read_text(encoding="utf-8", errors="ignore")
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        lower = stripped.lower().replace(" ", "")
        if lower in {"key,value", "name,value"}:
            continue

        if "," in stripped:
            key, value = stripped.split(",", 1)
            key = key.strip().strip('"').strip("'")
            value = value.strip().strip('"').strip("'")
            if key:
                cookies[key] = value
            continue

        if ";" in stripped and "=" in stripped:
            for part in stripped.split(";"):
                bit = part.strip()
                if not bit or "=" not in bit:
                    continue
                key, value = bit.split("=", 1)
                key = key.strip()
                value = value.strip()
                if key:
                    cookies[key] = value
            continue

        if "=" in stripped:
            key, value = stripped.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key:
                cookies[key] = value

    return cookies


def request_with_retries(session: requests.Session, url: str, retries: int, timeout: int) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout, stream=True)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < retries:
                time.sleep(min(5.0, attempt * 1.5))

    raise RuntimeError(f"Failed to download {url}: {last_error}")


def build_output_filename(url: str) -> str:
    parsed = urlparse(url)
    base_name = Path(parsed.path).name or "download.csv"
    if not base_name.lower().endswith(".csv"):
        base_name = f"{base_name}.csv"

    path_parts = [part for part in parsed.path.split("/") if part]
    resource_id = None
    for index, part in enumerate(path_parts):
        if part == "resource" and index + 1 < len(path_parts):
            resource_id = path_parts[index + 1]
            break

    if resource_id:
        return f"{resource_id}__{base_name}"

    short_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()[:10]
    return f"{short_hash}__{base_name}"


def is_csv_url(url: str) -> bool:
    return urlparse(url).path.lower().endswith(".csv")


def normalize_saved_filename(file_name: str) -> str:
    if "__" in file_name:
        return file_name.split("__", 1)[1]
    return file_name


def load_existing_results(results_path: Path) -> dict[str, dict[str, object]]:
    if not results_path.exists():
        return {}

    try:
        payload = json.loads(results_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {}

    files = payload.get("files", []) if isinstance(payload, dict) else []
    existing: dict[str, dict[str, object]] = {}
    for item in files:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        existing[url] = item

    return existing


def write_results_report(
    results_path: Path,
    sources_path: Path,
    output_dir: Path,
    cookies_loaded: int,
    total_links: int,
    results: list[dict[str, object]],
) -> None:
    downloaded_count = sum(1 for item in results if item.get("status") == "downloaded")
    failed_count = sum(1 for item in results if item.get("status") == "failed")
    already_downloaded_count = sum(1 for item in results if item.get("status") == "already_downloaded")

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sources_file": str(sources_path),
        "output_dir": str(output_dir),
        "results_file": str(results_path),
        "cookies_loaded": cookies_loaded,
        "total_links": total_links,
        "downloaded": downloaded_count,
        "already_downloaded": already_downloaded_count,
        "failed": failed_count,
        "files": sorted(results, key=lambda item: str(item.get("file", ""))),
    }

    # Atomic write so the report is always valid and up to date if interrupted.
    results_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=str(results_path.parent),
        prefix="download_results_",
        suffix=".tmp",
        delete=False,
    ) as tmp_handle:
        json.dump(report, tmp_handle, indent=2)
        tmp_handle.write("\n")
        tmp_handle.flush()
        os.fsync(tmp_handle.fileno())
        tmp_name = tmp_handle.name

    Path(tmp_name).replace(results_path)


def expand_csv_links(
    links: list[SourceLink],
    session: requests.Session,
    retries: int,
    timeout: int,
) -> list[SourceLink]:
    expanded: list[SourceLink] = []
    seen: set[str] = set()

    for link in links:
        url = link.url
        lower_url = url.lower()

        if lower_url.endswith("datapackage.json"):
            try:
                response = request_with_retries(session, url, retries=retries, timeout=timeout)
                package = response.json()
            except Exception:
                continue

            resources = package.get("resources", []) if isinstance(package, dict) else []
            for resource in resources:
                if not isinstance(resource, dict):
                    continue
                resource_url = resource.get("url")
                if not resource_url:
                    continue
                absolute_url = urljoin(url, str(resource_url))
                resource_format = str(resource.get("format", "")).lower()
                if not (is_csv_url(absolute_url) or resource_format == "csv"):
                    continue
                if absolute_url in seen:
                    continue
                expanded.append(SourceLink(dataset=link.dataset, url=absolute_url))
                seen.add(absolute_url)
            continue

        if not is_csv_url(url):
            continue

        if url in seen:
            continue

        expanded.append(link)
        seen.add(url)

    return expanded


def download_csv(
    source: SourceLink,
    session: requests.Session,
    output_dir: Path,
    retries: int,
    timeout: int,
    io_lock: threading.Lock,
) -> dict[str, object]:
    file_name = normalize_saved_filename(build_output_filename(source.url))
    dataset_dir = output_dir / safe_dataset_folder_name(source.dataset)
    dataset_dir.mkdir(parents=True, exist_ok=True)
    destination = dataset_dir / file_name
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    try:
        response = request_with_retries(session, source.url, retries=retries, timeout=timeout)

        with io_lock:
            fd, temp_name = tempfile.mkstemp(prefix="raw_", suffix=".part", dir=str(dataset_dir))
            temp_path = Path(temp_name)

        try:
            with open(fd, "wb", buffering=0) as handle:
                for chunk in response.iter_content(chunk_size=1024 * 64):
                    if chunk:
                        handle.write(chunk)

            temp_path.replace(destination)
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

        size_bytes = destination.stat().st_size
        return {
            "dataset": source.dataset,
            "url": source.url,
            "file": str(destination),
            "size_bytes": size_bytes,
            "last_downloaded": timestamp,
            "status": "downloaded",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "dataset": source.dataset,
            "url": source.url,
            "file": str(destination),
            "size_bytes": 0,
            "last_downloaded": None,
            "status": "failed",
            "error": str(exc),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="Initial National Grid raw CSV downloader")
    parser.add_argument("--sources", default=DEFAULT_SOURCES_FILE, help="Path to datasources.json")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Folder where raw CSV files are written (default: RAWData)",
    )
    parser.add_argument(
        "--results-file",
        default="",
        help="Deprecated: ignored. DownloadResults.json is always written in the script folder.",
    )
    parser.add_argument(
        "--cookie-file",
        default=DEFAULT_COOKIE_FILE,
        help="Path to cookie file. Supports key,value lines and Cookie header style.",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--max-concurrency", type=int, default=DEFAULT_MAX_CONCURRENCY)
    args = parser.parse_args()

    if args.max_concurrency < 1:
        raise SystemExit("--max-concurrency must be >= 1")

    sources_path = Path(args.sources).resolve()
    if not sources_path.exists():
        raise SystemExit(f"Sources file not found: {sources_path}")

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    script_dir = Path(__file__).resolve().parent
    results_path = script_dir / DEFAULT_RESULTS_FILENAME

    if args.results_file.strip():
        print("[INFO] --results-file is ignored. Using script folder for DownloadResults.json.")

    cookies = parse_cookie_file(Path(args.cookie_file).resolve())

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    if cookies:
        session.cookies.update(cookies)

    source_links = load_sources(sources_path)
    csv_links = expand_csv_links(
        links=source_links,
        session=session,
        retries=args.retries,
        timeout=args.timeout,
    )

    io_lock = threading.Lock()
    results: list[dict[str, object]] = []
    existing_by_url = load_existing_results(results_path)

    pending_links: list[SourceLink] = []
    for source in csv_links:
        existing = existing_by_url.get(source.url)
        if existing and existing.get("status") in {"downloaded", "already_downloaded"}:
            existing_file = str(existing.get("file", "")).strip()
            if existing_file and Path(existing_file).exists():
                resumed = dict(existing)
                resumed["status"] = "already_downloaded"
                results.append(resumed)
                print(f"[SKIP] {source.url} (already downloaded)")
                continue
        pending_links.append(source)

    print("=== Download Summary (Start) ===")
    print(f"Sources file: {sources_path}")
    print(f"Output directory: {output_dir}")
    print(f"Results file: {results_path}")
    print(f"Cookies loaded: {len(cookies)}")
    print(f"CSV links discovered: {len(csv_links)}")
    print(f"Already downloaded (resume): {len(results)}")
    print(f"Pending downloads: {len(pending_links)}")

    write_results_report(
        results_path=results_path,
        sources_path=sources_path,
        output_dir=output_dir,
        cookies_loaded=len(cookies),
        total_links=len(csv_links),
        results=results,
    )

    with ThreadPoolExecutor(max_workers=args.max_concurrency) as executor:
        futures = [
            executor.submit(
                download_csv,
                source=source,
                session=session,
                output_dir=output_dir,
                retries=args.retries,
                timeout=args.timeout,
                io_lock=io_lock,
            )
            for source in pending_links
        ]

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            status = result.get("status")
            if status == "downloaded":
                print(f"[OK] {result['file']} ({result['size_bytes']} bytes)")
            else:
                print(f"[ERROR] {result['url']} -> {result.get('error', 'unknown error')}")

            # Persist after each completed download attempt so interrupted runs can resume.
            write_results_report(
                results_path=results_path,
                sources_path=sources_path,
                output_dir=output_dir,
                cookies_loaded=len(cookies),
                total_links=len(csv_links),
                results=results,
            )

    downloaded_count = sum(1 for item in results if item.get("status") == "downloaded")
    already_downloaded_count = sum(1 for item in results if item.get("status") == "already_downloaded")
    failed_count = sum(1 for item in results if item.get("status") == "failed")

    print("\n=== Download Summary ===")
    print(f"CSV links discovered: {len(csv_links)}")
    print(f"Downloaded: {downloaded_count}")
    print(f"Already downloaded (resume): {already_downloaded_count}")
    print(f"Failed: {failed_count}")
    print(f"Output directory: {output_dir}")
    print(f"Results file: {results_path}")

    return 1 if failed_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
