#!/usr/bin/env python3
"""
Unified UAB Scholars data collection pipeline.

Three stages, all in one script:
  1. fetch_all_profiles  - paginate the /api/users endpoint to build
     the faculty JSONL file (run once, not chunked).
  2. fetch_committee_roles - hit the teachingActivities API for each
     faculty member and write per-faculty committee JSON files.
  3. fetch_enhanced_data - for faculty who have committee results
     (students), fetch email, publication keywords, and grant
     keywords.

Stages 2-3 run together per faculty inside process_faculty().
Chunking and parallelism are controlled via environment variables
set by the SLURM wrapper (submit_pipeline.sh).
"""

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BASE_URL = "https://scholars.uab.edu/api"
HEADERS = {"Content-Type": "application/json"}


# -------------------------------------------------------------------
# Stage 1: Fetch all faculty profiles
# -------------------------------------------------------------------

def fetch_all_profiles(
    output_path: Path,
    log_path: Path,
    page_size: int = 25,
) -> int:
    """Download every faculty profile and write one JSON object per
    line to *output_path*.  Returns the number of profiles written."""

    url = f"{BASE_URL}/users"
    filters = [
        {
            "name": n,
            "matchDocsWithMissingValues": True,
            "useValuesToFilter": False,
        }
        for n in ("customFilterOne", "department", "tags")
    ]

    # Determine total records with a single-record probe
    probe_payload = {
        "params": {
            "by": "text",
            "category": "user",
            "text": "",
        },
        "pagination": {"startFrom": 0, "perPage": 1},
        "sort": "relevance",
        "filters": filters,
    }
    resp = requests.post(
        url, headers=HEADERS, json=probe_payload, timeout=10
    )
    resp.raise_for_status()
    total = resp.json().get("pagination", {}).get("total", 0)
    logger.info(f"Found {total} profiles, fetching in pages of {page_size}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with (
        open(output_path, "w", encoding="utf-8") as jsonl,
        open(log_path, "w") as log,
    ):
        for start in range(0, total, page_size):
            payload = {
                "params": {
                    "by": "text",
                    "category": "user",
                    "text": "",
                },
                "pagination": {
                    "startFrom": start,
                    "perPage": page_size,
                },
                "sort": "relevance",
                "filters": filters,
            }
            r = requests.post(
                url, headers=HEADERS, json=payload, timeout=10
            )
            if r.status_code != 200:
                logger.error(f"HTTP {r.status_code} at offset {start}")
                break

            profiles = r.json().get("resource", [])
            for p in profiles:
                jsonl.write(
                    json.dumps(p, ensure_ascii=False) + "\n"
                )
                log.write(
                    f"{start},"
                    f"{p.get('discoveryId')},"
                    f"{p.get('firstNameLastName')}\n"
                )
                count += 1

            logger.info(
                f"Fetched {len(profiles)} profiles "
                f"(offset {start}-{start + page_size - 1})"
            )
            time.sleep(1)

    logger.info(f"Wrote {count} profiles to {output_path}")
    return count


# -------------------------------------------------------------------
# Stage 2: Committee roles for a single faculty member
# -------------------------------------------------------------------

def fetch_committee_roles(
    profile: dict,
    output_dir: Path,
    max_retries: int = 5,
    sleep_secs: int = 2,
) -> tuple[str | None, str, list[dict]]:
    """Fetch graduate committee participation for one faculty member.

    Returns (discovery_id | None, status_string, committee_records).
    The committee_records list is empty when nothing was found or the
    file already existed on disk.
    """
    discovery_id = str(profile.get("discoveryId", ""))
    if not discovery_id:
        return None, "no_discovery_id", []

    output_path = output_dir / f"{discovery_id}.json"

    # Skip if valid file already exists
    if output_path.exists():
        try:
            records = json.loads(output_path.read_text("utf-8"))
            return None, "already_exists", records
        except Exception:
            pass  # corrupted file, will retry

    url = f"{BASE_URL}/teachingActivities/linkedTo"
    name = profile.get("firstNameLastName", "Unknown")
    attempts = 0

    while attempts < max_retries:
        payload = {
            "objectId": discovery_id,
            "category": "user",
            "pagination": {"perPage": 100, "startFrom": 0},
            "sort": "dateDesc",
        }
        try:
            r = requests.post(
                url, headers=HEADERS, json=payload, timeout=20
            )
            if r.status_code == 200:
                activities = [
                    a
                    for a in r.json().get("resource", [])
                    if a.get("objectTypeDisplayName")
                    == "Graduate Committee Participation"
                ]

                records = _build_committee_records(
                    activities, discovery_id, name, profile
                )
                output_path.write_text(
                    json.dumps(records, indent=2, ensure_ascii=False),
                    encoding="utf-8",
                )
                status = "empty" if not records else "ok"
                return discovery_id, status, records

            if r.status_code >= 500:
                logger.warning(
                    f"Retry ({attempts + 1}) for {discovery_id} "
                    f"(HTTP {r.status_code})"
                )
                time.sleep(sleep_secs)
                attempts += 1
                continue

            return (
                discovery_id,
                f"failed_status_{r.status_code}",
                [],
            )

        except Exception as exc:
            logger.warning(
                f"Network error on {discovery_id}: {exc}"
            )
            time.sleep(sleep_secs)
            attempts += 1

    return discovery_id, "max_retries_exceeded", []


def _build_committee_records(
    activities: list[dict],
    discovery_id: str,
    name: str,
    profile: dict,
) -> list[dict]:
    """Transform raw teaching-activity objects into committee
    records."""
    records: list[dict] = []
    for act in activities:
        title = act.get("title", "")
        start = act.get("date1", {}).get("dateTime")
        end = act.get("date2", {}).get("dateTime")

        if end:
            status = (
                "No longer on student's committee "
                "or student has graduated"
            )
        elif "(Committee Member & Mentor)" in title:
            status = "Current mentor"
        elif "(Committee Member)" in title:
            status = "Current committee member"
        else:
            status = "Unknown"

        records.append({
            "userDiscoveryId": discovery_id,
            "userDiscoveryUrlId": profile.get("discoveryUrlId"),
            "userName": name,
            "teachingDiscoveryId": act.get("discoveryId"),
            "title": title,
            "status": status,
            "startDate": start,
            "endDate": end,
        })
    return records


# -------------------------------------------------------------------
# Stage 3: Enhanced data (email, pub/grant keywords) helpers
# -------------------------------------------------------------------

def extract_keywords(items: list[dict]) -> str:
    """Pull 'Research, Condition and Disease Categorization' labels
    from publication or grant objects."""
    keywords: set[str] = set()
    for item in items:
        for label in item.get("labels", []):
            if (
                label.get("schemeDisplayName")
                == "Research, Condition and Disease Categorization"
            ):
                value = label.get("value", "")
                if value:
                    keywords.add(value)
    return " ".join(sorted(keywords))


def fetch_user_details(
    discovery_id: str,
    max_retries: int = 3,
) -> dict | None:
    """Fetch user details (primarily email) from /api/users."""
    url = f"{BASE_URL}/users/{discovery_id}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                return r.json()
            elif r.status_code >= 500:
                time.sleep(2)
                continue
            else:
                return None
        except Exception:
            time.sleep(2)
    return None


def _fetch_linked_resources(
    resource_type: str,
    discovery_id: str,
    limit: int = 10,
    max_retries: int = 3,
) -> list[dict]:
    """Fetch publications or grants linked to a user."""
    url = f"{BASE_URL}/{resource_type}/linkedTo"
    payload = {
        "objectId": discovery_id,
        "category": "user",
        "pagination": {"perPage": limit, "startFrom": 0},
        "sort": "dateDesc",
    }
    for attempt in range(max_retries):
        try:
            r = requests.post(
                url, headers=HEADERS, json=payload, timeout=10
            )
            if r.status_code == 200:
                return r.json().get("resource", [])
            elif r.status_code >= 500:
                time.sleep(2)
                continue
            else:
                return []
        except Exception:
            time.sleep(2)
    return []


def fetch_publications(
    discovery_id: str,
    limit: int = 5,
    max_retries: int = 3,
) -> list[dict]:
    """Fetch recent publications for a user."""
    return _fetch_linked_resources(
        "publications", discovery_id, limit, max_retries
    )


def fetch_grants(
    discovery_id: str,
    limit: int = 10,
    max_retries: int = 3,
) -> list[dict]:
    """Fetch recent grants for a user."""
    return _fetch_linked_resources(
        "grants", discovery_id, limit, max_retries
    )


def fetch_enhanced_data(
    profile: dict,
    output_dir: Path,
) -> tuple[str | None, str]:
    """Fetch email, publication keywords, and grant keywords for one
    faculty member.  Writes to output_dir/{discovery_id}.json.

    Returns (discovery_id | None, status_string).
    """
    discovery_id = str(profile.get("discoveryId", ""))
    if not discovery_id:
        return None, "no_discovery_id"

    output_path = output_dir / f"{discovery_id}.json"

    # Skip if valid enhanced file already exists
    if output_path.exists():
        try:
            existing = json.loads(output_path.read_text("utf-8"))
            if (
                "email" in existing
                and "publication_keywords" in existing
            ):
                return None, "already_exists"
        except Exception:
            pass  # corrupted, will overwrite

    user_details = fetch_user_details(discovery_id)
    publications = fetch_publications(discovery_id)
    grants = fetch_grants(discovery_id)

    enhanced = profile.copy()
    enhanced.update({
        "email": (
            user_details.get("emailAddress", {}).get("address")
            if user_details
            else None
        ),
        "publication_keywords": extract_keywords(publications),
        "grant_keywords": extract_keywords(grants),
    })

    output_path.write_text(
        json.dumps(enhanced, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return discovery_id, "ok"


# -------------------------------------------------------------------
# Combined per-faculty processing (stages 2 + 3)
# -------------------------------------------------------------------

def process_faculty(
    profile: dict,
    committees_dir: Path,
    faculty_data_dir: Path,
) -> tuple[str | None, str]:
    """Process one faculty member: fetch committees, then fetch
    enhanced data only if the faculty has students.

    Returns (discovery_id | None, status_string).
    """
    did, committee_status, records = fetch_committee_roles(
        profile, committees_dir
    )

    # If the file already existed, records were loaded from disk
    has_students = bool(records)

    # For new fetches, check if we got any committee results
    if did is None and committee_status == "already_exists":
        has_students = bool(records)

    if not has_students:
        return did, committee_status

    # Faculty has students: fetch enhanced data
    enhanced_did, enhanced_status = fetch_enhanced_data(
        profile, faculty_data_dir
    )

    # Combine statuses for logging
    if enhanced_did is None and enhanced_status == "already_exists":
        return did, f"committees:{committee_status}|enhanced:cached"

    combined = f"committees:{committee_status}|enhanced:{enhanced_status}"
    return did or enhanced_did, combined


# -------------------------------------------------------------------
# Orchestration
# -------------------------------------------------------------------

def load_profiles(path: Path) -> list[dict]:
    """Read the faculty JSONL file into a list of dicts."""
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line.strip()) for line in f]


def partition_profiles(
    profiles: list[dict],
    chunk_id: int,
    chunk_total: int,
) -> list[dict]:
    """Return the slice of profiles assigned to this chunk."""
    size = len(profiles) // chunk_total + 1
    return profiles[chunk_id * size : (chunk_id + 1) * size]


def run_chunk(
    profiles: list[dict],
    committees_dir: Path,
    faculty_data_dir: Path,
    n_threads: int,
) -> list[tuple[str | None, str, str]]:
    """Process a chunk of faculty in parallel.  Returns a list of
    (discovery_id, faculty_name, status) tuples."""
    results: list[tuple[str | None, str, str]] = []

    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = {
            pool.submit(
                process_faculty, p, committees_dir, faculty_data_dir
            ): p
            for p in profiles
        }
        for future in as_completed(futures):
            profile = futures[future]
            name = profile.get("firstNameLastName", "Unknown")
            try:
                did, status = future.result()
                if did:
                    results.append((did, name, status))
            except Exception as exc:
                results.append((
                    str(profile.get("discoveryId")),
                    name,
                    f"fatal_error: {exc}",
                ))
    return results


def write_logs(
    results: list[tuple[str | None, str, str]],
    log_path: Path,
    error_path: Path,
    retry_path: Path,
) -> None:
    """Write run log, error log, and retry registry."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    failures: set[str] = set()

    with (
        open(log_path, "a") as log,
        open(error_path, "a") as err,
    ):
        for did, name, status in results:
            log.write(f"{did},{name},{status}\n")
            if any(
                k in status
                for k in ("fail", "error", "retry", "max_retries")
            ):
                err.write(f"{did},{name},{status}\n")
                if did:
                    failures.add(did)

    with open(retry_path, "w") as f:
        for rid in sorted(failures):
            f.write(f"{rid}\n")


def main() -> None:
    """Entry point.  Behavior depends on environment variables:

    FETCH_PROFILES=1  - run stage 1 (profile download) and exit.
    Otherwise          - run stages 2+3 on the assigned chunk.

    Chunk variables (set by SLURM):
      CHUNK_ID, CHUNK_TOTAL, N_THREADS, RETRY_REGISTRY
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    profiles_path = PROJECT_ROOT / "data/uab_scholars_profiles.jsonl"

    # Stage 1: profile download mode
    if os.getenv("FETCH_PROFILES") == "1":
        log_path = PROJECT_ROOT / "logs/uab_scholars_fetch.log"
        count = fetch_all_profiles(profiles_path, log_path)
        logger.info(f"Profile fetch complete: {count} profiles")
        return

    # Stages 2+3: chunk processing mode
    chunk_id = int(os.getenv("CHUNK_ID", "0"))
    chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
    n_threads = int(os.getenv("N_THREADS", "4"))
    retry_registry = os.getenv("RETRY_REGISTRY")

    committees_dir = PROJECT_ROOT / "data/committees_by_id"
    faculty_data_dir = PROJECT_ROOT / "data/faculty_data"
    committees_dir.mkdir(parents=True, exist_ok=True)
    faculty_data_dir.mkdir(parents=True, exist_ok=True)

    profiles = load_profiles(profiles_path)

    # Narrow to retry list when a registry file is provided
    if retry_registry and Path(retry_registry).exists():
        retry_ids = set(
            Path(retry_registry).read_text().splitlines()
        )
        retry_ids.discard("")
        profiles = [
            p for p in profiles
            if str(p.get("discoveryId")) in retry_ids
        ]

    chunk = partition_profiles(profiles, chunk_id, chunk_total)
    logger.info(
        f"Chunk {chunk_id}/{chunk_total}: "
        f"processing {len(chunk)} faculty"
    )

    results = run_chunk(
        chunk, committees_dir, faculty_data_dir, n_threads
    )

    log_dir = PROJECT_ROOT / "logs"
    write_logs(
        results,
        log_path=log_dir / f"chunk_{chunk_id}.log",
        error_path=log_dir / f"chunk_{chunk_id}_errors.log",
        retry_path=Path(
            retry_registry
            or str(log_dir / f"retry_registry_chunk_{chunk_id}.csv")
        ),
    )

    logger.info(
        f"Chunk {chunk_id} complete: {len(results)} faculty processed"
    )


if __name__ == "__main__":
    main()
