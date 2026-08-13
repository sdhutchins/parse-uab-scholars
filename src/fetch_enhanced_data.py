import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# All paths relative to project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}


def extract_keywords(items):
    """Extract keywords from publication/grant labels where
    schemeDisplayName is 'Research, Condition and Disease
    Categorization'."""
    keywords = set()
    for item in items:
        labels = item.get("labels", [])
        for label in labels:
            if (
                label.get("schemeDisplayName")
                == "Research, Condition and Disease Categorization"
            ):
                value = label.get("value", "")
                if value:
                    keywords.add(value)
    return " ".join(sorted(keywords))


def fetch_user_details(discovery_id, max_retries=3):
    """Fetch user details including email."""
    url = f"{base_url}/users/{discovery_id}"
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=10)
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
) -> list:
    """Fetch publications or grants linked to a user."""
    url = f"{base_url}/{resource_type}/linkedTo"
    payload = {
        "objectId": discovery_id,
        "category": "user",
        "pagination": {"perPage": limit, "startFrom": 0},
        "sort": "dateDesc",
    }

    for attempt in range(max_retries):
        try:
            r = requests.post(
                url, headers=headers, json=payload, timeout=10
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


def fetch_publications(discovery_id, limit=5, max_retries=3):
    """Fetch publications for a user."""
    return _fetch_linked_resources(
        "publications", discovery_id, limit, max_retries
    )


def fetch_grants(discovery_id, limit=10, max_retries=3):
    """Fetch grants for a user."""
    return _fetch_linked_resources(
        "grants", discovery_id, limit, max_retries
    )


def enhance_profile(profile, output_dir, max_retries=3):
    """Fetch email, publication/grant keywords for a single profile."""
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return None, "no_discovery_id"

    output_path = os.path.join(output_dir, f"{discovery_id}.json")

    # Skip if file already exists and is valid
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if (
                    "email" in existing_data
                    and "publication_keywords" in existing_data
                ):
                    return None, "already_exists"
        except Exception:
            pass  # corrupted file - will retry

    name = profile.get("firstNameLastName", "Unknown")

    user_details = fetch_user_details(discovery_id)
    publications = fetch_publications(discovery_id)
    grants = fetch_grants(discovery_id)

    pub_keywords = extract_keywords(publications)
    grant_keywords = extract_keywords(grants)

    enhanced_profile = profile.copy()
    enhanced_profile.update({
        "email": (
            user_details.get("emailAddress", {}).get("address")
            if user_details else None
        ),
        "publication_keywords": pub_keywords,
        "grant_keywords": grant_keywords,
    })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enhanced_profile, f, indent=2, ensure_ascii=False)

    return discovery_id, "ok"


if __name__ == "__main__":
    # Parallel config
    chunk_id = int(os.getenv("CHUNK_ID", "0"))
    chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
    n_threads = int(os.getenv("N_THREADS", "4"))
    retry_registry_file = os.getenv("RETRY_REGISTRY", None)

    # Paths
    input_file = PROJECT_ROOT / "data/uab_scholars_profiles.jsonl"
    output_dir = PROJECT_ROOT / "data/faculty_data"
    log_file = PROJECT_ROOT / f"logs/chunk_{chunk_id}_enhanced.log"
    error_file = (
        PROJECT_ROOT / f"logs/chunk_{chunk_id}_enhanced_errors.log"
    )

    # Ensure dirs
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    # Load profiles
    with open(input_file, "r", encoding="utf-8") as f:
        all_profiles = [json.loads(line.strip()) for line in f]

    # Load faculty who have students
    with open(
        PROJECT_ROOT / "faculty_with_students.txt", "r"
    ) as f:
        faculty_ids_with_students = set(
            line.strip() for line in f if line.strip()
        )

    # Filter to only faculty who have students
    all_profiles = [
        p for p in all_profiles
        if str(p.get("discoveryId")) in faculty_ids_with_students
    ]

    # Filter to retry list if provided
    if retry_registry_file and os.path.exists(retry_registry_file):
        with open(retry_registry_file, "r") as f:
            retry_ids = set(
                line.strip() for line in f if line.strip()
            )
        all_profiles = [
            p for p in all_profiles
            if str(p.get("discoveryId")) in retry_ids
        ]

    # Partition work
    chunk_size = len(all_profiles) // chunk_total + 1
    user_profiles = all_profiles[
        chunk_id * chunk_size:(chunk_id + 1) * chunk_size
    ]

    # === Run in Parallel ===
    results = []
    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        futures = {
            executor.submit(enhance_profile, profile, output_dir): profile
            for profile in user_profiles
        }
        for future in as_completed(futures):
            profile = futures[future]
            try:
                discovery_id, status = future.result()
                name = profile.get("firstNameLastName", "Unknown")
                if discovery_id:
                    results.append((discovery_id, name, status))
            except Exception as e:
                results.append((
                    profile.get("discoveryId"),
                    profile.get("firstNameLastName"),
                    f"fatal_error: {e}",
                ))

    # === Write Logs and Retry Registry ===
    current_failures = set()

    with open(log_file, "a") as log, open(error_file, "a") as err:
        for discovery_id, name, status in results:
            log.write(f"{discovery_id},{name},{status}\n")
            if any(
                x in status
                for x in ("fail", "error", "retry", "max_retries")
            ):
                err.write(f"{discovery_id},{name},{status}\n")
                current_failures.add(discovery_id)

    # === Always write retry registry ===
    registry_path = (
        retry_registry_file
        or f"logs/retry_registry_enhanced_chunk_{chunk_id}.csv"
    )
    with open(registry_path, "w") as f:
        for rid in sorted(current_failures):
            f.write(f"{rid}\n")

    print(
        f"🎓 Done: chunk {chunk_id}, "
        f"wrote {len(results)} entries to {output_dir}/"
    )
