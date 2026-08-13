#!/usr/bin/env python3
"""
Local test version of enhanced data collection.

Imports shared functions from src/fetch_enhanced_data.py to avoid
duplicating API logic.
"""

import os
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Allow imports from src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.fetch_uab_data import (
    extract_keywords,
    fetch_user_details,
    fetch_publications,
    fetch_grants,
)

# Test config (single chunk, single thread for local testing)
chunk_id = 0
chunk_total = 1
n_threads = 1
retry_registry_file = None

# Paths
input_file = PROJECT_ROOT / "data/uab_scholars_profiles.jsonl"
output_dir = Path(__file__).resolve().parent / "test_faculty_data"
log_file = Path(__file__).resolve().parent / "test_enhanced.log"
error_file = Path(__file__).resolve().parent / "test_enhanced_errors.log"

# Ensure dirs
os.makedirs(output_dir, exist_ok=True)

# Load profiles
print("📖 Loading profiles from JSONL...")
with open(input_file, "r", encoding="utf-8") as f:
    all_profiles = [json.loads(line.strip()) for line in f]

# Load faculty who have students (test file)
print("📖 Loading test faculty IDs...")
test_ids_file = Path(__file__).resolve().parent / "test_faculty_ids.txt"
with open(test_ids_file, "r") as f:
    faculty_ids_with_students = set(
        line.strip() for line in f if line.strip()
    )

print(
    f"🔍 Found {len(faculty_ids_with_students)} test faculty IDs: "
    f"{list(faculty_ids_with_students)}"
)

# Filter to only faculty who have students
all_profiles = [
    p for p in all_profiles
    if str(p.get("discoveryId")) in faculty_ids_with_students
]

print(f"✅ Filtered to {len(all_profiles)} profiles")

# Filter to retry list if provided
if retry_registry_file and os.path.exists(retry_registry_file):
    with open(retry_registry_file, "r") as f:
        retry_ids = set(line.strip() for line in f if line.strip())
    all_profiles = [
        p for p in all_profiles
        if str(p.get("discoveryId")) in retry_ids
    ]

# Partition work (for single chunk, this is just all profiles)
chunk_size = len(all_profiles) // chunk_total + 1
user_profiles = all_profiles[
    chunk_id * chunk_size:(chunk_id + 1) * chunk_size
]

print(f"🎯 Processing {len(user_profiles)} profiles in chunk {chunk_id}")


def enhance_profile(profile, max_retries=3):
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return None, "no_discovery_id"

    output_path = os.path.join(output_dir, f"{discovery_id}.json")

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
    print(f"🔍 Processing: {name} (ID: {discovery_id})")

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

    print(
        f"✅ Saved: {name} - Email: {enhanced_profile.get('email')} "
        f"- Pub keywords: {len(pub_keywords.split())} "
        f"- Grant keywords: {len(grant_keywords.split())}"
    )

    return discovery_id, "ok"


# === Run in Parallel ===
print("\n🚀 Starting enhanced data collection...")
results = []
with ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = {
        executor.submit(enhance_profile, profile): profile
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

# === Write Logs ===
current_failures = set()

with open(log_file, "w") as log, open(error_file, "w") as err:
    for discovery_id, name, status in results:
        log.write(f"{discovery_id},{name},{status}\n")
        if any(
            x in status
            for x in ("fail", "error", "retry", "max_retries")
        ):
            err.write(f"{discovery_id},{name},{status}\n")
            current_failures.add(discovery_id)

print(f"\n🎓 Done! Processed {len(results)} faculty")
print(f"📁 Output directory: {output_dir}")
print(f"📝 Log file: {log_file}")
print(f"❌ Error file: {error_file}")
print(
    f"🔍 Files created: "
    f"{len([f for f in os.listdir(output_dir) if f.endswith('.json')])}"
)
