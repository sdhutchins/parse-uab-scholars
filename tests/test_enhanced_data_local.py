#!/usr/bin/env python3
"""
Local test version of enhanced data collection
"""

import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Test config (single chunk, single thread for local testing)
chunk_id = 0
chunk_total = 1
n_threads = 1
retry_registry_file = None

# Paths
input_file = "../data/uab_scholars_profiles.jsonl"
output_dir = "test_faculty_data"
log_file = "test_enhanced.log"
error_file = "test_enhanced_errors.log"

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}

# Ensure dirs
os.makedirs(output_dir, exist_ok=True)

# Load profiles
print("📖 Loading profiles from JSONL...")
with open(input_file, "r", encoding="utf-8") as f:
    all_profiles = [json.loads(line.strip()) for line in f]

# Load faculty who have students (test file)
print("📖 Loading test faculty IDs...")
with open("test_faculty_ids.txt", "r") as f:
    faculty_ids_with_students = set(line.strip() for line in f if line.strip())

print(f"🔍 Found {len(faculty_ids_with_students)} test faculty IDs: {list(faculty_ids_with_students)}")

# Filter to only faculty who have students
all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in faculty_ids_with_students]

print(f"✅ Filtered to {len(all_profiles)} profiles")

# Filter to retry list if provided
if retry_registry_file and os.path.exists(retry_registry_file):
    with open(retry_registry_file, "r") as f:
        retry_ids = set(line.strip() for line in f if line.strip())
    all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in retry_ids]

# Partition work (for single chunk, this is just all profiles)
chunk_size = len(all_profiles) // chunk_total + 1
user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]

print(f"🎯 Processing {len(user_profiles)} profiles in chunk {chunk_id}")

def extract_keywords(items):
    """Extract keywords from publication/grant labels where schemeDisplayName is 'Research, Condition and Disease Categorization'."""
    keywords = set()
    for item in items:
        labels = item.get("labels", [])
        for label in labels:
            # Check if this is the research/disease categorization scheme
            if label.get("schemeDisplayName") == "Research, Condition and Disease Categorization":
                value = label.get("value", "")
                if value:
                    keywords.add(value)
    return " ".join(sorted(keywords))  # Return ALL keywords, sorted

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

def fetch_publications(discovery_id, limit=5, max_retries=3):
    """Fetch publications for a user."""
    url = f"{base_url}/publications/linkedTo"
    payload = {
        "objectId": discovery_id,
        "category": "user",
        "pagination": {"perPage": limit, "startFrom": 0},
        "sort": "dateDesc"
    }
    
    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
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

def fetch_grants(discovery_id, limit=10, max_retries=3):
    """Fetch grants for a user."""
    url = f"{base_url}/grants/linkedTo"
    payload = {
        "objectId": discovery_id,
        "category": "user",
        "pagination": {"perPage": limit, "startFrom": 0},
        "sort": "dateDesc"
    }
    
    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
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

def enhance_profile(profile, max_retries=3):
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return None, "no_discovery_id"

    output_path = os.path.join(output_dir, f"{discovery_id}.json")

    # Check if file already exists and is valid
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
                if "email" in existing_data and "publication_keywords" in existing_data:
                    return None, "already_exists"
        except Exception:
            pass  # corrupted file – will retry

    name = profile.get("firstNameLastName", "Unknown")
    print(f"🔍 Processing: {name} (ID: {discovery_id})")
    
    # Fetch user details for email
    user_details = fetch_user_details(discovery_id)
    
    # Fetch publications and grants using discovery_id directly
    publications = fetch_publications(discovery_id)
    grants = fetch_grants(discovery_id)
    
    # Extract keywords
    pub_keywords = extract_keywords(publications)
    grant_keywords = extract_keywords(grants)
    
    # Create enhanced profile
    enhanced_profile = profile.copy()
    enhanced_profile.update({
        "email": user_details.get("emailAddress", {}).get("address") if user_details else None,
        "publication_keywords": pub_keywords,
        "grant_keywords": grant_keywords
    })
    
    # Save to file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enhanced_profile, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Saved: {name} - Email: {enhanced_profile.get('email')} - Pub keywords: {len(pub_keywords.split())} - Grant keywords: {len(grant_keywords.split())}")
    
    return discovery_id, "ok"

# === Run in Parallel ===
print("\n🚀 Starting enhanced data collection...")
results = []
with ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = {executor.submit(enhance_profile, profile): profile for profile in user_profiles}
    for future in as_completed(futures):
        profile = futures[future]
        try:
            discovery_id, status = future.result()
            name = profile.get("firstNameLastName", "Unknown")
            if discovery_id:
                results.append((discovery_id, name, status))
        except Exception as e:
            results.append((profile.get("discoveryId"), profile.get("firstNameLastName"), f"fatal_error: {e}"))

# === Write Logs ===
current_failures = set()

with open(log_file, "w") as log, open(error_file, "w") as err:
    for discovery_id, name, status in results:
        log.write(f"{discovery_id},{name},{status}\n")
        if any(x in status for x in ("fail", "error", "retry", "max_retries")):
            err.write(f"{discovery_id},{name},{status}\n")
            current_failures.add(discovery_id)

print(f"\n🎓 Done! Processed {len(results)} faculty")
print(f"📁 Output directory: {output_dir}")
print(f"📝 Log file: {log_file}")
print(f"❌ Error file: {error_file}")
print(f"🔍 Files created: {len([f for f in os.listdir(output_dir) if f.endswith('.json')])}") 