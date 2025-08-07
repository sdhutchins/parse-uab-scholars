import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

# Parallel config
chunk_id = int(os.getenv("CHUNK_ID", "0"))
chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
n_threads = int(os.getenv("N_THREADS", "4"))
retry_registry_file = os.getenv("RETRY_REGISTRY", None)

# Paths
input_file = "data/uab_scholars_profiles.jsonl"
output_dir = "data/faculty_data"
log_file = f"logs/chunk_{chunk_id}_enhanced.log"
error_file = f"logs/chunk_{chunk_id}_enhanced_errors.log"

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}

# Ensure dirs
os.makedirs(output_dir, exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Load profiles
with open(input_file, "r", encoding="utf-8") as f:
    all_profiles = [json.loads(line.strip()) for line in f]

# Load faculty who have students
with open("faculty_with_students.txt", "r") as f:
    faculty_ids_with_students = set(line.strip() for line in f if line.strip())

# Filter to only faculty who have students
all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in faculty_ids_with_students]

# Filter to retry list if provided
if retry_registry_file and os.path.exists(retry_registry_file):
    with open(retry_registry_file, "r") as f:
        retry_ids = set(line.strip() for line in f if line.strip())
    all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in retry_ids]

# Partition work
chunk_size = len(all_profiles) // chunk_total + 1
user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]

def extract_keywords(items):
    """Extract keywords from publication/grant labels and titles."""
    keywords = set()
    for item in items:
        labels = item.get("labels", [])
        for label in labels:
            value = label.get("value", "")
            if value:
                cleaned = re.sub(r'\d+', '', value)
                cleaned = cleaned.replace(" and ", " ").replace("&", " ")
                words = re.findall(r'\b[a-zA-Z]{3,}\b', cleaned)
                keywords.update(words)

        title = item.get("title", "")
        if title:
            cleaned = re.sub(r'\d+', '', title)
            cleaned = cleaned.replace(" and ", " ").replace("&", " ")
            words = re.findall(r'\b[a-zA-Z]{3,}\b', cleaned)
            keywords.update(words)
    return " ".join(list(keywords)[:30])

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

def fetch_publications(object_id, limit=5, max_retries=3):
    """Fetch publications for a user."""
    url = f"{base_url}/publications/linkedTo"
    payload = {
        "objectId": object_id,
        "category": "user",
        "pagination": {"perPage": limit, "startFrom": 0},
        "sort": "dateDesc",
        "favouritesFirst": True
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

def fetch_grants(object_id, limit=10, max_retries=3):
    """Fetch grants for a user."""
    url = f"{base_url}/grants/linkedTo"
    payload = {
        "objectId": object_id,
        "category": "user",
        "pagination": {"perPage": limit, "startFrom": 0},
        "sort": "dateDesc",
        "favouritesFirst": True
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
    
    # Fetch user details for email and object_id
    user_details = fetch_user_details(discovery_id)
    object_id = user_details.get("objectId") if user_details else discovery_id
    
    # Fetch publications and grants
    publications = fetch_publications(object_id)
    grants = fetch_grants(object_id)
    
    # Extract keywords
    pub_keywords = extract_keywords(publications)
    grant_keywords = extract_keywords(grants)
    
    # Create enhanced profile
    enhanced_profile = profile.copy()
    enhanced_profile.update({
        "email": user_details.get("email") if user_details else None,
        "publication_keywords": pub_keywords,
        "grant_keywords": grant_keywords
    })
    
    # Save to file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enhanced_profile, f, indent=2, ensure_ascii=False)
    
    return discovery_id, "ok"

# === Run in Parallel ===
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

# === Write Logs and Retry Registry ===
current_failures = set()

with open(log_file, "a") as log, open(error_file, "a") as err:
    for discovery_id, name, status in results:
        log.write(f"{discovery_id},{name},{status}\n")
        if any(x in status for x in ("fail", "error", "retry", "max_retries")):
            err.write(f"{discovery_id},{name},{status}\n")
            current_failures.add(discovery_id)

# === Always write retry registry ===
registry_path = retry_registry_file or f"logs/retry_registry_enhanced_chunk_{chunk_id}.csv"
with open(registry_path, "w") as f:
    for rid in sorted(current_failures):
        f.write(f"{rid}\n")

print(f"🎓 Done: chunk {chunk_id}, wrote {len(results)} entries to {output_dir}/") 