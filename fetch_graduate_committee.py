import os
import json
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Parallel config
chunk_id = int(os.getenv("CHUNK_ID", "0"))
chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
n_threads = int(os.getenv("N_THREADS", "4"))
retry_registry_file = os.getenv("RETRY_REGISTRY", None)

# Paths
input_file = "data/uab_scholars_profiles.jsonl"
output_dir = "data/committees_by_id"
log_file = f"logs/chunk_{chunk_id}_grad_committee.log"
error_file = f"logs/chunk_{chunk_id}_grad_committee_errors.log"

# API
url = "https://scholars.uab.edu/api/teachingActivities/linkedTo"
headers = {"Content-Type": "application/json"}

# Ensure dirs
os.makedirs(output_dir, exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Load profiles
with open(input_file, "r", encoding="utf-8") as f:
    all_profiles = [json.loads(line.strip()) for line in f]

# Filter to retry list if provided
if retry_registry_file and os.path.exists(retry_registry_file):
    with open(retry_registry_file, "r") as f:
        retry_ids = set(line.strip() for line in f if line.strip())
    all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in retry_ids]

# Partition work
chunk_size = len(all_profiles) // chunk_total + 1
user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]

# === Fetch Function ===
def fetch_committee_roles(profile, max_retries=5, sleep_secs=2):
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return None, "no_discovery_id"

    output_path = os.path.join(output_dir, f"{discovery_id}.json")

    # Skip if valid JSON already exists
    if os.path.exists(output_path):
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                json.load(f)
            return None, "already_exists"
        except Exception:
            pass  # corrupted file – will retry

    ids_to_try = [profile.get("discoveryUrlId"), discovery_id]
    name = profile.get("firstNameLastName", "Unknown")
    attempts = 0

    while attempts < max_retries:
        for object_id in filter(None, ids_to_try):
            payload = {
                "objectId": object_id,
                "objectType": "user",
                "pagination": {"perPage": 100, "startFrom": 0},
                "sort": "dateDesc",
                "favouritesFirst": True
            }
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=20)
                if r.status_code == 200:
                    res = r.json().get("resource", [])
                    activities = [
                        a for a in res
                        if a.get("objectTypeDisplayName") == "Graduate Committee Participation"
                    ]

                    # Build results (even if empty)
                    result = []
                    for act in activities:
                        title = act.get("title", "")
                        start = act.get("date1", {}).get("dateTime")
                        end = act.get("date2", {}).get("dateTime")

                        if end:
                            status = "No longer on student's committee or student has graduated"
                        elif "(Committee Member & Mentor)" in title:
                            status = "Current mentor"
                        elif "(Committee Member)" in title:
                            status = "Current committee member"
                        else:
                            status = "Unknown"

                        result.append({
                            "userDiscoveryId": discovery_id,
                            "userDiscoveryUrlId": profile.get("discoveryUrlId"),
                            "userName": name,
                            "teachingDiscoveryId": act.get("discoveryId"),
                            "title": title,
                            "status": status,
                            "startDate": start,
                            "endDate": end
                        })

                    with open(output_path, "w", encoding="utf-8") as out_f:
                        json.dump(result, out_f, indent=2, ensure_ascii=False)

                    return discovery_id, "ok"

                elif r.status_code >= 500:
                    print(f"⏳ Retry ({attempts+1}) for {discovery_id} (HTTP {r.status_code})")
                    time.sleep(sleep_secs)
                    attempts += 1
                    continue
                else:
                    return discovery_id, f"failed_status_{r.status_code}"

            except Exception as e:
                print(f"⚠️ Network error on {discovery_id}: {e}")
                time.sleep(sleep_secs)
                attempts += 1

    return discovery_id, "max_retries_exceeded"

# === Run in Parallel ===
results = []
with ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = {executor.submit(fetch_committee_roles, profile): profile for profile in user_profiles}
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
with open(log_file, "a") as log, open(error_file, "a") as err:
    for discovery_id, name, status in results:
        log.write(f"{discovery_id},{name},{status}\n")
        if "fail" in status or "error" in status or "retry" in status:
            err.write(f"{discovery_id},{name},{status}\n")

print(f"🎓 Done: chunk {chunk_id}, wrote {len(results)} entries to {output_dir}/")
