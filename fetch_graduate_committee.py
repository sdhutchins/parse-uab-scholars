import os
import json
import time
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Parallel config
chunk_id = int(os.getenv("CHUNK_ID", "0"))
chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
n_threads = int(os.getenv("N_THREADS", "4"))
retry_registry_file = os.getenv("RETRY_REGISTRY", None)  # Optional

# Input and output paths
input_file = "data/uab_scholars_profiles.jsonl"
output_dir = "data/chunked_committees"
log_file = f"logs/chunk_{chunk_id}_grad_committee.log"
error_file = f"logs/chunk_{chunk_id}_grad_committee_errors.log"
output_file = f"{output_dir}/grad_committees_chunk_{chunk_id}.json"

# API
url = "https://scholars.uab.edu/api/teachingActivities/linkedTo"
headers = {"Content-Type": "application/json"}

# Ensure dirs
os.makedirs(output_dir, exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Clear logs/output for fresh retry loop
for path in [log_file, error_file, output_file]:
    if os.path.exists(path):
        os.remove(path)

# Load all profiles
with open(input_file, "r", encoding="utf-8") as f:
    all_profiles = [json.loads(line.strip()) for line in f]

# If retry mode, filter to just retry IDs
if retry_registry_file and os.path.exists(retry_registry_file):
    with open(retry_registry_file, "r") as f:
        retry_ids = set(line.strip() for line in f if line.strip())
    all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in retry_ids]

# Partition work
chunk_size = len(all_profiles) // chunk_total + 1
user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]

# Load already completed (optional if log reuse is ever added again)
completed_ids = set()

# Output containers
grouped = defaultdict(list)
errored_profiles = []
empty_profiles = []

def fetch_committee_roles(profile):
    discovery_id = str(profile.get("discoveryId"))
    name = profile.get("firstNameLastName", "Unknown")
    if not discovery_id:
        return None, None, None

    ids_to_try = [profile.get("discoveryUrlId"), discovery_id]
    activities = []

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
                activities = [a for a in res if a.get("objectTypeDisplayName") == "Graduate Committee Participation"]
                if activities:
                    break
        except Exception:
            continue

    roles = []
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

        roles.append({
            "userDiscoveryId": discovery_id,
            "userDiscoveryUrlId": profile.get("discoveryUrlId"),
            "userName": name,
            "teachingDiscoveryId": act.get("discoveryId"),
            "title": title,
            "status": status,
            "startDate": start,
            "endDate": end
        })

    return discovery_id, name, roles

# Run in parallel
with ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = {executor.submit(fetch_committee_roles, p): p for p in user_profiles}
    for future in as_completed(futures):
        profile = futures[future]
        try:
            discovery_id, name, roles = future.result()
            if discovery_id and roles:
                grouped[discovery_id].extend(roles)
                with open(log_file, "a") as log:
                    log.write(f"{discovery_id},{name},{len(roles)}\n")
                print(f"✅ {name}: {len(roles)} entries")
            elif discovery_id:
                empty_profiles.append(profile)
        except Exception as e:
            errored_profiles.append(profile)
            print(f"⚠️ Error for {profile.get('firstNameLastName')}: {e}")

# Retry serially for errors and empties
retry_these = errored_profiles + empty_profiles
for profile in retry_these:
    try:
        discovery_id, name, roles = fetch_committee_roles(profile)
        if discovery_id and roles:
            grouped[discovery_id].extend(roles)
            with open(log_file, "a") as log:
                log.write(f"{discovery_id},{name},{len(roles)}\n")
            print(f"🔁 Retry success: {name}")
        elif discovery_id:
            with open(error_file, "a") as f:
                f.write(f"{discovery_id},{name},EMPTY_AFTER_RETRY\n")
    except Exception as e:
        with open(error_file, "a") as f:
            f.write(f"{profile.get('discoveryId')},{profile.get('firstNameLastName')},ERROR\n")
        print(f"❌ Retry failed for {profile.get('firstNameLastName')}: {e}")

# Write results
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(grouped, f, indent=2, ensure_ascii=False)

print(f"🎓 Done: chunk {chunk_id}, saved {len(grouped)} users.")
