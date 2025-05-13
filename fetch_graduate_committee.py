import os
import json
import time
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Parallel chunk config from environment
chunk_id = int(os.getenv("CHUNK_ID", "0"))
chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
n_threads = int(os.getenv("N_THREADS", "4"))

# Paths
input_file = "data/uab_scholars_profiles.jsonl"
output_dir = "data/chunked_committees"
log_file = f"logs/chunk_{chunk_id}_grad_committee_fetch.log"
error_file = f"logs/chunk_{chunk_id}_grad_committee_errors.log"

# API settings
url = "https://scholars.uab.edu/api/teachingActivities/linkedTo"
headers = {"Content-Type": "application/json"}

# Ensure directories exist
os.makedirs(output_dir, exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# Load input
with open(input_file, "r", encoding="utf-8") as f:
    all_profiles = [json.loads(line.strip()) for line in f]

# Partition work
chunk_size = len(all_profiles) // chunk_total + 1
user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]

# Already completed?
completed_ids = set()
if os.path.exists(log_file):
    with open(log_file) as f:
        for line in f:
            completed_ids.add(line.strip().split(",")[0])

# Store results
grouped = defaultdict(list)
errored_profiles = []

def fetch_committee_roles(profile):
    """Fetch teaching activities for a profile and extract committee roles."""
    discovery_id = str(profile.get("discoveryId"))
    name = profile.get("firstNameLastName", "Unknown")
    if not discovery_id or discovery_id in completed_ids:
        return None, None, None

    ids_to_try = [profile.get("discoveryUrlId"), str(profile.get("discoveryId"))]
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
                resource = r.json().get("resource", [])
                activities = [a for a in resource if a.get("objectTypeDisplayName") == "Graduate Committee Participation"]
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

# Fetch in parallel
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
        except Exception as e:
            errored_profiles.append(profile)
            print(f"⚠️ Error: {profile.get('firstNameLastName')} - {e}")

# Retry any errors (serially)
for profile in errored_profiles[:]:
    try:
        discovery_id, name, roles = fetch_committee_roles(profile)
        if discovery_id and roles:
            grouped[discovery_id].extend(roles)
            with open(log_file, "a") as log:
                log.write(f"{discovery_id},{name},{len(roles)}\n")
            errored_profiles.remove(profile)
            print(f"🔁 Retry success: {name}")
    except Exception as e:
        print(f"❌ Retry failed for {profile.get('discoveryId')}: {e}")

# Save chunked result
with open(f"{output_dir}/grad_committees_chunk_{chunk_id}.json", "w", encoding="utf-8") as f:
    json.dump(grouped, f, indent=2, ensure_ascii=False)

# Save errors
with open(error_file, "w") as f:
    for p in errored_profiles:
        f.write(f"{p.get('discoveryId')},{p.get('firstNameLastName')}\n")

print(f"🎓 Done: chunk {chunk_id}, saved {len(grouped)} users.")
