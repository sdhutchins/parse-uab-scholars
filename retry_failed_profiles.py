import os
import json
import time
import requests
from collections import defaultdict

input_file = "data/uab_scholars_profiles.jsonl"
retry_list = "logs/retry_registry.csv"
output_file = "data/retry_grad_committees.json"
log_file = "logs/retry_fetch.log"

url = "https://scholars.uab.edu/api/teachingActivities/linkedTo"
headers = {"Content-Type": "application/json"}

# Load discovery IDs to retry
with open(retry_list) as f:
    retry_ids = set(line.strip() for line in f)

# Load profiles
with open(input_file) as f:
    profiles = [json.loads(line.strip()) for line in f]

# Index by discoveryId
profile_map = {str(p.get("discoveryId")): p for p in profiles}

grouped = defaultdict(list)

def fetch_committee_roles(profile):
    discovery_id = str(profile.get("discoveryId"))
    name = profile.get("firstNameLastName", "Unknown")
    ids_to_try = [profile.get("discoveryUrlId"), str(discovery_id)]

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

# Process retry list
with open(log_file, "w") as log:
    for disc_id in retry_ids:
        profile = profile_map.get(disc_id)
        if not profile:
            continue
        try:
            discovery_id, name, roles = fetch_committee_roles(profile)
            if roles:
                grouped[discovery_id].extend(roles)
                log.write(f"{discovery_id},{name},{len(roles)}\n")
                print(f"✅ {name}: {len(roles)} roles")
            else:
                log.write(f"{discovery_id},{name},0\n")
        except Exception as e:
            print(f"❌ Failed again: {disc_id} - {e}")
            log.write(f"{disc_id},{name},ERROR\n")
        time.sleep(1)

# Save output
with open(output_file, "w") as f:
    json.dump(grouped, f, indent=2, ensure_ascii=False)

print("🎯 Retry complete.")
