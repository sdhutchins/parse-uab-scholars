import requests
import json
import time

url = "https://scholars.uab.edu/api/users"
headers = {"Content-Type": "application/json"}

jsonl_path = "data/uab_scholars_profiles.jsonl"
log_path = "logs/uab_scholars_fetch.log"

# Step 1: Determine total records
init_payload = {
    "params": {
        "by": "text",
        "type": "user",
        "text": ""
    },
    "pagination": {
        "startFrom": 0,
        "perPage": 1
    },
    "sort": "relevance",
    "filters": [
        {"name": "customFilterOne", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
        {"name": "department", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
        {"name": "tags", "matchDocsWithMissingValues": True, "useValuesToFilter": False}
    ]
}

r = requests.post(url, headers=headers, json=init_payload, timeout=10)
total_records = r.json().get("pagination", {}).get("total", 0)
page_size = 25
print(f"Found {total_records} profiles")
print(f"Fetching {total_records // page_size + 1} pages of {page_size} each")

# Step 2: Fetch in pages
with open(jsonl_path, "w", encoding="utf-8") as jsonl_file, open(log_path, "w") as log_file:
    for start in range(0, total_records, page_size):
        payload = {
            "params": {
                "by": "text",
                "type": "user",
                "text": ""
            },
            "pagination": {
                "startFrom": start,
                "perPage": page_size
            },
            "sort": "relevance",
            "filters": [
                {"name": "customFilterOne", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
                {"name": "department", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
                {"name": "tags", "matchDocsWithMissingValues": True, "useValuesToFilter": False}
            ]
        }

        res = requests.post(url, headers=headers, json=payload, timeout=10)
        if res.status_code != 200:
            print(f"❌ Failed on batch starting at {start}")
            break

        profiles = res.json().get("resource", [])
        print(f"✅ Got {len(profiles)} profiles from {start} to {start + page_size - 1}")

        for profile in profiles:
            jsonl_file.write(json.dumps(profile, ensure_ascii=False) + "\n")
            log_file.write(f"{start},{profile.get('discoveryId')},{profile.get('firstNameLastName')}\n")

        time.sleep(1)

print("🎉 Done.")
