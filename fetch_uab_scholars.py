import requests
import json
import time

url = "https://scholars.uab.edu/api/users"
headers = {
    "Content-Type": "application/json"
}

all_profiles = []
total_pages = 65  # 6433 / 100 ≈ 65

for page in range(1, total_pages + 1):
    payload = {
        "params": {
            "by": "text",
            "type": "user",
            "page": page,
            "pageSize": 100
        },
        "filters": [
            {"name": "department", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
            {"name": "tags", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
            {"name": "customFilterOne", "matchDocsWithMissingValues": True, "useValuesToFilter": False}
        ]
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        data = response.json()
        all_profiles.extend(data.get("resource", []))
        print(f"✅ Fetched page {page}/{total_pages}")
    else:
        print(f"❌ Failed on page {page}: {response.status_code}")
        break

    time.sleep(0.5)  # Respectful delay

# Save all results
with open("uab_scholars_all_profiles.json", "w", encoding="utf-8") as f:
    json.dump(all_profiles, f, indent=2)

print(f"🎉 Saved {len(all_profiles)} profiles to uab_scholars_all_profiles.json")
