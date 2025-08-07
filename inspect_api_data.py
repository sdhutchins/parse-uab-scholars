#!/usr/bin/env python3
"""
Inspect the raw API response to see the data structure
"""

import requests
import json

# Test with Elizabeth Worthey (ID: 3694)
TEST_DISCOVERY_ID = "3694"

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}

def fetch_and_save_api_data():
    """Fetch API data and save to test.json"""
    
    print(f"🔍 Fetching API data for faculty ID: {TEST_DISCOVERY_ID}")
    
    # 1. User details
    print("1. Fetching user details...")
    user_url = f"{base_url}/users/{TEST_DISCOVERY_ID}"
    user_response = requests.get(user_url, headers=headers, timeout=10)
    user_data = user_response.json() if user_response.status_code == 200 else None
    
    # 2. Publications
    print("2. Fetching publications...")
    pub_url = f"{base_url}/publications/linkedTo"
    pub_payload = {
        "objectId": TEST_DISCOVERY_ID,
        "category": "user",
        "pagination": {"perPage": 5, "startFrom": 0},
        "sort": "dateDesc"
    }
    pub_response = requests.post(pub_url, headers=headers, json=pub_payload, timeout=10)
    pub_data = pub_response.json() if pub_response.status_code == 200 else None
    
    # 3. Grants
    print("3. Fetching grants...")
    grant_url = f"{base_url}/grants/linkedTo"
    grant_payload = {
        "objectId": TEST_DISCOVERY_ID,
        "category": "user",
        "pagination": {"perPage": 10, "startFrom": 0},
        "sort": "dateDesc"
    }
    grant_response = requests.post(grant_url, headers=headers, json=grant_payload, timeout=10)
    grant_data = grant_response.json() if grant_response.status_code == 200 else None
    
    # Combine all data
    all_data = {
        "user_details": user_data,
        "publications": pub_data,
        "grants": grant_data
    }
    
    # Save to test.json
    with open("test.json", "w", encoding="utf-8") as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    
    print("✅ Saved API data to test.json")
    
    # Print some basic info
    if user_data:
        print(f"📧 User email: {user_data.get('email', 'None')}")
    
    if pub_data:
        pub_count = len(pub_data.get("resource", []))
        print(f"📝 Publications: {pub_count}")
        if pub_count > 0:
            first_pub = pub_data["resource"][0]
            print(f"   First pub keys: {list(first_pub.keys())}")
    
    if grant_data:
        grant_count = len(grant_data.get("resource", []))
        print(f"💰 Grants: {grant_count}")
        if grant_count > 0:
            first_grant = grant_data["resource"][0]
            print(f"   First grant keys: {list(first_grant.keys())}")

if __name__ == "__main__":
    fetch_and_save_api_data() 