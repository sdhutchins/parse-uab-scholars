#!/usr/bin/env python3
"""
Test script to process one faculty member and verify enhanced data collection
"""

import os
import json
import time
import requests
import re

# Test with Elizabeth Worthey (ID: 3694)
TEST_DISCOVERY_ID = "3694"

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}

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
    print(f"🔍 Fetching user details: {url}")
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                print(f"✅ User details: email = {data.get('email', 'None')}")
                return data
            elif r.status_code >= 500:
                print(f"⚠️  Server error (attempt {attempt + 1})")
                time.sleep(2)
                continue
            else:
                print(f"❌ HTTP {r.status_code}")
                return None
        except Exception as e:
            print(f"❌ Exception: {e}")
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
    
    print(f"🔍 Fetching publications: {url}")
    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                data = r.json().get("resource", [])
                print(f"✅ Publications: {len(data)} found")
                return data
            elif r.status_code >= 500:
                print(f"⚠️  Server error (attempt {attempt + 1})")
                time.sleep(2)
                continue
            else:
                print(f"❌ HTTP {r.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception: {e}")
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
    
    print(f"🔍 Fetching grants: {url}")
    for attempt in range(max_retries):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                data = r.json().get("resource", [])
                print(f"✅ Grants: {len(data)} found")
                return data
            elif r.status_code >= 500:
                print(f"⚠️  Server error (attempt {attempt + 1})")
                time.sleep(2)
                continue
            else:
                print(f"❌ HTTP {r.status_code}")
                return []
        except Exception as e:
            print(f"❌ Exception: {e}")
            time.sleep(2)
    return []

def test_single_faculty():
    """Test processing one faculty member."""
    print(f"🧪 Testing enhanced data collection for faculty ID: {TEST_DISCOVERY_ID}")
    print("=" * 60)
    
    # Load the profile from the JSONL file
    print("📖 Loading profile from JSONL...")
    with open("../data/uab_scholars_profiles.jsonl", "r", encoding="utf-8") as f:
        for line in f:
            profile = json.loads(line.strip())
            if str(profile.get("discoveryId")) == TEST_DISCOVERY_ID:
                print(f"✅ Found profile: {profile.get('firstNameLastName')}")
                break
        else:
            print(f"❌ Profile not found for ID: {TEST_DISCOVERY_ID}")
            return
    
    print(f"📋 Original profile keys: {list(profile.keys())}")
    
    # Fetch enhanced data
    print("\n🔍 Fetching enhanced data...")
    
    # Fetch user details for email
    user_details = fetch_user_details(TEST_DISCOVERY_ID)
    
    # Fetch publications and grants
    publications = fetch_publications(TEST_DISCOVERY_ID)
    grants = fetch_grants(TEST_DISCOVERY_ID)
    
    # Extract keywords
    print("\n🔤 Extracting keywords...")
    pub_keywords = extract_keywords(publications)
    grant_keywords = extract_keywords(grants)
    
    print(f"📝 Publication keywords: {pub_keywords[:100]}...")
    print(f"💰 Grant keywords: {grant_keywords[:100]}...")
    
    # Create enhanced profile
    enhanced_profile = profile.copy()
    enhanced_profile.update({
        "email": user_details.get("emailAddress", {}).get("address") if user_details else None,
        "publication_keywords": pub_keywords,
        "grant_keywords": grant_keywords
    })
    
    print(f"\n📊 Enhanced profile keys: {list(enhanced_profile.keys())}")
    print(f"📧 Email: {enhanced_profile.get('email')}")
    print(f"📝 Publication keywords count: {len(pub_keywords.split())}")
    print(f"💰 Grant keywords count: {len(grant_keywords.split())}")
    
    # Save to test file
    test_output = f"test_faculty_{TEST_DISCOVERY_ID}.json"
    with open(test_output, "w", encoding="utf-8") as f:
        json.dump(enhanced_profile, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved test output to: {test_output}")
    print("✅ Test completed successfully!")

if __name__ == "__main__":
    test_single_faculty() 