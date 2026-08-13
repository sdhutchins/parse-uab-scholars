#!/usr/bin/env python3
"""
Test script to process one faculty member and verify enhanced data
collection. Imports shared functions from src/fetch_enhanced_data.py.
"""

import json
import sys
from pathlib import Path

# Allow imports from src/
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.fetch_enhanced_data import (
    extract_keywords,
    fetch_user_details,
    fetch_publications,
    fetch_grants,
)

# Test with Elizabeth Worthey (ID: 3694)
TEST_DISCOVERY_ID = "3694"


def test_single_faculty():
    """Test processing one faculty member."""
    print(
        f"🧪 Testing enhanced data collection for faculty ID: "
        f"{TEST_DISCOVERY_ID}"
    )
    print("=" * 60)

    # Load the profile from the JSONL file
    print("📖 Loading profile from JSONL...")
    profiles_path = PROJECT_ROOT / "data/uab_scholars_profiles.jsonl"
    with open(profiles_path, "r", encoding="utf-8") as f:
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

    user_details = fetch_user_details(TEST_DISCOVERY_ID)
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
        "email": (
            user_details.get("emailAddress", {}).get("address")
            if user_details else None
        ),
        "publication_keywords": pub_keywords,
        "grant_keywords": grant_keywords,
    })

    print(
        f"\n📊 Enhanced profile keys: {list(enhanced_profile.keys())}"
    )
    print(f"📧 Email: {enhanced_profile.get('email')}")
    print(f"📝 Publication keywords count: {len(pub_keywords.split())}")
    print(f"💰 Grant keywords count: {len(grant_keywords.split())}")

    # Save to test file
    test_output = (
        Path(__file__).resolve().parent
        / f"test_faculty_{TEST_DISCOVERY_ID}.json"
    )
    with open(test_output, "w", encoding="utf-8") as f:
        json.dump(enhanced_profile, f, indent=2, ensure_ascii=False)

    print(f"\n💾 Saved test output to: {test_output}")
    print("✅ Test completed successfully!")


if __name__ == "__main__":
    test_single_faculty()
