#!/usr/bin/env python3
"""
Unified UAB Scholars Data Pipeline
Combines fetching profiles, enhanced data, and graduate committees.
"""

import os
import json
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
import re

# Parallel config
chunk_id = int(os.getenv("CHUNK_ID", "0"))
chunk_total = int(os.getenv("CHUNK_TOTAL", "1"))
n_threads = int(os.getenv("N_THREADS", "4"))
retry_registry_file = os.getenv("RETRY_REGISTRY", None)
pipeline_stage = os.getenv("PIPELINE_STAGE", "profiles")  # profiles, enhanced, committees

# Paths
profiles_file = "data/uab_scholars_profiles.jsonl"
faculty_data_dir = "data/faculty_data"
log_file = f"logs/chunk_{chunk_id}_{pipeline_stage}.log"
error_file = f"logs/chunk_{chunk_id}_{pipeline_stage}_errors.log"

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}

# Ensure dirs
os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)
if pipeline_stage in ["enhanced", "committees"]:
    os.makedirs(faculty_data_dir, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def fetch_all_profiles():
    """Stage 1: Fetch all UAB scholar profiles."""
    url = f"{base_url}/users"
    
    # Get total count
    init_payload = {
        "params": {"by": "text", "type": "user", "text": ""},
        "pagination": {"startFrom": 0, "perPage": 1},
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
    
    logger.info(f"Found {total_records} profiles, fetching {total_records // page_size + 1} pages")
    
    with open(profiles_file, "w", encoding="utf-8") as f:
        for start in range(0, total_records, page_size):
            payload = {
                "params": {"by": "text", "type": "user", "text": ""},
                "pagination": {"startFrom": start, "perPage": page_size},
                "sort": "relevance",
                "filters": [
                    {"name": "customFilterOne", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
                    {"name": "department", "matchDocsWithMissingValues": True, "useValuesToFilter": False},
                    {"name": "tags", "matchDocsWithMissingValues": True, "useValuesToFilter": False}
                ]
            }
            
            res = requests.post(url, headers=headers, json=payload, timeout=10)
            if res.status_code != 200:
                logger.error(f"Failed on batch starting at {start}")
                break
            
            profiles = res.json().get("resource", [])
            logger.info(f"Got {len(profiles)} profiles from {start} to {start + page_size - 1}")
            
            for profile in profiles:
                f.write(json.dumps(profile, ensure_ascii=False) + "\n")
            
            time.sleep(1)

def fetch_user_details(discovery_id: str, max_retries: int = 3) -> Optional[Dict]:
    """Fetch detailed user information including email."""
    url = f"{base_url}/users/{discovery_id}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                data = response.json()
                return {
                    "objectId": data.get("objectId"),
                    "email": data.get("email")
                }
            elif response.status_code >= 500:
                logger.warning(f"Retry ({attempt+1}) for user {discovery_id}")
                time.sleep(2)
                continue
            else:
                return None
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
    
    return None

def fetch_publications(object_id: str, limit: int = 5, max_retries: int = 3) -> List[Dict]:
    """Fetch recent publications for a user."""
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
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get("resource", [])
            elif response.status_code >= 500:
                logger.warning(f"Retry ({attempt+1}) for publications {object_id}")
                time.sleep(2)
                continue
            else:
                return []
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
    
    return []

def fetch_grants(object_id: str, limit: int = 10, max_retries: int = 3) -> List[Dict]:
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
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get("resource", [])
            elif response.status_code >= 500:
                logger.warning(f"Retry ({attempt+1}) for grants {object_id}")
                time.sleep(2)
                continue
            else:
                return []
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
    
    return []

def extract_keywords(items: List[Dict]) -> str:
    """Extract keywords from publications/grants, removing 'and' and numbers."""
    keywords = set()
    
    for item in items:
        # Extract from labels (structured data)
        labels = item.get("labels", [])
        for label in labels:
            value = label.get("value", "")
            if value:
                cleaned = re.sub(r'\d+', '', value)
                cleaned = cleaned.replace(" and ", " ")
                cleaned = cleaned.replace("&", " ")
                words = re.findall(r'\b[a-zA-Z]{3,}\b', cleaned)
                keywords.update(words)
        
        # Also extract from title
        title = item.get("title", "")
        if title:
            cleaned = re.sub(r'\d+', '', title)
            cleaned = cleaned.replace(" and ", " ")
            cleaned = cleaned.replace("&", " ")
            words = re.findall(r'\b[a-zA-Z]{3,}\b', cleaned)
            keywords.update(words)
    
    return " ".join(list(keywords)[:30])

def enhance_profile(profile: Dict) -> Tuple[str, str]:
    """Stage 2: Enhance a single faculty profile with additional data."""
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return discovery_id, "no_discovery_id"
    
    name = profile.get("firstNameLastName", "Unknown")
    logger.info(f"Enhancing {name} (ID: {discovery_id})")
    
    # Load existing faculty data if it exists
    faculty_file = os.path.join(faculty_data_dir, f"{discovery_id}.json")
    faculty_data = profile.copy()  # Start with original profile
    
    if os.path.exists(faculty_file):
        try:
            with open(faculty_file, "r", encoding="utf-8") as f:
                faculty_data = json.load(f)
        except Exception:
            pass  # Use original profile if file is corrupted
    
    # Fetch user details and publications/grants
    user_details = fetch_user_details(discovery_id)
    object_id = user_details.get("objectId") if user_details else discovery_id
    
    publications = fetch_publications(object_id, limit=5)
    grants = fetch_grants(object_id, limit=10)
    
    # Extract keywords
    pub_keywords = extract_keywords(publications)
    grant_keywords = extract_keywords(grants)
    
    # Update faculty data with enhanced information
    faculty_data.update({
        "email": user_details.get("email") if user_details else None,
        "publication_keywords": pub_keywords,
        "grant_keywords": grant_keywords
    })
    
    # Save comprehensive faculty data
    with open(faculty_file, "w", encoding="utf-8") as f:
        json.dump(faculty_data, f, indent=2, ensure_ascii=False)
    
    return discovery_id, "ok"

def fetch_committee_roles(profile: Dict) -> Tuple[str, str]:
    """Stage 3: Fetch graduate committee roles for a faculty member."""
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return discovery_id, "no_discovery_id"
    
    name = profile.get("firstNameLastName", "Unknown")
    faculty_file = os.path.join(faculty_data_dir, f"{discovery_id}.json")
    
    # Load existing faculty data
    faculty_data = profile.copy()
    if os.path.exists(faculty_file):
        try:
            with open(faculty_file, "r", encoding="utf-8") as f:
                faculty_data = json.load(f)
        except Exception:
            pass
    
    logger.info(f"Fetching committees for {name} (ID: {discovery_id})")
    
    # Try different object IDs
    ids_to_try = [profile.get("discoveryUrlId"), discovery_id]
    
    for object_id in filter(None, ids_to_try):
        url = f"{base_url}/teachingActivities/linkedTo"
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
                
                # Add committee data to faculty file
                faculty_data["committee_roles"] = result
                
                # Save updated faculty data
                with open(faculty_file, "w", encoding="utf-8") as f:
                    json.dump(faculty_data, f, indent=2, ensure_ascii=False)
                
                return discovery_id, "empty" if not result else "ok"
            
            elif r.status_code >= 500:
                logger.warning(f"Retry for {discovery_id} (HTTP {r.status_code})")
                time.sleep(2)
                continue
            else:
                return discovery_id, f"failed_status_{r.status_code}"
                
        except Exception as e:
            logger.error(f"Network error on {discovery_id}: {e}")
            time.sleep(2)
    
    return discovery_id, "max_retries_exceeded"

def run_pipeline():
    """Main pipeline execution."""
    if pipeline_stage == "profiles":
        logger.info("🚀 Stage 1: Fetching all UAB scholar profiles")
        fetch_all_profiles()
        logger.info("✅ Stage 1 complete")
        
    elif pipeline_stage == "enhanced":
        logger.info("🚀 Stage 2: Enhancing profiles with publications/grants")
        
        # Load profiles
        with open(profiles_file, "r", encoding="utf-8") as f:
            all_profiles = [json.loads(line.strip()) for line in f]

        # Load faculty who have students from our list
        with open("faculty_with_students.txt", "r") as f:
            faculty_ids_with_students = set(line.strip() for line in f if line.strip())

        # Filter to only faculty who have students
        all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in faculty_ids_with_students]

        logger.info(f"Processing {len(all_profiles)} faculty who have students (out of {len(faculty_ids_with_students)} total)")

        # Filter to retry list if provided
        if retry_registry_file and os.path.exists(retry_registry_file):
            with open(retry_registry_file, "r") as f:
                retry_ids = set(line.strip() for line in f if line.strip())
            all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in retry_ids]
        
        # Partition work
        chunk_size = len(all_profiles) // chunk_total + 1
        user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]
        
        # Process in parallel
        results = []
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = {executor.submit(enhance_profile, profile): profile for profile in user_profiles}
            for future in as_completed(futures):
                profile = futures[future]
                try:
                    discovery_id, status = future.result()
                    results.append((discovery_id, profile.get("firstNameLastName"), status))
                except Exception as e:
                    results.append((profile.get("discoveryId"), profile.get("firstNameLastName"), f"fatal_error: {e}"))
        
        # Write logs
        current_failures = set()
        with open(log_file, "a") as log, open(error_file, "a") as err:
            for discovery_id, name, status in results:
                log.write(f"{discovery_id},{name},{status}\n")
                if any(x in status for x in ("fail", "error", "retry", "max_retries")):
                    err.write(f"{discovery_id},{name},{status}\n")
                    current_failures.add(discovery_id)
        
        registry_path = retry_registry_file or f"logs/retry_registry_enhanced_chunk_{chunk_id}.csv"
        with open(registry_path, "w") as f:
            for rid in sorted(current_failures):
                f.write(f"{rid}\n")
        
        logger.info(f"✅ Stage 2 complete: chunk {chunk_id}, processed {len(results)} profiles")
        
    elif pipeline_stage == "committees":
        logger.info("🚀 Stage 3: Fetching graduate committee roles")
        
        # Load profiles
        with open(profiles_file, "r", encoding="utf-8") as f:
            all_profiles = [json.loads(line.strip()) for line in f]

        # Load faculty who have students from our list
        with open("faculty_with_students.txt", "r") as f:
            faculty_ids_with_students = set(line.strip() for line in f if line.strip())

        # Filter to only faculty who have students
        all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in faculty_ids_with_students]

        logger.info(f"Processing {len(all_profiles)} faculty who have students (out of {len(faculty_ids_with_students)} total)")
        
        # Filter to retry list if provided
        if retry_registry_file and os.path.exists(retry_registry_file):
            with open(retry_registry_file, "r") as f:
                retry_ids = set(line.strip() for line in f if line.strip())
            all_profiles = [p for p in all_profiles if str(p.get("discoveryId")) in retry_ids]
        
        # Partition work
        chunk_size = len(all_profiles) // chunk_total + 1
        user_profiles = all_profiles[chunk_id * chunk_size:(chunk_id + 1) * chunk_size]
        
        # Process in parallel
        results = []
        with ThreadPoolExecutor(max_workers=n_threads) as executor:
            futures = {executor.submit(fetch_committee_roles, profile): profile for profile in user_profiles}
            for future in as_completed(futures):
                profile = futures[future]
                try:
                    discovery_id, status = future.result()
                    results.append((discovery_id, profile.get("firstNameLastName"), status))
                except Exception as e:
                    results.append((profile.get("discoveryId"), profile.get("firstNameLastName"), f"fatal_error: {e}"))
        
        # Write logs
        current_failures = set()
        with open(log_file, "a") as log, open(error_file, "a") as err:
            for discovery_id, name, status in results:
                log.write(f"{discovery_id},{name},{status}\n")
                if any(x in status for x in ("fail", "error", "retry", "max_retries")):
                    err.write(f"{discovery_id},{name},{status}\n")
                    current_failures.add(discovery_id)
        
        registry_path = retry_registry_file or f"logs/retry_registry_committees_chunk_{chunk_id}.csv"
        with open(registry_path, "w") as f:
            for rid in sorted(current_failures):
                f.write(f"{rid}\n")
        
        logger.info(f"✅ Stage 3 complete: chunk {chunk_id}, processed {len(results)} profiles")

if __name__ == "__main__":
    run_pipeline() 