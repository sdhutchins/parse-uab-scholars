#!/usr/bin/env python3
"""
Enhanced data collection for UAB faculty profiles.
Fetches publications, grants, and email addresses from UAB Scholars API.
Designed to run in chunks on a cluster.
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

# Paths
input_file = "data/uab_scholars_profiles.jsonl"
output_file = "data/enhanced_profiles.jsonl"
log_file = f"logs/chunk_{chunk_id}_enhanced_data.log"
error_file = f"logs/chunk_{chunk_id}_enhanced_data_errors.log"

# API
base_url = "https://scholars.uab.edu/api"
headers = {"Content-Type": "application/json"}

# Ensure dirs
os.makedirs(os.path.dirname(output_file), exist_ok=True)
os.makedirs(os.path.dirname(log_file), exist_ok=True)

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

def fetch_user_details(discovery_id: str, max_retries: int = 3) -> Optional[Dict]:
    """Fetch detailed user information including email and ORCID."""
    url = f"{base_url}/users/{discovery_id}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=20)
            if response.status_code == 200:
                data = response.json()
                return {
                    "objectId": data.get("objectId"),
                    "email": data.get("email"),
                    "orcid": data.get("orcid"),
                    "degrees": data.get("degrees", [])
                }
            elif response.status_code >= 500:
                logger.warning(f"Retry ({attempt+1}) for user {discovery_id} (HTTP {response.status_code})")
                time.sleep(2)
                continue
            else:
                logger.warning(f"Failed to fetch user {discovery_id}: HTTP {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"Error fetching user {discovery_id}: {e}")
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
                publications = data.get("resource", [])
                
                # Extract key information for search keywords
                enhanced_pubs = []
                for pub in publications:
                    enhanced_pubs.append({
                        "title": pub.get("title", ""),
                        "abstract": pub.get("abstract", ""),
                        "journal": pub.get("journal", ""),
                        "doi": pub.get("doi", ""),
                        "publicationDate": pub.get("publicationDate", {}),
                        "labels": [label.get("value", "") for label in pub.get("labels", [])]
                    })
                return enhanced_pubs
            elif response.status_code >= 500:
                logger.warning(f"Retry ({attempt+1}) for publications {object_id} (HTTP {response.status_code})")
                time.sleep(2)
                continue
            else:
                logger.warning(f"Failed to fetch publications for {object_id}: HTTP {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error fetching publications for {object_id}: {e}")
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
                grants = data.get("resource", [])
                
                # Extract key information
                enhanced_grants = []
                for grant in grants:
                    enhanced_grants.append({
                        "title": grant.get("title", ""),
                        "abstract": grant.get("abstract", ""),
                        "funder": grant.get("funder", ""),
                        "amount": grant.get("amount", ""),
                        "startDate": grant.get("date1", {}),
                        "endDate": grant.get("date2", {}),
                        "labels": [label.get("value", "") for label in grant.get("labels", [])]
                    })
                return enhanced_grants
            elif response.status_code >= 500:
                logger.warning(f"Retry ({attempt+1}) for grants {object_id} (HTTP {response.status_code})")
                time.sleep(2)
                continue
            else:
                logger.warning(f"Failed to fetch grants for {object_id}: HTTP {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Error fetching grants for {object_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
    
    return []

def extract_publication_keywords(publications: List[Dict]) -> str:
    """Extract keywords from publication labels and titles, removing 'and' and numbers."""
    
    keywords = set()
    
    for pub in publications:
        # Extract from labels (structured data)
        labels = pub.get("labels", [])
        for label in labels:
            value = label.get("value", "")
            if value:
                # Remove numbers and "and", split into words
                cleaned = re.sub(r'\d+', '', value)  # Remove numbers
                cleaned = cleaned.replace(" and ", " ")  # Remove "and"
                cleaned = cleaned.replace("&", " ")  # Replace & with space
                words = re.findall(r'\b[a-zA-Z]{3,}\b', cleaned)  # Get words 3+ chars
                keywords.update(words)
        
        # Also extract from title (backup)
        title = pub.get("title", "")
        if title:
            cleaned = re.sub(r'\d+', '', title)
            cleaned = cleaned.replace(" and ", " ")
            cleaned = cleaned.replace("&", " ")
            words = re.findall(r'\b[a-zA-Z]{3,}\b', cleaned)
            keywords.update(words)
    
    # Limit to 30 keywords and return as space-separated string
    return " ".join(list(keywords)[:30])

def enhance_profile(profile: Dict, max_retries: int = 3) -> Tuple[str, str]:
    """Enhance a single faculty profile with additional data."""
    discovery_id = str(profile.get("discoveryId"))
    if not discovery_id:
        return discovery_id, "no_discovery_id"
    
    name = profile.get("firstNameLastName", "Unknown")
    logger.info(f"Processing {name} (ID: {discovery_id})")
    
    # Fetch user details (email, ORCID, etc.)
    user_details = fetch_user_details(discovery_id, max_retries)
    
    # Get objectId for API calls
    object_id = user_details.get("objectId") if user_details else discovery_id
    
    # Fetch publications and grants
    publications = fetch_publications(object_id, limit=5, max_retries=max_retries)
    grants = fetch_grants(object_id, limit=10, max_retries=max_retries)
    
    # Extract keywords from publications and grants
    publication_keywords = extract_publication_keywords(publications)
    grant_keywords = extract_publication_keywords(grants)  # Reuse same function
    
    # Create enhanced profile (keep it compact)
    enhanced_profile = {
        **profile,  # Keep all original data
        "email": user_details.get("email") if user_details else None,
        "publication_keywords": publication_keywords,  # Hidden search keywords
        "grant_keywords": grant_keywords  # Hidden search keywords
    }
    
    # Write to JSONL file
    with open(output_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(enhanced_profile, ensure_ascii=False) + "\n")
    
    return discovery_id, "ok"

# === Run in Parallel ===
results = []
with ThreadPoolExecutor(max_workers=n_threads) as executor:
    futures = {executor.submit(enhance_profile, profile): profile for profile in user_profiles}
    for future in as_completed(futures):
        profile = futures[future]
        try:
            discovery_id, status = future.result()
            name = profile.get("firstNameLastName", "Unknown")
            if discovery_id:
                results.append((discovery_id, name, status))
        except Exception as e:
            results.append((profile.get("discoveryId"), profile.get("firstNameLastName"), f"fatal_error: {e}"))

# === Write Logs and Retry Registry ===
current_failures = set()

with open(log_file, "a") as log, open(error_file, "a") as err:
    for discovery_id, name, status in results:
        log.write(f"{discovery_id},{name},{status}\n")
        if any(x in status for x in ("fail", "error", "retry", "max_retries")):
            err.write(f"{discovery_id},{name},{status}\n")
            current_failures.add(discovery_id)

# === Always write retry registry ===
registry_path = retry_registry_file or f"logs/retry_registry_enhanced_chunk_{chunk_id}.csv"
with open(registry_path, "w") as f:
    for rid in sorted(current_failures):
        f.write(f"{rid}\n")

logger.info(f"🎓 Done: chunk {chunk_id}, processed {len(results)} profiles")
logger.info(f"📊 Success: {len([r for r in results if r[2] == 'ok'])}")
logger.info(f"❌ Failures: {len(current_failures)}") 