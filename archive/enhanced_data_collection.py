#!/usr/bin/env python3
"""
Enhanced UAB Scholars Data Collection

This script enhances the existing data by fetching individual user details
including email addresses, publications, and grants information.
"""

import json
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional, Set
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedDataCollector:
    def __init__(self):
        self.base_url = "https://scholars.uab.edu/api"
        self.headers = {"Content-Type": "application/json"}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
        # Paths
        self.data_dir = Path("data")
        self.enhanced_dir = self.data_dir / "enhanced"
        self.enhanced_dir.mkdir(exist_ok=True)
        
        # Files
        self.profiles_file = self.data_dir / "uab_scholars_profiles.jsonl"
        self.enhanced_profiles_file = self.enhanced_dir / "enhanced_profiles.jsonl"
        self.publications_file = self.enhanced_dir / "publications.jsonl"
        self.grants_file = self.enhanced_dir / "grants.jsonl"
        
    def load_existing_profiles(self) -> List[Dict]:
        """Load existing profiles from JSONL file."""
        profiles = []
        with open(self.profiles_file, 'r', encoding='utf-8') as f:
            for line in f:
                profiles.append(json.loads(line.strip()))
        logger.info(f"Loaded {len(profiles)} existing profiles")
        return profiles
    
    def fetch_user_details(self, discovery_id: str, max_retries: int = 3) -> Optional[Dict]:
        """Fetch detailed user information including email."""
        url = f"{self.base_url}/users/{discovery_id}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 404:
                    logger.warning(f"User {discovery_id} not found")
                    return None
                else:
                    logger.warning(f"HTTP {response.status_code} for user {discovery_id}")
                    
            except Exception as e:
                logger.error(f"Error fetching user {discovery_id}: {e}")
                
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                
        return None
    
    def extract_publication_keywords(self, publications: List[Dict]) -> Set[str]:
        """Extract keywords from publication titles."""
        keywords = set()
        
        for pub in publications:
            title = pub.get("title", "")
            if title:
                # Simple keyword extraction - split on common delimiters
                words = title.replace('-', ' ').replace(':', ' ').split()
                # Filter out common words and short words
                filtered_words = [
                    word.lower() for word in words 
                    if len(word) > 3 and word.lower() not in {
                        'the', 'and', 'for', 'with', 'from', 'this', 'that', 'have', 'been', 'they',
                        'will', 'their', 'said', 'each', 'which', 'she', 'will', 'would', 'there',
                        'could', 'been', 'were', 'more', 'very', 'what', 'when', 'where', 'over',
                        'just', 'into', 'than', 'only', 'other', 'some', 'time', 'about', 'many',
                        'then', 'them', 'these', 'people', 'through', 'because', 'during', 'before',
                        'should', 'between', 'under', 'never', 'always', 'while', 'often', 'until',
                        'against', 'among', 'those', 'being', 'such', 'here', 'again', 'around',
                        'another', 'within', 'without', 'through', 'during', 'before', 'after',
                        'above', 'below', 'since', 'until', 'upon', 'toward', 'towards', 'against',
                        'among', 'between', 'behind', 'beneath', 'beside', 'beyond', 'inside',
                        'outside', 'throughout', 'within', 'without'
                    }
                ]
                keywords.update(filtered_words[:10])  # Limit to first 10 words per publication
                
        return keywords
    
    def enhance_profile(self, profile: Dict) -> Dict:
        """Enhance a profile with additional data."""
        discovery_id = profile.get("discoveryId")
        if not discovery_id:
            return profile
            
        logger.info(f"Enhancing profile for {discovery_id}")
        
        # Fetch detailed user information
        user_details = self.fetch_user_details(discovery_id)
        if user_details:
            # Extract email
            email = user_details.get("emailAddress", {}).get("address")
            if email:
                profile["email"] = email
                
            # Extract additional fields
            profile["orcid"] = user_details.get("orcid", {}).get("value")
            profile["degrees"] = user_details.get("degrees", [])
            
            # Get objectId for publications and grants (use the numeric discoveryId)
            object_id = user_details.get("objectId")
            
            # Fetch publications using objectId
            if object_id:
                publications = self.fetch_publications_by_object_id(object_id, limit=5)
                if publications:
                    profile["recent_publications"] = [
                        {
                            "title": pub.get("title", ""),
                            "year": pub.get("date1", {}).get("year"),
                            "discoveryId": pub.get("discoveryId"),
                            "abstract": pub.get("abstract", ""),
                            "journal": pub.get("journal", ""),
                            "doi": pub.get("doi", "")
                        }
                        for pub in publications
                    ]
                    
                    # Extract keywords from publications
                    keywords = self.extract_publication_keywords(publications)
                    profile["publication_keywords"] = list(keywords)
                
                # Fetch grants using objectId
                grants = self.fetch_grants_by_object_id(object_id, limit=5)
                if grants:
                    profile["recent_grants"] = [
                        {
                            "title": grant.get("title", ""),
                            "year": grant.get("date1", {}).get("year"),
                            "discoveryId": grant.get("discoveryId"),
                            "amount": grant.get("amount"),
                            "funder": grant.get("funder")
                        }
                        for grant in grants
                    ]
            
        return profile
    
    def fetch_publications_by_object_id(self, object_id: str, limit: int = 5) -> List[Dict]:
        """Fetch recent publications for a user using objectId."""
        url = f"{self.base_url}/publications/linkedTo"
        payload = {
            "objectId": object_id,
            "category": "user",
            "pagination": {"perPage": limit, "startFrom": 0},
            "sort": "dateDesc",
            "favouritesFirst": True
        }
        
        try:
            response = self.session.post(url, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get("resource", [])
            else:
                logger.warning(f"Failed to fetch publications for objectId {object_id}: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching publications for objectId {object_id}: {e}")
            
        return []
    
    def fetch_grants_by_object_id(self, object_id: str, limit: int = 10) -> List[Dict]:
        """Fetch grants for a user using objectId."""
        url = f"{self.base_url}/grants/linkedTo"
        payload = {
            "objectId": object_id,
            "category": "user",
            "pagination": {"perPage": limit, "startFrom": 0},
            "sort": "dateDesc",
            "favouritesFirst": True
        }
        
        try:
            response = self.session.post(url, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get("resource", [])
            else:
                logger.warning(f"Failed to fetch grants for objectId {object_id}: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching grants for objectId {object_id}: {e}")
            
        return []
    
    def run_enhancement(self, limit: Optional[int] = None):
        """Run the enhancement process."""
        profiles = self.load_existing_profiles()
        
        if limit:
            profiles = profiles[:limit]
            logger.info(f"Processing limited to {limit} profiles")
        
        enhanced_count = 0
        email_count = 0
        publication_count = 0
        grant_count = 0
        
        with open(self.enhanced_profiles_file, 'w', encoding='utf-8') as f:
            for i, profile in enumerate(profiles, 1):
                logger.info(f"Processing {i}/{len(profiles)}: {profile.get('firstNameLastName', 'Unknown')}")
                
                enhanced_profile = self.enhance_profile(profile)
                f.write(json.dumps(enhanced_profile, ensure_ascii=False) + '\n')
                
                enhanced_count += 1
                if enhanced_profile.get("email"):
                    email_count += 1
                if enhanced_profile.get("recent_publications"):
                    publication_count += 1
                if enhanced_profile.get("recent_grants"):
                    grant_count += 1
                
                # Rate limiting
                time.sleep(1)
                
                # Progress update every 10 profiles
                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(profiles)} - Emails: {email_count}, Publications: {publication_count}, Grants: {grant_count}")
        
        logger.info(f"Enhancement complete!")
        logger.info(f"Enhanced profiles: {enhanced_count}")
        logger.info(f"Profiles with emails: {email_count}")
        logger.info(f"Profiles with publications: {publication_count}")
        logger.info(f"Profiles with grants: {grant_count}")
        
        return {
            "enhanced_count": enhanced_count,
            "email_count": email_count,
            "publication_count": publication_count,
            "grant_count": grant_count
        }

def main():
    """Main function to run the enhancement process."""
    collector = EnhancedDataCollector()
    
    # Test with a larger sample to get more emails
    logger.info("Starting enhanced data collection...")
    results = collector.run_enhancement(limit=100)  # Increase to 100 profiles
    
    logger.info("Results:")
    for key, value in results.items():
        logger.info(f"  {key}: {value}")

if __name__ == "__main__":
    main() 