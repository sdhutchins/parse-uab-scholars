#!/usr/bin/env python3
"""
Enhanced Faculty Data Creation

This script creates enhanced faculty data using publication keywords
and email addresses from the enhanced data collection.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Set

def extract_student_name(title: str) -> Optional[str]:
    """
    Extract student name from committee title.
    Format: "Thesis Committee for Kaitlyn S Ryan (Mentor)"
    Returns: "Kaitlyn S Ryan"
    """
    pattern = r'for\s+([^(]+?)\s*\('
    match = re.search(pattern, title)
    if match:
        return match.group(1).strip()
    return None

def get_all_committee_roles() -> Set[str]:
    """Get all committee roles."""
    return {
        "Mentor",
        "Committee Chair & Mentor",
        "Advisor",
        "Committee Member",
        "Chairperson",
        "Co-Chairperson",
        "Ad Hoc Committee Member",
        "Adjunct Committee Member"
    }

def load_enhanced_profiles() -> Dict[str, Dict]:
    """Load enhanced profiles with emails and publication keywords."""
    enhanced_file = Path("data/enhanced/enhanced_profiles.jsonl")
    if not enhanced_file.exists():
        print("⚠️  Enhanced profiles not found. Run enhanced_data_collection.py first.")
        return {}
    
    profiles = {}
    with open(enhanced_file, 'r', encoding='utf-8') as f:
        for line in f:
            profile = json.loads(line.strip())
            discovery_id = profile.get("discoveryId")
            if discovery_id:
                profiles[discovery_id] = profile
    
    print(f"Loaded {len(profiles)} enhanced profiles")
    return profiles

def process_enhanced_faculty_data() -> List[Dict]:
    """Process faculty data with enhanced information."""
    # Load enhanced profiles
    enhanced_profiles = load_enhanced_profiles()
    
    # Load committee data
    committees_dir = Path("data/committees_by_id")
    committee_roles = get_all_committee_roles()
    
    faculty_data = []
    
    for json_file in committees_dir.glob("*.json"):
        discovery_id = json_file.stem
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                committees = json.load(f)
        except Exception as e:
            print(f"Error reading {json_file}: {e}")
            continue
        
        if not committees:
            continue
        
        faculty_name = committees[0]["userName"]
        research_tags = []
        scholars_url = None
        email = None
        publication_keywords = []
        recent_publications = []
        
        # Get enhanced profile data
        if discovery_id in enhanced_profiles:
            profile = enhanced_profiles[discovery_id]
            
            # Get research tags
            if "tags" in profile and "explicit" in profile["tags"]:
                research_tags = [tag["value"] for tag in profile["tags"]["explicit"]]
            
            # Get scholars URL
            if "discoveryUrlId" in profile:
                scholars_url = f"https://scholars.uab.edu/{profile['discoveryUrlId']}"
            
            # Get email
            email = profile.get("email")
            
            # Get publication keywords
            publication_keywords = profile.get("publication_keywords", [])
            
            # Get recent publications
            recent_publications = profile.get("recent_publications", [])
        
        # Process committee memberships
        students_list = []
        for committee in committees:
            title = committee["title"]
            role_match = re.search(r'\(([^)]+)\)', title)
            if role_match:
                role = role_match.group(1)
                if role in committee_roles:
                    student_name = extract_student_name(title)
                    if student_name:
                        students_list.append({
                            "name": student_name,
                            "role": role,
                            "status": committee["status"],
                            "startDate": committee["startDate"],
                            "endDate": committee["endDate"]
                        })
        
        if students_list:
            student_names_str = ", ".join([s["name"] for s in students_list])
            research_areas_str = ", ".join(research_tags) if research_tags else ""
            
            # Create enhanced faculty data
            faculty_entry = {
                "discoveryId": discovery_id,
                "userName": faculty_name,
                "researchAreas": research_areas_str,
                "scholarsUrl": scholars_url,
                "students": student_names_str,
                "email": email,
                "publicationKeywords": publication_keywords,
                "recentPublications": recent_publications
            }
            
            faculty_data.append(faculty_entry)
    
    # Sort by faculty name
    faculty_data.sort(key=lambda x: x["userName"])
    
    return faculty_data

def main():
    """Main function to create enhanced faculty data."""
    print("Creating enhanced faculty data...")
    
    faculty_data = process_enhanced_faculty_data()
    
    # Save enhanced faculty data
    output_file = Path("data/enhanced_faculty_students.json")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(faculty_data, f, indent=2, ensure_ascii=False)
    
    # Print statistics
    total_faculty = len(faculty_data)
    total_students = sum(len(f["students"].split(", ")) for f in faculty_data if f["students"])
    faculty_with_emails = sum(1 for f in faculty_data if f.get("email"))
    faculty_with_keywords = sum(1 for f in faculty_data if f.get("publicationKeywords"))
    faculty_with_publications = sum(1 for f in faculty_data if f.get("recentPublications"))
    
    print(f"\n📊 Enhanced Faculty Data Statistics:")
    print(f"  Total faculty: {total_faculty}")
    print(f"  Total students: {total_students}")
    print(f"  Faculty with emails: {faculty_with_emails}")
    print(f"  Faculty with publication keywords: {faculty_with_keywords}")
    print(f"  Faculty with recent publications: {faculty_with_publications}")
    
    # Show sample data
    if faculty_data:
        sample = faculty_data[0]
        print(f"\n📋 Sample Enhanced Faculty Entry:")
        print(f"  Name: {sample['userName']}")
        print(f"  Email: {sample.get('email', 'Not available')}")
        print(f"  Research areas: {sample['researchAreas'][:100]}...")
        print(f"  Students: {len(sample['students'].split(', '))}")
        print(f"  Publication keywords: {len(sample.get('publicationKeywords', []))}")
        print(f"  Recent publications: {len(sample.get('recentPublications', []))}")
    
    print(f"\n✅ Enhanced faculty data saved to: {output_file}")

if __name__ == "__main__":
    main() 