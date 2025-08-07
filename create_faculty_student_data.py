#!/usr/bin/env python3
"""
Create faculty-student mentoring data structure for HTML table generation.

This script combines:
1. Faculty names from committee files
2. Research tags from scholars profiles
3. Student mentoring relationships from committee titles
"""

import json
import re
import os
from pathlib import Path
from typing import Dict, List, Optional, Set


def extract_student_name(title: str) -> Optional[str]:
    """
    Extract student name from committee title.
    
    Format: "Thesis Committee for Kaitlyn S Ryan (Mentor)"
    Returns: "Kaitlyn S Ryan"
    """
    # Pattern to match "for [Student Name] (Role)"
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


def process_faculty_data() -> List[Dict]:
    """
    Process all faculty data and create combined structure.
    
    Returns:
        List of faculty objects with names, research tags, and students
    """
    faculty_data = []
    
    # Load scholars profiles for research tags
    print("Loading scholars profiles...")
    scholars_profiles = {}
    with open("data/uab_scholars_profiles.jsonl", "r") as f:
        for line in f:
            profile = json.loads(line.strip())
            scholars_profiles[profile["discoveryId"]] = profile
    
    # Process committee files
    committees_dir = Path("data/committees_by_id")
    committee_roles = get_all_committee_roles()
    
    print("Processing committee files...")
    for json_file in committees_dir.glob("*.json"):
        discovery_id = json_file.stem
        
        # Skip empty files
        if json_file.stat().st_size <= 2:
            continue
            
        with open(json_file, "r") as f:
            committees = json.load(f)
        
        if not committees:
            continue
        
        # Get faculty name from first committee entry
        faculty_name = committees[0]["userName"]
        
        # Get research tags and scholars URL from scholars profile
        research_tags = []
        scholars_url = None
        if discovery_id in scholars_profiles:
            profile = scholars_profiles[discovery_id]
            if "tags" in profile and "explicit" in profile["tags"]:
                research_tags = [tag["value"] for tag in profile["tags"]["explicit"]]
            if "discoveryUrlId" in profile:
                scholars_url = f"https://scholars.uab.edu/{profile['discoveryUrlId']}"
        
        # Extract all committee memberships
        students = []
        for committee in committees:
            title = committee["title"]
            role_match = re.search(r'\(([^)]+)\)', title)
            
            if role_match:
                role = role_match.group(1)
                if role in committee_roles:
                    student_name = extract_student_name(title)
                    if student_name:
                        students.append({
                            "name": student_name,
                            "role": role,
                            "status": committee["status"],
                            "startDate": committee["startDate"],
                            "endDate": committee["endDate"]
                        })
        
        # Only include faculty with students
        if students:
            # Create comma-delimited lists
            student_names = ", ".join([student["name"] for student in students])
            research_areas = ", ".join(research_tags) if research_tags else ""
            
            faculty_data.append({
                "discoveryId": discovery_id,
                "userName": faculty_name,
                "researchAreas": research_areas,
                "scholarsUrl": scholars_url,
                "students": student_names
            })
    
    return faculty_data


def main():
    """Main processing function."""
    print("Starting faculty-student data processing...")
    
    # Process the data
    faculty_data = process_faculty_data()
    
    # Sort by faculty name
    faculty_data.sort(key=lambda x: x["userName"])
    
    # Create output directory
    output_dir = Path("data/processed")
    output_dir.mkdir(exist_ok=True)
    
    # Save combined data
    output_file = output_dir / "faculty_students.json"
    with open(output_file, "w") as f:
        json.dump(faculty_data, f, indent=2)
    
    # Print summary statistics
    total_faculty = len(faculty_data)
    total_students = sum(len(f["students"].split(", ")) for f in faculty_data if f["students"])
    faculty_with_tags = sum(1 for f in faculty_data if f["researchAreas"])
    
    print(f"\nProcessing complete!")
    print(f"Output file: {output_file}")
    print(f"Faculty with students: {total_faculty}")
    print(f"Total student relationships: {total_students}")
    print(f"Faculty with research tags: {faculty_with_tags}")
    
    # Show sample of first few entries
    if faculty_data:
        print(f"\nSample entry:")
        sample = faculty_data[0]
        print(f"  Faculty: {sample['userName']} (ID: {sample['discoveryId']})")
        print(f"  Research areas: {sample['researchAreas'][:100]}...")
        print(f"  Students: {len(sample['students'].split(', '))}")


if __name__ == "__main__":
    main() 