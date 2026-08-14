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
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from zoneinfo import ZoneInfo

# All paths relative to project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


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


def sanitize_research_areas(research_tags: List[str]) -> List[str]:
    """
    Clean and split research areas, handling parenthetical text.
    
    Examples:
    - "Epigenetics (incl. Genome Methylation and Epigenomics)" 
      → ["Epigenetics", "Genome Methylation", "Epigenomics"]
    - "Cancer (Oncology)" → ["Cancer", "Oncology"]
    """
    cleaned_areas = []
    
    for tag in research_tags:
        # Check if tag contains parentheses
        if '(' in tag and ')' in tag:
            # Extract main term (before parentheses)
            main_term = tag.split('(')[0].strip()
            if main_term:
                cleaned_areas.append(main_term)
            
            # Extract content inside parentheses
            paren_content = re.search(r'\(([^)]+)\)', tag)
            if paren_content:
                content = paren_content.group(1).strip()
                
                # Handle common prefixes like "incl.", "including", "e.g.", etc.
                content = re.sub(r'^(incl\.?|including|e\.g\.?|for example|such as)\s*', '', content, flags=re.IGNORECASE)
                
                # Split by common separators
                if ' and ' in content:
                    # Split by " and " and clean each part
                    parts = content.split(' and ')
                    for part in parts:
                        part = part.strip()
                        if part:
                            cleaned_areas.append(part)
                elif ',' in content:
                    # Split by commas and clean each part
                    parts = content.split(',')
                    for part in parts:
                        part = part.strip()
                        if part:
                            cleaned_areas.append(part)
                else:
                    # Single term in parentheses
                    if content:
                        cleaned_areas.append(content)
        else:
            # No parentheses, just add the tag as is
            cleaned_areas.append(tag.strip())
    
    # Remove duplicates while preserving order
    seen = set()
    unique_areas = []
    for area in cleaned_areas:
        if area and area not in seen:
            seen.add(area)
            unique_areas.append(area)
    
    return unique_areas


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
    
    # Load scholars profiles for basic info
    print("Loading scholars profiles...")
    scholars_profiles = {}
    with open(PROJECT_ROOT / "data/uab_scholars_profiles.jsonl", "r") as f:
        for line in f:
            profile = json.loads(line.strip())
            scholars_profiles[profile["discoveryId"]] = profile
    
    # Load enhanced faculty data for keywords and email
    print("Loading enhanced faculty data...")
    enhanced_profiles = {}
    faculty_data_dir = PROJECT_ROOT / "data/faculty_data"
    for json_file in faculty_data_dir.glob("*.json"):
        discovery_id = json_file.stem
        with open(json_file, "r") as f:
            enhanced_profiles[discovery_id] = json.load(f)
    
    # Process committee files
    committees_dir = PROJECT_ROOT / "data/committees_by_id"
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
        
        # Get research tags, email, and scholars URL
        research_tags = []
        email = None
        scholars_url = None
        search_keywords = ""
        
        # Get basic info from scholars profile
        if discovery_id in scholars_profiles:
            profile = scholars_profiles[discovery_id]
            if "tags" in profile and "explicit" in profile["tags"]:
                raw_research_tags = [tag["value"] for tag in profile["tags"]["explicit"]]
                # Sanitize research areas
                research_tags = sanitize_research_areas(raw_research_tags)
            if "discoveryUrlId" in profile:
                scholars_url = f"https://scholars.uab.edu/{profile['discoveryUrlId']}"
        
        # Get enhanced data (email and keywords)
        if discovery_id in enhanced_profiles:
            enhanced = enhanced_profiles[discovery_id]
            email = enhanced.get("email")
            pub_keywords = enhanced.get("publication_keywords", "")
            grant_keywords = enhanced.get("grant_keywords", "")
            # Combine all keywords for search (not visible in table)
            search_keywords = f"{pub_keywords} {grant_keywords}".strip()
        
        # Extract all committee memberships
        students = []
        current_students = []
        for committee in committees:
            title = committee["title"]
            role_match = re.search(r'\(([^)]+)\)', title)
            
            if role_match:
                role = role_match.group(1)
                if role in committee_roles:
                    student_name = extract_student_name(title)
                    if student_name:
                        # Check if this is a current student
                        is_current = (committee["status"] == "Unknown" and committee["endDate"] is None)
                        
                        students.append({
                            "name": student_name,
                            "role": role,
                            "status": committee["status"],
                            "startDate": committee["startDate"],
                            "endDate": committee["endDate"]
                        })
                        
                        if is_current:
                            current_students.append(student_name)
        
        # Only include faculty with students
        if students:
            # Create comma-delimited lists
            student_names = ", ".join([student["name"] for student in students])
            research_areas = ", ".join(research_tags) if research_tags else ""
            
            faculty_data.append({
                "discoveryId": discovery_id,
                "userName": faculty_name,
                "researchAreas": research_areas,
                "email": email,
                "scholarsUrl": scholars_url,
                "students": student_names,
                "currentStudents": current_students,  # List of current student names
                "searchKeywords": search_keywords  # Hidden field for search
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
    output_dir = PROJECT_ROOT / "data/processed"
    output_dir.mkdir(exist_ok=True)
    
    # Record when this website dataset was generated so the deployed page
    # reports data provenance rather than a Git or server file timestamp.
    dataset = {
        "updatedAt": datetime.now(
            ZoneInfo("America/Chicago")
        ).isoformat(timespec="seconds"),
        "faculty": faculty_data,
    }

    # Save combined data
    output_file = output_dir / "faculty_students.json"
    with open(output_file, "w") as f:
        json.dump(dataset, f, indent=2)
    
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