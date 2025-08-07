#!/usr/bin/env python3
"""
Update docs folder with latest faculty-student data for GitHub Pages.

This script:
1. Regenerates the faculty-student data
2. Copies assets to docs folder
3. Updates the JSON data file
"""

import shutil
import subprocess
import sys
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle errors."""
    print(f"Running: {description}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error in {description}: {e}")
        print(f"Error output: {e.stderr}")
        return False


def main():
    """Main update process."""
    print("Starting docs update process...")
    
    # Step 1: Regenerate faculty-student data
    if not run_command("python create_faculty_student_data.py", "Faculty data generation"):
        print("Failed to generate faculty data. Exiting.")
        sys.exit(1)
    
    # Step 2: Copy JSON data to docs
    print("Copying data files to docs folder...")
    try:
        shutil.copy("data/processed/faculty_students.json", "docs/")
        print("✓ Data file copied to docs folder")
    except Exception as e:
        print(f"✗ Error copying data file: {e}")
        sys.exit(1)
    
    # Step 3: Ensure assets are in docs folder
    print("Ensuring assets are in docs folder...")
    docs_assets = Path("docs/assets")
    docs_assets.mkdir(exist_ok=True)
    
    # Copy CSS and JS files
    try:
        shutil.copy("data/processed/assets/styles.css", "docs/assets/")
        shutil.copy("data/processed/assets/script.js", "docs/assets/")
        print("✓ Assets copied to docs folder")
    except Exception as e:
        print(f"✗ Error copying assets: {e}")
        sys.exit(1)
    
    print("\n🎉 Docs update completed successfully!")
    print("\nNext steps:")
    print("1. Commit and push your changes to GitHub")
    print("2. Enable GitHub Pages in your repository settings")
    print("3. Set the source to 'Deploy from a branch' and select 'main' branch")
    print("4. Set the folder to '/docs'")
    print("5. Your table will be available at: https://[username].github.io/[repo-name]/")


if __name__ == "__main__":
    main() 