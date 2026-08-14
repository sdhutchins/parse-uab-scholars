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

# All paths relative to project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_command(command: list[str], description: str) -> bool:
    """Run a command and handle errors."""
    print(f"Running: {description}")
    try:
        subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Error in {description}: {e}")
        print(f"Error output: {e.stderr}")
        return False


def main():
    """Main update process."""
    print("Starting docs update process...")
    
    src_dir = PROJECT_ROOT / "src"
    data_dir = PROJECT_ROOT / "data"
    docs_dir = PROJECT_ROOT / "docs"

    # Step 1: Regenerate faculty-student data
    command = [
        sys.executable,
        str(src_dir / "create_faculty_student_data.py"),
    ]
    if not run_command(command, "Faculty data generation"):
        print("Failed to generate faculty data. Exiting.")
        sys.exit(1)

    # Step 2: Copy JSON data to docs
    print("Copying data files to docs folder...")
    try:
        shutil.copy(
            data_dir / "processed/faculty_students.json",
            docs_dir,
        )
        print("✓ Data file copied to docs folder")
    except Exception as e:
        print(f"✗ Error copying data file: {e}")
        sys.exit(1)

    # Step 3: Ensure assets are in docs folder
    print("Ensuring assets are in docs folder...")
    docs_assets = docs_dir / "assets"
    docs_assets.mkdir(exist_ok=True)

    # Copy CSS and JS files
    try:
        shutil.copy(
            data_dir / "processed/assets/styles.css",
            docs_assets,
        )
        shutil.copy(
            data_dir / "processed/assets/script.js",
            docs_assets,
        )
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
