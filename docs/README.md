# UAB Faculty Committee Memberships

This folder contains the GitHub Pages website for the UAB Faculty Committee Memberships table.

## Files

- `index.html` - Main HTML page with the interactive table
- `faculty_students.json` - Data file containing faculty and student information
- `assets/styles.css` - Custom CSS styling
- `assets/script.js` - JavaScript for search, filtering, and interactivity

## Features

- **Search**: Search by faculty name, research areas, or student names
- **Filter by Status**: Current, Completed, or Unknown committee status
- **Filter by Role**: Mentor, Chair, or Advisor roles
- **Committee Memberships**: View all students mentored through graduate committees
- **Responsive Design**: Works on desktop and mobile devices
- **Real-time Updates**: Statistics update as you filter

## Data Structure

The `faculty_students.json` file contains:
- Faculty names and discovery IDs
- Research area tags
- Student mentoring relationships with roles and status

## GitHub Pages Setup

1. Push this repository to GitHub
2. Go to repository Settings → Pages
3. Set source to "Deploy from a branch"
4. Select "main" branch and "/docs" folder
5. Save - your site will be available at `https://[username].github.io/[repo-name]/`

## Updating Data

Run the update script to regenerate the data and copy files to docs:

```bash
python update_docs.py
```

This will:
1. Regenerate faculty-student data from source files
2. Copy the JSON data to docs folder
3. Ensure all assets are up to date
