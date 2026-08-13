# UAB Faculty Committee Memberships

This project extracts and visualizes faculty committee memberships and mentoring relationships from the University of Alabama at Birmingham (UAB) Scholars database. 

The system processes faculty profiles, research classifications, and committee assignments to create an interactive web-based table for exploring all students currently or formerly mentored by faculty at UAB through graduate committee memberships.

## Table of Contents

- [Project Background](mdc:#project-background)
- [Install & Setup](mdc:#install--setup)
- [Usage](mdc:#usage)
- [Directory Structure](mdc:#directory-structure)
- [Contributing](mdc:#contributing)
- [License](mdc:#license)
- [Authors](mdc:#authors)

## Project Background

The UAB Faculty Committee Memberships project addresses the need for a comprehensive view of faculty involvement in graduate education through committee memberships. While this data exists on individual UAB Scholars pages, this system provides an easy way to view all students currently or formerly mentored by faculty at UAB. By combining data from UAB Scholars profiles and graduate committee records, this system provides insights into:

- Faculty research areas and specializations
- Student mentoring relationships and roles
- Committee participation patterns
- Academic collaboration networks

The project processes structured data from multiple sources to create a searchable, filterable interface that helps researchers, administrators, and students understand the mentoring landscape at UAB through graduate committee memberships.

## Install & Setup

- Python 3.8+
- SLURM cluster access (for data collection)
- Web browser (for viewing the generated HTML table)

### Instructions

1. **Clone the repository:**
   ```bash
   git clone https://github.com/[username]/parse-uab-scholars.git
   cd parse-uab-scholars
   ```

2. **Install Python dependencies:**
   ```bash
   pip install requests
   ```

## Usage

### Data Collection (Cluster)

**Step 1: Run the unified pipeline on your cluster**
```bash
sbatch src/submit_enhanced_data_jobs.sh
sbatch src/submit_grad_committee_jobs.sh
```

This creates:
- `data/uab_scholars_profiles.jsonl` - All faculty profiles
- `data/faculty_data/` - Individual faculty files with emails, keywords, and committee roles

**Step 2: Wait for completion**
- 128 parallel jobs process faculty in chunks
- Each faculty gets one complete data file
- Failed jobs are automatically retried

### Data Processing

**Step 3: Generate the combined dataset**
```bash
python src/create_faculty_student_data.py
```

This creates `data/processed/faculty_students.json` containing:
- Faculty names and research areas
- Student mentoring relationships
- Committee role and status information
- Hidden search keywords from publications/grants

### Web Interface

**Step 4: Update the GitHub Pages website**
```bash
python src/update_docs.py
```

This script:
1. Regenerates faculty-student data
2. Copies assets to the `docs/` folder
3. Prepares files for GitHub Pages deployment

### GitHub Pages Deployment

**Step 5: Deploy to GitHub Pages**
1. Push changes to GitHub
2. Enable GitHub Pages in repository settings
3. Set source to "Deploy from a branch" → "main" → "/docs"
4. Access the table at `https://[username].github.io/[repo-name]/`

### Features

- **Search**: Faculty names, research areas, student names
- **Filter by Research Area**: Dropdown with autocomplete suggestions
- **Faculty Profile Links**: Direct links to UAB Scholars profiles
- **Committee Memberships**: View all students mentored through graduate committees
- **Responsive Design**: Mobile-friendly interface
- **Real-time Statistics**: Dynamic updates during filtering
- **Hidden Keywords**: Publication and grant keywords for enhanced search

## Directory Structure

```sh
$ tree -a parse-uab-scholars/
parse-uab-scholars/
├── README.md                       <- This file
├── LICENSE                         <- License for the repo
├── CONTRIBUTING.md                 <- Contribution guidelines
├── src/                            <- All scripts
│   ├── fetch_enhanced_data.py      <- Enhanced faculty data collection
│   ├── fetch_graduate_committee.py <- Graduate committee data collection
│   ├── create_faculty_student_data.py <- Data processing for HTML table
│   ├── create_html_table.py        <- HTML table generation
│   ├── update_docs.py              <- Documentation updates
│   ├── merge_committee_chunks.py   <- Merge committee chunk files
│   ├── inspect_api_data.py         <- API data inspection utility
│   ├── submit_enhanced_data_jobs.sh <- SLURM job: enhanced data
│   └── submit_grad_committee_jobs.sh <- SLURM job: committees
├── archive/                        <- Old/unused scripts
├── data/
│   ├── uab_scholars_profiles.jsonl <- Faculty profiles
│   ├── faculty_data/               <- Individual faculty files
│   ├── committees_by_id/           <- Committee data per faculty
│   └── processed/
│       └── faculty_students.json   <- Combined data for HTML table
├── docs/                           <- GitHub Pages site
│   ├── index.html                  <- Main web interface
│   ├── faculty_students.json       <- Data for web interface
│   └── assets/                     <- CSS and JavaScript files
├── tests/                          <- Test scripts and data
└── logs/                           <- Pipeline execution logs
```

## Contributing

We welcome contributions! [See the docs for guidelines](mdc:CONTRIBUTING.md).

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with sample data
5. Submit a pull request

### Code Style

- Follow PEP 8 for Python code
- Use descriptive variable names
- Include docstrings for functions
- Add comments explaining complex logic

## License

View the [LICENSE](mdc:LICENSE) for this project.

## Authors

- Shaurita Hutchins [email](mdc:mailto:sdhutchins@uab.edu) | Graduate Research Assistant | [sdhutchins](mdc:https://github.com/sdhutchins)
