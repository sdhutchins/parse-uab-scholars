#!/usr/bin/env python3
"""
Generate HTML table from faculty-student data for GitHub Pages hosting.

Creates a responsive, searchable table with faculty mentoring relationships.
"""

import json
from pathlib import Path
from typing import List, Dict

# All paths relative to project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def create_html_table(faculty_data: List[Dict]) -> str:
    """
    Create HTML table with faculty-student mentoring data.
    
    Args:
        faculty_data: List of faculty objects with students
        
    Returns:
        Complete HTML document as string
    """
    
    html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>UAB Faculty Mentoring Relationships</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .header {
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }
        .header h1 {
            margin: 0;
            font-size: 2.5em;
            font-weight: 300;
        }
        .header p {
            margin: 10px 0 0 0;
            opacity: 0.9;
            font-size: 1.1em;
        }
        .controls {
            padding: 20px 30px;
            background: #f8f9fa;
            border-bottom: 1px solid #dee2e6;
        }
        .search-box {
            width: 100%;
            max-width: 400px;
            padding: 12px 16px;
            border: 2px solid #ddd;
            border-radius: 6px;
            font-size: 16px;
            outline: none;
            transition: border-color 0.3s;
        }
        .search-box:focus {
            border-color: #1e3c72;
        }
        .stats {
            margin-top: 15px;
            font-size: 14px;
            color: #666;
        }
        .table-container {
            overflow-x: auto;
            max-height: 70vh;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }
        th {
            background: #f8f9fa;
            padding: 15px 12px;
            text-align: left;
            font-weight: 600;
            color: #495057;
            border-bottom: 2px solid #dee2e6;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        td {
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }
        tr:hover {
            background-color: #f8f9fa;
        }
        .faculty-name {
            font-weight: 600;
            color: #1e3c72;
        }
        .research-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 4px;
        }
        .tag {
            background: #e3f2fd;
            color: #1976d2;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
            white-space: nowrap;
        }
        .student-list {
            margin: 0;
            padding: 0;
            list-style: none;
        }
        .student-item {
            margin-bottom: 8px;
            padding: 8px;
            background: #f8f9fa;
            border-radius: 4px;
            border-left: 3px solid #28a745;
        }
        .student-name {
            font-weight: 500;
            color: #333;
        }
        .student-role {
            font-size: 12px;
            color: #666;
            margin-top: 2px;
        }
        .student-status {
            font-size: 11px;
            color: #999;
            margin-top: 2px;
        }
        .status-current {
            color: #28a745;
        }
        .status-completed {
            color: #6c757d;
        }
        .status-unknown {
            color: #ffc107;
        }
        .no-results {
            text-align: center;
            padding: 40px;
            color: #666;
            font-style: italic;
        }
        @media (max-width: 768px) {
            .header h1 {
                font-size: 2em;
            }
            th, td {
                padding: 8px 6px;
                font-size: 13px;
            }
            .research-tags {
                flex-direction: column;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>UAB Faculty Mentoring Relationships</h1>
            <p>Faculty members and their graduate student mentoring relationships</p>
        </div>
        
        <div class="controls">
            <input type="text" id="searchInput" class="search-box" placeholder="Search faculty names, research areas, or student names...">
            <div class="stats">
                <span id="totalFaculty">Total Faculty: {total_faculty}</span> | 
                <span id="totalStudents">Total Students: {total_students}</span> | 
                <span id="showingResults">Showing: {total_faculty} faculty</span>
            </div>
        </div>
        
        <div class="table-container">
            <table id="facultyTable">
                <thead>
                    <tr>
                        <th>Faculty Name</th>
                        <th>Research Areas</th>
                        <th>Students</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
    </div>

    <script>
        const facultyData = {faculty_data_json};
        
        function updateTable(filteredData) {
            const tbody = document.querySelector('#facultyTable tbody');
            const showingResults = document.getElementById('showingResults');
            
            if (filteredData.length === 0) {
                tbody.innerHTML = '<tr><td colspan="3" class="no-results">No faculty found matching your search criteria.</td></tr>';
                showingResults.textContent = 'Showing: 0 faculty';
                return;
            }
            
            tbody.innerHTML = filteredData.map(faculty => `
                <tr>
                    <td class="faculty-name">${faculty.userName}</td>
                    <td>
                        <div class="research-tags">
                            ${faculty.researchTags.map(tag => `<span class="tag">${tag}</span>`).join('')}
                        </div>
                    </td>
                    <td>
                        <ul class="student-list">
                            ${faculty.students.map(student => `
                                <li class="student-item">
                                    <div class="student-name">${student.name}</div>
                                    <div class="student-role">${student.role}</div>
                                    <div class="student-status ${getStatusClass(student.status)}">${student.status}</div>
                                </li>
                            `).join('')}
                        </ul>
                    </td>
                </tr>
            `).join('');
            
            showingResults.textContent = `Showing: ${filteredData.length} faculty`;
        }
        
        function getStatusClass(status) {
            if (status.includes('Current')) return 'status-current';
            if (status.includes('graduated') || status.includes('No longer')) return 'status-completed';
            return 'status-unknown';
        }
        
        function filterData(searchTerm) {
            if (!searchTerm) return facultyData;
            
            const term = searchTerm.toLowerCase();
            return facultyData.filter(faculty => {
                // Search faculty name
                if (faculty.userName.toLowerCase().includes(term)) return true;
                
                // Search research tags
                if (faculty.researchTags.some(tag => tag.toLowerCase().includes(term))) return true;
                
                // Search student names
                if (faculty.students.some(student => student.name.toLowerCase().includes(term))) return true;
                
                return false;
            });
        }
        
        // Search functionality
        document.getElementById('searchInput').addEventListener('input', function(e) {
            const searchTerm = e.target.value;
            const filteredData = filterData(searchTerm);
            updateTable(filteredData);
        });
        
        // Initialize table
        updateTable(facultyData);
    </script>
</body>
</html>
"""
    
    # Generate table rows
    table_rows = []
    for faculty in faculty_data:
        # Create research tags HTML
        tags_html = ''.join([f'<span class="tag">{tag}</span>' for tag in faculty['researchTags']])
        
        # Create students HTML
        students_html = ''
        for student in faculty['students']:
            status_class = 'status-unknown'
            if 'Current' in student['status']:
                status_class = 'status-current'
            elif 'graduated' in student['status'] or 'No longer' in student['status']:
                status_class = 'status-completed'
            
            students_html += f'''
                <li class="student-item">
                    <div class="student-name">{student['name']}</div>
                    <div class="student-role">{student['role']}</div>
                    <div class="student-status {status_class}">{student['status']}</div>
                </li>
            '''
        
        table_rows.append(f'''
            <tr>
                <td class="faculty-name">{faculty['userName']}</td>
                <td>
                    <div class="research-tags">
                        {tags_html}
                    </div>
                </td>
                <td>
                    <ul class="student-list">
                        {students_html}
                    </ul>
                </td>
            </tr>
        ''')
    
    # Calculate totals
    total_faculty = len(faculty_data)
    total_students = sum(len(f['students']) for f in faculty_data)
    
    # Create the HTML
    html_content = html_template.format(
        total_faculty=total_faculty,
        total_students=total_students,
        table_rows=''.join(table_rows),
        faculty_data_json=json.dumps(faculty_data)
    )
    
    return html_content


def main():
    """Generate HTML table from faculty-student data."""
    print("Loading faculty-student data...")
    
    # Load the processed data
    with open(PROJECT_ROOT / "data/processed/faculty_students.json", "r") as f:
        faculty_data = json.load(f)
    
    print(f"Loaded {len(faculty_data)} faculty records")
    
    # Generate HTML
    print("Generating HTML table...")
    html_content = create_html_table(faculty_data)
    
    # Save HTML file
    output_file = PROJECT_ROOT / "data/processed/faculty_mentoring_table.html"
    with open(output_file, "w") as f:
        f.write(html_content)
    
    print(f"HTML table saved to: {output_file}")
    print("You can now host this file on GitHub Pages!")


if __name__ == "__main__":
    main() 