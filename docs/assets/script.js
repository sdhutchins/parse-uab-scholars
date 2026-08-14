// UAB Faculty Mentoring Table JavaScript

class FacultyTable {
    constructor() {
        this.facultyData = [];
        this.filteredData = [];
        this.currentFilters = {
            search: '',
            status: 'all',
            role: 'all'
        };
        this.init();
    }

    async init() {
        try {
            await this.loadData();
            this.setupEventListeners();
            this.renderTable();
            this.updateStats();
        } catch (error) {
            console.error('Error initializing table:', error);
            this.showError('Failed to load data. Please refresh the page.');
        }
    }

    async loadData() {
        const response = await fetch('faculty_students.json');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        this.facultyData = await response.json();
        this.filteredData = [...this.facultyData];
    }

    setupEventListeners() {
        // Search input
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.currentFilters.search = e.target.value;
                this.filterData();
            });
        }

        // Status filter buttons
        const statusButtons = document.querySelectorAll('[data-filter="status"]');
        statusButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.setActiveFilter('status', e.target.dataset.value);
                this.filterData();
            });
        });

        // Role filter buttons
        const roleButtons = document.querySelectorAll('[data-filter="role"]');
        roleButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                this.setActiveFilter('role', e.target.dataset.value);
                this.filterData();
            });
        });

        // Clear filters button
        const clearBtn = document.getElementById('clearFilters');
        if (clearBtn) {
            clearBtn.addEventListener('click', () => {
                this.clearFilters();
            });
        }
    }

    setActiveFilter(type, value) {
        // Remove active class from all buttons of this type
        document.querySelectorAll(`[data-filter="${type}"]`).forEach(btn => {
            btn.classList.remove('active');
        });
        
        // Add active class to clicked button
        const activeBtn = document.querySelector(`[data-filter="${type}"][data-value="${value}"]`);
        if (activeBtn) {
            activeBtn.classList.add('active');
        }
        
        this.currentFilters[type] = value;
    }

    filterData() {
        let filtered = [...this.facultyData];

        // Search filter
        if (this.currentFilters.search) {
            const searchTerm = this.currentFilters.search.toLowerCase();
            filtered = filtered.filter(faculty => {
                // Search faculty name
                if (faculty.userName.toLowerCase().includes(searchTerm)) return true;
                
                // Search research tags
                if (faculty.researchTags.some(tag => tag.toLowerCase().includes(searchTerm))) return true;
                
                // Search student names
                if (faculty.students.some(student => student.name.toLowerCase().includes(searchTerm))) return true;
                
                return false;
            });
        }

        // Status filter
        if (this.currentFilters.status !== 'all') {
            filtered = filtered.filter(faculty => {
                return faculty.students.some(student => {
                    if (this.currentFilters.status === 'current') {
                        return student.status.includes('Current');
                    } else if (this.currentFilters.status === 'completed') {
                        return student.status.includes('graduated') || student.status.includes('No longer');
                    } else if (this.currentFilters.status === 'unknown') {
                        return student.status === 'Unknown';
                    }
                    return true;
                });
            });
        }

        // Role filter
        if (this.currentFilters.role !== 'all') {
            filtered = filtered.filter(faculty => {
                return faculty.students.some(student => {
                    if (this.currentFilters.role === 'mentor') {
                        return student.role.includes('Mentor');
                    } else if (this.currentFilters.role === 'chair') {
                        return student.role.includes('Chair');
                    } else if (this.currentFilters.role === 'advisor') {
                        return student.role.includes('Advisor');
                    }
                    return true;
                });
            });
        }

        this.filteredData = filtered;
        this.renderTable();
        this.updateStats();
    }

    clearFilters() {
        // Reset search input
        const searchInput = document.getElementById('searchInput');
        if (searchInput) {
            searchInput.value = '';
        }

        // Reset filter buttons
        document.querySelectorAll('[data-filter]').forEach(btn => {
            btn.classList.remove('active');
        });

        // Reset filters
        this.currentFilters = {
            search: '',
            status: 'all',
            role: 'all'
        };

        this.filteredData = [...this.facultyData];
        this.renderTable();
        this.updateStats();
    }

    renderTable() {
        const tbody = document.querySelector('#facultyTable tbody');
        if (!tbody) return;

        if (this.filteredData.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="4" class="no-results">
                        <i class="fas fa-search"></i>
                        <br>No faculty found matching your search criteria.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.filteredData.map(faculty => this.renderFacultyRow(faculty)).join('');
    }

    renderFacultyRow(faculty) {
        const researchTags = faculty.researchTags.map(tag => 
            `<span class="research-tag">${this.highlightText(tag)}</span>`
        ).join('');

        const students = faculty.students.map(student => this.renderStudentItem(student)).join('');

        return `
            <tr>
                <td class="faculty-name">${this.highlightText(faculty.userName)}</td>
                <td>
                    <div class="research-tags">
                        ${researchTags}
                    </div>
                </td>
                <td>
                    <ul class="student-list">
                        ${students}
                    </ul>
                </td>
            </tr>
        `;
    }

    renderStudentItem(student) {
        const statusClass = this.getStatusClass(student.status);
        const highlightedName = this.highlightText(student.name);
        
        return `
            <li class="student-item">
                <div class="student-name">${highlightedName}</div>
                <div class="student-role">${student.role}</div>
                <div class="student-status ${statusClass}">${student.status}</div>
            </li>
        `;
    }

    getStatusClass(status) {
        if (status.includes('Current')) return 'status-current';
        if (status.includes('graduated') || status.includes('No longer')) return 'status-completed';
        return 'status-unknown';
    }

    highlightText(text) {
        if (!this.currentFilters.search) return text;
        
        const searchTerm = this.currentFilters.search;
        const regex = new RegExp(`(${searchTerm})`, 'gi');
        return text.replace(regex, '<span class="highlight">$1</span>');
    }

    updateStats() {
        const totalFaculty = document.getElementById('totalFaculty');
        const totalStudents = document.getElementById('totalStudents');
        const showingResults = document.getElementById('showingResults');

        if (totalFaculty) {
            totalFaculty.textContent = `Total Faculty: ${this.facultyData.length}`;
        }

        if (totalStudents) {
            const allStudents = this.facultyData.reduce((sum, faculty) => sum + faculty.students.length, 0);
            totalStudents.textContent = `Total Students: ${allStudents}`;
        }

        if (showingResults) {
            showingResults.textContent = `Showing: ${this.filteredData.length} faculty`;
        }
    }

    showError(message) {
        const tbody = document.querySelector('#facultyTable tbody');
        if (tbody) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="4" class="text-center text-danger">
                        <i class="fas fa-exclamation-triangle"></i>
                        <br>${message}
                    </td>
                </tr>
            `;
        }
    }
}

// Initialize the table when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new FacultyTable();
});

// Export for potential external use
window.FacultyTable = FacultyTable; 