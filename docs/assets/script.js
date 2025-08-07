// UAB Faculty Mentoring Table JavaScript

class FacultyTable {
    constructor() {
        this.facultyData = [];
        this.filteredData = [];
        this.currentFilters = {
            search: '',
            researchArea: ''
        };
        this.init();
    }

    async init() {
        try {
            await this.loadData();
            this.setupEventListeners();
            this.populateResearchAreas();
            this.renderTable();
            this.updateStats();
        } catch (error) {
            console.error('Error initializing table:', error);
            this.showError('Failed to load data. Please refresh the page.');
        }
    }

    async loadData() {
        try {
            const response = await fetch('faculty_students.json');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            this.facultyData = await response.json();
            this.filteredData = [...this.facultyData];
        } catch (error) {
            console.error('Error loading data:', error);
            throw error;
        }
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

        // Research area filter
        const researchFilter = document.getElementById('researchFilter');
        if (researchFilter) {
            researchFilter.addEventListener('input', (e) => {
                this.currentFilters.researchArea = e.target.value;
                this.filterData();
            });
        }

        // Clear filters button
        const clearFiltersBtn = document.getElementById('clearFilters');
        if (clearFiltersBtn) {
            clearFiltersBtn.addEventListener('click', () => {
                this.clearFilters();
            });
        }
    }

    filterData() {
        let filtered = [...this.facultyData];
        const searchTerm = this.currentFilters.search.toLowerCase();
        const researchAreaFilter = this.currentFilters.researchArea.toLowerCase();

        if (searchTerm) {
            filtered = filtered.filter(faculty => {
                return faculty.userName.toLowerCase().includes(searchTerm) ||
                       faculty.researchAreas.toLowerCase().includes(searchTerm) ||
                       faculty.students.toLowerCase().includes(searchTerm);
            });
        }

        if (researchAreaFilter) {
            filtered = filtered.filter(faculty => {
                return faculty.researchAreas.toLowerCase().includes(researchAreaFilter);
            });
        }

        this.filteredData = filtered;
        this.renderTable();
        this.updateStats();
    }

    clearFilters() {
        const searchInput = document.getElementById('searchInput');
        if (searchInput) searchInput.value = '';
        
        const researchFilter = document.getElementById('researchFilter');
        if (researchFilter) researchFilter.value = '';

        this.currentFilters = { search: '', researchArea: '' };
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
                    <td colspan="3" class="no-results">
                        <i class="fas fa-search me-2"></i>
                        No faculty found matching your criteria.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = this.filteredData.map(faculty => this.renderFacultyRow(faculty)).join('');
    }

    renderFacultyRow(faculty) {
        let facultyNameHtml = this.highlightText(faculty.userName);
        if (faculty.scholarsUrl) {
            facultyNameHtml = `<a href="${faculty.scholarsUrl}" target="_blank" class="faculty-link">${facultyNameHtml} <i class="fas fa-external-link-alt"></i></a>`;
        }

        return `
            <tr>
                <td class="faculty-name">${facultyNameHtml}</td>
                <td class="research-areas">${this.highlightText(faculty.researchAreas)}</td>
                <td class="students">${this.highlightText(faculty.students)}</td>
            </tr>
        `;
    }

    populateResearchAreas() {
        const datalist = document.getElementById('researchAreasList');
        if (!datalist) return;

        const allResearchAreas = new Set();
        this.facultyData.forEach(faculty => {
            if (faculty.researchAreas) {
                faculty.researchAreas.split(', ').forEach(area => {
                    if (area.trim()) {
                        allResearchAreas.add(area.trim());
                    }
                });
            }
        });

        Array.from(allResearchAreas).sort().forEach(area => {
            const option = document.createElement('option');
            option.value = area;
            datalist.appendChild(option);
        });
    }

    highlightText(text) {
        if (!text || !this.currentFilters.search) return text;
        
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
            const allStudents = this.facultyData.reduce((sum, faculty) => {
                return sum + (faculty.students ? faculty.students.split(',').length : 0);
            }, 0);
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
                    <td colspan="3" class="text-center text-danger">
                        <i class="fas fa-exclamation-triangle me-2"></i>
                        ${message}
                    </td>
                </tr>
            `;
        }
    }
}

// Initialize the table when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new FacultyTable();
}); 