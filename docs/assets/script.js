// UAB Faculty Mentoring Table JavaScript

class FacultyTable {
    constructor() {
        this.data = [];
        this.filteredData = [];
        this.currentPage = 1;
        this.pageSize = 20;
        this.currentFilters = {
            search: '',
            researchArea: ''
        };
    }

    async init() {
        try {
            await this.loadData();
            this.setupEventListeners();
            this.populateResearchAreas();
            this.renderTable();
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
        this.data = await response.json();
        this.filteredData = [...this.data];
    }

    setupEventListeners() {
        // Search input
        document.getElementById('searchInput').addEventListener('input', (e) => {
            this.currentFilters.search = e.target.value.toLowerCase();
            this.currentPage = 1;
            this.filterData();
        });

        // Research area filter
        document.getElementById('researchFilter').addEventListener('input', (e) => {
            this.currentFilters.researchArea = e.target.value.toLowerCase();
            this.currentPage = 1;
            this.filterData();
        });

        // Clear filters
        document.getElementById('clearFilters').addEventListener('click', () => {
            this.clearFilters();
        });

        // Download CSV
        document.getElementById('downloadCSV').addEventListener('click', () => {
            this.downloadCSV();
        });

        // Pagination
        document.getElementById('prevPage').addEventListener('click', () => {
            if (this.currentPage > 1) {
                this.currentPage--;
                this.renderTable();
            }
        });

        document.getElementById('nextPage').addEventListener('click', () => {
            const maxPage = Math.ceil(this.filteredData.length / this.pageSize);
            if (this.currentPage < maxPage) {
                this.currentPage++;
                this.renderTable();
            }
        });
    }

    populateResearchAreas() {
        const researchAreas = new Set();
        this.data.forEach(faculty => {
            if (faculty.researchAreas) {
                faculty.researchAreas.split(', ').forEach(area => {
                    if (area.trim()) researchAreas.add(area.trim());
                });
            }
        });

        const datalist = document.getElementById('researchAreasList');
        datalist.innerHTML = '';
        Array.from(researchAreas).sort().forEach(area => {
            const option = document.createElement('option');
            option.value = area;
            datalist.appendChild(option);
        });
    }

    filterData() {
        this.filteredData = this.data.filter(faculty => {
            const matchesSearch = !this.currentFilters.search || 
                faculty.userName.toLowerCase().includes(this.currentFilters.search) ||
                (faculty.researchAreas && faculty.researchAreas.toLowerCase().includes(this.currentFilters.search)) ||
                (faculty.students && faculty.students.toLowerCase().includes(this.currentFilters.search));

            const matchesResearchArea = !this.currentFilters.researchArea ||
                (faculty.researchAreas && faculty.researchAreas.toLowerCase().includes(this.currentFilters.researchArea));

            return matchesSearch && matchesResearchArea;
        });

        this.renderTable();
    }

    clearFilters() {
        document.getElementById('searchInput').value = '';
        document.getElementById('researchFilter').value = '';
        this.currentFilters = { search: '', researchArea: '' };
        this.currentPage = 1;
        this.filterData();
    }

    renderTable() {
        const startIndex = (this.currentPage - 1) * this.pageSize;
        const endIndex = startIndex + this.pageSize;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        const tbody = document.getElementById('facultyTableBody');
        tbody.innerHTML = '';

        if (pageData.length === 0) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="4" class="text-center text-muted">
                        <i class="fas fa-search"></i> No faculty found matching your criteria
                    </td>
                </tr>
            `;
        } else {
            pageData.forEach(faculty => {
                tbody.appendChild(this.renderFacultyRow(faculty));
            });
        }

        this.updatePagination();
        this.updateStats();
    }

    renderFacultyRow(faculty) {
        const row = document.createElement('tr');
        
        const scholarsUrl = faculty.scholarsUrl || `https://scholars.uab.edu/${faculty.discoveryUrlId}`;
        
        row.innerHTML = `
            <td>
                <strong>${this.highlightText(faculty.userName)}</strong>
                ${faculty.email ? `<br><small class="text-muted">${faculty.email}</small>` : ''}
            </td>
            <td>${faculty.researchAreas ? this.highlightText(faculty.researchAreas) : '<em class="text-muted">Not specified</em>'}</td>
            <td>${faculty.students ? this.highlightText(faculty.students) : '<em class="text-muted">No students</em>'}</td>
            <td>
                <a href="${scholarsUrl}" target="_blank" class="btn btn-outline-primary btn-sm">
                    <i class="fas fa-external-link-alt"></i> Profile
                </a>
            </td>
        `;
        
        return row;
    }

    highlightText(text) {
        if (!this.currentFilters.search) return text;
        
        const regex = new RegExp(`(${this.currentFilters.search})`, 'gi');
        return text.replace(regex, '<mark>$1</mark>');
    }

    updatePagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.pageSize);
        const startIndex = (this.currentPage - 1) * this.pageSize + 1;
        const endIndex = Math.min(this.currentPage * this.pageSize, this.filteredData.length);

        // Update page info
        document.getElementById('pageInfo').textContent = `Page ${this.currentPage} of ${totalPages}`;
        document.getElementById('showingStart').textContent = startIndex;
        document.getElementById('showingEnd').textContent = endIndex;
        document.getElementById('totalResults').textContent = this.filteredData.length;

        // Update navigation buttons
        document.getElementById('prevPage').disabled = this.currentPage <= 1;
        document.getElementById('nextPage').disabled = this.currentPage >= totalPages;
    }

    updateStats() {
        // Update page size buttons
        document.querySelectorAll('.btn-group .btn').forEach(btn => {
            btn.classList.remove('active');
        });
        document.querySelector(`[onclick="changePageSize(${this.pageSize})"]`).classList.add('active');
    }

    downloadCSV() {
        const headers = ['Faculty Name', 'Email', 'Research Areas', 'Students', 'Profile URL'];
        const csvContent = [
            headers.join(','),
            ...this.filteredData.map(faculty => [
                `"${faculty.userName || ''}"`,
                `"${faculty.email || ''}"`,
                `"${faculty.researchAreas || ''}"`,
                `"${faculty.students || ''}"`,
                `"${faculty.scholarsUrl || `https://scholars.uab.edu/${faculty.discoveryUrlId}`}"`
            ].join(','))
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', `uab_faculty_committee_memberships_${new Date().toISOString().split('T')[0]}.csv`);
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    showError(message) {
        const tbody = document.getElementById('facultyTableBody');
        tbody.innerHTML = `
            <tr>
                <td colspan="4" class="text-center text-danger">
                    <i class="fas fa-exclamation-triangle"></i> ${message}
                </td>
            </tr>
        `;
    }
}

// Global function for page size changes
function changePageSize(size) {
    if (window.facultyTable) {
        window.facultyTable.pageSize = size;
        window.facultyTable.currentPage = 1;
        window.facultyTable.renderTable();
    }
}

// Initialize the table when the page loads
document.addEventListener('DOMContentLoaded', () => {
    window.facultyTable = new FacultyTable();
    window.facultyTable.init();
}); 