// UAB Faculty Mentoring Table JavaScript

class FacultyTable {
    constructor() {
        this.facultyData = [];
        this.filteredData = [];
        this.currentPage = 1;
        this.pageSize = 20;
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
                this.currentPage = 1;
                this.filterData();
            });
        }

        // Research area filter
        const researchFilter = document.getElementById('researchFilter');
        if (researchFilter) {
            researchFilter.addEventListener('input', (e) => {
                this.currentFilters.researchArea = e.target.value;
                this.currentPage = 1;
                this.filterData();
                this.updateResearchAreasDropdown(e.target.value);
            });

            // Show dropdown on focus
            researchFilter.addEventListener('focus', () => {
                this.showResearchAreasDropdown();
            });

            // Hide dropdown when clicking outside
            document.addEventListener('click', (e) => {
                if (!researchFilter.contains(e.target) && !document.getElementById('researchAreasDropdown').contains(e.target)) {
                    this.hideResearchAreasDropdown();
                }
            });
        }

        // Clear filters button
        const clearFiltersBtn = document.getElementById('clearFilters');
        if (clearFiltersBtn) {
            clearFiltersBtn.addEventListener('click', () => {
                this.clearFilters();
            });
        }

        // Download CSV button
        const downloadCSVBtn = document.getElementById('downloadCSV');
        if (downloadCSVBtn) {
            downloadCSVBtn.addEventListener('click', () => {
                this.downloadCSV();
            });
        }

        // Pagination
        const prevPageBtn = document.getElementById('prevPage');
        if (prevPageBtn) {
            prevPageBtn.addEventListener('click', () => {
                if (this.currentPage > 1) {
                    this.currentPage--;
                    this.renderTable();
                }
            });
        }

        const nextPageBtn = document.getElementById('nextPage');
        if (nextPageBtn) {
            nextPageBtn.addEventListener('click', () => {
                const maxPage = Math.ceil(this.filteredData.length / this.pageSize);
                if (this.currentPage < maxPage) {
                    this.currentPage++;
                    this.renderTable();
                }
            });
        }
    }

    populateResearchAreas() {
        const dropdown = document.getElementById('researchAreasDropdown');
        if (!dropdown) return;

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

        // Store all areas for filtering
        this.allResearchAreas = Array.from(allResearchAreas).sort();
        
        // Populate dropdown with all areas
        this.updateResearchAreasDropdown('');
    }

    updateResearchAreasDropdown(filterText = '') {
        const dropdown = document.getElementById('researchAreasDropdown');
        if (!dropdown || !this.allResearchAreas) return;

        const filteredAreas = this.allResearchAreas.filter(area => 
            area.toLowerCase().includes(filterText.toLowerCase())
        );

        dropdown.innerHTML = filteredAreas.map(area => `
            <a class="dropdown-item" href="#" data-value="${area}">${area}</a>
        `).join('');

        // Add click handlers for dropdown items
        dropdown.querySelectorAll('.dropdown-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                const researchFilter = document.getElementById('researchFilter');
                if (researchFilter) {
                    researchFilter.value = item.dataset.value;
                    this.currentFilters.researchArea = item.dataset.value;
                    this.currentPage = 1;
                    this.filterData();
                    this.hideResearchAreasDropdown();
                }
            });
        });
    }

    showResearchAreasDropdown() {
        const dropdown = document.getElementById('researchAreasDropdown');
        if (dropdown) {
            dropdown.classList.add('show');
        }
    }

    hideResearchAreasDropdown() {
        const dropdown = document.getElementById('researchAreasDropdown');
        if (dropdown) {
            dropdown.classList.remove('show');
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
                // Check if the typed research area matches any of the faculty's research areas
                if (!faculty.researchAreas) return false;
                const facultyAreas = faculty.researchAreas.toLowerCase().split(', ');
                return facultyAreas.some(area => area.includes(researchAreaFilter));
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
        this.currentPage = 1;
        this.filteredData = [...this.facultyData];
        this.renderTable();
        this.updateStats();
    }

    renderTable() {
        const tbody = document.querySelector('#facultyTable tbody');
        if (!tbody) return;

        const startIndex = (this.currentPage - 1) * this.pageSize;
        const endIndex = startIndex + this.pageSize;
        const pageData = this.filteredData.slice(startIndex, endIndex);

        if (pageData.length === 0) {
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

        tbody.innerHTML = pageData.map(faculty => this.renderFacultyRow(faculty)).join('');
        this.updatePagination();
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

    highlightText(text) {
        if (!text || !this.currentFilters.search) return text;
        
        const searchTerm = this.currentFilters.search;
        const regex = new RegExp(`(${searchTerm})`, 'gi');
        return text.replace(regex, '<span class="highlight">$1</span>');
    }

    updatePagination() {
        const totalPages = Math.ceil(this.filteredData.length / this.pageSize);
        const startIndex = (this.currentPage - 1) * this.pageSize + 1;
        const endIndex = Math.min(this.currentPage * this.pageSize, this.filteredData.length);

        // Update page info
        const pageInfo = document.getElementById('pageInfo');
        if (pageInfo) {
            pageInfo.textContent = `Page ${this.currentPage} of ${totalPages}`;
        }

        // Update showing info
        const showingStart = document.getElementById('showingStart');
        const showingEnd = document.getElementById('showingEnd');
        const totalResults = document.getElementById('totalResults');
        
        if (showingStart) showingStart.textContent = startIndex;
        if (showingEnd) showingEnd.textContent = endIndex;
        if (totalResults) totalResults.textContent = this.filteredData.length;

        // Update navigation buttons
        const prevPage = document.getElementById('prevPage');
        const nextPage = document.getElementById('nextPage');
        
        if (prevPage) prevPage.disabled = this.currentPage <= 1;
        if (nextPage) nextPage.disabled = this.currentPage >= totalPages;
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

    downloadCSV() {
        const headers = ['Faculty Name', 'Research Areas', 'Students', 'Profile URL'];
        const csvContent = [
            headers.join(','),
            ...this.filteredData.map(faculty => [
                `"${faculty.userName || ''}"`,
                `"${faculty.researchAreas || ''}"`,
                `"${faculty.students || ''}"`,
                `"${faculty.scholarsUrl || ''}"`
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
}); 