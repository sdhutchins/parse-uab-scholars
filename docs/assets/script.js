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
        this.selectedFaculty = new Set();
        this.allResearchAreas = [];
        this.showingSelectedOnly = false;
        this.originalFilteredData = [];
        this.init();
    }

    async init() {
        try {
            await this.loadData();
            this.setupEventListeners();
            this.populateResearchAreas();
            this.renderTable();
            this.updateStats();
            
            // Initialize Bootstrap tooltips
            const tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
            tooltipTriggerList.map(function (tooltipTriggerEl) {
                return new bootstrap.Tooltip(tooltipTriggerEl);
            });
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
        // Search input with autocomplete
        const searchInput = document.getElementById('searchInput');
        const searchSuggestions = document.getElementById('searchSuggestions');
        
        if (searchInput) {
            searchInput.addEventListener('input', (e) => {
                this.currentFilters.search = e.target.value;
                this.currentPage = 1;
                this.filterData();
                this.showSearchSuggestions(e.target.value);
            });

            // Keyboard navigation
            searchInput.addEventListener('keydown', (e) => {
                const suggestions = searchSuggestions.querySelectorAll('.suggestion-item');
                const currentIndex = Array.from(suggestions).findIndex(item => item.classList.contains('selected'));
                
                switch(e.key) {
                    case 'ArrowDown':
                        e.preventDefault();
                        this.navigateSuggestions(currentIndex, 1, suggestions);
                        break;
                    case 'ArrowUp':
                        e.preventDefault();
                        this.navigateSuggestions(currentIndex, -1, suggestions);
                        break;
                    case 'Enter':
                        e.preventDefault();
                        if (currentIndex >= 0 && suggestions[currentIndex]) {
                            const selectedText = suggestions[currentIndex].getAttribute('data-value');
                            searchInput.value = selectedText;
                            this.currentFilters.search = selectedText;
                            this.currentPage = 1;
                            this.filterData();
                            this.hideSearchSuggestions();
                        }
                        break;
                    case 'Escape':
                        this.hideSearchSuggestions();
                        break;
                }
            });

            // Hide suggestions when clicking outside
            document.addEventListener('click', (e) => {
                if (!searchInput.contains(e.target) && !searchSuggestions.contains(e.target)) {
                    this.hideSearchSuggestions();
                }
            });

            // Handle suggestion clicks
            searchSuggestions.addEventListener('click', (e) => {
                if (e.target.classList.contains('suggestion-item')) {
                    const selectedText = e.target.getAttribute('data-value');
                    searchInput.value = selectedText;
                    this.currentFilters.search = selectedText;
                    this.currentPage = 1;
                    this.filterData();
                    this.hideSearchSuggestions();
                }
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

        // Select All button
        const selectAllBtn = document.getElementById('selectAll');
        if (selectAllBtn) {
            selectAllBtn.addEventListener('click', () => {
                this.selectAllFaculty();
            });
        }

        // Deselect All button
        const deselectAllBtn = document.getElementById('deselectAll');
        if (deselectAllBtn) {
            deselectAllBtn.addEventListener('click', () => {
                this.deselectAllFaculty();
            });
        }

        // Show Selected button
        const showSelectedBtn = document.getElementById('showSelected');
        if (showSelectedBtn) {
            showSelectedBtn.addEventListener('click', () => {
                this.showSelectedFaculty();
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

    showSearchSuggestions(query) {
        const suggestions = document.getElementById('searchSuggestions');
        if (!suggestions || !query || query.length < 2) {
            this.hideSearchSuggestions();
            return;
        }

        const allSuggestions = new Set();
        const lowerQuery = query.toLowerCase();

        // Collect suggestions from faculty names, research areas, and students
        this.facultyData.forEach(faculty => {
            // Faculty names
            if (faculty.userName.toLowerCase().includes(lowerQuery)) {
                allSuggestions.add(faculty.userName);
            }

            // Research areas
            if (faculty.researchAreas) {
                faculty.researchAreas.split(', ').forEach(area => {
                    if (area.toLowerCase().includes(lowerQuery)) {
                        allSuggestions.add(area.trim());
                    }
                });
            }

            // Students (ignore middle names in search)
            if (faculty.students) {
                faculty.students.split(', ').forEach(student => {
                    const studentTrimmed = student.trim();
                    if (!studentTrimmed) return; // Skip empty strings
                    
                    // Create search-friendly version without middle names
                    const studentWords = studentTrimmed.split(' ').filter(word => word.trim());
                    if (studentWords.length >= 2) {
                        // Use first and last name for search
                        const searchFriendlyName = `${studentWords[0]} ${studentWords[studentWords.length - 1]}`;
                        if (searchFriendlyName.toLowerCase().includes(lowerQuery)) {
                            allSuggestions.add(studentTrimmed);
                        }
                    } else if (studentWords.length === 1) {
                        // Single name
                        if (studentWords[0].toLowerCase().includes(lowerQuery)) {
                            allSuggestions.add(studentTrimmed);
                        }
                    }
                });
            }
        });

        // Convert to array and limit to 10 suggestions
        const suggestionsList = Array.from(allSuggestions).slice(0, 10);

        if (suggestionsList.length > 0) {
            suggestions.innerHTML = suggestionsList.map(suggestion => 
                `<div class="suggestion-item p-2 border-bottom" style="cursor: pointer; hover: background-color: #f8f9fa;" data-value="${suggestion}">
                    <i class="fas fa-search me-2 text-muted"></i>${suggestion}
                </div>`
            ).join('');
            suggestions.style.display = 'block';
        } else {
            this.hideSearchSuggestions();
        }
    }

    hideSearchSuggestions() {
        const suggestions = document.getElementById('searchSuggestions');
        if (suggestions) {
            suggestions.style.display = 'none';
            // Remove any selected state
            suggestions.querySelectorAll('.suggestion-item').forEach(item => {
                item.classList.remove('selected');
            });
        }
    }

    navigateSuggestions(currentIndex, direction, suggestions) {
        const maxIndex = suggestions.length - 1;
        let newIndex;
        
        if (currentIndex === -1) {
            // No current selection
            newIndex = direction > 0 ? 0 : maxIndex;
        } else {
            newIndex = currentIndex + direction;
            if (newIndex < 0) newIndex = maxIndex;
            if (newIndex > maxIndex) newIndex = 0;
        }
        
        // Remove previous selection
        suggestions.forEach(item => item.classList.remove('selected'));
        
        // Add selection to new item
        if (suggestions[newIndex]) {
            suggestions[newIndex].classList.add('selected');
            suggestions[newIndex].scrollIntoView({ block: 'nearest' });
        }
    }

    filterData() {
        let filtered = [...this.facultyData];
        const searchTerm = this.currentFilters.search.toLowerCase();
        const researchAreaFilter = this.currentFilters.researchArea.toLowerCase();

        if (searchTerm) {
            filtered = filtered.filter(faculty => {
                // Check faculty name
                if (faculty.userName.toLowerCase().includes(searchTerm)) return true;
                
                // Check research areas
                if (faculty.researchAreas.toLowerCase().includes(searchTerm)) return true;
                
                // Check students (ignore middle names)
                if (faculty.students) {
                    const students = faculty.students.split(', ');
                    for (const student of students) {
                        const studentTrimmed = student.trim();
                        if (!studentTrimmed) continue; // Skip empty strings
                        
                        // Check if the search term matches the full student name
                        if (studentTrimmed.toLowerCase().includes(searchTerm)) return true;
                        
                        // Also check search-friendly version (first + last name)
                        const studentWords = studentTrimmed.split(' ').filter(word => word.trim());
                        if (studentWords.length >= 2) {
                            const searchFriendlyName = `${studentWords[0]} ${studentWords[studentWords.length - 1]}`;
                            if (searchFriendlyName.toLowerCase().includes(searchTerm)) return true;
                        } else if (studentWords.length === 1) {
                            if (studentWords[0].toLowerCase().includes(searchTerm)) return true;
                        }
                    }
                }
                
                // Check email
                if (faculty.email && faculty.email.toLowerCase().includes(searchTerm)) return true;
                
                // Check hidden keywords
                if (faculty.searchKeywords && faculty.searchKeywords.toLowerCase().includes(searchTerm)) return true;
                
                return false;
            });
        }

        if (researchAreaFilter) {
            filtered = filtered.filter(faculty => {
                // Check if the typed research area matches any of the faculty's research areas
                let matches = false;
                
                // Check visible research areas
                if (faculty.researchAreas) {
                    const facultyAreas = faculty.researchAreas.toLowerCase().split(', ');
                    matches = facultyAreas.some(area => area.includes(researchAreaFilter));
                }
                
                // Also check hidden keywords if no match found in visible areas
                if (!matches && faculty.searchKeywords) {
                    matches = faculty.searchKeywords.toLowerCase().includes(researchAreaFilter);
                }
                
                return matches;
            });
        }

        this.filteredData = filtered;
        this.originalFilteredData = [...filtered]; // Keep a backup of the filtered data
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
                    <td colspan="5" class="no-results">
                        <i class="fas fa-search me-2"></i>
                        No faculty found matching your criteria.
                    </td>
                </tr>
            `;
            return;
        }

        tbody.innerHTML = pageData.map(faculty => this.renderFacultyRow(faculty)).join('');
        this.updatePagination();
        
        // Add event listeners for checkboxes
        const checkboxes = tbody.querySelectorAll('.faculty-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.addEventListener('change', (e) => {
                const discoveryId = e.target.getAttribute('data-discovery-id');
                if (e.target.checked) {
                    this.selectedFaculty.add(discoveryId);
                } else {
                    this.selectedFaculty.delete(discoveryId);
                }
            });
        });
    }

    renderFacultyRow(faculty) {
        let facultyNameHtml = this.highlightText(faculty.userName);
        if (faculty.scholarsUrl) {
            facultyNameHtml = `<a href="${faculty.scholarsUrl}" target="_blank" class="faculty-link">${facultyNameHtml} <i class="fas fa-external-link-alt"></i></a>`;
        }

        // Create email link if email exists
        let emailHtml = '';
        if (faculty.email) {
            emailHtml = `<a href="mailto:${faculty.email}" class="email-link">${this.highlightText(faculty.email)}</a>`;
        } else {
            emailHtml = '<span class="text-muted">No email</span>';
        }

        // Highlight current students
        let studentsHtml = this.highlightText(faculty.students);
        if (faculty.currentStudents && faculty.currentStudents.length > 0) {
            console.log('Current students for', faculty.userName, ':', faculty.currentStudents);
            // Highlight current students with green color
            faculty.currentStudents.forEach(currentStudent => {
                const regex = new RegExp(`(${currentStudent.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'g');
                studentsHtml = studentsHtml.replace(regex, '<span class="current-student">$1</span>');
            });
        }

                    return `
                <tr>
                    <td class="text-center">
                        <input type="checkbox" class="faculty-checkbox" data-discovery-id="${faculty.discoveryId}" ${this.selectedFaculty.has(faculty.discoveryId) ? 'checked' : ''}>
                    </td>
                    <td class="faculty-name">${facultyNameHtml}</td>
                    <td class="email">${emailHtml}</td>
                    <td class="research-areas">${this.highlightText(faculty.researchAreas)}</td>
                    <td class="students">${studentsHtml}</td>
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

    selectAllFaculty() {
        const checkboxes = document.querySelectorAll('.faculty-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.checked = true;
            this.selectedFaculty.add(checkbox.getAttribute('data-discovery-id'));
        });
    }

    deselectAllFaculty() {
        const checkboxes = document.querySelectorAll('.faculty-checkbox');
        checkboxes.forEach(checkbox => {
            checkbox.checked = false;
            this.selectedFaculty.delete(checkbox.getAttribute('data-discovery-id'));
        });
    }

    showSelectedFaculty() {
        if (this.selectedFaculty.size === 0) {
            alert('No faculty selected. Please select faculty members first.');
            return;
        }

        if (this.showingSelectedOnly) {
            // Switch back to showing all filtered results
            this.showingSelectedOnly = false;
            this.filteredData = [...this.originalFilteredData];
            document.getElementById('showSelected').innerHTML = '<i class="fas fa-eye me-1"></i>Show Selected';
            document.getElementById('showSelected').classList.remove('btn-warning');
            document.getElementById('showSelected').classList.add('btn-info');
        } else {
            // Switch to showing only selected faculty
            this.showingSelectedOnly = true;
            this.originalFilteredData = [...this.filteredData];
            this.filteredData = this.filteredData.filter(faculty => 
                this.selectedFaculty.has(faculty.discoveryId)
            );
            document.getElementById('showSelected').innerHTML = '<i class="fas fa-list me-1"></i>Show All';
            document.getElementById('showSelected').classList.remove('btn-info');
            document.getElementById('showSelected').classList.add('btn-warning');
        }

        this.currentPage = 1;
        this.renderTable();
        this.updateStats();
    }

    downloadCSV() {
        // Use selected faculty if any are selected, otherwise use filtered data
        const data = this.selectedFaculty.size > 0 
            ? this.filteredData.filter(faculty => this.selectedFaculty.has(faculty.discoveryId))
            : this.filteredData;
            
        const headers = ['Faculty Name', 'Email', 'Research Areas', 'Students', 'Profile URL'];
        const csvContent = [
            headers.join(','),
            ...data.map(faculty => [
                `"${faculty.userName || ''}"`,
                `"${faculty.email || ''}"`,
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
                    <td colspan="5" class="text-center text-danger">
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