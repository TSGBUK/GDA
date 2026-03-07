// Sort table
    function sortTable(tableId, columnIndex) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const header = table.querySelectorAll('thead th')[columnIndex];

        const isAscending = !header.classList.contains('sort-asc');

        // Remove sort classes from all headers
        table.querySelectorAll('thead th').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
        });

        // Add appropriate sort class
        header.classList.add(isAscending ? 'sort-asc' : 'sort-desc');

        rows.sort((a, b) => {
            const aValue = a.cells[columnIndex].textContent.trim();
            const bValue = b.cells[columnIndex].textContent.trim();

            // Try to parse as number
            const aNum = parseFloat(aValue.replace(/[^0-9.-]/g, ''));
            const bNum = parseFloat(bValue.replace(/[^0-9.-]/g, ''));

            if (!isNaN(aNum) && !isNaN(bNum)) {
                return isAscending ? aNum - bNum : bNum - aNum;
            }

            // Compare as strings
            return isAscending
                ? aValue.localeCompare(bValue)
                : bValue.localeCompare(aValue);
        });

        rows.forEach(row => tbody.appendChild(row));
    }

    // Search table
    function searchTable(inputId, tableId) {
        const input = document.getElementById(inputId);
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = tbody.querySelectorAll('tr');
        const searchTerm = input.value.toLowerCase();

        rows.forEach(row => {
            const text = row.textContent.toLowerCase();
            if (text.includes(searchTerm)) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
            }
        });
    }

    // Paginate table
    let currentPages = {};

    function paginateTable(tableId, rowsPerPage) {
        if (!currentPages[tableId]) {
            currentPages[tableId] = 1;
        }

        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const totalPages = Math.ceil(rows.length / rowsPerPage);
        const start = (currentPages[tableId] - 1) * rowsPerPage;
        const end = start + rowsPerPage;

        rows.forEach((row, index) => {
            if (index >= start && index < end) {
                row.style.display = '';
            } else {
                row.style.display = 'none';
            }
        });

        // Update page info
        const pageInfo = document.getElementById(tableId + '-page-info');
        if (pageInfo) {
            pageInfo.textContent = `Page ${currentPages[tableId]} of ${totalPages}`;
        }

        // Update buttons
        const prevBtn = document.getElementById(tableId + '-prev');
        const nextBtn = document.getElementById(tableId + '-next');
        if (prevBtn) prevBtn.disabled = currentPages[tableId] === 1;
        if (nextBtn) nextBtn.disabled = currentPages[tableId] === totalPages;
    }

    function prevPage(tableId, rowsPerPage) {
        if (currentPages[tableId] > 1) {
            currentPages[tableId]--;
            paginateTable(tableId, rowsPerPage);
        }
    }

    function nextPage(tableId, rowsPerPage) {
        const table = document.getElementById(tableId);
        const tbody = table.querySelector('tbody');
        const rows = tbody.querySelectorAll('tr');
        const totalPages = Math.ceil(rows.length / rowsPerPage);

        if (currentPages[tableId] < totalPages) {
            currentPages[tableId]++;
            paginateTable(tableId, rowsPerPage);
        }
    }

    // Initialize on page load
    document.addEventListener('DOMContentLoaded', function() {
        // Initialize pagination
        paginateTable('paginatedTable', 5);
    });