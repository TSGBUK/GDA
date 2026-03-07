// Basic Pagination
    let basicCurrentPage = 1;
    const basicTotalPages = 10;

    function updateBasicPagination() {
        document.getElementById('basicPageInfo').textContent = `Page ${basicCurrentPage} of ${basicTotalPages}`;
        document.getElementById('basicPrevBtn').disabled = basicCurrentPage === 1;
        document.getElementById('basicNextBtn').disabled = basicCurrentPage === basicTotalPages;

        // Update number buttons
        const container = document.getElementById('basicNumberButtons');
        container.innerHTML = '';

        for (let i = 1; i <= basicTotalPages; i++) {
            if (i === 1 || i === basicTotalPages || (i >= basicCurrentPage - 1 && i <= basicCurrentPage + 1)) {
                const btn = document.createElement('button');
                btn.className = `pagination-button px-4 py-2 rounded-lg font-medium ${i === basicCurrentPage ? 'bg-blue-500 text-white active' : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600'}`;
                btn.textContent = i;
                btn.onclick = () => goToBasicPage(i);
                container.appendChild(btn);
            } else if (i === basicCurrentPage - 2 || i === basicCurrentPage + 2) {
                const span = document.createElement('span');
                span.className = 'px-2 text-gray-500';
                span.textContent = '...';
                container.appendChild(span);
            }
        }
    }

    function goToBasicPage(page) {
        basicCurrentPage = page;
        updateBasicPagination();
    }

    function basicPrevPage() {
        if (basicCurrentPage > 1) {
            basicCurrentPage--;
            updateBasicPagination();
        }
    }

    function basicNextPage() {
        if (basicCurrentPage < basicTotalPages) {
            basicCurrentPage++;
            updateBasicPagination();
        }
    }

    // Rounded Pagination
    let roundedCurrentPage = 1;
    const roundedTotalPages = 5;

    function updateRoundedPagination() {
        document.getElementById('roundedPageInfo').textContent = `Page ${roundedCurrentPage} of ${roundedTotalPages}`;
        document.getElementById('roundedPrevBtn').disabled = roundedCurrentPage === 1;
        document.getElementById('roundedNextBtn').disabled = roundedCurrentPage === roundedTotalPages;

        const container = document.getElementById('roundedNumberButtons');
        container.innerHTML = '';

        for (let i = 1; i <= roundedTotalPages; i++) {
            const btn = document.createElement('button');
            btn.className = `pagination-button px-4 py-2 rounded-full font-medium ${i === roundedCurrentPage ? 'bg-gradient-to-r from-purple-500 to-pink-500 text-white active' : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600'}`;
            btn.textContent = i;
            btn.onclick = () => goToRoundedPage(i);
            container.appendChild(btn);
        }
    }

    function goToRoundedPage(page) {
        roundedCurrentPage = page;
        updateRoundedPagination();
    }

    function roundedPrevPage() {
        if (roundedCurrentPage > 1) {
            roundedCurrentPage--;
            updateRoundedPagination();
        }
    }

    function roundedNextPage() {
        if (roundedCurrentPage < roundedTotalPages) {
            roundedCurrentPage++;
            updateRoundedPagination();
        }
    }

    // Compact Pagination
    let compactCurrentPage = 1;
    const compactTotalPages = 20;

    function updateCompactPagination() {
        document.getElementById('compactPageInput').value = compactCurrentPage;
        document.getElementById('compactTotalPages').textContent = compactTotalPages;
        document.getElementById('compactPrevBtn').disabled = compactCurrentPage === 1;
        document.getElementById('compactNextBtn').disabled = compactCurrentPage === compactTotalPages;
    }

    function goToCompactPage() {
        const input = document.getElementById('compactPageInput');
        const page = parseInt(input.value);
        if (page >= 1 && page <= compactTotalPages) {
            compactCurrentPage = page;
            updateCompactPagination();
        } else {
            input.value = compactCurrentPage;
        }
    }

    function compactPrevPage() {
        if (compactCurrentPage > 1) {
            compactCurrentPage--;
            updateCompactPagination();
        }
    }

    function compactNextPage() {
        if (compactCurrentPage < compactTotalPages) {
            compactCurrentPage++;
            updateCompactPagination();
        }
    }

    // Bordered Pagination
    let borderedCurrentPage = 1;
    const borderedTotalPages = 8;

    function updateBorderedPagination() {
        const container = document.getElementById('borderedButtons');
        container.innerHTML = '';

        for (let i = 1; i <= borderedTotalPages; i++) {
            const btn = document.createElement('button');
            btn.className = `pagination-button px-4 py-2 border-2 font-medium ${i === borderedCurrentPage ? 'border-green-500 bg-green-50 dark:bg-green-900/20 text-green-600 dark:text-green-400 active' : 'border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:border-green-300 hover:bg-green-50 dark:hover:bg-green-900/10'}`;
            btn.textContent = i;
            btn.onclick = () => goToBorderedPage(i);
            container.appendChild(btn);
        }
    }

    function goToBorderedPage(page) {
        borderedCurrentPage = page;
        updateBorderedPagination();
    }

    // Icon Pagination
    let iconCurrentPage = 1;
    const iconTotalPages = 12;

    function updateIconPagination() {
        document.getElementById('iconPageInfo').textContent = `${iconCurrentPage} / ${iconTotalPages}`;
        document.getElementById('iconPrevBtn').disabled = iconCurrentPage === 1;
        document.getElementById('iconNextBtn').disabled = iconCurrentPage === iconTotalPages;
        document.getElementById('iconFirstBtn').disabled = iconCurrentPage === 1;
        document.getElementById('iconLastBtn').disabled = iconCurrentPage === iconTotalPages;
    }

    function iconFirstPage() {
        iconCurrentPage = 1;
        updateIconPagination();
    }

    function iconPrevPage() {
        if (iconCurrentPage > 1) {
            iconCurrentPage--;
            updateIconPagination();
        }
    }

    function iconNextPage() {
        if (iconCurrentPage < iconTotalPages) {
            iconCurrentPage++;
            updateIconPagination();
        }
    }

    function iconLastPage() {
        iconCurrentPage = iconTotalPages;
        updateIconPagination();
    }

    // Size Pagination
    let sizeCurrentPage = 1;
    const sizeTotalPages = 15;

    function updateSizePagination() {
        document.getElementById('sizePageInfo').textContent = `Page ${sizeCurrentPage} of ${sizeTotalPages}`;
        document.getElementById('sizePrevBtn').disabled = sizeCurrentPage === 1;
        document.getElementById('sizeNextBtn').disabled = sizeCurrentPage === sizeTotalPages;
    }

    function sizePrevPage() {
        if (sizeCurrentPage > 1) {
            sizeCurrentPage--;
            updateSizePagination();
        }
    }

    function sizeNextPage() {
        if (sizeCurrentPage < sizeTotalPages) {
            sizeCurrentPage++;
            updateSizePagination();
        }
    }

    // Table Pagination
    let tableCurrentPage = 1;
    const tableRowsPerPage = 5;
    const tableData = [
        { id: 1, name: 'John Doe', email: 'john@example.com', role: 'Admin', status: 'Active' },
        { id: 2, name: 'Jane Smith', email: 'jane@example.com', role: 'User', status: 'Active' },
        { id: 3, name: 'Bob Johnson', email: 'bob@example.com', role: 'User', status: 'Inactive' },
        { id: 4, name: 'Alice Brown', email: 'alice@example.com', role: 'Manager', status: 'Active' },
        { id: 5, name: 'Charlie Wilson', email: 'charlie@example.com', role: 'User', status: 'Active' },
        { id: 6, name: 'Diana Davis', email: 'diana@example.com', role: 'User', status: 'Active' },
        { id: 7, name: 'Edward Miller', email: 'edward@example.com', role: 'Admin', status: 'Active' },
        { id: 8, name: 'Fiona Garcia', email: 'fiona@example.com', role: 'User', status: 'Inactive' },
        { id: 9, name: 'George Martinez', email: 'george@example.com', role: 'Manager', status: 'Active' },
        { id: 10, name: 'Hannah Rodriguez', email: 'hannah@example.com', role: 'User', status: 'Active' },
        { id: 11, name: 'Ian Lopez', email: 'ian@example.com', role: 'User', status: 'Active' },
        { id: 12, name: 'Julia Hernandez', email: 'julia@example.com', role: 'Admin', status: 'Active' },
        { id: 13, name: 'Kevin Moore', email: 'kevin@example.com', role: 'User', status: 'Inactive' },
        { id: 14, name: 'Laura Taylor', email: 'laura@example.com', role: 'Manager', status: 'Active' },
        { id: 15, name: 'Michael Anderson', email: 'michael@example.com', role: 'User', status: 'Active' }
    ];
    const tableTotalPages = Math.ceil(tableData.length / tableRowsPerPage);

    function updateTablePagination() {
        const start = (tableCurrentPage - 1) * tableRowsPerPage;
        const end = start + tableRowsPerPage;
        const pageData = tableData.slice(start, end);

        const tbody = document.getElementById('tableBody');
        tbody.innerHTML = '';

        pageData.forEach((row, index) => {
            const tr = document.createElement('tr');
            tr.className = 'fade-in border-b border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50';
            tr.style.animationDelay = `${index * 0.05}s`;
            tr.innerHTML = `
                <td class="px-6 py-4 text-sm text-gray-900 dark:text-gray-100">${row.id}</td>
                <td class="px-6 py-4 text-sm font-medium text-gray-900 dark:text-gray-100">${row.name}</td>
                <td class="px-6 py-4 text-sm text-gray-600 dark:text-gray-400">${row.email}</td>
                <td class="px-6 py-4 text-sm text-gray-900 dark:text-gray-100">${row.role}</td>
                <td class="px-6 py-4">
                    <span class="px-2 py-1 text-xs font-semibold rounded-full ${row.status === 'Active' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400' : 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400'}">
                        ${row.status}
                    </span>
                </td>
            `;
            tbody.appendChild(tr);
        });

        document.getElementById('tablePageInfo').textContent = `Showing ${start + 1} to ${Math.min(end, tableData.length)} of ${tableData.length} entries`;
        document.getElementById('tablePrevBtn').disabled = tableCurrentPage === 1;
        document.getElementById('tableNextBtn').disabled = tableCurrentPage === tableTotalPages;

        // Update page buttons
        const container = document.getElementById('tablePageButtons');
        container.innerHTML = '';

        for (let i = 1; i <= tableTotalPages; i++) {
            const btn = document.createElement('button');
            btn.className = `pagination-button px-3 py-1 rounded text-sm font-medium ${i === tableCurrentPage ? 'bg-gradient-to-r from-orange-500 to-red-500 text-white active' : 'bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-600'}`;
            btn.textContent = i;
            btn.onclick = () => goToTablePage(i);
            container.appendChild(btn);
        }
    }

    function goToTablePage(page) {
        tableCurrentPage = page;
        updateTablePagination();
    }

    function tablePrevPage() {
        if (tableCurrentPage > 1) {
            tableCurrentPage--;
            updateTablePagination();
        }
    }

    function tableNextPage() {
        if (tableCurrentPage < tableTotalPages) {
            tableCurrentPage++;
            updateTablePagination();
        }
    }

    // Initialize all paginations on page load
    document.addEventListener('DOMContentLoaded', function() {
        updateBasicPagination();
        updateRoundedPagination();
        updateCompactPagination();
        updateBorderedPagination();
        updateIconPagination();
        updateSizePagination();
        updateTablePagination();
    });