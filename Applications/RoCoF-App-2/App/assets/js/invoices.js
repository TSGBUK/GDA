// Invoice Management JavaScript

// Sample invoice data (in a real app, this would come from an API)
let invoicesData = [
    {
        id: 'INV-2024-001',
        project: 'Website Redesign',
        client: 'John Doe',
        clientEmail: 'john.doe@example.com',
        avatar: 'https://ui-avatars.com/api/?name=John+Doe&background=6366f1&color=fff',
        date: '2024-01-15',
        dueDate: '2024-02-15',
        amount: 2500,
        status: 'paid'
    },
    {
        id: 'INV-2024-002',
        project: 'Mobile App Development',
        client: 'Jane Smith',
        clientEmail: 'jane.smith@example.com',
        avatar: 'https://ui-avatars.com/api/?name=Jane+Smith&background=10b981&color=fff',
        date: '2024-01-18',
        dueDate: '2024-02-18',
        amount: 5800,
        status: 'pending'
    },
    {
        id: 'INV-2024-003',
        project: 'SEO Optimization',
        client: 'Bob Johnson',
        clientEmail: 'bob.johnson@example.com',
        avatar: 'https://ui-avatars.com/api/?name=Bob+Johnson&background=f59e0b&color=fff',
        date: '2023-12-28',
        dueDate: '2024-01-28',
        amount: 1200,
        status: 'overdue'
    },
    {
        id: 'INV-2024-004',
        project: 'E-commerce Platform',
        client: 'Alice Williams',
        clientEmail: 'alice.williams@example.com',
        avatar: 'https://ui-avatars.com/api/?name=Alice+Williams&background=8b5cf6&color=fff',
        date: '2024-01-20',
        dueDate: '2024-02-20',
        amount: 8900,
        status: 'pending'
    },
    {
        id: 'INV-2024-005',
        project: 'Brand Identity Design',
        client: 'Charlie Brown',
        clientEmail: 'charlie.brown@example.com',
        avatar: 'https://ui-avatars.com/api/?name=Charlie+Brown&background=ec4899&color=fff',
        date: '2024-01-22',
        dueDate: '2024-02-22',
        amount: 3400,
        status: 'draft'
    }
];

// Filter invoices based on search, status, and date
function filterInvoices() {
    const searchTerm = document.getElementById('invoiceSearch').value.toLowerCase();
    const statusFilter = document.getElementById('statusFilter').value;
    const dateFilter = document.getElementById('dateFilter').value;

    let filtered = invoicesData.filter(invoice => {
        // Search filter
        const matchesSearch =
            invoice.id.toLowerCase().includes(searchTerm) ||
            invoice.project.toLowerCase().includes(searchTerm) ||
            invoice.client.toLowerCase().includes(searchTerm);

        // Status filter
        const matchesStatus = statusFilter === 'all' || invoice.status === statusFilter;

        // Date filter (simplified - in real app would use actual date comparison)
        let matchesDate = true;
        if (dateFilter !== 'all') {
            const invoiceDate = new Date(invoice.date);
            const today = new Date();

            if (dateFilter === 'today') {
                matchesDate = invoiceDate.toDateString() === today.toDateString();
            } else if (dateFilter === 'week') {
                const weekAgo = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
                matchesDate = invoiceDate >= weekAgo;
            } else if (dateFilter === 'month') {
                matchesDate = invoiceDate.getMonth() === today.getMonth();
            } else if (dateFilter === 'year') {
                matchesDate = invoiceDate.getFullYear() === today.getFullYear();
            }
        }

        return matchesSearch && matchesStatus && matchesDate;
    });

    renderInvoiceTable(filtered);
}

// Render invoice table
function renderInvoiceTable(invoices) {
    const tbody = document.getElementById('invoiceTableBody');

    if (invoices.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="7" class="px-6 py-12 text-center text-gray-500 dark:text-gray-400">
                    <i class="fas fa-inbox text-4xl mb-4"></i>
                    <p class="text-lg">No invoices found</p>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = invoices.map(invoice => {
        const statusColors = {
            paid: 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200',
            pending: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200',
            overdue: 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200',
            draft: 'bg-gray-100 text-gray-800 dark:bg-gray-700 dark:text-gray-300'
        };

        return `
            <tr class="invoice-row border-b dark:border-gray-700">
                <td class="px-6 py-4">
                    <input type="checkbox" class="invoice-checkbox w-4 h-4 text-indigo-600 rounded focus:ring-indigo-500"
                           data-invoice-id="${invoice.id}">
                </td>
                <td class="px-6 py-4">
                    <div class="text-sm font-medium text-gray-900 dark:text-white">${invoice.id}</div>
                    <div class="text-sm text-gray-500 dark:text-gray-400">${invoice.project}</div>
                </td>
                <td class="px-6 py-4">
                    <div class="flex items-center">
                        <img src="${invoice.avatar}" alt="${invoice.client}"
                             class="w-8 h-8 rounded-full mr-3">
                        <div>
                            <div class="text-sm font-medium text-gray-900 dark:text-white">${invoice.client}</div>
                            <div class="text-sm text-gray-500 dark:text-gray-400">${invoice.clientEmail}</div>
                        </div>
                    </div>
                </td>
                <td class="px-6 py-4 text-sm text-gray-900 dark:text-white">${invoice.date}</td>
                <td class="px-6 py-4 text-sm text-gray-900 dark:text-white">${invoice.dueDate}</td>
                <td class="px-6 py-4 text-sm font-semibold text-gray-900 dark:text-white">$${invoice.amount.toLocaleString()}</td>
                <td class="px-6 py-4">
                    <span class="status-badge ${statusColors[invoice.status]}">${invoice.status}</span>
                </td>
                <td class="px-6 py-4">
                    <div class="flex items-center gap-2">
                        <button onclick="viewInvoice('${invoice.id}')"
                                class="action-btn p-2 text-indigo-600 hover:bg-indigo-50 dark:hover:bg-indigo-900 rounded"
                                title="View">
                            <i class="fas fa-eye"></i>
                        </button>
                        <button onclick="downloadInvoice('${invoice.id}')"
                                class="action-btn p-2 text-green-600 hover:bg-green-50 dark:hover:bg-green-900 rounded"
                                title="Download">
                            <i class="fas fa-download"></i>
                        </button>
                        <button onclick="editInvoice('${invoice.id}')"
                                class="action-btn p-2 text-blue-600 hover:bg-blue-50 dark:hover:bg-blue-900 rounded"
                                title="Edit">
                            <i class="fas fa-edit"></i>
                        </button>
                        <button onclick="deleteInvoice('${invoice.id}')"
                                class="action-btn p-2 text-red-600 hover:bg-red-50 dark:hover:bg-red-900 rounded"
                                title="Delete">
                            <i class="fas fa-trash"></i>
                        </button>
                        ${invoice.status === 'overdue' ? `
                            <button onclick="sendReminder('${invoice.id}')"
                                    class="action-btn p-2 text-orange-600 hover:bg-orange-50 dark:hover:bg-orange-900 rounded"
                                    title="Send Reminder">
                                <i class="fas fa-bell"></i>
                            </button>
                        ` : ''}
                        ${invoice.status === 'draft' ? `
                            <button onclick="sendInvoice('${invoice.id}')"
                                    class="action-btn p-2 text-purple-600 hover:bg-purple-50 dark:hover:bg-purple-900 rounded"
                                    title="Send Invoice">
                                <i class="fas fa-paper-plane"></i>
                            </button>
                        ` : ''}
                    </div>
                </td>
            </tr>
        `;
    }).join('');
}

// Export invoices to CSV
function exportInvoices() {
    const selectedCheckboxes = document.querySelectorAll('.invoice-checkbox:checked');
    const invoicesToExport = selectedCheckboxes.length > 0
        ? Array.from(selectedCheckboxes).map(cb => cb.dataset.invoiceId)
        : invoicesData.map(inv => inv.id);

    // Create CSV content
    const headers = ['Invoice ID', 'Project', 'Client', 'Email', 'Date', 'Due Date', 'Amount', 'Status'];
    const csvContent = [
        headers.join(','),
        ...invoicesData
            .filter(inv => invoicesToExport.includes(inv.id))
            .map(inv => [
                inv.id,
                `"${inv.project}"`,
                `"${inv.client}"`,
                inv.clientEmail,
                inv.date,
                inv.dueDate,
                inv.amount,
                inv.status
            ].join(','))
    ].join('\n');

    // Download CSV
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `invoices_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    window.URL.revokeObjectURL(url);

    alert(`Exported ${invoicesToExport.length} invoice(s) to CSV`);
}

// Toggle select all checkboxes
function toggleSelectAll() {
    const selectAllCheckbox = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.invoice-checkbox');

    checkboxes.forEach(checkbox => {
        checkbox.checked = selectAllCheckbox.checked;
    });
}

// View invoice details
function viewInvoice(id) {
    const invoice = invoicesData.find(inv => inv.id === id);
    if (invoice) {
        alert(`View Invoice: ${id}\n\nProject: ${invoice.project}\nClient: ${invoice.client}\nAmount: $${invoice.amount}\nStatus: ${invoice.status}\n\n(In a real app, this would open a detailed invoice view)`);
    }
}

// Download invoice as PDF
function downloadInvoice(id) {
    const invoice = invoicesData.find(inv => inv.id === id);
    if (invoice) {
        alert(`Downloading invoice ${id} as PDF...\n\n(In a real app, this would generate and download a PDF)`);
        console.log('Invoice data for PDF:', invoice);
    }
}

// Edit invoice
function editInvoice(id) {
    const invoice = invoicesData.find(inv => inv.id === id);
    if (invoice) {
        openInvoiceModal('edit', invoice);
    }
}

// Delete invoice
function deleteInvoice(id) {
    if (confirm(`Are you sure you want to delete invoice ${id}?`)) {
        invoicesData = invoicesData.filter(inv => inv.id !== id);
        filterInvoices();
        alert(`Invoice ${id} has been deleted successfully!`);
    }
}

// Send payment reminder
function sendReminder(id) {
    const invoice = invoicesData.find(inv => inv.id === id);
    if (invoice) {
        alert(`Sending payment reminder to ${invoice.client} (${invoice.clientEmail})\n\nInvoice: ${id}\nAmount Due: $${invoice.amount}\n\n(In a real app, this would send an email reminder)`);
    }
}

// Send draft invoice to client
function sendInvoice(id) {
    const invoice = invoicesData.find(inv => inv.id === id);
    if (invoice && invoice.status === 'draft') {
        if (confirm(`Send invoice ${id} to ${invoice.client}?`)) {
            // Update status to pending
            invoice.status = 'pending';
            filterInvoices();
            alert(`Invoice ${id} has been sent to ${invoice.clientEmail}`);
        }
    }
}

// Open invoice modal for create/edit
function openInvoiceModal(mode, invoice = null) {
    const modalTitle = mode === 'create' ? 'Create New Invoice' : 'Edit Invoice';
    const submitButtonText = mode === 'create' ? 'Create Invoice' : 'Update Invoice';

    const modalHTML = `
        <div class="modal-backdrop" onclick="closeInvoiceModal()"></div>
        <div class="invoice-modal bg-white dark:bg-gray-800 rounded-lg shadow-2xl p-6 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <div class="flex justify-between items-center mb-4">
                <h3 class="text-xl font-bold text-gray-900 dark:text-white">${modalTitle}</h3>
                <button onclick="closeInvoiceModal()" class="text-gray-400 hover:text-gray-600">
                    <i class="fas fa-times text-xl"></i>
                </button>
            </div>

            <form id="invoiceForm" class="space-y-4">
                <div class="grid grid-cols-2 gap-4">
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Invoice ID
                        </label>
                        <input type="text" name="id" value="${invoice?.id || ''}"
                               class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                               ${mode === 'edit' ? 'readonly' : 'required'}>
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Status
                        </label>
                        <select name="status"
                                class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white">
                            <option value="draft" ${invoice?.status === 'draft' ? 'selected' : ''}>Draft</option>
                            <option value="pending" ${invoice?.status === 'pending' ? 'selected' : ''}>Pending</option>
                            <option value="paid" ${invoice?.status === 'paid' ? 'selected' : ''}>Paid</option>
                            <option value="overdue" ${invoice?.status === 'overdue' ? 'selected' : ''}>Overdue</option>
                        </select>
                    </div>
                </div>

                <div>
                    <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                        Project Name
                    </label>
                    <input type="text" name="project" value="${invoice?.project || ''}"
                           class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                           required>
                </div>

                <div class="grid grid-cols-2 gap-4">
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Client Name
                        </label>
                        <input type="text" name="client" value="${invoice?.client || ''}"
                               class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                               required>
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Client Email
                        </label>
                        <input type="email" name="clientEmail" value="${invoice?.clientEmail || ''}"
                               class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                               required>
                    </div>
                </div>

                <div class="grid grid-cols-3 gap-4">
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Date
                        </label>
                        <input type="date" name="date" value="${invoice?.date || ''}"
                               class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                               required>
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Due Date
                        </label>
                        <input type="date" name="dueDate" value="${invoice?.dueDate || ''}"
                               class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                               required>
                    </div>
                    <div>
                        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
                            Amount ($)
                        </label>
                        <input type="number" name="amount" value="${invoice?.amount || ''}"
                               class="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white"
                               required>
                    </div>
                </div>

                <div class="flex justify-end gap-2 mt-6">
                    <button type="button" onclick="closeInvoiceModal()"
                            class="px-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700">
                        Cancel
                    </button>
                    <button type="submit"
                            class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700">
                        ${submitButtonText}
                    </button>
                </div>
            </form>
        </div>
    `;

    const modalContainer = document.createElement('div');
    modalContainer.id = 'invoiceModalContainer';
    modalContainer.innerHTML = modalHTML;
    document.body.appendChild(modalContainer);

    // Handle form submission
    document.getElementById('invoiceForm').addEventListener('submit', function(e) {
        e.preventDefault();
        const formData = new FormData(e.target);
        const invoiceData = Object.fromEntries(formData.entries());

        if (mode === 'create') {
            // Add new invoice
            invoicesData.push({
                ...invoiceData,
                amount: parseFloat(invoiceData.amount),
                avatar: `https://ui-avatars.com/api/?name=${encodeURIComponent(invoiceData.client)}&background=6366f1&color=fff`
            });
            alert(`Invoice ${invoiceData.id} created successfully!`);
        } else {
            // Update existing invoice
            const index = invoicesData.findIndex(inv => inv.id === invoiceData.id);
            if (index !== -1) {
                invoicesData[index] = {
                    ...invoicesData[index],
                    ...invoiceData,
                    amount: parseFloat(invoiceData.amount)
                };
                alert(`Invoice ${invoiceData.id} updated successfully!`);
            }
        }

        closeInvoiceModal();
        filterInvoices();
    });
}

// Close invoice modal
function closeInvoiceModal() {
    const modalContainer = document.getElementById('invoiceModalContainer');
    if (modalContainer) {
        modalContainer.remove();
    }
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    // Render initial table
    renderInvoiceTable(invoicesData);

    // Add event listeners
    document.getElementById('invoiceSearch')?.addEventListener('input', filterInvoices);
    document.getElementById('statusFilter')?.addEventListener('change', filterInvoices);
    document.getElementById('dateFilter')?.addEventListener('change', filterInvoices);
    document.getElementById('selectAll')?.addEventListener('change', toggleSelectAll);
});
