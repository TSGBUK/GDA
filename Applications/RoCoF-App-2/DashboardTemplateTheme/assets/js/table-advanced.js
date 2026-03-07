// Table Advanced Functions

// Inline Editable Table
function saveTableData(tableId) {
    const table = document.getElementById(tableId);
    const rows = table.querySelectorAll('tbody tr');
    const data = [];

    rows.forEach(row => {
        const cells = row.querySelectorAll('.editable-cell');
        const rowData = Array.from(cells).map(cell => cell.textContent.trim());
        data.push(rowData);
    });

    console.log('Saved data:', data);
    alert('Changes saved successfully!\n\nData has been logged to console.');
}

// Resizable Columns
let isResizing = false;
let currentResizer = null;
let startX = 0;
let startWidth = 0;

document.addEventListener('DOMContentLoaded', function() {
    document.querySelectorAll('.column-resizer').forEach(resizer => {
        resizer.addEventListener('mousedown', function(e) {
            isResizing = true;
            currentResizer = this;
            const th = this.parentElement;
            startX = e.pageX;
            startWidth = th.offsetWidth;

            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);
            e.preventDefault();
        });
    });

    function handleMouseMove(e) {
        if (!isResizing) return;

        const th = currentResizer.parentElement;
        const width = startWidth + (e.pageX - startX);

        if (width > 50) {
            th.style.width = width + 'px';
        }
    }

    function handleMouseUp() {
        isResizing = false;
        currentResizer = null;
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
    }

    // Drag & Drop Reorder
    let draggedRow = null;

    document.querySelectorAll('.draggable-row').forEach(row => {
        row.addEventListener('dragstart', function(e) {
            draggedRow = this;
            this.classList.add('dragging');
            e.dataTransfer.effectAllowed = 'move';
        });

        row.addEventListener('dragend', function() {
            this.classList.remove('dragging');
            document.querySelectorAll('.draggable-row').forEach(r => r.classList.remove('drag-over'));
        });

        row.addEventListener('dragover', function(e) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'move';

            if (draggedRow !== this) {
                this.classList.add('drag-over');
            }
        });

        row.addEventListener('dragleave', function() {
            this.classList.remove('drag-over');
        });

        row.addEventListener('drop', function(e) {
            e.preventDefault();

            if (draggedRow !== this) {
                const tbody = this.parentNode;
                const allRows = Array.from(tbody.querySelectorAll('.draggable-row'));
                const draggedIndex = allRows.indexOf(draggedRow);
                const targetIndex = allRows.indexOf(this);

                if (draggedIndex < targetIndex) {
                    tbody.insertBefore(draggedRow, this.nextSibling);
                } else {
                    tbody.insertBefore(draggedRow, this);
                }
            }

            this.classList.remove('drag-over');
        });
    });
});

// Expandable Row Details
function toggleExpand(expandId) {
    const content = document.getElementById(expandId);
    const iconId = expandId.replace('expand', 'icon');
    const icon = document.getElementById(iconId);

    content.classList.toggle('show');

    if (content.classList.contains('show')) {
        icon.style.transform = 'rotate(90deg)';
    } else {
        icon.style.transform = 'rotate(0deg)';
    }
}

// Multi-Select with Bulk Actions
function toggleSelectAll() {
    const selectAll = document.getElementById('selectAll');
    const checkboxes = document.querySelectorAll('.row-checkbox');

    checkboxes.forEach(checkbox => {
        checkbox.checked = selectAll.checked;
    });

    updateBulkActions();
}

function updateBulkActions() {
    const checkboxes = document.querySelectorAll('.row-checkbox');
    const checkedBoxes = document.querySelectorAll('.row-checkbox:checked');
    const selectAll = document.getElementById('selectAll');
    const bulkActions = document.getElementById('bulkActions');
    const selectedCount = document.getElementById('selectedCount');

    // Update select all checkbox state
    if (checkedBoxes.length === 0) {
        selectAll.checked = false;
        selectAll.indeterminate = false;
    } else if (checkedBoxes.length === checkboxes.length) {
        selectAll.checked = true;
        selectAll.indeterminate = false;
    } else {
        selectAll.checked = false;
        selectAll.indeterminate = true;
    }

    // Show/hide bulk actions
    if (checkedBoxes.length > 0) {
        bulkActions.classList.remove('hidden');
        selectedCount.textContent = `${checkedBoxes.length} row(s) selected`;
    } else {
        bulkActions.classList.add('hidden');
        selectedCount.textContent = '';
    }
}

function bulkAction(action) {
    const checkedBoxes = document.querySelectorAll('.row-checkbox:checked');
    const count = checkedBoxes.length;

    if (action === 'delete') {
        if (confirm(`Are you sure you want to delete ${count} selected row(s)?`)) {
            checkedBoxes.forEach(checkbox => {
                checkbox.closest('tr').remove();
            });
            updateBulkActions();
            alert(`${count} row(s) deleted successfully!`);
        }
    } else if (action === 'export') {
        alert(`Exporting ${count} selected row(s)...\n\nData would be exported to CSV/Excel format.`);
    }
}
