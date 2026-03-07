// Grid.js Table Initialization

// 1. Basic Grid.js Table
if (document.getElementById('basicGrid')) {
    new gridjs.Grid({
        columns: [
            { name: 'ID', width: '80px' },
            { name: 'Name', width: '150px' },
            { name: 'Email', width: '200px' },
            { name: 'Department', width: '150px' },
            { name: 'Salary', width: '120px' }
        ],
        data: [
            ['1', 'John Smith', 'john.smith@company.com', 'Engineering', '$95,000'],
            ['2', 'Sarah Johnson', 'sarah.johnson@company.com', 'Marketing', '$78,000'],
            ['3', 'Michael Brown', 'michael.brown@company.com', 'Sales', '$82,000'],
            ['4', 'Emily Davis', 'emily.davis@company.com', 'Design', '$88,000'],
            ['5', 'David Wilson', 'david.wilson@company.com', 'Engineering', '$102,000'],
            ['6', 'Lisa Anderson', 'lisa.anderson@company.com', 'HR', '$72,000'],
            ['7', 'James Taylor', 'james.taylor@company.com', 'Finance', '$91,000'],
            ['8', 'Jennifer Martinez', 'jennifer.martinez@company.com', 'Marketing', '$76,000'],
            ['9', 'Robert Garcia', 'robert.garcia@company.com', 'Sales', '$85,000'],
            ['10', 'Maria Rodriguez', 'maria.rodriguez@company.com', 'Engineering', '$98,000'],
            ['11', 'William Lee', 'william.lee@company.com', 'Design', '$86,000'],
            ['12', 'Patricia White', 'patricia.white@company.com', 'HR', '$74,000'],
            ['13', 'Christopher Harris', 'christopher.harris@company.com', 'Finance', '$93,000'],
            ['14', 'Linda Clark', 'linda.clark@company.com', 'Marketing', '$79,000'],
            ['15', 'Daniel Lewis', 'daniel.lewis@company.com', 'Sales', '$87,000']
        ],
        search: true,
        sort: true,
        pagination: {
            enabled: true,
            limit: 5
        }
    }).render(document.getElementById('basicGrid'));
}

// 2. Server-Side Pagination
if (document.getElementById('serverGrid')) {
    new gridjs.Grid({
        columns: [
            'ID',
            'Title',
            'User ID',
            'Completed'
        ],
        server: {
            url: 'https://jsonplaceholder.typicode.com/todos',
            then: data => data.slice(0, 50).map(item => [
                item.id,
                item.title,
                item.userId,
                item.completed ? '✅ Yes' : '❌ No'
            ])
        },
        search: true,
        sort: true,
        pagination: {
            enabled: true,
            limit: 10
        }
    }).render(document.getElementById('serverGrid'));
}

// 3. Custom Cell Formatting
if (document.getElementById('formattedGrid')) {
    new gridjs.Grid({
        columns: [
            { name: 'Product', width: '200px' },
            { name: 'Price', width: '100px' },
            { name: 'Stock', width: '100px' },
            {
                name: 'Status',
                width: '120px',
                formatter: (cell) => {
                    if (cell === 'In Stock') {
                        return gridjs.html('<span class="badge badge-success">In Stock</span>');
                    } else if (cell === 'Low Stock') {
                        return gridjs.html('<span class="badge badge-warning">Low Stock</span>');
                    } else {
                        return gridjs.html('<span class="badge badge-danger">Out of Stock</span>');
                    }
                }
            },
            {
                name: 'Actions',
                width: '150px',
                formatter: (cell, row) => {
                    return gridjs.html('<button class="px-3 py-1 bg-blue-500 text-white rounded text-sm hover:bg-blue-600 mr-2" onclick="editProduct(\'' + row.cells[0].data + '\')"><i class="fas fa-edit"></i></button><button class="px-3 py-1 bg-red-500 text-white rounded text-sm hover:bg-red-600" onclick="deleteProduct(\'' + row.cells[0].data + '\')"><i class="fas fa-trash"></i></button>');
                }
            }
        ],
        data: [
            ['Wireless Mouse', '$29.99', '145', 'In Stock'],
            ['USB-C Cable', '$12.99', '23', 'Low Stock'],
            ['Mechanical Keyboard', '$89.99', '67', 'In Stock'],
            ['Monitor Stand', '$45.99', '0', 'Out of Stock'],
            ['Laptop Sleeve', '$24.99', '89', 'In Stock'],
            ['Webcam HD', '$59.99', '12', 'Low Stock'],
            ['Desk Lamp', '$34.99', '156', 'In Stock'],
            ['Phone Holder', '$15.99', '203', 'In Stock'],
            ['Cable Organizer', '$8.99', '5', 'Low Stock'],
            ['USB Hub', '$22.99', '78', 'In Stock']
        ],
        search: true,
        sort: true,
        pagination: {
            enabled: true,
            limit: 5
        }
    }).render(document.getElementById('formattedGrid'));
}

// 4. Fixed Header Table
if (document.getElementById('fixedHeaderGrid')) {
    new gridjs.Grid({
        columns: [
            { name: 'Date', width: '120px' },
            { name: 'Transaction ID', width: '150px' },
            { name: 'Customer', width: '180px' },
            { name: 'Amount', width: '100px' },
            { name: 'Payment Method', width: '150px' },
            { name: 'Status', width: '120px' }
        ],
        data: Array.from({ length: 30 }, (_, i) => [
            new Date(2024, 0, i + 1).toLocaleDateString(),
            '#TXN-' + (1000 + i),
            'Customer ' + (i + 1),
            '$' + ((Math.random() * 500 + 50).toFixed(2)),
            ['Credit Card', 'PayPal', 'Bank Transfer'][Math.floor(Math.random() * 3)],
            ['Completed', 'Pending', 'Failed'][Math.floor(Math.random() * 3)]
        ]),
        search: true,
        sort: true,
        fixedHeader: true,
        height: '400px',
        pagination: {
            enabled: true,
            limit: 8
        }
    }).render(document.getElementById('fixedHeaderGrid'));
}

// 5. Hidden Columns & Column Width
if (document.getElementById('hiddenColumnsGrid')) {
    new gridjs.Grid({
        columns: [
            {
                name: 'ID',
                width: '60px',
                hidden: true
            },
            { name: 'Project Name', width: '250px' },
            { name: 'Manager', width: '150px' },
            { name: 'Budget', width: '120px' },
            {
                name: 'Internal Code',
                width: '150px',
                hidden: true
            },
            { name: 'Status', width: '120px' },
            { name: 'Progress', width: '100px' }
        ],
        data: [
            ['1', 'E-Commerce Platform', 'Alex Thompson', '$150,000', 'EC-2024-001', 'Active', '65%'],
            ['2', 'Mobile App Redesign', 'Sarah Williams', '$85,000', 'MA-2024-002', 'Active', '92%'],
            ['3', 'CRM Integration', 'Michael Chen', '$200,000', 'CRM-2024-003', 'Active', '28%'],
            ['4', 'Website Optimization', 'Emily Rodriguez', '$45,000', 'WO-2023-004', 'Completed', '100%'],
            ['5', 'API Development', 'David Lee', '$120,000', 'API-2024-005', 'Active', '45%'],
            ['6', 'Data Migration', 'Sophie Brown', '$95,000', 'DM-2024-006', 'Planning', '10%'],
            ['7', 'Security Audit', 'James Wilson', '$75,000', 'SA-2024-007', 'Active', '73%'],
            ['8', 'Cloud Migration', 'Maria Garcia', '$180,000', 'CM-2024-008', 'Active', '55%']
        ],
        search: true,
        sort: true,
        pagination: {
            enabled: true,
            limit: 5
        }
    }).render(document.getElementById('hiddenColumnsGrid'));
}

// 6. Async Data Loading with Actions
if (document.getElementById('asyncGrid')) {
    new gridjs.Grid({
        columns: [
            { name: 'Avatar', width: '80px' },
            'Name',
            'Username',
            'Email',
            {
                name: 'Actions',
                width: '200px',
                formatter: (cell, row) => {
                    return gridjs.html(
                        '<button class="px-2 py-1 bg-green-500 text-white rounded text-xs hover:bg-green-600 mr-1" onclick="viewUser(\'' + row.cells[2].data + '\')"><i class="fas fa-eye"></i> View</button>' +
                        '<button class="px-2 py-1 bg-blue-500 text-white rounded text-xs hover:bg-blue-600 mr-1" onclick="editUser(\'' + row.cells[2].data + '\')"><i class="fas fa-edit"></i> Edit</button>' +
                        '<button class="px-2 py-1 bg-red-500 text-white rounded text-xs hover:bg-red-600" onclick="deleteUser(\'' + row.cells[2].data + '\')"><i class="fas fa-trash"></i> Delete</button>'
                    );
                }
            }
        ],
        server: {
            url: 'https://jsonplaceholder.typicode.com/users',
            then: data => data.map(user => [
                gridjs.html('<img src="https://i.pravatar.cc/50?u=' + user.id + '" alt="' + user.name + '" class="w-10 h-10 rounded-full">'),
                user.name,
                user.username,
                user.email
            ])
        },
        search: true,
        sort: true,
        pagination: {
            enabled: true,
            limit: 5
        }
    }).render(document.getElementById('asyncGrid'));
}

// Action Functions for Grid.js Tables
function editProduct(product) {
    alert('Edit product: ' + product);
}

function deleteProduct(product) {
    if (confirm('Are you sure you want to delete ' + product + '?')) {
        alert('Product ' + product + ' deleted successfully!');
    }
}

function viewUser(username) {
    alert('Viewing user profile: ' + username);
}

function editUser(username) {
    alert('Editing user: ' + username);
}

function deleteUser(username) {
    if (confirm('Are you sure you want to delete user ' + username + '?')) {
        alert('User ' + username + ' deleted successfully!');
    }
}
