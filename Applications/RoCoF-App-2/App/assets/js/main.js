// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    initializeTheme();
    initializeSidebar();
    initializeCharts();
    initializeEventListeners();
    console.log('Nexus Admin Dashboard loaded successfully');
});

// Theme Management
function initializeTheme() {
    const themeToggle = document.getElementById('themeToggle');
    const htmlElement = document.documentElement;

    // Check for saved theme preference or default to 'light'
    const currentTheme = localStorage.getItem('theme') || 'light';
    htmlElement.classList.remove('light', 'dark');
    htmlElement.classList.add(currentTheme);

    if (themeToggle) {
        themeToggle.addEventListener('click', () => {
            if (htmlElement.classList.contains('dark')) {
                htmlElement.classList.remove('dark');
                htmlElement.classList.add('light');
                localStorage.setItem('theme', 'light');
            } else {
                htmlElement.classList.remove('light');
                htmlElement.classList.add('dark');
                localStorage.setItem('theme', 'dark');
            }

            // Reload page to update chart colors
            setTimeout(() => {
                location.reload();
            }, 300);
        });
    }
}

// Sidebar Management
function initializeSidebar() {
    const sidebar = document.getElementById('sidebar');
    const openSidebarBtn = document.getElementById('openSidebar');
    const closeSidebarBtn = document.getElementById('closeSidebar');
    const sidebarOverlay = document.getElementById('sidebarOverlay');

    if (!sidebar || !openSidebarBtn || !closeSidebarBtn || !sidebarOverlay) return;

    function openSidebar() {
        sidebar.classList.remove('-translate-x-full');
        sidebarOverlay.classList.remove('hidden');
    }

    function closeSidebar() {
        sidebar.classList.add('-translate-x-full');
        sidebarOverlay.classList.add('hidden');
    }

    openSidebarBtn.addEventListener('click', openSidebar);
    closeSidebarBtn.addEventListener('click', closeSidebar);
    sidebarOverlay.addEventListener('click', closeSidebar);

    // Set active page in sidebar
    const currentPage = window.location.pathname.split('/').pop() || 'index.html';
    document.querySelectorAll('.sidebar-link').forEach(link => {
        const linkPage = link.getAttribute('href');
        if (linkPage === currentPage) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });

    document.querySelectorAll('.sidebar-sub-link').forEach(link => {
        const linkPage = link.getAttribute('href');
        if (linkPage === currentPage) {
            link.classList.add('bg-gray-100', 'dark:bg-gray-700', 'text-gray-900', 'dark:text-white', 'font-medium');
            const submenu = link.closest('.sidebar-submenu');
            if (submenu) {
                submenu.classList.remove('hidden');
                const toggle = document.querySelector(`[data-sidebar-dropdown-toggle="${submenu.id}"]`);
                if (toggle) {
                    toggle.setAttribute('aria-expanded', 'true');
                    const chevron = toggle.querySelector('[data-sidebar-chevron]');
                    if (chevron) {
                        chevron.classList.add('rotate-180');
                    }
                }
            }
        }
    });

    document.querySelectorAll('[data-sidebar-dropdown-toggle]').forEach(toggle => {
        const targetId = toggle.getAttribute('data-sidebar-dropdown-toggle');
        const submenu = document.getElementById(targetId);
        if (!submenu) return;

        toggle.addEventListener('click', () => {
            const isExpanded = toggle.getAttribute('aria-expanded') === 'true';
            toggle.setAttribute('aria-expanded', String(!isExpanded));
            submenu.classList.toggle('hidden');

            const chevron = toggle.querySelector('[data-sidebar-chevron]');
            if (chevron) {
                chevron.classList.toggle('rotate-180', !isExpanded);
            }
        });
    });
}

// Chart Initialization
function initializeCharts() {
    // Check if Chart.js is loaded
    if (typeof Chart === 'undefined') {
       // console.error('Chart.js is not loaded');
        return;
    }

    const htmlElement = document.documentElement;
    const isDark = htmlElement.classList.contains('dark');
    const textColor = isDark ? '#9ca3af' : '#6b7280';
    const gridColor = isDark ? '#374151' : '#e5e7eb';

    // Revenue Chart
    const revenueCtx = document.getElementById('revenueChart');
    if (revenueCtx) {
        try {
            new Chart(revenueCtx, {
                type: 'line',
                data: {
                    labels: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul'],
                    datasets: [{
                        label: 'Revenue',
                        data: [12000, 19000, 15000, 25000, 22000, 30000, 28000],
                        borderColor: '#0ea5e9',
                        backgroundColor: 'rgba(14, 165, 233, 0.1)',
                        borderWidth: 2,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 4,
                        pointBackgroundColor: '#0ea5e9',
                        pointBorderColor: '#fff',
                        pointBorderWidth: 2,
                        pointHoverRadius: 6
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: {
                        intersect: false,
                        mode: 'index'
                    },
                    plugins: {
                        legend: {
                            display: false
                        },
                        tooltip: {
                            backgroundColor: isDark ? '#1f2937' : '#ffffff',
                            titleColor: isDark ? '#f3f4f6' : '#111827',
                            bodyColor: isDark ? '#f3f4f6' : '#111827',
                            borderColor: isDark ? '#374151' : '#e5e7eb',
                            borderWidth: 1,
                            padding: 12,
                            displayColors: false,
                            callbacks: {
                                label: function(context) {
                                    return '$' + context.parsed.y.toLocaleString();
                                }
                            }
                        }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            grid: {
                                color: gridColor,
                                drawBorder: false
                            },
                            ticks: {
                                color: textColor,
                                callback: function(value) {
                                    return '$' + (value / 1000) + 'k';
                                }
                            }
                        },
                        x: {
                            grid: {
                                display: false,
                                drawBorder: false
                            },
                            ticks: {
                                color: textColor
                            }
                        }
                    }
                }
            });
        } catch (error) {
            console.error('Error creating revenue chart:', error);
        }
    }

    // Sales Chart
    const salesCtx = document.getElementById('salesChart');
    if (salesCtx) {
        try {
            new Chart(salesCtx, {
                type: 'doughnut',
                data: {
                    labels: ['Electronics', 'Clothing', 'Food', 'Home & Garden', 'Others'],
                    datasets: [{
                        data: [35, 25, 20, 12, 8],
                        backgroundColor: [
                            '#0ea5e9',
                            '#8b5cf6',
                            '#10b981',
                            '#f59e0b',
                            '#ef4444'
                        ],
                        borderWidth: 0,
                        hoverOffset: 10
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: {
                            position: 'bottom',
                            labels: {
                                padding: 20,
                                color: textColor,
                                usePointStyle: true,
                                pointStyle: 'circle',
                                font: {
                                    size: 12
                                }
                            }
                        },
                        tooltip: {
                            backgroundColor: isDark ? '#1f2937' : '#ffffff',
                            titleColor: isDark ? '#f3f4f6' : '#111827',
                            bodyColor: isDark ? '#f3f4f6' : '#111827',
                            borderColor: isDark ? '#374151' : '#e5e7eb',
                            borderWidth: 1,
                            padding: 12,
                            callbacks: {
                                label: function(context) {
                                    return context.label + ': ' + context.parsed + '%';
                                }
                            }
                        }
                    },
                    cutout: '70%'
                }
            });
        } catch (error) {
            console.error('Error creating sales chart:', error);
        }
    }
}

// Other Event Listeners
function initializeEventListeners() {
    // User Menu Toggle
    const userMenuButton = document.getElementById('userMenuButton');
    if (userMenuButton) {
        let isUserMenuOpen = false;

        userMenuButton.addEventListener('click', (e) => {
            e.stopPropagation();
            isUserMenuOpen = !isUserMenuOpen;
            // Add dropdown menu functionality here if needed
        });

        // Close user menu when clicking outside
        document.addEventListener('click', () => {
            if (isUserMenuOpen) {
                isUserMenuOpen = false;
                // Close dropdown logic here
            }
        });
    }

    // Smooth scroll for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({
                    behavior: 'smooth',
                    block: 'start'
                });
            }
        });
    });

    // Notification animation (optional)
    const notificationBell = document.querySelector('button[class*="relative"] svg');
    if (notificationBell) {
        setInterval(() => {
            notificationBell.classList.add('animate-bounce');
            setTimeout(() => {
                notificationBell.classList.remove('animate-bounce');
            }, 1000);
        }, 10000);
    }
}
