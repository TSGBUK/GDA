let toastPosition = 'top-right';
    let toastCount = 0;

    function createToastContainer(position) {
        let container = document.getElementById('toast-container-' + position);
        if (!container) {
            container = document.createElement('div');
            container.id = 'toast-container-' + position;
            container.className = 'toast-container ' + position;
            document.body.appendChild(container);
        }
        return container;
    }

    function showToast(options) {
        const {
            type = 'info',
            title = '',
            message = '',
            duration = 5000,
            position = toastPosition,
            showProgress = true,
            closeButton = true,
            animation = 'slide'
        } = options;

        const container = createToastContainer(position);
        const toast = document.createElement('div');
        const toastId = 'toast-' + (++toastCount);
        toast.id = toastId;

        const typeColors = {
            success: {
                bg: 'bg-white dark:bg-gray-800',
                border: 'border-l-4 border-green-500',
                iconBg: 'bg-green-100 dark:bg-green-900/30',
                iconColor: 'text-green-600 dark:text-green-400',
                progressBg: 'bg-green-500'
            },
            error: {
                bg: 'bg-white dark:bg-gray-800',
                border: 'border-l-4 border-red-500',
                iconBg: 'bg-red-100 dark:bg-red-900/30',
                iconColor: 'text-red-600 dark:text-red-400',
                progressBg: 'bg-red-500'
            },
            warning: {
                bg: 'bg-white dark:bg-gray-800',
                border: 'border-l-4 border-yellow-500',
                iconBg: 'bg-yellow-100 dark:bg-yellow-900/30',
                iconColor: 'text-yellow-600 dark:text-yellow-400',
                progressBg: 'bg-yellow-500'
            },
            info: {
                bg: 'bg-white dark:bg-gray-800',
                border: 'border-l-4 border-blue-500',
                iconBg: 'bg-blue-100 dark:bg-blue-900/30',
                iconColor: 'text-blue-600 dark:text-blue-400',
                progressBg: 'bg-blue-500'
            }
        };

        const typeIcons = {
            success: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>',
            error: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>',
            warning: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>',
            info: '<svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>'
        };

        const colors = typeColors[type];
        const icon = typeIcons[type];

        const closeBtn = closeButton ? `
            <button onclick="closeToast('${toastId}')" class="ml-auto flex-shrink-0 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 transition-colors">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/>
                </svg>
            </button>
        ` : '';

        const progressBar = showProgress ? `<div class="toast-progress ${colors.progressBg}" style="animation-duration: ${duration}ms;"></div>` : '';

        toast.className = `toast ${colors.bg} ${colors.border} p-4 ${animation === 'bounce' ? 'bounce' : ''}`;
        toast.innerHTML = `
            <div class="flex items-start gap-3">
                <div class="${colors.iconBg} ${colors.iconColor} w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0">
                    ${icon}
                </div>
                <div class="flex-1 min-w-0">
                    ${title ? `<h4 class="font-semibold text-gray-900 dark:text-white mb-1">${title}</h4>` : ''}
                    <p class="text-sm text-gray-600 dark:text-gray-400">${message}</p>
                </div>
                ${closeBtn}
            </div>
            ${progressBar}
        `;

        container.appendChild(toast);

        // Show toast
        setTimeout(() => {
            toast.classList.add('show');
        }, 10);

        // Auto remove after duration
        if (duration > 0) {
            setTimeout(() => {
                closeToast(toastId);
            }, duration);
        }
    }

    function closeToast(toastId) {
        const toast = document.getElementById(toastId);
        if (toast) {
            toast.classList.add('hiding');
            toast.classList.remove('show');

            setTimeout(() => {
                if (toast.parentNode) {
                    toast.parentNode.removeChild(toast);
                }
            }, 300);
        }
    }

    function setToastPosition(position) {
        toastPosition = position;
    }

    function clearAllToasts() {
        document.querySelectorAll('.toast').forEach(toast => {
            closeToast(toast.id);
        });
    }