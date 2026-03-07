let currentAlert = null;

    function showSweetAlert(options) {
        // Close existing alert
        if (currentAlert) {
            closeSweetAlert();
        }

        // Create overlay
        const overlay = document.createElement('div');
        overlay.className = 'sweet-alert-overlay';
        overlay.id = 'sweetAlertOverlay';

        // Create alert box
        const alertBox = document.createElement('div');
        alertBox.className = 'sweet-alert-box p-6';

        // Add animation class if specified
        if (options.animation) {
            alertBox.classList.add('sweet-alert-' + options.animation);
        }

        // Icon
        let iconHTML = '';
        if (options.icon) {
            const iconColors = {
                success: 'bg-green-100 dark:bg-green-900/30',
                error: 'bg-red-100 dark:bg-red-900/30',
                warning: 'bg-yellow-100 dark:bg-yellow-900/30',
                info: 'bg-blue-100 dark:bg-blue-900/30',
                question: 'bg-purple-100 dark:bg-purple-900/30'
            };

            const iconSVGs = {
                success: '<svg class="w-10 h-10 text-green-600 dark:text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"/></svg>',
                error: '<svg class="w-10 h-10 text-red-600 dark:text-red-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12"/></svg>',
                warning: '<svg class="w-10 h-10 text-yellow-600 dark:text-yellow-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/></svg>',
                info: '<svg class="w-10 h-10 text-blue-600 dark:text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>',
                question: '<svg class="w-10 h-10 text-purple-600 dark:text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"/></svg>'
            };

            const pulseClass = options.iconPulse ? 'sweet-alert-icon-pulse' : '';
            iconHTML = `<div class="sweet-alert-icon ${iconColors[options.icon]} ${pulseClass}">${iconSVGs[options.icon]}</div>`;
        }

        // Title
        const titleHTML = options.title ? `<h3 class="text-xl font-bold text-gray-900 dark:text-white mb-2 text-center">${options.title}</h3>` : '';

        // Text
        const textHTML = options.text ? `<p class="text-gray-600 dark:text-gray-400 mb-4 text-center">${options.text}</p>` : '';

        // Input (for prompt type)
        const inputHTML = options.input ? `<input type="${options.inputType || 'text'}" id="sweetAlertInput" placeholder="${options.inputPlaceholder || ''}" class="w-full px-4 py-2 mb-4 bg-gray-50 dark:bg-gray-700 border border-gray-300 dark:border-gray-600 rounded-lg text-gray-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500" />` : '';

        // Buttons
        let buttonsHTML = '<div class="flex gap-3 justify-center">';

        if (options.showCancelButton) {
            buttonsHTML += `<button onclick="closeSweetAlert(false)" class="px-6 py-2 bg-gray-200 dark:bg-gray-700 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-300 dark:hover:bg-gray-600 transition-colors">${options.cancelButtonText || 'Cancel'}</button>`;
        }

        const confirmColor = options.confirmButtonColor || 'blue';
        const confirmColorClasses = {
            blue: 'bg-blue-500 hover:bg-blue-600',
            green: 'bg-green-500 hover:bg-green-600',
            red: 'bg-red-500 hover:bg-red-600',
            yellow: 'bg-yellow-500 hover:bg-yellow-600',
            purple: 'bg-purple-500 hover:bg-purple-600'
        };

        buttonsHTML += `<button onclick="confirmSweetAlert()" class="px-6 py-2 ${confirmColorClasses[confirmColor]} text-white rounded-lg transition-colors">${options.confirmButtonText || 'OK'}</button>`;
        buttonsHTML += '</div>';

        // Assemble alert
        alertBox.innerHTML = iconHTML + titleHTML + textHTML + inputHTML + buttonsHTML;
        overlay.appendChild(alertBox);
        document.body.appendChild(overlay);

        // Show with animation
        setTimeout(() => {
            overlay.classList.add('show');
        }, 10);

        // Store current alert
        currentAlert = {
            overlay: overlay,
            onConfirm: options.onConfirm,
            onCancel: options.onCancel,
            input: options.input
        };

        // Auto close timer
        if (options.timer) {
            setTimeout(() => {
                closeSweetAlert(true);
            }, options.timer);
        }

        // Close on overlay click
        if (options.closeOnClickOutside !== false) {
            overlay.addEventListener('click', function(e) {
                if (e.target === overlay) {
                    closeSweetAlert(false);
                }
            });
        }

        // Prevent body scroll
        document.body.style.overflow = 'hidden';
    }

    function confirmSweetAlert() {
        if (currentAlert) {
            let inputValue = null;
            if (currentAlert.input) {
                const input = document.getElementById('sweetAlertInput');
                inputValue = input ? input.value : null;
            }

            if (currentAlert.onConfirm) {
                currentAlert.onConfirm(inputValue);
            }

            closeSweetAlert(true);
        }
    }

    function closeSweetAlert(confirmed = false) {
        if (currentAlert) {
            const overlay = currentAlert.overlay;

            if (!confirmed && currentAlert.onCancel) {
                currentAlert.onCancel();
            }

            overlay.classList.remove('show');

            setTimeout(() => {
                if (overlay.parentNode) {
                    overlay.parentNode.removeChild(overlay);
                }
            }, 300);

            currentAlert = null;
            document.body.style.overflow = '';
        }
    }

    // Close on ESC key
    document.addEventListener('keydown', function(e) {
        if (e.key === 'Escape' && currentAlert) {
            closeSweetAlert(false);
        }
    });