let activePopover = null;

    function showPopover(triggerId, popoverId, position = 'top') {
        // Hide any active popover first
        if (activePopover && activePopover !== popoverId) {
            hidePopover(activePopover);
        }

        const trigger = document.getElementById(triggerId);
        const popover = document.getElementById(popoverId);

        if (!trigger || !popover) return;

        // Toggle if clicking same trigger
        if (activePopover === popoverId) {
            hidePopover(popoverId);
            return;
        }

        const triggerRect = trigger.getBoundingClientRect();
        const popoverRect = popover.getBoundingClientRect();

        // Position the popover
        let top, left;

        switch(position) {
            case 'top':
                top = triggerRect.top - popoverRect.height - 12;
                left = triggerRect.left + (triggerRect.width / 2) - (popoverRect.width / 2);
                popover.className = popover.className.replace(/popover-(top|bottom|left|right)/, '') + ' popover-top';
                break;
            case 'bottom':
                top = triggerRect.bottom + 12;
                left = triggerRect.left + (triggerRect.width / 2) - (popoverRect.width / 2);
                popover.className = popover.className.replace(/popover-(top|bottom|left|right)/, '') + ' popover-bottom';
                break;
            case 'left':
                top = triggerRect.top + (triggerRect.height / 2) - (popoverRect.height / 2);
                left = triggerRect.left - popoverRect.width - 12;
                popover.className = popover.className.replace(/popover-(top|bottom|left|right)/, '') + ' popover-left';
                break;
            case 'right':
                top = triggerRect.top + (triggerRect.height / 2) - (popoverRect.height / 2);
                left = triggerRect.right + 12;
                popover.className = popover.className.replace(/popover-(top|bottom|left|right)/, '') + ' popover-right';
                break;
        }

        popover.style.top = top + window.scrollY + 'px';
        popover.style.left = left + window.scrollX + 'px';
        popover.classList.add('show');
        activePopover = popoverId;
    }

    function hidePopover(popoverId) {
        const popover = document.getElementById(popoverId);
        if (popover) {
            popover.classList.remove('show');
        }
        if (activePopover === popoverId) {
            activePopover = null;
        }
    }

    function toggleCustomPopover(triggerId, popoverId, position = 'top') {
        const popover = document.getElementById(popoverId);
        if (popover && popover.classList.contains('show')) {
            hidePopover(popoverId);
        } else {
            showPopover(triggerId, popoverId, position);
        }
    }

    // Initialize hover events for all popover triggers
    document.addEventListener('DOMContentLoaded', function() {
        // Find all buttons with data-popover attributes
        document.querySelectorAll('[data-popover-id]').forEach(trigger => {
            const popoverId = trigger.getAttribute('data-popover-id');
            const position = trigger.getAttribute('data-popover-position') || 'top';

            trigger.addEventListener('mouseenter', function() {
                showPopover(trigger.id, popoverId, position);
            });

            trigger.addEventListener('mouseleave', function() {
                // Delay hiding to allow mouse to move to popover
                setTimeout(() => {
                    const popover = document.getElementById(popoverId);
                    if (popover && !popover.matches(':hover') && !trigger.matches(':hover')) {
                        hidePopover(popoverId);
                    }
                }, 100);
            });
        });

        // Keep popover visible when hovering over it
        document.querySelectorAll('.popover').forEach(popover => {
            popover.addEventListener('mouseenter', function() {
                // Keep it visible
            });

            popover.addEventListener('mouseleave', function() {
                hidePopover(popover.id);
            });
        });
    });

    // Reposition popovers on window resize
    window.addEventListener('resize', function() {
        if (activePopover) {
            hidePopover(activePopover);
        }
    });