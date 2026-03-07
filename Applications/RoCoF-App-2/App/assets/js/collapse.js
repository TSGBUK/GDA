// Improved toggle collapse function with smooth animations
        function toggleCollapse(id) {
            const element = document.getElementById(id);
            const isHidden = element.classList.contains('hidden');

            if (isHidden) {
                // Opening
                element.classList.remove('hidden');
                element.style.maxHeight = '0px';
                element.style.opacity = '0';

                // Trigger reflow
                element.offsetHeight;

                // Animate open
                requestAnimationFrame(() => {
                    element.style.maxHeight = element.scrollHeight + 'px';
                    element.style.opacity = '1';
                    element.classList.add('collapse-content', 'active');
                });
            } else {
                // Closing
                element.style.maxHeight = element.scrollHeight + 'px';
                element.classList.remove('active');

                // Trigger reflow
                element.offsetHeight;

                // Animate close
                requestAnimationFrame(() => {
                    element.style.maxHeight = '0px';
                    element.style.opacity = '0';
                });

                // Hide after animation
                setTimeout(() => {
                    element.classList.add('hidden');
                    element.classList.remove('collapse-content');
                }, 400);
            }
        }

        // Improved accordion with smooth icon rotation
        function toggleAccordion(id, iconId) {
            const element = document.getElementById(id);
            const icon = document.getElementById(iconId);
            const isHidden = element.classList.contains('hidden');

            if (isHidden) {
                // Opening
                element.classList.remove('hidden');
                element.style.maxHeight = '0px';
                element.style.opacity = '0';

                // Trigger reflow
                element.offsetHeight;

                // Animate open
                requestAnimationFrame(() => {
                    element.style.maxHeight = element.scrollHeight + 'px';
                    element.style.opacity = '1';
                    element.classList.add('collapse-content', 'active');
                });

                icon.classList.add('rotated');
            } else {
                // Closing
                element.style.maxHeight = element.scrollHeight + 'px';
                element.classList.remove('active');

                // Trigger reflow
                element.offsetHeight;

                // Animate close
                requestAnimationFrame(() => {
                    element.style.maxHeight = '0px';
                    element.style.opacity = '0';
                });

                icon.classList.remove('rotated');

                // Hide after animation
                setTimeout(() => {
                    element.classList.add('hidden');
                    element.classList.remove('collapse-content');
                }, 400);
            }
        }

        // Improved horizontal collapse with smooth animation
        function toggleHorizontal(id) {
            const element = document.getElementById(id);
            const isHidden = element.classList.contains('hidden');

            if (isHidden) {
                // Opening
                element.classList.remove('hidden');
                element.classList.add('horizontal-collapse');

                // Trigger reflow
                element.offsetHeight;

                // Animate open
                requestAnimationFrame(() => {
                    element.classList.add('active');
                });
            } else {
                // Closing
                element.classList.remove('active');

                // Hide after animation
                setTimeout(() => {
                    element.classList.add('hidden');
                    element.classList.remove('horizontal-collapse');
                }, 500);
            }
        }

        // Initialize all icons with transition class
        document.addEventListener('DOMContentLoaded', function() {
            document.querySelectorAll('[id^="icon"]').forEach(icon => {
                icon.classList.add('collapse-icon');
            });

            document.querySelectorAll('[id^="accIcon"]').forEach(icon => {
                icon.classList.add('collapse-icon');
            });
        });