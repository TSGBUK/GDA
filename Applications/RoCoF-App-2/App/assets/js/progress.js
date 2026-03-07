// Animated Progress Bar
    function animateProgress(barId, targetPercent) {
        const bar = document.getElementById(barId);
        const text = document.getElementById(barId + 'Text');
        let currentPercent = 0;
        const increment = targetPercent / 60; // 60 frames for smooth animation

        const interval = setInterval(() => {
            currentPercent += increment;
            if (currentPercent >= targetPercent) {
                currentPercent = targetPercent;
                clearInterval(interval);
            }
            bar.style.width = currentPercent + '%';
            if (text) {
                text.textContent = Math.round(currentPercent) + '%';
            }
        }, 25);
    }

    // Circular Progress
    function setCircularProgress(circleId, percent) {
        const circle = document.getElementById(circleId);
        const radius = circle.r.baseVal.value;
        const circumference = 2 * Math.PI * radius;
        const offset = circumference - (percent / 100) * circumference;

        circle.style.strokeDasharray = circumference;
        circle.style.strokeDashoffset = circumference;

        setTimeout(() => {
            circle.style.strokeDashoffset = offset;
        }, 100);

        // Update text
        const textId = circleId.replace('Circle', 'Text');
        const text = document.getElementById(textId);
        if (text) {
            let current = 0;
            const increment = percent / 60;
            const interval = setInterval(() => {
                current += increment;
                if (current >= percent) {
                    current = percent;
                    clearInterval(interval);
                }
                text.textContent = Math.round(current) + '%';
            }, 25);
        }
    }

    // Multi-step Progress
    let currentStep = 1;
    const totalSteps = 4;

    function nextStep() {
        if (currentStep < totalSteps) {
            currentStep++;
            updateSteps();
        }
    }

    function prevStep() {
        if (currentStep > 1) {
            currentStep--;
            updateSteps();
        }
    }

    function updateSteps() {
        for (let i = 1; i <= totalSteps; i++) {
            const step = document.getElementById('step' + i);
            const line = document.getElementById('line' + i);

            if (i < currentStep) {
                step.className = step.className.replace(/bg-gray-\d+/g, 'bg-green-500');
                step.className = step.className.replace(/text-gray-\d+/g, 'text-white');
                if (line) {
                    line.className = line.className.replace(/bg-gray-\d+/g, 'bg-green-500');
                }
            } else if (i === currentStep) {
                step.className = step.className.replace(/bg-gray-\d+/g, 'bg-blue-500');
                step.className = step.className.replace(/bg-green-\d+/g, 'bg-blue-500');
                step.className = step.className.replace(/text-gray-\d+/g, 'text-white');
                if (line) {
                    line.className = line.className.replace(/bg-green-\d+/g, 'bg-gray-300');
                }
            } else {
                step.className = step.className.replace(/bg-blue-\d+/g, 'bg-gray-300');
                step.className = step.className.replace(/bg-green-\d+/g, 'bg-gray-300');
                step.className = step.className.replace(/text-white/g, 'text-gray-600');
                if (line) {
                    line.className = line.className.replace(/bg-green-\d+/g, 'bg-gray-300');
                }
            }
        }

        document.getElementById('prevBtn').disabled = currentStep === 1;
        document.getElementById('nextBtn').disabled = currentStep === totalSteps;
    }

    // Upload Progress Simulation
    function simulateUpload() {
        const bar = document.getElementById('uploadBar');
        const text = document.getElementById('uploadText');
        const button = document.getElementById('uploadBtn');

        button.disabled = true;
        let progress = 0;

        const interval = setInterval(() => {
            progress += Math.random() * 15;
            if (progress >= 100) {
                progress = 100;
                clearInterval(interval);
                button.disabled = false;
                button.textContent = 'Upload Complete!';
                setTimeout(() => {
                    button.textContent = 'Start Upload';
                    bar.style.width = '0%';
                    text.textContent = '0%';
                }, 2000);
            }
            bar.style.width = progress + '%';
            text.textContent = Math.round(progress) + '%';
        }, 300);
    }

    // Initialize on page load
    document.addEventListener('DOMContentLoaded', function() {
        // Animate basic progress bars
        animateProgress('basicBar1', 75);
        animateProgress('basicBar2', 50);
        animateProgress('basicBar3', 90);

        // Animated striped bars
        animateProgress('stripedBar1', 60);
        animateProgress('stripedBar2', 80);

        // Gradient bars
        animateProgress('gradientBar1', 70);
        animateProgress('gradientBar2', 85);
        animateProgress('gradientBar3', 45);

        // Labeled bars
        animateProgress('labeledBar1', 65);
        animateProgress('labeledBar2', 40);
        animateProgress('labeledBar3', 95);

        // Circular progress
        setTimeout(() => {
            setCircularProgress('circle1', 75);
            setCircularProgress('circle2', 60);
            setCircularProgress('circle3', 90);
        }, 200);

        // Initialize steps
        updateSteps();
    });