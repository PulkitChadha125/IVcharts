// Chart initialization
let ivChart = null;
let fetchInterval = null;

// Initialize chart
function initChart() {
    const ctx = document.getElementById('ivChart').getContext('2d');
    
    ivChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: [],
            datasets: [{
                label: 'Implied Volatility (%)',
                data: [],
                borderColor: '#8b5cf6',
                backgroundColor: 'rgba(139, 92, 246, 0.1)',
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 3,
                pointHoverRadius: 5,
                pointBackgroundColor: '#8b5cf6',
                pointBorderColor: '#ffffff',
                pointBorderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    labels: {
                        color: '#a0a0b8',
                        font: {
                            size: 12
                        }
                    }
                },
                tooltip: {
                    backgroundColor: '#1a1a2e',
                    titleColor: '#ffffff',
                    bodyColor: '#a0a0b8',
                    borderColor: '#8b5cf6',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: true
                },
                zoom: {
                    zoom: {
                        wheel: {
                            enabled: true,
                            modifierKey: 'ctrl',
                        },
                        pinch: {
                            enabled: true
                        },
                        mode: 'xy',
                        speed: 0.1
                    },
                    pan: {
                        enabled: true,
                        modifierKey: 'ctrl',
                        buttons: ['right'],
                        mode: 'xy',
                        speed: 0.5
                    },
                    limits: {
                        x: { min: 'original', max: 'original' },
                        y: { min: 'original', max: 'original' }
                    }
                }
            },
            scales: {
                x: {
                    ticks: {
                        color: '#a0a0b8',
                        maxTicksLimit: 10
                    },
                    grid: {
                        color: '#2d2d44',
                        drawBorder: false
                    }
                },
                y: {
                    ticks: {
                        color: '#a0a0b8'
                    },
                    grid: {
                        color: '#2d2d44',
                        drawBorder: false
                    },
                    title: {
                        display: true,
                        text: 'IV (%)',
                        color: '#a0a0b8'
                    }
                }
            }
        }
    });
}

// Check login status
async function checkLoginStatus() {
    try {
        const response = await fetch('/api/check_login');
        const data = await response.json();
        
        if (data.logged_in) {
            document.getElementById('loginStatus').style.display = 'flex';
            document.getElementById('statusText').textContent = 'Logged in';
            document.getElementById('settingsSection').style.display = 'block';
            document.getElementById('loginBtn').textContent = 'Logged In';
            document.getElementById('loginBtn').disabled = true;
        }
    } catch (error) {
        console.error('Error checking login status:', error);
    }
}

// Login to Fyers API
async function loginToAPI() {
    const btn = document.getElementById('loginBtn');
    btn.disabled = true;
    btn.textContent = 'Logging in...';
    
    try {
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            document.getElementById('loginStatus').style.display = 'flex';
            document.getElementById('statusText').textContent = 'Logged in successfully';
            document.getElementById('settingsSection').style.display = 'block';
            btn.textContent = 'Logged In';
            showNotification('Login successful!', 'success');
        } else {
            btn.disabled = false;
            btn.textContent = 'Login to API';
            showNotification(data.message || 'Login failed', 'error');
        }
    } catch (error) {
        btn.disabled = false;
        btn.textContent = 'Login to API';
        showNotification('Error connecting to server', 'error');
        console.error('Login error:', error);
    }
}

// Start fetching data
async function startFetching() {
    const symbol = document.getElementById('symbol').value.trim();
    const timeframe = document.getElementById('timeframe').value;
    const strike = document.getElementById('strike').value;
    const expiry = document.getElementById('expiry').value;
    const optionType = document.getElementById('optionType').value;
    
    if (!symbol) {
        showNotification('Please enter a symbol', 'error');
        return;
    }
    
    // Build request payload
    const payload = { symbol, timeframe };
    
    // Add optional parameters if provided
    if (strike) {
        payload.strike = parseFloat(strike);
    }
    if (expiry) {
        payload.expiry = expiry;
    }
    if (optionType) {
        payload.option_type = optionType;
    }
    
    try {
        const response = await fetch('/api/start_fetching', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });
        
        const data = await response.json();
        
        if (data.success) {
            document.getElementById('fetchStatus').textContent = 'Fetching...';
            document.getElementById('fetchStatus').classList.add('active');
            showNotification('Data fetching started', 'success');
            
            // Start polling for IV data
            startPollingIVData(symbol);
        } else {
            showNotification(data.message || 'Failed to start fetching', 'error');
        }
    } catch (error) {
        showNotification('Error starting data fetch', 'error');
        console.error('Start fetching error:', error);
    }
}

// Stop fetching data
async function stopFetching() {
    try {
        const response = await fetch('/api/stop_fetching', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });
        
        const data = await response.json();
        
        if (data.success) {
            document.getElementById('fetchStatus').textContent = 'Stopped';
            document.getElementById('fetchStatus').classList.remove('active');
            showNotification('Data fetching stopped', 'info');
            
            // Stop polling
            if (fetchInterval) {
                clearInterval(fetchInterval);
                fetchInterval = null;
            }
        }
    } catch (error) {
        showNotification('Error stopping data fetch', 'error');
        console.error('Stop fetching error:', error);
    }
}

// Poll for IV data updates
function startPollingIVData(symbol) {
    // Clear any existing interval
    if (fetchInterval) {
        clearInterval(fetchInterval);
    }
    
    // Fetch immediately
    fetchIVData(symbol);
    
    // Then poll every 2 seconds
    fetchInterval = setInterval(() => {
        fetchIVData(symbol);
        checkFetchStatus();
    }, 2000);
}

// Zoom to latest values (show last N data points)
function zoomToLatest(dataPoints = 50) {
    if (!ivChart || !ivChart.data.labels || ivChart.data.labels.length === 0) {
        return;
    }
    
    const totalPoints = ivChart.data.labels.length;
    if (totalPoints <= dataPoints) {
        // If we have fewer points than requested, show all
        return;
    }
    
    // Calculate the range to show (last N points)
    const startIndex = totalPoints - dataPoints;
    const endIndex = totalPoints - 1;
    
    // Get the x-axis scale
    const xScale = ivChart.scales.x;
    if (xScale) {
        // Get the min and max values for the range
        const startValue = ivChart.data.labels[startIndex];
        const endValue = ivChart.data.labels[endIndex];
        
        // Use chart zoom plugin to zoom to this range
        if (ivChart.zoomScale) {
            // Zoom using the plugin's zoomScale method
            ivChart.zoomScale('x', {
                min: startValue,
                max: endValue
            });
        } else {
            // Fallback: directly set scale options
            xScale.options.min = startValue;
            xScale.options.max = endValue;
            ivChart.update('none');
        }
    }
}

// Fetch IV data and update chart
async function fetchIVData(symbol) {
    try {
        const response = await fetch(`/api/get_iv_data?symbol=${encodeURIComponent(symbol)}`);
        const data = await response.json();
        
        if (data.timestamps && data.timestamps.length > 0) {
            const previousLength = ivChart.data.labels.length;
            
            // Update chart
            ivChart.data.labels = data.timestamps;
            ivChart.data.datasets[0].data = data.iv_values;
            
            // Update chart first
            ivChart.update('none');
            
            // If this is the first data load or new data was added, zoom to latest
            if (previousLength === 0 || data.timestamps.length > previousLength) {
                // Zoom to latest 50 points after a short delay to ensure chart is rendered
                setTimeout(() => {
                    zoomToLatest(50);
                }, 200);
            }
        } else if (data.timestamps && data.timestamps.length === 0) {
            // No data available yet - this is normal when starting
            console.log('Waiting for data...');
        }
    } catch (error) {
        console.error('Error fetching IV data:', error);
    }
}

// Check fetching status periodically
async function checkFetchStatus() {
    try {
        const response = await fetch('/api/get_status');
        const status = await response.json();
        
        if (!status.active && document.getElementById('fetchStatus').textContent === 'Fetching...') {
            // Fetching was stopped
            document.getElementById('fetchStatus').textContent = 'Stopped';
            document.getElementById('fetchStatus').classList.remove('active');
        }
    } catch (error) {
        console.error('Error checking status:', error);
    }
}

// Reset zoom function
function resetZoom() {
    if (ivChart) {
        ivChart.resetZoom();
        showNotification('Zoom reset', 'info');
    }
}

// Show notification
function showNotification(message, type = 'info') {
    // Create notification element
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.textContent = message;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 16px 24px;
        background-color: ${type === 'success' ? '#10b981' : type === 'error' ? '#ef4444' : '#3b82f6'};
        color: white;
        border-radius: 8px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
        z-index: 10000;
        animation: slideIn 0.3s ease;
    `;
    
    document.body.appendChild(notification);
    
    // Remove after 3 seconds
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        setTimeout(() => {
            document.body.removeChild(notification);
        }, 300);
    }, 3000);
}

// Add CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    @keyframes slideOut {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
`;
document.head.appendChild(style);

// Event listeners
document.addEventListener('DOMContentLoaded', () => {
    // Register zoom plugin - chartjs-plugin-zoom makes it available as zoomPlugin
    if (typeof zoomPlugin !== 'undefined') {
        Chart.register(zoomPlugin);
    }
    
    initChart();
    checkLoginStatus();
    
    document.getElementById('loginBtn').addEventListener('click', loginToAPI);
    document.getElementById('startBtn').addEventListener('click', startFetching);
    document.getElementById('stopBtn').addEventListener('click', stopFetching);
    
    const resetBtn = document.getElementById('resetZoomBtn');
    if (resetBtn) {
        resetBtn.addEventListener('click', resetZoom);
    }
    
    // Double-click on chart to reset zoom
    const chartCanvas = document.getElementById('ivChart');
    if (chartCanvas) {
        chartCanvas.addEventListener('dblclick', resetZoom);
        
        // Custom handler for Ctrl + Right-click + Drag to pan
        let isPanning = false;
        let lastPanPoint = null;
        
        chartCanvas.addEventListener('contextmenu', (e) => {
            // Prevent default right-click menu
            if (e.ctrlKey || e.metaKey) {
                e.preventDefault();
                isPanning = true;
                lastPanPoint = { x: e.clientX, y: e.clientY };
                chartCanvas.style.cursor = 'grabbing';
            }
        });
        
        chartCanvas.addEventListener('mousemove', (e) => {
            if (isPanning && lastPanPoint && (e.ctrlKey || e.metaKey)) {
                const deltaX = e.clientX - lastPanPoint.x;
                const deltaY = e.clientY - lastPanPoint.y;
                
                if (ivChart) {
                    // Get chart scales
                    const xScale = ivChart.scales.x;
                    const yScale = ivChart.scales.y;
                    
                    if (xScale && yScale) {
                        // Calculate pixel to value conversion
                        const xRange = xScale.max - xScale.min;
                        const yRange = yScale.max - yScale.min;
                        const chartRect = chartCanvas.getBoundingClientRect();
                        const chartWidth = chartRect.width;
                        const chartHeight = chartRect.height;
                        
                        // Convert pixel movement to value movement
                        const xDelta = -(deltaX / chartWidth) * xRange;
                        const yDelta = (deltaY / chartHeight) * yRange;
                        
                        // Update scale ranges
                        xScale.options.min = xScale.min + xDelta;
                        xScale.options.max = xScale.max + xDelta;
                        yScale.options.min = yScale.min + yDelta;
                        yScale.options.max = yScale.max + yDelta;
                        
                        // Update chart
                        ivChart.update('none');
                    }
                }
                
                lastPanPoint = { x: e.clientX, y: e.clientY };
            }
        });
        
        chartCanvas.addEventListener('mouseup', () => {
            if (isPanning) {
                isPanning = false;
                lastPanPoint = null;
                chartCanvas.style.cursor = 'crosshair';
            }
        });
        
        chartCanvas.addEventListener('mouseleave', () => {
            if (isPanning) {
                isPanning = false;
                lastPanPoint = null;
                chartCanvas.style.cursor = 'crosshair';
            }
        });
    }
});

