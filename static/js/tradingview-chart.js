// TradingView Lightweight Charts implementation
let chart = null;
let series = null;
let fetchInterval = null;

// Export loginToAPI to window for onclick handler (will be set after function definition)

// Initialize TradingView chart
function initChart() {
    try {
        const chartContainer = document.getElementById('ivChart');
        if (!chartContainer) {
            console.error('Chart container not found');
            return false;
        }
        
        console.log('Initializing TradingView chart...');
        console.log('Chart container found:', chartContainer);
        console.log('Container size:', chartContainer.clientWidth, 'x', chartContainer.clientHeight);
        
        // Clear any existing chart
        chartContainer.innerHTML = '';
        
        // Check if LightweightCharts is available
        if (typeof LightweightCharts === 'undefined') {
            console.error('LightweightCharts library not loaded!');
            return false;
        }
        
        // Create chart with TradingView styling
        chart = LightweightCharts.createChart(chartContainer, {
            layout: {
                background: { type: 'solid', color: '#0f0f23' },
                textColor: '#a0a0b8',
                fontSize: 12,
                fontFamily: 'Segoe UI, Tahoma, Geneva, Verdana, sans-serif',
            },
            grid: {
                vertLines: {
                    color: '#2d2d44',
                    style: 1,
                    visible: true,
                },
                horzLines: {
                    color: '#2d2d44',
                    style: 1,
                    visible: true,
                },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
                vertLine: {
                    color: '#8b5cf6',
                    width: 1,
                    style: 2,
                    labelBackgroundColor: '#8b5cf6',
                },
                horzLine: {
                    color: '#8b5cf6',
                    width: 1,
                    style: 2,
                    labelBackgroundColor: '#8b5cf6',
                },
            },
            rightPriceScale: {
                borderColor: '#2d2d44',
                scaleMargins: {
                    top: 0.1,
                    bottom: 0.1,
                },
            },
            timeScale: {
                borderColor: '#2d2d44',
                timeVisible: true,
                secondsVisible: false,
                rightOffset: 12,
                barSpacing: 3,
                fixLeftEdge: false,
                lockVisibleTimeRangeOnResize: false,
                rightBarStaysOnScroll: true,
                allowShiftVisibleRangeOnWhitespaceClick: true,
            },
            handleScroll: {
                mouseWheel: true,
                pressedMouseMove: true,
                horzTouchDrag: true,
                vertTouchDrag: true,
            },
            handleScale: {
                axisPressedMouseMove: {
                    time: true,
                    price: true,
                },
                axisDoubleClickReset: {
                    time: true,
                    price: true,
                },
                axisTouchDrag: {
                    time: true,
                    price: true,
                },
                mouseWheel: true,
                pinch: true,
            },
        });
        
        // Verify chart was created successfully
        if (!chart) {
            throw new Error('Chart creation returned null or undefined');
        }
        
        // Check if addLineSeries method exists
        if (typeof chart.addLineSeries !== 'function') {
            console.error('Chart object:', chart);
            console.error('Chart type:', typeof chart);
            console.error('Available methods:', Object.getOwnPropertyNames(chart));
            throw new Error('chart.addLineSeries is not a function. Chart may not be properly initialized.');
        }
        
        console.log('Chart created successfully, adding line series...');
        
        // Create line series for IV with TradingView style
        series = chart.addLineSeries({
            color: '#8b5cf6',  // Purple color matching the theme
            lineWidth: 2,
            priceLineVisible: false,
            lastValueVisible: true,
            crosshairMarkerVisible: true,
            crosshairMarkerRadius: 4,
            priceFormat: {
                type: 'price',
                precision: 2,
                minMove: 0.01,
            },
            title: 'Implied Volatility (%)',
            lineStyle: 0,  // Solid line
            pointMarkersVisible: false,  // No markers for cleaner look
        });
    
        // Enable crosshair tracking
        chart.subscribeCrosshairMove((param) => {
            if (param.point === undefined || !param.time || param.point.x < 0 || param.point.x > chartContainer.clientWidth || param.point.y < 0 || param.point.y > chartContainer.clientHeight) {
                return;
            }
        });
        
        console.log('Chart and series initialized successfully');
        console.log('Chart container size:', chartContainer.clientWidth, 'x', chartContainer.clientHeight);
        console.log('Series object:', series);
        return true;
    } catch (error) {
        console.error('Error initializing chart:', error);
        console.error('Error stack:', error.stack);
        chart = null;
        series = null;
        return false;
    }
}

// Reset zoom function
function resetZoom() {
    if (chart && series) {
        chart.timeScale().fitContent();
        showNotification('Zoom reset', 'info');
    }
}

// Zoom to latest values
function zoomToLatest(dataPoints = 50) {
    if (!chart || !series) return;
    
    const data = series.data();
    if (!data || data.length === 0) return;
    
    const totalPoints = data.length;
    if (totalPoints <= dataPoints) {
        chart.timeScale().fitContent();
        return;
    }
    
    // Get the last N data points
    const startIndex = totalPoints - dataPoints;
    const startTime = data[startIndex].time;
    const endTime = data[data.length - 1].time;
    
    // Set visible range
    chart.timeScale().setVisibleRange({
        from: startTime,
        to: endTime,
    });
}

// Fetch IV data and update chart
async function fetchIVData(symbol) {
    try {
        console.log('Fetching IV data for symbol:', symbol);
        const response = await fetch(`/api/get_iv_data?symbol=${encodeURIComponent(symbol)}`);
        const data = await response.json();
        
        console.log('Received IV data:', {
            timestamps: data.timestamps?.length || 0,
            iv_values: data.iv_values?.length || 0,
            has_series: !!series
        });
        
        if (!series) {
            console.warn('Chart series not initialized, attempting to initialize...');
            // Try to initialize chart if it hasn't been initialized yet
            if (!chart) {
                initChart();
            }
            // If still no series after initialization attempt, wait a bit and retry
            if (!series) {
                console.error('Chart series still not initialized after attempt. Please refresh the page.');
                return;
            }
        }
        
        if (data.timestamps && data.timestamps.length > 0) {
            const previousLength = series.data().length;
            
            console.log('Sample timestamp:', data.timestamps[0]);
            console.log('Sample IV value:', data.iv_values[0]);
            
            // Convert data to TradingView format
            const chartData = data.timestamps.map((timestamp, index) => {
                // Parse timestamp - handle different formats
                let time;
                if (typeof timestamp === 'string') {
                    // Handle format like "2025-10-27 10:15:00" or "2025-10-27T10:15:00"
                    // Remove timezone info if present
                    let cleanTimestamp = timestamp.replace(/[+-]\d{2}:\d{2}$/, '').trim();
                    
                    // Try parsing as ISO or standard format
                    const date = new Date(cleanTimestamp);
                    if (isNaN(date.getTime())) {
                        // If parsing fails, try manual parsing
                        const match = cleanTimestamp.match(/(\d{4})-(\d{2})-(\d{2})[\sT](\d{2}):(\d{2}):(\d{2})/);
                        if (match) {
                            const [, year, month, day, hour, minute, second] = match;
                            const date2 = new Date(year, month - 1, day, hour, minute, second || 0);
                            time = Math.floor(date2.getTime() / 1000);
                        } else {
                            console.warn('Could not parse timestamp:', timestamp);
                            return null;
                        }
                    } else {
                        time = Math.floor(date.getTime() / 1000); // Convert to Unix timestamp (seconds)
                    }
                } else if (typeof timestamp === 'number') {
                    // If it's already a number, check if it's in seconds or milliseconds
                    time = timestamp > 10000000000 ? Math.floor(timestamp / 1000) : timestamp;
                } else {
                    console.warn('Invalid timestamp type:', typeof timestamp, timestamp);
                    return null;
                }
                
                const ivValue = parseFloat(data.iv_values[index]);
                // Don't skip zero values - they might be valid (just plot them)
                if (isNaN(ivValue)) {
                    console.warn('Invalid IV value at index', index, ':', data.iv_values[index]);
                    return null;
                }
                
                return {
                    time: time,
                    value: ivValue,
                };
            }).filter(item => item !== null); // Remove null entries
            
            console.log(`Updating chart with ${chartData.length} data points (previous: ${previousLength})`);
            console.log('First data point:', chartData[0]);
            console.log('Last data point:', chartData[chartData.length - 1]);
            
            if (chartData.length > 0) {
                // Sort data by time to ensure proper line rendering
                chartData.sort((a, b) => a.time - b.time);
                
                // Update series data - TradingView expects array of {time, value}
                try {
                    console.log('Setting chart data...', chartData.length, 'points');
                    console.log('Sample data point:', JSON.stringify(chartData[0]));
                    
                    // Set the data directly
                    series.setData(chartData);
                    
                    console.log(`Chart data set successfully. Series now has ${series.data().length} points`);
                    
                    // Force chart to fit content and update
                    setTimeout(() => {
                        chart.timeScale().fitContent();
                        console.log('Chart fitContent called');
                    }, 100);
                    
                    console.log(`Chart updated with ${chartData.length} data points`);
                    const values = chartData.map(d => d.value).filter(v => !isNaN(v));
                    if (values.length > 0) {
                        console.log(`IV range: ${Math.min(...values).toFixed(2)}% - ${Math.max(...values).toFixed(2)}%`);
                    }
                    
                    // If this is the first data load, zoom to show all data
                    if (previousLength === 0) {
                        setTimeout(() => {
                            chart.timeScale().fitContent();
                            console.log('First load - fitContent called');
                        }, 500);
                    } else if (data.timestamps.length > previousLength) {
                        // New data added - zoom to latest
                        setTimeout(() => {
                            zoomToLatest(50);
                        }, 300);
                    }
                } catch (error) {
                    console.error('Error setting chart data:', error);
                    console.error('Chart data sample:', chartData.slice(0, 3));
                    console.error('Error details:', error.message, error.stack);
                }
            } else {
                console.warn('No valid IV data points to plot (all values are 0 or invalid)');
                console.warn('Timestamps:', data.timestamps.slice(0, 3));
                console.warn('IV values:', data.iv_values.slice(0, 3));
            }
        } else if (data.timestamps && data.timestamps.length === 0) {
            console.log('Waiting for data... (no timestamps received)');
        } else {
            console.warn('No timestamps in response:', data);
        }
    } catch (error) {
        console.error('Error fetching IV data:', error);
    }
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
    console.log('loginToAPI function called');
    
    // Ensure function is globally accessible
    if (typeof window !== 'undefined') {
        window.loginToAPI = loginToAPI;
    }
    
    const btn = document.getElementById('loginBtn');
    if (!btn) {
        console.error('Login button not found');
        alert('Login button not found. Please refresh the page.');
        return;
    }
    
    // Check if already logged in
    if (btn.disabled && btn.textContent === 'Logged In') {
        console.log('Already logged in');
        return;
    }
    
    btn.disabled = true;
    btn.textContent = 'Logging in...';
    
    // Show loading notification
    try {
        showNotification('Initiating login... This may take 10-15 seconds', 'info');
    } catch (e) {
        console.error('Error showing notification:', e);
    }
    
    try {
        console.log('Sending login request...');
        
        // Create abort controller for timeout (more compatible)
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 60000); // 60 seconds timeout
        
        const response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        console.log('Login response status:', response.status);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ message: `HTTP ${response.status}: ${response.statusText}` }));
            throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log('Login response data:', data);
        
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
        
        let errorMessage = 'Error connecting to server';
        if (error.name === 'AbortError' || error.name === 'TimeoutError' || error.message.includes('aborted')) {
            errorMessage = 'Login request timed out (60s). The login process may take longer. Please check server logs and try again.';
        } else if (error.message) {
            errorMessage = error.message;
        }
        
        showNotification(errorMessage, 'error');
        console.error('Login error:', error);
    }
}

// Start fetching data
async function startFetching() {
    console.log('startFetching function called');
    
    const symbolInput = document.getElementById('symbol');
    const timeframeInput = document.getElementById('timeframe');
    const strikeInput = document.getElementById('strike');
    const expiryInput = document.getElementById('expiry');
    const optionTypeInput = document.getElementById('optionType');
    
    if (!symbolInput || !timeframeInput) {
        console.error('Required input fields not found');
        showNotification('Error: Input fields not found. Please refresh the page.', 'error');
        return;
    }
    
    const symbol = symbolInput.value.trim();
    const timeframe = timeframeInput.value;
    const strike = strikeInput ? strikeInput.value : '';
    const expiry = expiryInput ? expiryInput.value : '';
    const optionType = optionTypeInput ? optionTypeInput.value : '';
    
    console.log('Fetching parameters:', { symbol, timeframe, strike, expiry, optionType });
    
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
    
    console.log('Sending start_fetching request with payload:', payload);
    
    try {
        const response = await fetch('/api/start_fetching', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(payload)
        });
        
        console.log('Start fetching response status:', response.status);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ message: `HTTP ${response.status}: ${response.statusText}` }));
            throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        console.log('Start fetching response data:', data);
        
        if (data.success) {
            const fetchStatus = document.getElementById('fetchStatus');
            if (fetchStatus) {
                fetchStatus.textContent = 'Fetching...';
                fetchStatus.classList.add('active');
            }
            showNotification('Data fetching started', 'success');
            
            // Reset chart zoom when starting new fetch
            if (chart) {
                chart.timeScale().fitContent();
            }
            
            // Start polling for IV data
            startPollingIVData(symbol);
        } else {
            showNotification(data.message || 'Failed to start fetching', 'error');
        }
    } catch (error) {
        showNotification(error.message || 'Error starting data fetch', 'error');
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
    
    console.log(`Starting polling for IV data: ${symbol}`);
    
    // Fetch immediately
    fetchIVData(symbol);
    
    // Then poll every 2 seconds
    fetchInterval = setInterval(() => {
        fetchIVData(symbol);
        checkFetchStatus();
    }, 2000);
    
    console.log('Polling started - fetching IV data every 2 seconds');
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
    console.log('DOM loaded, initializing...');
    
    try {
        initChart();
        checkLoginStatus();
        
        // Make loginToAPI globally accessible for onclick handler
        window.loginToAPI = loginToAPI;
        
        // Attach login button event listener
        const loginBtn = document.getElementById('loginBtn');
        if (loginBtn) {
            loginBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                console.log('Login button clicked (event listener)');
                loginToAPI();
            });
            console.log('Login button event listener attached');
        } else {
            console.error('Login button not found in DOM');
        }
        
        // Make startFetching globally accessible
        window.startFetching = startFetching;
        window.stopFetching = stopFetching;
        
        // Attach other button listeners with error handling
        const startBtn = document.getElementById('startBtn');
        if (startBtn) {
            startBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                console.log('Start button clicked');
                startFetching();
            });
            console.log('Start button event listener attached');
        } else {
            console.error('Start button not found');
        }
        
        const stopBtn = document.getElementById('stopBtn');
        if (stopBtn) {
            stopBtn.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                console.log('Stop button clicked');
                stopFetching();
            });
            console.log('Stop button event listener attached');
        } else {
            console.error('Stop button not found');
        }
    } catch (error) {
        console.error('Error during initialization:', error);
    }
    
    const resetBtn = document.getElementById('resetZoomBtn');
    if (resetBtn) {
        resetBtn.addEventListener('click', resetZoom);
    }
    
    // Double-click on chart to reset zoom
    const chartContainer = document.getElementById('ivChart');
    if (chartContainer) {
        chartContainer.addEventListener('dblclick', resetZoom);
    }
    
    // Handle window resize
    window.addEventListener('resize', () => {
        if (chart) {
            chart.applyOptions({ width: chartContainer.clientWidth, height: chartContainer.clientHeight });
        }
    });
});

