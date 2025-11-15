// TradingView Lightweight Charts implementation
let chart = null;
let series = null;
let fetchInterval = null;
// Store full data for crosshair tooltip (IV, option price, underlying price by timestamp)
let chartDataMap = new Map(); // Maps timestamp (Unix seconds) to {iv, optionPrice, underlyingPrice}

// Helper function to convert IST timestamp string to Unix timestamp
// CSV timestamps are in IST format: "2025-11-13 15:29:00"
// We need to create a Unix timestamp that represents this IST time correctly
// The chart library will display it in the browser's timezone, so we need to ensure
// the Unix timestamp correctly represents the IST time
function convertToIST(timestamp) {
    try {
        if (typeof timestamp === 'string') {
            // Parse timestamp string (format: "2025-11-13 15:29:00" or "2025-11-13T15:29:00")
            // Remove timezone info if present
            let cleanTimestamp = timestamp.replace(/[+-]\d{2}:\d{2}$/, '').trim();
            
            // Try to parse the date components
            const match = cleanTimestamp.match(/(\d{4})-(\d{2})-(\d{2})[\sT](\d{2}):(\d{2}):(\d{2})/);
            if (match) {
                const [, year, month, day, hour, minute, second] = match;
                
                // Parse components as integers
                const yearInt = parseInt(year);
                const monthInt = parseInt(month) - 1; // Month is 0-indexed
                const dayInt = parseInt(day);
                const hourInt = parseInt(hour);
                const minuteInt = parseInt(minute);
                const secondInt = parseInt(second || 0);
                
                // CSV timestamps are in IST (UTC+5:30)
                // Parse the timestamp with explicit IST timezone offset (+05:30)
                // This ensures JavaScript Date correctly interprets it as IST time
                // Format: "2025-11-13T15:29:00+05:30"
                const istTimestampString = `${yearInt}-${String(monthInt + 1).padStart(2, '0')}-${String(dayInt).padStart(2, '0')}T${String(hourInt).padStart(2, '0')}:${String(minuteInt).padStart(2, '0')}:${String(secondInt).padStart(2, '0')}+05:30`;
                
                const istDate = new Date(istTimestampString);
                
                if (isNaN(istDate.getTime())) {
                    // Fallback: Manual calculation
                    // Create UTC date and subtract IST offset
                    const utcDate = new Date(Date.UTC(yearInt, monthInt, dayInt, hourInt, minuteInt, secondInt));
                    const istOffsetMs = 5.5 * 60 * 60 * 1000; // 5 hours 30 minutes
                    const correctUTCDate = new Date(utcDate.getTime() - istOffsetMs);
                    
                    if (!isNaN(correctUTCDate.getTime())) {
                        return Math.floor(correctUTCDate.getTime() / 1000);
                    }
                    
                    // Last resort: parse as local timezone
                    const lastResort = new Date(cleanTimestamp);
                    if (!isNaN(lastResort.getTime())) {
                        return Math.floor(lastResort.getTime() / 1000);
                    }
                    return null;
                }
                
                return Math.floor(istDate.getTime() / 1000);
            } else {
                // Try standard Date parsing as fallback
                const date = new Date(cleanTimestamp);
                if (!isNaN(date.getTime())) {
                    return Math.floor(date.getTime() / 1000);
                }
                return null;
            }
        } else if (typeof timestamp === 'number') {
            // If it's already a Unix timestamp, return as-is (Unix timestamps are timezone-agnostic)
            return timestamp > 10000000000 ? Math.floor(timestamp / 1000) : timestamp;
        }
        return null;
    } catch (e) {
        console.warn('Error converting timestamp to IST:', timestamp, e);
        return null;
    }
}

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
        
        // Ensure container has proper dimensions
        const containerWidth = chartContainer.clientWidth || chartContainer.offsetWidth || 800;
        const containerHeight = chartContainer.clientHeight || chartContainer.offsetHeight || 500;
        
        console.log('Container size:', containerWidth, 'x', containerHeight);
        
        // Set explicit dimensions if not set
        if (containerWidth === 0 || containerHeight === 0) {
            console.warn('Container has zero dimensions, setting defaults');
            chartContainer.style.width = '100%';
            chartContainer.style.height = '500px';
        }
        
        // Clear any existing chart
        chartContainer.innerHTML = '';
        
        // Check if LightweightCharts is available (try different possible global names)
        let LightweightChartsLib = null;
        if (typeof LightweightCharts !== 'undefined') {
            LightweightChartsLib = LightweightCharts;
        } else if (typeof lightweightCharts !== 'undefined') {
            LightweightChartsLib = lightweightCharts;
        } else if (typeof window.LightweightCharts !== 'undefined') {
            LightweightChartsLib = window.LightweightCharts;
        } else {
            console.error('LightweightCharts library not loaded!');
            console.error('Available globals:', Object.keys(window).filter(k => k.toLowerCase().includes('chart')));
            return false;
        }
        
        if (!LightweightChartsLib || typeof LightweightChartsLib.createChart !== 'function') {
            console.error('LightweightCharts.createChart is not a function');
            return false;
        }
        
        console.log('Using LightweightCharts library:', LightweightChartsLib);
        console.log('createChart method:', typeof LightweightChartsLib.createChart);
        
        // Create chart with TradingView styling - add explicit width and height
        chart = LightweightChartsLib.createChart(chartContainer, {
            width: containerWidth,
            height: containerHeight,
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
                mode: LightweightChartsLib.CrosshairMode.Normal,
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
    
        // Enable crosshair tracking for tooltip
        chart.subscribeCrosshairMove((param) => {
            const tooltip = document.getElementById('crosshairTooltip');
            if (!tooltip) return;
            
            if (param.point === undefined || !param.time || param.point.x < 0 || param.point.x > chartContainer.clientWidth || param.point.y < 0 || param.point.y > chartContainer.clientHeight) {
                tooltip.style.display = 'none';
                return;
            }
            
            // Get timestamp from crosshair
            const timestamp = typeof param.time === 'number' ? param.time : param.time.getTime() / 1000;
            
            // Find closest data point
            const dataPoint = findClosestDataPoint(timestamp);
            
            if (dataPoint) {
                // Update tooltip values
                const tooltipIV = document.getElementById('tooltipIV');
                const tooltipOptionPrice = document.getElementById('tooltipOptionPrice');
                const tooltipUnderlyingPrice = document.getElementById('tooltipUnderlyingPrice');
                
                if (tooltipIV) {
                    tooltipIV.textContent = dataPoint.iv !== null && dataPoint.iv !== undefined ? `${dataPoint.iv.toFixed(2)}%` : '-';
                }
                if (tooltipOptionPrice) {
                    tooltipOptionPrice.textContent = dataPoint.optionPrice !== null && dataPoint.optionPrice !== undefined ? dataPoint.optionPrice.toFixed(2) : '-';
                }
                if (tooltipUnderlyingPrice) {
                    tooltipUnderlyingPrice.textContent = dataPoint.underlyingPrice !== null && dataPoint.underlyingPrice !== undefined ? dataPoint.underlyingPrice.toFixed(2) : '-';
                }
                
                // Show tooltip
                tooltip.style.display = 'block';
            } else {
                tooltip.style.display = 'none';
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

// Find closest data point for a given timestamp
function findClosestDataPoint(timestamp) {
    if (chartDataMap.size === 0) return null;
    
    let closestTime = null;
    let minDiff = Infinity;
    
    // Find closest timestamp
    for (const [time, data] of chartDataMap.entries()) {
        const diff = Math.abs(time - timestamp);
        if (diff < minDiff) {
            minDiff = diff;
            closestTime = time;
        }
    }
    
    // Only return if within 60 seconds (1 minute tolerance)
    if (minDiff <= 60 && closestTime !== null) {
        return chartDataMap.get(closestTime);
    }
    
    return null;
}

// Update chart title with contract name
function updateChartTitle(symbol) {
    const chartContractName = document.getElementById('chartContractName');
    const chartTitle = document.getElementById('chartTitle');
    
    if (chartContractName && chartTitle) {
        if (symbol) {
            // Display contract name before "Implied Volatility Chart"
            chartContractName.textContent = `${symbol} - `;
            chartTitle.textContent = 'Implied Volatility Chart';
        } else {
            // No symbol, show default title
            chartContractName.textContent = '';
            chartTitle.textContent = 'Implied Volatility Chart';
        }
    }
}

// Fetch IV data and update chart
async function fetchIVData(symbol) {
    try {
        // Update chart title with current symbol
        updateChartTitle(symbol);
        
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
            
            // Convert data to TradingView format with validation
            const chartData = data.timestamps.map((timestamp, index) => {
                // Validate timestamp exists
                if (!timestamp && timestamp !== 0) {
                    return null;
                }
                
                // Parse timestamp and convert to IST
                let time;
                try {
                    // Convert timestamp to IST and get Unix timestamp
                    time = convertToIST(timestamp);
                    
                    if (!time || isNaN(time) || time <= 0) {
                        console.warn('Invalid timestamp after IST conversion:', timestamp);
                        return null;
                    }
                } catch (e) {
                    console.warn('Error parsing timestamp:', timestamp, e);
                    return null;
                }
                
                // Validate IV value
                const ivValue = parseFloat(data.iv_values[index]);
                if (isNaN(ivValue) || ivValue < 0) {
                    console.warn('Invalid IV value at index', index, ':', data.iv_values[index]);
                    return null;
                }
                
                // Get option price and underlying price for this timestamp
                const optionPrice = data.close_prices && data.close_prices[index] !== undefined ? parseFloat(data.close_prices[index]) : null;
                const underlyingPrice = data.fclose_prices && data.fclose_prices[index] !== undefined ? parseFloat(data.fclose_prices[index]) : null;
                
                // Store in data map for crosshair tooltip
                chartDataMap.set(time, {
                    iv: ivValue,
                    optionPrice: optionPrice,
                    underlyingPrice: underlyingPrice
                });
                
                return {
                    time: time,
                    value: ivValue,
                };
            }).filter(item => item !== null && item.time > 0 && !isNaN(item.time) && !isNaN(item.value) && item.value >= 0); // Remove null entries and validate
            
            console.log(`Updating chart with ${chartData.length} data points (previous: ${previousLength})`);
            console.log('First data point:', chartData[0]);
            console.log('Last data point:', chartData[chartData.length - 1]);
            
            if (chartData.length > 0) {
                // Sort data by time to ensure proper line rendering
                chartData.sort((a, b) => a.time - b.time);
                
                // Update series data - TradingView expects array of {time, value}
                try {
                    console.log('Setting chart data...', chartData.length, 'points');
                    if (chartData.length > 0) {
                        console.log('Sample data point:', JSON.stringify(chartData[0]));
                    }
                    
                    // Validate series exists and is ready
                    if (!series || typeof series.setData !== 'function') {
                        console.error('Series is not ready or setData is not a function');
                        return;
                    }
                    
                    // Check if we have existing data to determine update strategy
                    const existingData = series.data();
                    const hasExistingData = existingData && existingData.length > 0;
                    
                    if (hasExistingData && previousLength > 0) {
                        // Incremental update: Preserve current zoom when updating data
                        const currentVisibleRange = chart.timeScale().getVisibleRange();
                        const dataLength = series.data().length;
                        const isAtLatestPoint = dataLength > 0 && currentVisibleRange && currentVisibleRange.to;
                        
                        // Check if user is viewing the latest point (within 5 minutes tolerance)
                        let shouldPreserveZoom = true;
                        if (isAtLatestPoint && dataLength > 0) {
                            const latestDataTime = series.data()[dataLength - 1].time;
                            const visibleEndTime = typeof currentVisibleRange.to === 'number' ? currentVisibleRange.to : currentVisibleRange.to.getTime() / 1000;
                            const timeDiff = Math.abs(latestDataTime - visibleEndTime);
                            // If user is within 5 minutes of latest point, allow auto-scroll
                            if (timeDiff <= 300) {
                                shouldPreserveZoom = false;
                            }
                        }
                        
                        // Update all data
                        series.setData(chartData);
                        
                        // Restore the previous visible range to preserve zoom only if user has panned away
                        if (shouldPreserveZoom && currentVisibleRange && currentVisibleRange.from && currentVisibleRange.to) {
                            setTimeout(() => {
                                try {
                                    chart.timeScale().setVisibleRange(currentVisibleRange);
                                    console.log('Preserved zoom/pan position (user has panned away)');
                                } catch (e) {
                                    console.warn('Could not restore visible range:', e);
                                }
                            }, 50);
                        } else {
                            console.log('User at latest point - allowing auto-scroll');
                        }
                        console.log(`Updated chart data`);
                    } else {
                        // First load: Set all data and fit content
                        series.setData(chartData);
                        console.log(`Chart data set successfully. Series now has ${series.data().length} points`);
                        
                        // Only fit content on first load
                        setTimeout(() => {
                            chart.timeScale().fitContent();
                            console.log('First load - fitContent called');
                        }, 100);
                    }
                    
                    console.log(`Chart updated with ${chartData.length} data points`);
                    const values = chartData.map(d => d.value).filter(v => !isNaN(v));
                    if (values.length > 0) {
                        console.log(`IV range: ${Math.min(...values).toFixed(2)}% - ${Math.max(...values).toFixed(2)}%`);
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
// Toggle between manual and automatic mode
function toggleMode() {
    const mode = document.getElementById('mode').value;
    const manualFields = document.getElementById('manualModeFields');
    const automaticFields = document.getElementById('automaticModeFields');
    
    if (mode === 'automatic') {
        manualFields.style.display = 'none';
        automaticFields.style.display = 'block';
    } else {
        manualFields.style.display = 'block';
        automaticFields.style.display = 'none';
    }
}

// Make toggleMode available globally
window.toggleMode = toggleMode;

async function startFetching() {
    console.log('startFetching function called');
    
    const modeInput = document.getElementById('mode');
    const timeframeInput = document.getElementById('timeframe');
    
    if (!modeInput || !timeframeInput) {
        console.error('Required input fields not found');
        showNotification('Error: Input fields not found. Please refresh the page.', 'error');
        return;
    }
    
    const mode = modeInput.value;
    const timeframe = timeframeInput.value;
    
    // Get risk-free rate (default 10% = 0.10)
    const riskFreeRateInput = document.getElementById('riskFreeRate');
    const riskFreeRate = riskFreeRateInput && riskFreeRateInput.value ? parseFloat(riskFreeRateInput.value) / 100 : 0.10;
    
    // Build request payload
    const payload = { mode, timeframe, risk_free_rate: riskFreeRate };
    
    if (mode === 'automatic') {
        // Automatic mode: Get future symbol, expiry type, expiry date, option type
        const futureSymbolInput = document.getElementById('futureSymbol');
        const expiryTypeInput = document.getElementById('expiryType');
        const autoExpiryDateInput = document.getElementById('autoExpiryDate');
        const autoOptionTypeInput = document.getElementById('autoOptionType');
        
        if (!futureSymbolInput || !expiryTypeInput || !autoExpiryDateInput || !autoOptionTypeInput) {
            showNotification('Error: Automatic mode fields not found. Please refresh the page.', 'error');
            return;
        }
        
        const futureSymbol = futureSymbolInput.value.trim();
        const expiryType = expiryTypeInput.value;
        const expiryDate = autoExpiryDateInput.value;
        const optionType = autoOptionTypeInput.value;
        const strikeStepInput = document.getElementById('strikeStep');
        const strikeStep = strikeStepInput && strikeStepInput.value ? parseFloat(strikeStepInput.value) : null;
        
        if (!futureSymbol) {
            showNotification('Please enter a future symbol', 'error');
            return;
        }
        
        if (!expiryDate) {
            showNotification('Please select an expiry date', 'error');
            return;
        }
        
        payload.future_symbol = futureSymbol;
        payload.expiry_type = expiryType;
        payload.expiry_date = expiryDate;
        payload.option_type = optionType;
        if (strikeStep !== null && strikeStep > 0) {
            payload.strike_step = strikeStep;
        }
        
        console.log('Automatic mode parameters:', payload);
    } else {
        // Manual mode: Get symbol and optional parameters
        const symbolInput = document.getElementById('symbol');
        const strikeInput = document.getElementById('strike');
        const expiryInput = document.getElementById('expiry');
        const optionTypeInput = document.getElementById('optionType');
        
        if (!symbolInput) {
            showNotification('Error: Symbol field not found. Please refresh the page.', 'error');
            return;
        }
        
        const symbol = symbolInput.value.trim();
        
        if (!symbol) {
            showNotification('Please enter a symbol', 'error');
            return;
        }
        
        payload.symbol = symbol;
        
        // Add optional parameters if provided
        if (strikeInput && strikeInput.value) {
            payload.strike = parseFloat(strikeInput.value);
        }
        if (expiryInput && expiryInput.value) {
            payload.expiry = expiryInput.value;
        }
        if (optionTypeInput && optionTypeInput.value) {
            payload.option_type = optionTypeInput.value;
        }
        
        console.log('Manual mode parameters:', payload);
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
            
            let message = 'Data fetching started';
            if (mode === 'automatic' && data.generated_symbol) {
                const strikeStepInfo = data.strike_step ? `, Strike Step: ${data.strike_step}` : '';
                message = `Automatic mode started. Generated symbol: ${data.generated_symbol} (Future LTP: ${data.future_ltp}, ATM Strike: ${data.atm_strike}${strikeStepInfo})`;
            }
            showNotification(message, 'success');
            
            // Clear chart data before loading new data
            if (series) {
                console.log('Clearing chart data before loading new symbol...');
                series.setData([]);
                chartDataMap.clear(); // Clear the data map
            }
            
            // Update chart title with current symbol
            const symbolToPoll = mode === 'automatic' ? data.generated_symbol : payload.symbol;
            if (symbolToPoll) {
                updateChartTitle(symbolToPoll);
            }
            
            // Start polling for IV data (use generated symbol for automatic mode)
            startPollingIVData(symbolToPoll);
            
            // Also try to load CSV data for this symbol after a delay (once first data is saved)
            // This ensures we load fresh CSV data if it exists
            setTimeout(() => {
                if (symbolToPoll) {
                    console.log(`Attempting to load CSV data for ${symbolToPoll}...`);
                    loadCSVData(symbolToPoll).catch(err => {
                        console.log('CSV data not available yet, will load when available:', err);
                    });
                }
            }, 5000); // Wait 5 seconds for first data to be saved
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
    
    // Then poll every 1 second
    // In automatic mode, get the current symbol from status (updated every second)
    fetchInterval = setInterval(async () => {
        try {
            // Check if we're in automatic mode and get current symbol from status
            const statusResponse = await fetch('/api/get_status');
            if (statusResponse.ok) {
                const status = await statusResponse.json();
                if (status.active && status.mode === 'automatic' && status.symbol) {
                    // Use the current symbol from status (updated every second in automatic mode)
                    // Update chart title if symbol changed
                    updateChartTitle(status.symbol);
                    fetchIVData(status.symbol);
                } else if (status.active && status.symbol) {
                    // Manual mode or fallback
                    // Update chart title if symbol changed
                    updateChartTitle(status.symbol);
                    fetchIVData(status.symbol);
                } else {
                    // Fallback to original symbol
                    fetchIVData(symbol);
                }
            } else {
                // Fallback to original symbol
                fetchIVData(symbol);
            }
        } catch (error) {
            console.error('Error getting status for polling:', error);
            // Fallback to original symbol
            fetchIVData(symbol);
        }
        checkFetchStatus();
    }, 1000);
    
    console.log('Polling started - fetching IV data every 1 second');
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

// Load CSV data on page load
async function loadCSVData(symbol = null) {
    try {
        console.log('Loading CSV data...', symbol ? `for symbol: ${symbol}` : '');
        // If symbol is provided, load that specific symbol's CSV, otherwise load most recent
        const url = symbol ? `/api/load_csv_data?symbol=${encodeURIComponent(symbol)}` : '/api/load_csv_data';
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.success && data.timestamps && data.timestamps.length > 0) {
            console.log(`Loaded ${data.data_points} data points from CSV for symbol: ${data.symbol}`);
            
            // Update chart title with symbol from CSV
            if (data.symbol) {
                updateChartTitle(data.symbol);
            }
            
            // Ensure chart is initialized
            if (!chart || !series) {
                console.log('Chart not initialized, initializing now...');
                if (!initChart()) {
                    console.error('Failed to initialize chart');
                    return;
                }
            }
            
            // Wait a bit for chart to be ready
            await new Promise(resolve => setTimeout(resolve, 100));
            
            // Convert CSV data to chart format and plot
            const chartData = [];
            
            for (let index = 0; index < data.timestamps.length; index++) {
                const timestamp = data.timestamps[index];
                const ivValue = data.iv_values[index];
                
                // Skip if timestamp or IV value is missing
                if (!timestamp || ivValue === null || ivValue === undefined) {
                    continue;
                }
                
                let time;
                try {
                    // Convert timestamp to IST and get Unix timestamp
                    time = convertToIST(timestamp);
                    
                    // Debug: Log first few conversions to verify
                    if (index < 3 || index === data.timestamps.length - 1) {
                        const testDate = new Date(time * 1000);
                        console.log(`CSV Timestamp: ${timestamp} -> Unix: ${time} -> Display: ${testDate.toLocaleString('en-IN', {timeZone: 'Asia/Kolkata'})}`);
                    }
                    
                    // Validate time is valid
                    if (!time || time <= 0 || isNaN(time)) {
                        continue;
                    }
                } catch (e) {
                    continue; // Skip on error
                }
                
                // Validate IV value
                const parsedIv = parseFloat(ivValue);
                if (isNaN(parsedIv) || parsedIv < 0) {
                    continue;
                }
                
                // Get option price and underlying price for this timestamp
                const optionPrice = data.close_prices && data.close_prices[index] !== undefined ? parseFloat(data.close_prices[index]) : null;
                const underlyingPrice = data.fclose_prices && data.fclose_prices[index] !== undefined ? parseFloat(data.fclose_prices[index]) : null;
                
                // Store in data map for crosshair tooltip
                chartDataMap.set(time, {
                    iv: parsedIv,
                    optionPrice: optionPrice,
                    underlyingPrice: underlyingPrice
                });
                
                // Create data point - ensure both time and value are valid numbers
                const dataPoint = {
                    time: time,
                    value: parsedIv
                };
                
                // Final validation before adding
                if (dataPoint.time && dataPoint.value && 
                    !isNaN(dataPoint.time) && !isNaN(dataPoint.value) &&
                    dataPoint.time > 0 && dataPoint.value >= 0) {
                    chartData.push(dataPoint);
                }
            }
            
            console.log(`Validated ${chartData.length} data points from ${data.timestamps.length} total points`);
            
            // Debug: Check first few data points
            if (chartData.length > 0) {
                console.log('Sample data points:', chartData.slice(0, 3));
                console.log('Last data points:', chartData.slice(-3));
                
                // Check for any null/undefined values
                const hasNulls = chartData.some(item => item === null || item === undefined || item.time === null || item.value === null);
                if (hasNulls) {
                    console.warn('WARNING: Found null values in chartData!');
                    const nullItems = chartData.filter(item => item === null || item === undefined || item.time === null || item.value === null);
                    console.warn('Null items:', nullItems);
                }
            }
            
            if (chartData.length > 0) {
                // Validate series exists
                if (!series || typeof series.setData !== 'function') {
                    console.error('Series is not ready for CSV data');
                    return;
                }
                
                // Sort data by time to ensure proper rendering
                chartData.sort((a, b) => {
                    if (a.time !== b.time) {
                        return a.time - b.time;
                    }
                    // If timestamps are equal, sort by value (for deduplication)
                    return a.value - b.value;
                });
                
                // Final validation - remove any remaining invalid entries
                let finalData = chartData.filter(item => {
                    return item && 
                           typeof item.time === 'number' && 
                           typeof item.value === 'number' &&
                           !isNaN(item.time) && 
                           !isNaN(item.value) &&
                           item.time > 0 && 
                           item.value >= 0;
                });
                
                // CRITICAL FIX: Remove duplicate timestamps
                // The library throws "Value is null" errors when there are duplicate timestamps
                // We'll keep only the last value for each unique timestamp
                const uniqueData = new Map();
                for (const item of finalData) {
                    // If timestamp already exists, keep the one with the later index (more recent)
                    if (!uniqueData.has(item.time)) {
                        uniqueData.set(item.time, item);
                    } else {
                        // Replace with current item (since data is sorted, this keeps the last occurrence)
                        uniqueData.set(item.time, item);
                    }
                }
                
                // Convert Map back to array and sort
                finalData = Array.from(uniqueData.values()).sort((a, b) => a.time - b.time);
                
                console.log(`After deduplication: ${finalData.length} points (removed ${chartData.length - finalData.length} duplicates)`);
                
                // Remove flat line segments - filter out consecutive points with the same value
                // This removes the flat horizontal line that appears when data stops changing
                const filteredData = [];
                const MIN_VALUE_CHANGE = 0.0001; // Minimum change to consider significant
                
                for (let i = 0; i < finalData.length; i++) {
                    const current = finalData[i];
                    const previous = filteredData[filteredData.length - 1];
                    
                    // Always keep the first point
                    if (filteredData.length === 0) {
                        filteredData.push(current);
                        continue;
                    }
                    
                    // Keep point if value changed significantly OR if time gap is large (new day/session)
                    const valueChanged = Math.abs(current.value - previous.value) >= MIN_VALUE_CHANGE;
                    const timeGap = current.time - previous.time;
                    const largeTimeGap = timeGap > 3600; // More than 1 hour gap
                    
                    if (valueChanged || largeTimeGap) {
                        filteredData.push(current);
                    }
                    // Otherwise, skip this point (same value as previous)
                }
                
                // Also remove trailing flat segments (where last N points all have the same value)
                // This handles cases where data collection stopped but same value keeps repeating
                if (filteredData.length > 10) {
                    const lastValue = filteredData[filteredData.length - 1].value;
                    let flatSegmentStart = filteredData.length - 1;
                    
                    // Find where the flat segment starts
                    for (let i = filteredData.length - 2; i >= 0; i--) {
                        if (Math.abs(filteredData[i].value - lastValue) < MIN_VALUE_CHANGE) {
                            flatSegmentStart = i;
                        } else {
                            break;
                        }
                    }
                    
                    // If flat segment is more than 10% of data, remove it
                    const flatSegmentLength = filteredData.length - flatSegmentStart;
                    if (flatSegmentLength > filteredData.length * 0.1 && flatSegmentStart > 0) {
                        console.log(`Removing trailing flat segment: ${flatSegmentLength} points with value ${lastValue.toFixed(4)}`);
                        filteredData.splice(flatSegmentStart + 1); // Keep one point at the start of flat segment
                    }
                }
                
                finalData = filteredData;
                
                console.log(`After removing flat segments: ${finalData.length} points (removed ${chartData.length - finalData.length} total)`);
                
                // Check for remaining duplicates
                const timeSet = new Set();
                const duplicates = [];
                finalData.forEach((item, index) => {
                    if (timeSet.has(item.time)) {
                        duplicates.push({ index, time: item.time, value: item.value });
                    }
                    timeSet.add(item.time);
                });
                
                if (duplicates.length > 0) {
                    console.warn(`WARNING: Still found ${duplicates.length} duplicate timestamps after deduplication!`, duplicates.slice(0, 5));
                }
                
                if (finalData.length === 0) {
                    console.error('No valid data points to plot');
                    return;
                }
                
                // For large datasets, use update method instead of setData
                // This is more efficient and less error-prone
                try {
                    // Clear any existing data first
                    series.setData([]);
                    
                    // Wait a moment for the clear to take effect
                    await new Promise(resolve => setTimeout(resolve, 50));
                    
                    // Set all data at once - the library should handle it
                    // But we'll do it in one go with validated data
                    series.setData(finalData);
                    
                    console.log(`Successfully set ${finalData.length} data points`);
                } catch (e) {
                    console.error('Error setting CSV data to chart:', e);
                    console.error('Error details:', e.message, e.stack);
                    
                    // Try setting in smaller chunks as fallback
                    console.log('Attempting to set data in smaller chunks...');
                    const CHUNK_SIZE = 500;
                    try {
                        series.setData([]);
                        for (let i = 0; i < finalData.length; i += CHUNK_SIZE) {
                            const chunk = finalData.slice(0, i + CHUNK_SIZE);
                            series.setData(chunk);
                            await new Promise(resolve => setTimeout(resolve, 20));
                        }
                        console.log('Data set successfully in chunks');
                    } catch (chunkError) {
                        console.error('Failed to set data even in chunks:', chunkError);
                    }
                }
                
                console.log(`Successfully plotted ${chartData.length} data points from CSV`);
                
                // Only auto-zoom if chart is empty (first load)
                // Otherwise preserve user's current zoom/pan position
                const existingData = series.data();
                if (!existingData || existingData.length === 0) {
                    setTimeout(() => {
                        if (chart && chart.timeScale) {
                            chart.timeScale().fitContent();
                            console.log('First CSV load - fitContent called');
                        }
                    }, 200);
                } else {
                    console.log('CSV data loaded - preserving zoom/pan position');
                }
                
                // Update fetch status
                const fetchStatus = document.getElementById('fetchStatus');
                if (fetchStatus) {
                    fetchStatus.textContent = `Loaded ${chartData.length} points from CSV`;
                    fetchStatus.style.backgroundColor = '#10b981';
                }
            } else {
                console.warn('No valid data points to plot from CSV');
            }
        } else {
            console.log('No CSV data available or empty CSV file');
        }
    } catch (error) {
        console.error('Error loading CSV data:', error);
    }
}

// Event listeners
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, initializing...');
    
    try {
        // Wait for LightweightCharts library to load and DOM to be ready
        const checkLibrary = setInterval(() => {
            const chartContainer = document.getElementById('ivChart');
            const hasLibrary = typeof LightweightCharts !== 'undefined' || typeof lightweightCharts !== 'undefined' || typeof window.LightweightCharts !== 'undefined';
            const hasContainer = chartContainer && chartContainer.offsetWidth > 0 && chartContainer.offsetHeight > 0;
            
            if (hasLibrary && hasContainer) {
                clearInterval(checkLibrary);
                console.log('LightweightCharts library loaded and container ready, initializing chart...');
                
                // Small delay to ensure everything is ready
                setTimeout(() => {
                    if (initChart()) {
                        checkLoginStatus();
                        
                        // Load CSV data after chart is initialized
                        setTimeout(() => {
                            loadCSVData();
                        }, 300);
                    } else {
                        console.error('Chart initialization failed');
                    }
                }, 100);
            } else if (hasLibrary && !hasContainer) {
                console.log('Library loaded but container not ready yet...');
            }
        }, 100);
        
        // Timeout after 5 seconds
        setTimeout(() => {
            clearInterval(checkLibrary);
            if (!chart) {
                console.error('LightweightCharts library failed to load or container not ready after 5 seconds');
                const chartContainer = document.getElementById('ivChart');
                if (chartContainer) {
                    console.error('Container dimensions:', chartContainer.offsetWidth, 'x', chartContainer.offsetHeight);
                }
            }
        }, 5000);
        
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

