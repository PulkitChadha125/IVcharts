// TradingView Lightweight Charts implementation
let chart = null;
let series = null;
let fetchInterval = null;
// Store full data for crosshair tooltip (IV, option price, underlying price by timestamp)
let chartDataMap = new Map(); // Maps timestamp (Unix seconds) to {iv, optionPrice, underlyingPrice}
// Track current symbol to detect symbol changes
let currentSymbol = null;

// ============================================================================
// CENTRALIZED CHART UPDATE MANAGER - Prevents race conditions and breaks
// ============================================================================

class ChartUpdateManager {
    constructor() {
        this.updateQueue = [];
        this.isProcessing = false;
        this.currentSymbol = null;
        this.lastValidData = null; // Keep last valid data as fallback
        this.updateLock = false;
    }
    
    /**
     * Queue a chart update - ensures only one update happens at a time
     * @param {string} symbol - Symbol to update
     * @param {Object} data - Data object with timestamps, iv_values, close_prices, fclose_prices
     * @param {string} source - Source of data ('api' or 'csv')
     */
    async queueUpdate(symbol, data, source = 'api') {
        // If symbol changed, clear queue and reset
        if (symbol && symbol !== this.currentSymbol) {
            console.log(`[ChartManager] Symbol changed: ${this.currentSymbol} -> ${symbol}, clearing queue and resetting...`);
            this.updateQueue = [];
            this.currentSymbol = symbol;
            this.resetChart();
            await this.wait(300); // Wait for reset to complete
        }
        
        // Add to queue
        this.updateQueue.push({ symbol, data, source, timestamp: Date.now() });
        
        // Process queue if not already processing
        if (!this.isProcessing) {
            this.processQueue();
        }
    }
    
    /**
     * Process the update queue sequentially
     */
    async processQueue() {
        if (this.isProcessing || this.updateQueue.length === 0) {
            return;
        }
        
        this.isProcessing = true;
        console.log(`[ChartManager] Processing queue with ${this.updateQueue.length} updates`);
        
        while (this.updateQueue.length > 0) {
            const update = this.updateQueue.shift();
            
            try {
                // Skip if symbol changed while in queue (but allow if currentSymbol is null - first load)
                if (update.symbol !== this.currentSymbol && this.currentSymbol !== null) {
                    console.log(`[ChartManager] Skipping queued update for ${update.symbol} (current: ${this.currentSymbol})`);
                    continue;
                }
                
                console.log(`[ChartManager] Processing update for symbol: ${update.symbol}, data points: ${update.data?.timestamps?.length || 0}`);
                await this.updateChart(update.symbol, update.data, update.source);
                console.log(`[ChartManager] Successfully updated chart for ${update.symbol}`);
            } catch (error) {
                console.error(`[ChartManager] Error processing update:`, error);
                console.error(`[ChartManager] Error details:`, error.message, error.stack);
                // On error, try to restore last valid data
                if (this.lastValidData) {
                    console.log(`[ChartManager] Restoring last valid data due to error...`);
                    try {
                        await this.updateChart(this.currentSymbol, this.lastValidData.data, this.lastValidData.source);
                    } catch (restoreError) {
                        console.error(`[ChartManager] Failed to restore last valid data:`, restoreError);
                    }
                }
            }
        }
        
        this.isProcessing = false;
        console.log(`[ChartManager] Queue processing complete`);
    }
    
    /**
     * Validate and prepare chart data
     */
    validateAndPrepareData(data) {
        if (!data || !data.timestamps || !data.iv_values) {
            throw new Error('Invalid data structure: missing timestamps or iv_values');
        }
        
        if (data.timestamps.length !== data.iv_values.length) {
            throw new Error(`Data length mismatch: ${data.timestamps.length} timestamps vs ${data.iv_values.length} IV values`);
        }
        
        const chartData = [];
        const dataMap = new Map();
        const seenTimes = new Set(); // Track duplicates
        
        for (let index = 0; index < data.timestamps.length; index++) {
            const timestamp = data.timestamps[index];
            const ivValue = data.iv_values[index];
            
            // Validate timestamp
            if (!timestamp && timestamp !== 0) {
                continue;
            }
            
            // Convert timestamp
            let time;
            try {
                // Log first and last few timestamps for debugging
                if (index < 3 || index >= data.timestamps.length - 3) {
                    console.log(`[ChartManager] Converting timestamp ${index}: "${timestamp}"`);
                }
                time = convertToIST(timestamp);
                if (!time || isNaN(time) || time <= 0) {
                    if (index < 3 || index >= data.timestamps.length - 3) {
                        console.warn(`[ChartManager] Invalid converted time at index ${index}: ${time}`);
                    }
                    continue;
                }
                // Log converted time for first and last few
                if (index < 3 || index >= data.timestamps.length - 3) {
                    const convertedDate = new Date(time * 1000);
                    console.log(`[ChartManager] Converted to Unix: ${time}, displays as: ${convertedDate.toLocaleString()}`);
                }
            } catch (e) {
                console.error(`[ChartManager] Error converting timestamp at index ${index}:`, e);
                continue;
            }
            
            // Skip duplicates (keep last one)
            if (seenTimes.has(time)) {
                continue;
            }
            seenTimes.add(time);
            
            // Validate IV value (allow 0 and positive values)
            const parsedIv = parseFloat(ivValue);
            if (isNaN(parsedIv) || parsedIv < 0) {
                console.warn(`[ChartManager] Skipping invalid IV value at index ${index}: ${ivValue}`);
                continue;
            }
            
            // Log first few valid IV values for debugging
            if (index < 3) {
                console.log(`[ChartManager] Valid IV value at index ${index}: ${parsedIv}%`);
            }
            
            // Get option and underlying prices
            const optionPrice = data.close_prices && data.close_prices[index] !== undefined 
                ? parseFloat(data.close_prices[index]) : null;
            const underlyingPrice = data.fclose_prices && data.fclose_prices[index] !== undefined 
                ? parseFloat(data.fclose_prices[index]) : null;
            
            // Store in data map for crosshair
            dataMap.set(time, {
                iv: parsedIv,
                optionPrice: optionPrice,
                underlyingPrice: underlyingPrice
            });
            
            chartData.push({
                time: time,
                value: parsedIv
            });
        }
        
        // Sort by time
        chartData.sort((a, b) => a.time - b.time);
        
        // Limit to latest 500 records for chart display
        const MAX_CHART_RECORDS = 500;
        if (chartData.length > MAX_CHART_RECORDS) {
            // Keep only the latest 500 records
            const startIndex = chartData.length - MAX_CHART_RECORDS;
            chartData = chartData.slice(startIndex);
            
            // Also update dataMap to only include the latest 500 records
            const latestTimes = new Set(chartData.map(d => d.time));
            const filteredDataMap = new Map();
            for (const [time, value] of dataMap.entries()) {
                if (latestTimes.has(time)) {
                    filteredDataMap.set(time, value);
                }
            }
            dataMap = filteredDataMap;
            
            console.log(`[ChartManager] Limited chart display to latest ${MAX_CHART_RECORDS} records (from ${data.timestamps.length} total)`);
        }
        
        // Remove flat trailing segments (consecutive same values at end)
        if (chartData.length > 2) {
            const lastValue = chartData[chartData.length - 1].value;
            let trailingFlatCount = 0;
            for (let i = chartData.length - 2; i >= 0; i--) {
                if (Math.abs(chartData[i].value - lastValue) < 0.0001) {
                    trailingFlatCount++;
                } else {
                    break;
                }
            }
            if (trailingFlatCount > 0 && trailingFlatCount < chartData.length) {
                chartData.splice(-trailingFlatCount);
            }
        }
        
        if (chartData.length === 0) {
            console.error('[ChartManager] No valid data points after validation. Input data:', {
                timestamps: data.timestamps?.length || 0,
                iv_values: data.iv_values?.length || 0,
                sampleTimestamps: data.timestamps?.slice(0, 5),
                sampleIVs: data.iv_values?.slice(0, 5)
            });
            throw new Error('No valid data points after validation');
        }
        
        console.log(`[ChartManager] Validated ${chartData.length} data points from ${data.timestamps.length} input points`);
        return { chartData, dataMap };
    }
    
    /**
     * Update chart with validated data
     */
    async updateChart(symbol, data, source) {
        // Ensure chart is initialized
        if (!chart || !series) {
            console.log('[ChartManager] Chart not initialized, initializing...');
            if (!initChart()) {
                throw new Error('Failed to initialize chart');
            }
            await this.wait(100);
        }
        
        // Validate and prepare data
        let validatedData;
        try {
            validatedData = this.validateAndPrepareData(data);
        } catch (error) {
            console.error(`[ChartManager] Data validation failed:`, error);
            // If we have last valid data, use it instead of failing
            if (this.lastValidData) {
                console.log('[ChartManager] Using last valid data due to validation failure');
                validatedData = this.validateAndPrepareData(this.lastValidData.data);
                symbol = this.currentSymbol;
            } else {
                throw error;
            }
        }
        
        const { chartData, dataMap } = validatedData;
        
        // Update chart title
        updateChartTitle(symbol);
        
        // Check if this is a new symbol or first load (check BEFORE updating currentSymbol)
        const existingData = series.data();
        const previousSymbol = this.currentSymbol;
        const isNewSymbol = existingData.length === 0 || symbol !== previousSymbol;
        
        // Update currentSymbol AFTER checking
        this.currentSymbol = symbol;
        
        console.log(`[ChartManager] Updating chart: symbol=${symbol}, isNewSymbol=${isNewSymbol}, existingDataLength=${existingData.length}, previousSymbol=${previousSymbol}`);
        
        try {
            if (isNewSymbol || existingData.length === 0) {
                // New symbol: Clear and set all data
                console.log(`[ChartManager] Setting ${chartData.length} data points for new symbol: ${symbol}`);
                console.log(`[ChartManager] Sample data points:`, chartData.slice(0, 3));
                series.setData([]);
                await this.wait(50);
                series.setData(chartData);
                
                // Verify data was set
                await this.wait(100);
                const verifyData = series.data();
                console.log(`[ChartManager] Verification: series.data() returned ${verifyData.length} points`);
                if (verifyData.length === 0 && chartData.length > 0) {
                    console.warn('[ChartManager] Data not set, retrying...');
                    series.setData(chartData);
                    await this.wait(100);
                    const verifyData2 = series.data();
                    console.log(`[ChartManager] After retry: series.data() returned ${verifyData2.length} points`);
                }
                
                // Update data map
                chartDataMap.clear();
                dataMap.forEach((value, key) => chartDataMap.set(key, value));
                
                // Fit content to show all data
                chart.timeScale().fitContent();
                
                // Auto-scale price scale
                this.autoScalePriceScale(chartData);
                
                console.log(`[ChartManager] Successfully set ${chartData.length} data points for ${symbol}`);
            } else {
                // Same symbol: Incremental update (preserve zoom if user has panned)
                const currentVisibleRange = chart.timeScale().getVisibleRange();
                const dataLength = existingData.length;
                
                // Check if user is at latest point
                let shouldPreserveZoom = true;
                if (dataLength > 0 && currentVisibleRange && currentVisibleRange.to) {
                    const latestDataTime = existingData[dataLength - 1].time;
                    const visibleEndTime = typeof currentVisibleRange.to === 'number' 
                        ? currentVisibleRange.to 
                        : currentVisibleRange.to.getTime() / 1000;
                    const timeDiff = Math.abs(latestDataTime - visibleEndTime);
                    if (timeDiff <= 300) { // Within 5 minutes
                        shouldPreserveZoom = false;
                    }
                }
                
                // Update data
                series.setData(chartData);
                
                // Update data map
                chartDataMap.clear();
                dataMap.forEach((value, key) => chartDataMap.set(key, value));
                
                // Auto-scale price scale
                this.autoScalePriceScale(chartData);
                
                // Preserve zoom if user has panned away
                if (shouldPreserveZoom && currentVisibleRange) {
                    await this.wait(50);
                    try {
                        chart.timeScale().setVisibleRange(currentVisibleRange);
                    } catch (e) {
                        console.warn('[ChartManager] Could not preserve zoom:', e);
                    }
                }
                
                console.log(`[ChartManager] Incrementally updated ${chartData.length} data points for ${symbol}`);
            }
            
            // Save as last valid data
            this.lastValidData = { symbol, data, source };
            
        } catch (error) {
            console.error(`[ChartManager] Error updating chart:`, error);
            // Try to restore last valid data
            if (this.lastValidData && this.lastValidData.data) {
                console.log('[ChartManager] Attempting to restore last valid data...');
                try {
                    const restored = this.validateAndPrepareData(this.lastValidData.data);
                    series.setData(restored.chartData);
                    chartDataMap.clear();
                    restored.dataMap.forEach((value, key) => chartDataMap.set(key, value));
                } catch (e) {
                    console.error('[ChartManager] Failed to restore last valid data:', e);
                }
            }
            throw error;
        }
    }
    
    /**
     * Auto-scale price scale to fit data
     */
    autoScalePriceScale(chartData) {
        try {
            const values = chartData.map(d => d.value).filter(v => v != null && !isNaN(v) && v > 0);
            if (values.length > 0) {
                const priceScale = chart.priceScale('right');
                if (priceScale) {
                    priceScale.applyOptions({ autoScale: true });
                }
            }
        } catch (e) {
            console.warn('[ChartManager] Could not auto-scale price scale:', e);
        }
    }
    
    /**
     * Reset chart completely
     */
    resetChart() {
        console.log('[ChartManager] Resetting chart...');
        
        // Clear data map
        if (chartDataMap) {
            chartDataMap.clear();
        }
        
        // Clear series data
        if (series && typeof series.setData === 'function') {
            try {
                series.setData([]);
            } catch (e) {
                console.warn('[ChartManager] Error clearing series:', e);
            }
        }
        
        // Reset time scale
        if (chart && chart.timeScale) {
            try {
                chart.timeScale().fitContent();
            } catch (e) {
                console.warn('[ChartManager] Error resetting time scale:', e);
            }
        }
        
        console.log('[ChartManager] Chart reset complete');
    }
    
    /**
     * Wait utility
     */
    wait(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Create global chart update manager instance
const chartUpdateManager = new ChartUpdateManager();

// Helper function to convert timestamp string to Unix timestamp
// Timestamps from backend are in IST format: "2025-11-13T15:29:00+05:30" or "2025-11-13 15:29:00"
// CSV timestamps are correct IST times - we need to display them as IST on the chart
// LightweightCharts displays times in browser's local timezone, so we adjust the Unix timestamp
// so that when displayed, it shows the IST time from CSV
function convertToIST(timestamp) {
    try {
        if (typeof timestamp === 'string') {
            // Extract IST time components from CSV timestamp
            // Backend sends: "2025-12-01T17:30:00+05:30" (IST time with timezone indicator)
            let csvYear, csvMonth, csvDay, csvHour, csvMinute, csvSecond;
            
            // Parse timestamp string to extract date/time components (before timezone)
            // Remove timezone info first to get clean timestamp
            let cleanTimestamp = timestamp.replace(/[+-]\d{2}:\d{2}$/, '').trim();
            const match = cleanTimestamp.match(/(\d{4})-(\d{2})-(\d{2})[\sT](\d{2}):(\d{2}):(\d{2})/);
            
            if (!match) {
                return null;
            }
            
            [, csvYear, csvMonth, csvDay, csvHour, csvMinute, csvSecond] = match;
            
            const yearInt = parseInt(csvYear);
            const monthInt = parseInt(csvMonth) - 1; // Month is 0-indexed
            const dayInt = parseInt(csvDay);
            const hourInt = parseInt(csvHour);
            const minuteInt = parseInt(csvMinute);
            const secondInt = parseInt(csvSecond || 0);
            
            // CSV has IST time (e.g., 17:30:00 IST)
            // We want chart to display: 17:30:00 (matching CSV)
            // Chart library displays Unix timestamps in browser's local timezone
            
            // Simple strategy: Treat CSV IST time as UTC time for the Unix timestamp
            // This makes the chart display the CSV time directly, regardless of browser timezone
            // Example: CSV "17:30:00 IST" -> Create Unix timestamp for "17:30:00 UTC"
            // - Browser in UTC: displays "17:30:00" ✓ (matches CSV)
            // - Browser in IST: displays "23:00:00" (17:30 + 5:30) - but we want 17:30
            //
            // To handle IST browsers: We need to subtract 5:30 so it displays as IST time
            // But we want to show IST time, so if browser is IST, we use actual UTC (which displays as IST)
            
            // Always treat CSV IST time as UTC for the Unix timestamp
            // This makes chart display CSV time directly in UTC browsers
            // For IST browsers, we'll adjust below
            const istAsUTC = new Date(Date.UTC(yearInt, monthInt, dayInt, hourInt, minuteInt, secondInt));
            
            // Get browser's timezone offset (in minutes, positive = behind UTC)
            // IST is UTC+5:30, so IST offset is -330 minutes
            const browserOffsetMinutes = new Date().getTimezoneOffset();
            const istOffsetMinutes = -330;
            
            // Check if browser is in IST (within 10 minutes tolerance for DST, etc.)
            const isISTBrowser = Math.abs(browserOffsetMinutes - istOffsetMinutes) < 10;
            
            // Debug logging for first few conversions
            const shouldLog = yearInt === 2025 && monthInt === 11 && dayInt === 1 && hourInt >= 17;
            
            if (shouldLog) {
                console.log(`[convertToIST] Input: ${timestamp}, Extracted: ${yearInt}-${monthInt+1}-${dayInt} ${hourInt}:${minuteInt}:${secondInt}`);
                console.log(`[convertToIST] Browser offset: ${browserOffsetMinutes}, IST offset: ${istOffsetMinutes}, Is IST browser: ${isISTBrowser}`);
            }
            
            // The chart appears to display Unix timestamps in UTC (based on evidence: showing 12:07 instead of 17:35)
            // We want to display the CSV IST time directly
            // Strategy: Always use CSV IST time as UTC timestamp
            // CSV 17:30 IST -> timestamp for 17:30 UTC -> chart displays as 17:30 ✓
            // This works regardless of browser timezone if chart displays in UTC
            
            const unixTs = Math.floor(istAsUTC.getTime() / 1000);
            if (shouldLog) {
                const displayDate = new Date(unixTs * 1000);
                console.log(`[convertToIST] Using CSV IST time as UTC: ${unixTs} (${displayDate.toUTCString()}), chart should display: ${displayDate.toUTCString().match(/\d{2}:\d{2}:\d{2}/)?.[0]}`);
            }
            return unixTs;
            
        } else if (typeof timestamp === 'number') {
            // If it's already a Unix timestamp, return as-is (Unix timestamps are timezone-agnostic)
            return timestamp > 10000000000 ? Math.floor(timestamp / 1000) : timestamp;
        }
        return null;
    } catch (e) {
        console.warn('Error converting timestamp:', timestamp, e);
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
        
        // Enable auto-scaling on the price scale (Y-axis)
        // The price scale is automatically created and will auto-scale by default
        // But we can configure it explicitly
        try {
            const priceScale = chart.priceScale('right');
            if (priceScale) {
                priceScale.applyOptions({
                    autoScale: true,
                    scaleMargins: {
                        top: 0.1,
                        bottom: 0.1,
                    },
                });
            }
        } catch (e) {
            console.warn('Could not configure price scale:', e);
        }
    
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

// Reset chart completely - clear all data and reset scales
function resetChart() {
    console.log('Resetting chart completely...');
    
    // Clear all data
    if (chartDataMap) {
        chartDataMap.clear();
        console.log('Cleared chartDataMap');
    }
    
    // Clear series data - this is critical to prevent zoom issues
    if (series && typeof series.setData === 'function') {
        series.setData([]);
        console.log('Cleared series data');
        
        // Wait a moment to ensure clear takes effect
        setTimeout(() => {
            const verifyClear = series.data();
            if (verifyClear.length > 0) {
                console.warn('Series still has data after clear, forcing clear again...');
                series.setData([]);
            }
        }, 50);
    }
    
    // Reset time scale
    if (chart) {
        try {
            chart.timeScale().fitContent();
            console.log('Reset time scale (fitContent)');
        } catch (e) {
            console.warn('Error resetting time scale:', e);
        }
    }
    
    // Reset price scale
    if (chart) {
        try {
            const priceScale = chart.priceScale('right');
            if (priceScale) {
                priceScale.applyOptions({
                    autoScale: true,
                });
                console.log('Reset price scale (autoScale)');
            }
        } catch (e) {
            console.warn('Error resetting price scale:', e);
        }
    }
    
    console.log('Chart reset complete');
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

// Fetch IV data and update chart (uses ChartUpdateManager)
async function fetchIVData(symbol) {
    try {
        if (!symbol) {
            console.warn('[fetchIVData] No symbol provided');
            return;
        }
        
        console.log('[fetchIVData] Fetching IV data for symbol:', symbol);
        const response = await fetch(`/api/get_iv_data?symbol=${encodeURIComponent(symbol)}`);
        
        if (!response.ok) {
            console.error(`[fetchIVData] HTTP error ${response.status}: ${response.statusText}`);
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        console.log('[fetchIVData] Raw response data:', {
            hasData: !!data,
            timestamps: data?.timestamps?.length || 0,
            iv_values: data?.iv_values?.length || 0,
            close_prices: data?.close_prices?.length || 0,
            fclose_prices: data?.fclose_prices?.length || 0,
            firstFewTimestamps: data?.timestamps?.slice(0, 3),
            firstFewIVs: data?.iv_values?.slice(0, 3)
        });
        
        if (!data || !data.timestamps || data.timestamps.length === 0) {
            console.warn('[fetchIVData] No data received or empty timestamps for symbol:', symbol);
            console.warn('[fetchIVData] Response data:', data);
            // Try to use last valid data if available
            if (chartUpdateManager.lastValidData) {
                console.log('[fetchIVData] Using last valid data as fallback');
                await chartUpdateManager.queueUpdate(symbol, chartUpdateManager.lastValidData.data, 'api');
            }
            return;
        }
        
        // Check if IV values are all zero or invalid
        const validIVs = data.iv_values.filter(iv => !isNaN(parseFloat(iv)) && parseFloat(iv) >= 0);
        if (validIVs.length === 0) {
            console.warn('[fetchIVData] All IV values are invalid or NaN. IV values:', data.iv_values.slice(0, 10));
        }
        
        console.log('[fetchIVData] Received IV data:', {
            timestamps: data.timestamps.length,
            iv_values: data.iv_values?.length || 0,
            validIVs: validIVs.length,
            close_prices: data.close_prices?.length || 0,
            fclose_prices: data.fclose_prices?.length || 0
        });
        
        // Queue update through ChartUpdateManager (handles all validation and updates)
        console.log('[fetchIVData] Queueing chart update for symbol:', symbol);
        await chartUpdateManager.queueUpdate(symbol, data, 'api');
        console.log('[fetchIVData] Chart update queued successfully');
        
        // Update currentSymbol tracking
        currentSymbol = symbol;
    } catch (error) {
        console.error('[fetchIVData] Error fetching IV data:', error);
        // Try to use last valid data if available
        if (chartUpdateManager.lastValidData) {
            console.log('[fetchIVData] Using last valid data as fallback due to error');
            await chartUpdateManager.queueUpdate(symbol, chartUpdateManager.lastValidData.data, 'api');
        }
    }
}

// Check login status
// Check symbol download status after login
let symbolCheckInterval = null;
function checkSymbolDownloadStatus() {
    let checkCount = 0;
    const maxChecks = 30; // Check for up to 30 seconds (30 checks * 1 second)
    
    symbolCheckInterval = setInterval(async () => {
        checkCount++;
        
        try {
            const response = await fetch('/api/list_symbol_files');
            const data = await response.json();
            
            if (data.success && data.count > 0) {
                // Symbols have been downloaded
                clearInterval(symbolCheckInterval);
                symbolCheckInterval = null;
                
                const statusText = document.getElementById('statusText');
                if (statusText) {
                    statusText.textContent = `Logged in - ${data.total_symbols.toLocaleString()} symbols available`;
                }
                
                showNotification(`Symbol download complete! ${data.total_symbols.toLocaleString()} symbols available from ${data.count} exchange(s)`, 'success');
            } else if (checkCount >= maxChecks) {
                // Timeout - stop checking
                clearInterval(symbolCheckInterval);
                symbolCheckInterval = null;
                
                const statusText = document.getElementById('statusText');
                if (statusText) {
                    statusText.textContent = 'Logged in - Symbol download in progress...';
                }
            }
        } catch (error) {
            console.error('Error checking symbol download status:', error);
            if (checkCount >= maxChecks) {
                clearInterval(symbolCheckInterval);
                symbolCheckInterval = null;
            }
        }
    }, 1000); // Check every second
}

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
// Define loginToAPI function and make it globally available immediately
window.loginToAPI = async function loginToAPI() {
    console.log('[loginToAPI] Function called');
    
    const btn = document.getElementById('loginBtn');
    if (!btn) {
        console.error('[loginToAPI] Login button not found');
        alert('Login button not found. Please refresh the page.');
        return;
    }
    
    console.log('[loginToAPI] Button found:', btn);
    
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
        console.log('Sending login request to /api/login...');
        
        // Create abort controller for timeout (more compatible)
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 60000); // 60 seconds timeout
        
        let response;
        try {
            response = await fetch('/api/login', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            signal: controller.signal
        });
        } catch (fetchError) {
            clearTimeout(timeoutId);
            console.error('Fetch error:', fetchError);
            if (fetchError.name === 'AbortError') {
                throw new Error('Login request timed out after 60 seconds. Please check your connection and try again.');
            }
            throw new Error(`Failed to connect to server: ${fetchError.message}`);
        }
        
        clearTimeout(timeoutId);
        
        console.log('Login response status:', response.status);
        console.log('Login response headers:', response.headers);
        
        // Try to get response text first for debugging
        const responseText = await response.text();
        console.log('Login response text:', responseText);
        
        let data;
        try {
            data = JSON.parse(responseText);
        } catch (e) {
            console.error('Failed to parse login response as JSON:', e);
            throw new Error(`Invalid response from server: ${responseText.substring(0, 100)}`);
        }
        
        console.log('Login response data:', data);
        
        if (!response.ok) {
            throw new Error(data.message || `HTTP ${response.status}: ${response.statusText}`);
        }
        
        if (data.success) {
            document.getElementById('loginStatus').style.display = 'flex';
            document.getElementById('statusText').textContent = 'Logged in successfully';
            document.getElementById('settingsSection').style.display = 'block';
            btn.textContent = 'Logged In';
            
            // Show appropriate notification based on symbol download status
            if (data.downloading_symbols) {
                showNotification('Login successful! Downloading all symbols in background...', 'info');
                // Update status text to show symbol download
                setTimeout(() => {
                    const statusText = document.getElementById('statusText');
                    if (statusText) {
                        statusText.textContent = 'Logged in - Downloading symbols...';
                    }
                }, 500);
                
                // Check symbol download status periodically
                checkSymbolDownloadStatus();
            } else {
                showNotification('Login successful!', 'success');
            }
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
// Store symbol settings data
let symbolSettingsData = [];

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

// Load symbols from API
async function loadSymbolSettings() {
    try {
        const response = await fetch('/api/get_symbol_settings');
        const data = await response.json();
        
        if (data.success && data.symbols) {
            symbolSettingsData = data.symbols;
            populateFutureSymbolDropdown(data.symbols);
        } else {
            console.error('Failed to load symbol settings:', data.message);
        }
    } catch (error) {
        console.error('Error loading symbol settings:', error);
    }
}

// Populate future symbol dropdown
function populateFutureSymbolDropdown(symbols) {
    const dropdown = document.getElementById('futureSymbol');
    if (!dropdown) {
        console.error('Future symbol dropdown not found');
        return;
    }
    
    dropdown.innerHTML = '<option value="">Select Future Symbol</option>';
    
    console.log(`[populateFutureSymbolDropdown] Loading ${symbols.length} symbols into dropdown`);
    
    symbols.forEach((sym, index) => {
        const option = document.createElement('option');
        option.value = sym.future_symbol;
        option.textContent = sym.future_symbol;
        option.dataset.strikeStep = sym.strike_step || '';
        option.dataset.expiryDate = sym.expiry_date || '';
        option.dataset.originalSymbol = sym.symbol || ''; // Store original symbol for debugging
        dropdown.appendChild(option);
        console.log(`[populateFutureSymbolDropdown] Added option ${index + 1}: ${sym.future_symbol} (original: ${sym.symbol})`);
    });
    
    console.log(`[populateFutureSymbolDropdown] Total options in dropdown: ${dropdown.options.length}`);
    
    // Select first option if available
    if (symbols.length > 0) {
        dropdown.value = symbols[0].future_symbol;
        onFutureSymbolChange();
    }
}

// Handle future symbol change
function onFutureSymbolChange() {
    const dropdown = document.getElementById('futureSymbol');
    const selectedOption = dropdown.options[dropdown.selectedIndex];
    
    if (selectedOption && selectedOption.dataset) {
        // Auto-fill strike step
        const strikeStep = selectedOption.dataset.strikeStep;
        const strikeStepInput = document.getElementById('strikeStep');
        if (strikeStep && strikeStep !== 'None' && strikeStep !== '') {
            strikeStepInput.value = strikeStep;
        } else {
            strikeStepInput.value = '';
        }
        
        // Auto-fill expiry date
        const expiryDate = selectedOption.dataset.expiryDate;
        const expiryDateInput = document.getElementById('autoExpiryDate');
        if (expiryDate && expiryDate !== '') {
            expiryDateInput.value = expiryDate;
        }
    }
}

// Load and display logs
async function loadLogs() {
    try {
        const levelFilter = document.getElementById('logLevelFilter').value;
        const url = levelFilter ? `/api/get_logs?level=${levelFilter}&limit=200` : '/api/get_logs?limit=200';
        
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.success && data.logs) {
            displayLogs(data.logs);
        } else {
            console.error('Failed to load logs:', data.message);
        }
    } catch (error) {
        console.error('Error loading logs:', error);
    }
}

// Display logs in the container
function displayLogs(logs) {
    const container = document.getElementById('logsContainer');
    
    if (!logs || logs.length === 0) {
        container.innerHTML = '<div style="color: #a0c4ff; text-align: center; padding: 20px;">No logs available</div>';
        return;
    }
    
    let html = '<div style="display: flex; flex-direction: column; gap: 8px;">';
    
    logs.forEach(log => {
        const levelColor = {
            'ERROR': '#ff6b6b',
            'WARNING': '#ffd93d',
            'INFO': '#6bcf7f',
            'DEBUG': '#a0c4ff'
        }[log.level] || '#a0c4ff';
        
        html += `
            <div style="padding: 8px 12px; background: rgba(30, 58, 95, 0.3); border-left: 3px solid ${levelColor}; border-radius: 4px;">
                <div style="display: flex; gap: 12px; align-items: baseline; flex-wrap: wrap;">
                    <span style="color: #888; font-size: 11px; min-width: 160px;">${log.timestamp}</span>
                    <span style="color: ${levelColor}; font-weight: bold; min-width: 60px;">[${log.level}]</span>
                    <span style="color: #fff; flex: 1;">${escapeHtml(log.message)}</span>
                </div>
                ${log.details ? `<div style="color: #888; font-size: 11px; margin-top: 4px; margin-left: 232px; white-space: pre-wrap; word-break: break-all;">${escapeHtml(log.details)}</div>` : ''}
            </div>
        `;
    });
    
    html += '</div>';
    container.innerHTML = html;
    
    // Auto-scroll to top (most recent logs)
    container.scrollTop = 0;
}

// Clear logs
async function clearLogs() {
    if (!confirm('Are you sure you want to clear all logs?')) {
        return;
    }
    
    try {
        const response = await fetch('/api/clear_logs', { method: 'POST' });
        const data = await response.json();
        
        if (data.success) {
            loadLogs();
        } else {
            alert('Failed to clear logs: ' + data.message);
        }
    } catch (error) {
        console.error('Error clearing logs:', error);
        alert('Error clearing logs: ' + error.message);
    }
}

// Escape HTML to prevent XSS
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

// Make functions available globally
window.toggleMode = toggleMode;
window.onFutureSymbolChange = onFutureSymbolChange;
window.loadLogs = loadLogs;
window.clearLogs = clearLogs;

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
    
    // Get risk-free rate (default 7% = 0.07 = 91-day Indian T-Bill yield)
    const riskFreeRateInput = document.getElementById('riskFreeRate');
    const riskFreeRate = riskFreeRateInput && riskFreeRateInput.value ? parseFloat(riskFreeRateInput.value) / 100 : 0.07;
    
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
            
            // Get the symbol we're about to fetch
            const symbolToPoll = mode === 'automatic' ? data.generated_symbol : payload.symbol;
            
            // If symbol changed, completely reset chart
            if (symbolToPoll && symbolToPoll !== currentSymbol) {
                console.log(`Symbol changed from ${currentSymbol} to ${symbolToPoll} - resetting chart completely...`);
                resetChart();
                currentSymbol = symbolToPoll;
                
                // Wait a moment for reset to complete
                await new Promise(resolve => setTimeout(resolve, 300));
            } else if (!currentSymbol) {
                // First time, just reset
                console.log('First fetch - resetting chart...');
                resetChart();
                currentSymbol = symbolToPoll;
                await new Promise(resolve => setTimeout(resolve, 300));
            }
            
            // Update chart title with current symbol
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
            
            // Clear chart data and reset
            if (chartDataMap) {
                chartDataMap.clear();
            }
            
            // Clear series data
            if (series && typeof series.setData === 'function') {
                series.setData([]);
            }
            
            // Reset zoom and chart scale
            if (chart) {
                chart.timeScale().fitContent();
            }
            
            // Clear contract name display
            const contractNameEl = document.getElementById('chartContractName');
            if (contractNameEl) {
                contractNameEl.textContent = '';
            }
            
            // Stop polling
            if (fetchInterval) {
                clearInterval(fetchInterval);
                fetchInterval = null;
            }
            
            showNotification(data.message || 'Data fetching stopped and chart reset', 'info');
        } else {
            showNotification('Error stopping data fetch: ' + (data.message || 'Unknown error'), 'error');
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
                    const currentStatusSymbol = status.symbol;
                    
                    // If symbol changed, reset chart
                    if (currentStatusSymbol && currentStatusSymbol !== currentSymbol) {
                        console.log(`[Polling] Symbol changed from ${currentSymbol} to ${currentStatusSymbol} - resetting chart`);
                        resetChart();
                        currentSymbol = currentStatusSymbol;
                        await new Promise(resolve => setTimeout(resolve, 300));
                    }
                    
                    // Update chart title if symbol changed
                    updateChartTitle(currentStatusSymbol);
                    fetchIVData(currentStatusSymbol);
                } else if (status.active && status.symbol) {
                    // Manual mode or fallback
                    const currentStatusSymbol = status.symbol;
                    
                    // If symbol changed, reset chart
                    if (currentStatusSymbol && currentStatusSymbol !== currentSymbol) {
                        console.log(`[Polling] Symbol changed from ${currentSymbol} to ${currentStatusSymbol} - resetting chart`);
                        resetChart();
                        currentSymbol = currentStatusSymbol;
                        await new Promise(resolve => setTimeout(resolve, 300));
                    }
                    
                    // Update chart title if symbol changed
                    updateChartTitle(currentStatusSymbol);
                    fetchIVData(currentStatusSymbol);
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

// Load CSV data for a specific symbol (symbol is REQUIRED)
async function loadCSVData(symbol) {
    try {
        if (!symbol) {
            console.warn('[loadCSVData] Symbol is required - cannot load CSV without explicit symbol');
            return;
        }
        
        console.log('[loadCSVData] Loading CSV data for symbol:', symbol);
        
        // Validate symbol matches current symbol before loading
        if (currentSymbol && currentSymbol !== symbol) {
            console.warn(`[loadCSVData] Symbol mismatch: current=${currentSymbol}, requested=${symbol}. Resetting chart.`);
            // ChartUpdateManager will handle the reset, but we should ensure it happens
            resetChart();
            currentSymbol = symbol;
            await new Promise(resolve => setTimeout(resolve, 300));
        }
        
        const url = `/api/load_csv_data?symbol=${encodeURIComponent(symbol)}`;
        const response = await fetch(url);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ message: response.statusText }));
            throw new Error(errorData.message || `HTTP ${response.status}: ${response.statusText}`);
        }
        
        const data = await response.json();
        
        if (!data.success || !data.timestamps || data.timestamps.length === 0) {
            console.warn('[loadCSVData] No data received or empty timestamps for symbol:', symbol);
            return;
        }
        
        // STRICT VALIDATION: Verify loaded symbol matches requested symbol
        if (data.symbol && data.symbol !== symbol) {
            console.error(`[loadCSVData] Symbol mismatch: requested '${symbol}' but got '${data.symbol}' from CSV. Rejecting data.`);
            return;
        }
        
        console.log(`[loadCSVData] Loaded ${data.data_points} data points from CSV for symbol: ${symbol}`);
        
        // Queue update through ChartUpdateManager (handles all validation and updates)
        await chartUpdateManager.queueUpdate(symbol, data, 'csv');
        
        // Update currentSymbol tracking
        currentSymbol = symbol;
    } catch (error) {
        console.error('[loadCSVData] Error loading CSV data:', error);
        // Don't use fallback data - if CSV load fails, chart should remain empty or show current data
    }
}

// Event listeners
document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM loaded, initializing...');
    
    // Initialize: Show automatic mode by default
    toggleMode();
    
    // Load symbol settings
    loadSymbolSettings();
    
    // Load logs
    loadLogs();
    
    // Auto-refresh logs every 5 seconds
    setInterval(loadLogs, 5000);
    
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
                        
                        // Don't auto-load CSV on page load - only load when explicit symbol is provided
                        // CSV will be loaded when user starts fetching for a specific symbol
                        console.log('Chart initialized - CSV will be loaded when symbol is selected');
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
        
        // Verify loginToAPI is available
        if (typeof window.loginToAPI === 'function') {
            console.log('[DOMContentLoaded] ✓ loginToAPI function is available');
        } else {
            console.error('[DOMContentLoaded] ✗ loginToAPI function NOT available!');
        }
        
        // Attach event listener to login button (SIMPLE - just call the function)
        const loginBtn = document.getElementById('loginBtn');
        if (loginBtn) {
            loginBtn.addEventListener('click', function(e) {
                e.preventDefault();
                e.stopPropagation();
                console.log('[Login Button] Clicked');
                
                if (window.loginToAPI && typeof window.loginToAPI === 'function') {
                    window.loginToAPI().catch(error => {
                        console.error('[Login Button] Error:', error);
                    });
                } else {
                    console.error('[Login Button] loginToAPI not available');
                    alert('Login function not loaded. Please refresh the page.');
                }
            });
            console.log('[DOMContentLoaded] ✓ Login button event listener attached');
        } else {
            console.error('[DOMContentLoaded] ✗ Login button not found');
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

