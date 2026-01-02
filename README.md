# IV Charts - Implied Volatility Dashboard

A Flask web application for calculating and visualizing Implied Volatility (IV) from Fyers API historical data. Supports both NSE (equity/options) and MCX (commodity) markets with automatic and manual symbol selection modes.

## Features

- **Fyers API Integration**: Automated login and authentication with Fyers API
- **Dual Mode Operation**:
  - **Automatic Mode**: Automatically generates option symbols based on future LTP, ATM strike calculation, and SymbolSetting.csv configuration
  - **Manual Mode**: Direct symbol input with optional strike, expiry, and option type parameters
- **Real-time Data Fetching**: Continuously fetch historical OHLC data with market hours awareness
- **IV Calculation**:
  - **Options**: True Implied Volatility using `py_vollib` Black-Scholes model (for options on futures)
  - **Underlying Assets**: Historical Volatility as a proxy for IV
- **Interactive Charts**: Real-time line chart visualization using TradingView Lightweight Charts
- **Data Persistence**: CSV files stored in `data/` folder for historical data retention
- **Market Hours Awareness**: Automatically respects market hours (NSE: 9:15-15:30, MCX: 9:00-23:30)
- **Chart Update Manager**: Centralized queue system prevents chart breaks and ensures data integrity
- **Symbol Validation**: Strict symbol matching prevents data mixing between exchanges
- **Dark Theme UI**: Modern, dark-themed dashboard interface

## Installation

1. **Install required dependencies**:
```bash
pip install -r requirements.txt
```

2. **Configure Fyers API credentials**:
   Create `FyersCredentials.csv` with the following columns:
   - `client_id`
   - `secret_key`
   - `FY_ID`
   - `totpkey`
   - `PIN`
   - `redirect_uri`

3. **Configure Symbol Settings** (Optional):
   Edit `SymbolSetting.csv` to add/modify symbols for automatic mode:
   ```csv
   Prefix,SYMBOL,EXPIERY,StrikeStep
   NSE,NIFTY,25-11-2025,50
   MCX,CRUDEOIL,25-12-2025,50
   MCX,SILVER,05-12-2025,250
   ```
   - `Prefix`: Exchange prefix (NSE or MCX)
   - `SYMBOL`: Underlying symbol (e.g., NIFTY, CRUDEOIL, SILVER)
   - `EXPIERY`: Future expiry date (DD-MM-YYYY format)
   - `StrikeStep`: Strike step size for ATM calculation

## Usage

1. **Start the Flask application**:
```bash
python main.py
```

2. **Open your browser** and navigate to:
```
http://localhost:3000
```

3. **Login to Fyers API**:
   - Click the "Login to API" button
   - The application will handle authentication automatically

4. **Select Mode**:

   **Automatic Mode** (Recommended):
   - Select a future symbol from the dropdown (loaded from SymbolSetting.csv)
   - Choose expiry type (Weekly/Monthly)
   - Enter option expiry date (for option symbol generation)
   - Select option type (Call/Put)
   - The system will:
     - Fetch future LTP
     - Calculate ATM strike based on strike step
     - Generate option symbol automatically
     - Use future expiry from SymbolSetting.csv for future symbol
     - Use option expiry for option symbol and time_to_expiry calculation

   **Manual Mode**:
   - Enter the full option symbol (e.g., `NSE:NIFTY25N1825500CE`)
   - Optionally specify strike, expiry date/time, and option type
   - Select timeframe

5. **Configure Settings**:
   - Select timeframe (1 Second, 1 Minute, 5 Minutes, 15 Minutes, 30 Minutes, 1 Hour, 2 Hours, or 1 Day)
   - Set risk-free rate (default: 10% or 0.10)

6. **Start Fetching**: Click "Start Fetching" to begin fetching historical data and calculating IV

7. **View Chart**: The IV chart will update in real-time as new data is fetched

8. **Stop Fetching**: Click "Stop Fetching" to halt data collection (CSV files are preserved)

## Symbol Format

### Automatic Mode
- Future symbols are generated from `SymbolSetting.csv` (e.g., `NSE:NIFTY25NOVFUT`, `MCX:SILVER25DECFUT`)
- Option symbols are auto-generated based on:
  - Future LTP (for ATM strike calculation)
  - Strike step from SymbolSetting.csv
  - Option expiry date (user input)
  - Option type (Call/Put)

### Manual Mode
Use Fyers symbol format, for example:
- `NSE:NIFTY25N1825500CE` (NIFTY Call Option)
- `NSE:NIFTY25N1825500PE` (NIFTY Put Option)
- `MCX:CRUDEOIL25DEC5150CE` (Crude Oil Call Option)
- `NSE:RELIANCE-EQ` (Equity)
- `NSE:NIFTY50-INDEX` (Index)

## Timeframes

Supported timeframes:
- `1s` - 1 Second
- `1` - 1 Minute
- `5` - 5 Minutes
- `15` - 15 Minutes
- `30` - 30 Minutes
- `60` - 1 Hour
- `120` - 2 Hours
- `1D` - 1 Day

## IV Calculation

### For Options (using py_vollib Black Model)

The application calculates **True Implied Volatility** using the Black-Scholes model (appropriate for options on futures):

```
IV = implied_volatility(option_price, future_price, strike, risk_free_rate, time_to_expiry, option_type)
```

Where:
- `option_price` = Option close price from historical OHLC data
- `future_price` = Corresponding future close price (merged by date)
- `strike` = Strike price (extracted from option symbol or manual input)
- `risk_free_rate` = Risk-free interest rate (default: 0.07 or 7% = 91-day Indian T-Bill yield)
- `time_to_expiry` = (option_expiry_date - row_date) / 365 days
- `option_type` = 'c' for Call (CE), 'p' for Put (PE)

**Important Notes**:
- Future expiry (from SymbolSetting.csv) is used for **future symbol generation**
- Option expiry (user input) is used for **option symbol generation** and **time_to_expiry calculation**

### For Underlying Assets (Historical Volatility)

For non-option symbols, the application calculates **Historical Volatility** as a proxy:

1. **Calculate Log Returns**: `r_t = ln(P_t / P_{t-1})`
2. **Rolling Standard Deviation**: `σ_rolling = std(r_t)` over 20-period window
3. **Annualize**: `IV = σ_rolling × √(periods_per_year) × 100`

## Market Hours

The application respects market hours and only fetches data during active trading:
- **NSE**: 9:15 AM - 3:30 PM IST
- **MCX**: 9:00 AM - 11:30 PM IST

Data fetching automatically pauses outside market hours.

## Data Storage

- **CSV Files**: All historical IV data is stored in the `data/` folder
- **File Format**: `{sanitized_symbol}.csv` (e.g., `MCX_CRUDEOIL25DEC5150CE.csv`, `NSE_NIFTY25N1825500CE.csv`)
- **Persistence**: CSV files are preserved when stopping data fetching
- **Validation**: Strict symbol validation ensures CSV content matches requested symbol

## API Endpoints

- `GET /` - Main dashboard page
- `POST /api/login` - Login to Fyers API
- `GET /api/check_login` - Check login status
- `GET /api/get_symbols` - Get list of symbols from SymbolSetting.csv
- `POST /api/start_fetching` - Start fetching data (automatic or manual mode)
- `POST /api/stop_fetching` - Stop fetching data (preserves CSV files)
- `GET /api/get_iv_data?symbol=<symbol>` - Get IV data for charting
- `GET /api/load_csv_data?symbol=<symbol>` - Load historical data from CSV
- `GET /api/get_status` - Get current fetching status
- `GET /api/get_logs` - Get application logs

## Project Structure

```
IV Charts/
├── main.py                 # Flask backend, API endpoints, IV calculation
├── FyresIntegration.py     # Fyers API integration (login, OHLC, quotes)
├── SymbolSetting.csv       # Symbol configuration for automatic mode
├── FyersCredentials.csv    # Fyers API credentials (create this)
├── requirements.txt        # Python dependencies
├── data/                   # CSV files with historical IV data
├── templates/
│   └── index.html         # Main dashboard HTML
├── static/
│   ├── css/
│   │   └── style.css      # Dashboard styling
│   └── js/
│       ├── main.js        # Main JavaScript logic
│       └── tradingview-chart.js  # Chart initialization and updates
└── README.md              # This file
```

## Key Components

### Chart Update Manager
- Centralized queue system for chart updates
- Prevents race conditions and chart breaks
- Data validation and error handling
- Fallback to last valid data on errors
- Automatic chart reset on symbol change

### Symbol Generation
- **Future Symbol**: Generated from SymbolSetting.csv using future expiry date
- **Option Symbol**: Generated from user input using option expiry date
- Supports both NSE (equity/options) and MCX (commodity) formats
- Automatic ATM strike calculation based on future LTP

## Dependencies

- `flask` - Web framework
- `pandas` - Data manipulation
- `numpy` - Numerical computations
- `fyers_apiv3` - Fyers API client
- `pyotp` - TOTP authentication
- `requests` - HTTP requests
- `pytz` - Timezone handling
- `py_vollib` - Black-Scholes IV calculation (optional but recommended)
- `setuptools` - Package management

## Notes

- The application fetches data every 5 seconds when active (during market hours)
- IV is calculated using a rolling window approach for historical volatility
- The chart updates automatically as new data arrives
- Ensure you have a stable internet connection for API calls
- CSV files are stored permanently in the `data/` folder
- Symbol validation prevents accidental data mixing between exchanges
- Market hours awareness prevents unnecessary API calls outside trading hours

## Troubleshooting

1. **Login fails**: Check `FyersCredentials.csv` and ensure all fields are correct
2. **No data on chart**: Verify market hours, check symbol format, and ensure API login is successful
3. **CSV not loading**: Ensure the symbol parameter matches the CSV filename and content
4. **IV calculation errors**: Ensure `py_vollib` is installed for option IV calculation
5. **Market hours issues**: Verify `pytz` is installed for timezone handling

## License

This project is for personal/educational use. Ensure compliance with Fyers API terms of service.
