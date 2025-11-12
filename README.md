# IV Charts - Implied Volatility Dashboard

A Flask web application for calculating and visualizing Implied Volatility (IV) from Fyers API historical data.

## Features

- **Fyers API Integration**: Login to Fyers API with automated authentication
- **Real-time Data Fetching**: Continuously fetch historical OHLC data based on symbol and timeframe
- **IV Calculation**: Calculate Implied Volatility (Historical Volatility) from price data
- **Interactive Charts**: Real-time line chart visualization of IV data
- **Dark Theme UI**: Modern, dark-themed dashboard interface

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure your `FyersCredentials.csv` file is properly configured with:
   - `client_id`
   - `secret_key`
   - `FY_ID`
   - `totpkey`
   - `PIN`
   - `redirect_uri`

## Usage

1. Start the Flask application:
```bash
python main.py
```

2. Open your browser and navigate to:
```
http://localhost:5000
```

3. **Login**: Click the "Login to API" button to authenticate with Fyers API

4. **Configure Settings**: 
   - Enter the symbol (e.g., `NSE:RELIANCE-EQ`)
   - Select the timeframe (1 Minute, 5 Minutes, 15 Minutes, 30 Minutes, 1 Hour, or 1 Day)

5. **Start Fetching**: Click "Start Fetching" to begin fetching historical data and calculating IV

6. **View Chart**: The IV chart will update in real-time as new data is fetched

7. **Stop Fetching**: Click "Stop Fetching" to halt data collection

## Symbol Format

Use Fyers symbol format, for example:
- `NSE:RELIANCE-EQ` (Equity)
- `NSE:NIFTY50-INDEX` (Index)
- `NSE:SBIN-EQ` (Equity)

## Timeframes

Supported timeframes:
- `1` - 1 Minute
- `5` - 5 Minutes
- `15` - 15 Minutes
- `30` - 30 Minutes
- `60` - 1 Hour
- `1D` - 1 Day

## IV Calculation

The application calculates Historical Volatility (used as a proxy for Implied Volatility) using:
- Rolling standard deviation of log returns
- Annualized volatility based on the selected timeframe
- 20-period rolling window (adjustable)

## API Endpoints

- `GET /` - Main dashboard page
- `POST /api/login` - Login to Fyers API
- `GET /api/check_login` - Check login status
- `POST /api/start_fetching` - Start fetching data
- `POST /api/stop_fetching` - Stop fetching data
- `GET /api/get_iv_data?symbol=<symbol>` - Get IV data for charting
- `GET /api/get_status` - Get current fetching status

## Notes

- The application fetches data every 5 seconds when active
- IV is calculated using a rolling window approach
- The chart updates automatically as new data arrives
- Ensure you have a stable internet connection for API calls

