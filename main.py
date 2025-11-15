from flask import Flask, render_template, request, jsonify, session
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import csv
import re
from FyresIntegration import automated_login, fetchOHLC, fyres_quote
import FyresIntegration
import threading
import time

# Import py_vollib for Black model IV calculation (for options on futures)
try:
    from py_vollib.black.implied_volatility import implied_volatility
    PY_VOLLIB_AVAILABLE = True
except ImportError:
    PY_VOLLIB_AVAILABLE = False
    print("Warning: py_vollib not installed. Install with: pip install py_vollib")

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production

# Global variables for data storage
iv_data_store = {}
fetching_status = {"active": False, "symbol": None, "timeframe": None}

# Create data folder if it doesn't exist
DATA_FOLDER = 'data'
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
    print(f"Created data folder: {DATA_FOLDER}")

def delete_csv_files(symbol=None):
    """
    Delete CSV files from data folder
    If symbol is provided, delete only that symbol's CSV file
    If symbol is None, delete all CSV files
    """
    try:
        if not os.path.exists(DATA_FOLDER):
            return 0
        
        deleted_count = 0
        if symbol:
            # Delete specific symbol's CSV file
            safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
            safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
            filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
            if os.path.exists(filename):
                os.remove(filename)
                print(f"Deleted CSV file for symbol {symbol}: {filename}")
                deleted_count = 1
        else:
            # Delete all CSV files
            csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]
            for csv_file in csv_files:
                filepath = os.path.join(DATA_FOLDER, csv_file)
                try:
                    os.remove(filepath)
                    deleted_count += 1
                    print(f"Deleted CSV file: {filepath}")
                except Exception as e:
                    print(f"Error deleting {filepath}: {e}")
        
        print(f"Deleted {deleted_count} CSV file(s)")
        return deleted_count
    except Exception as e:
        print(f"Error deleting CSV files: {e}")
        import traceback
        traceback.print_exc()
        return 0

def load_credentials():
    """Load Fyers credentials from CSV file"""
    credentials = {}
    try:
        with open('FyersCredentials.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                credentials[row['Title']] = row['Value']
        return credentials
    except Exception as e:
        print(f"Error loading credentials: {e}")
        return None

def save_iv_to_csv(symbol, df_with_iv, timeframe=None, strike=None, expiry=None, option_type=None):
    """
    Save IV calculation results to CSV file in data folder
    File name: symbolname.csv (sanitized)
    
    Includes: date, option_price, option_name, underlying_price, underlying_name, iv
    """
    try:
        # Sanitize symbol name for filename (remove invalid characters)
        safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
        safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
        
        # Create filename
        filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
        
        # Prepare data for CSV
        csv_data = df_with_iv.copy()
        
        # Ensure date column is properly formatted
        if 'date' in csv_data.columns:
            csv_data['date'] = pd.to_datetime(csv_data['date'])
            csv_data['date'] = csv_data['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Define priority columns in order
        priority_columns = [
            'date',
            'option_name',
            'underlying_name',
            'close',  # Option close price
            'fclose',  # Future close price
            'strike',
            'expiry',
            'iv'
        ]
        
        # Additional columns to include if available (but exclude o, h, l)
        additional_columns = [
            'option_type',
            'timeframe',
            'volume'
        ]
        
        # Map old column names to new ones if they exist
        if 'option_price' in csv_data.columns and 'close' not in csv_data.columns:
            csv_data['close'] = csv_data['option_price']
        if 'underlying_price' in csv_data.columns and 'fclose' not in csv_data.columns:
            csv_data['fclose'] = csv_data['underlying_price']
        
        # Build columns list - prioritize required columns
        columns_to_save = []
        
        # Add priority columns if they exist
        for col in priority_columns:
            if col in csv_data.columns:
                columns_to_save.append(col)
        
        # Add additional columns if they exist
        for col in additional_columns:
            if col in csv_data.columns and col not in columns_to_save:
                columns_to_save.append(col)
        
        # Ensure we have at least date and iv
        if 'date' not in columns_to_save and 'date' in csv_data.columns:
            columns_to_save.insert(0, 'date')
        if 'iv' not in columns_to_save and 'iv' in csv_data.columns:
            columns_to_save.append('iv')
        
        # Debug: Print available columns and columns to save
        print(f"Available columns in dataframe: {list(csv_data.columns)}")
        print(f"Columns to save: {columns_to_save}")
        
        # Save to CSV with proper formatting
        csv_data[columns_to_save].to_csv(filename, index=False, float_format='%.4f')
        
        print(f"IV data saved to: {filename}")
        print(f"  Saved {len(csv_data)} rows with columns: {', '.join(columns_to_save)}")
        return filename
    except Exception as e:
        print(f"Error saving IV data to CSV for {symbol}: {e}")
        import traceback
        traceback.print_exc()
        return None

def parse_option_symbol(symbol):
    """
    Parse Indian option symbol format (e.g., NIFTY25N1825700PE, RELIANCE25N1825700CE, MCX:CRUDEOILM25NOV5300CE)
    Returns: dict with underlying, expiry_date, strike, option_type, or None if not an option
    """
    # Pattern for Indian options: SYMBOL + YY + M + STRIKE + CE/PE
    # Example: NIFTY25N1825700PE, RELIANCE25DEC1825700CE, MCX:CRUDEOILM25NOV5300CE
    
    # Check if it ends with CE or PE (Call/Put European)
    if not (symbol.endswith('CE') or symbol.endswith('PE')):
        return None
    
    option_type = 'c' if symbol.endswith('CE') else 'p'
    
    # Handle MCX symbols (MCX:COMMODITY...)
    is_mcx = symbol.startswith('MCX:')
    if is_mcx:
        # Remove MCX: prefix for parsing
        symbol_without_exchange = symbol[4:]  # Remove "MCX:"
    else:
        symbol_without_exchange = symbol
    
    # Remove CE/PE suffix
    base = symbol_without_exchange[:-2]
    
    # Try to extract strike price (usually last 4-6 digits before CE/PE)
    # Common formats: NIFTY25N1825700PE, NIFTY25DEC1825700PE, MCX:CRUDEOILM25NOV5300CE
    strike_match = re.search(r'(\d{4,6})(CE|PE)$', symbol_without_exchange)
    if strike_match:
        strike_str = strike_match.group(1)
        # Handle strike with decimals (e.g., 18257.00 -> 18257)
        if len(strike_str) >= 4:
            # Last 2 digits might be decimal part
            if len(strike_str) == 6:
                strike = float(strike_str[:4] + '.' + strike_str[4:])
            else:
                strike = float(strike_str)
        else:
            strike = float(strike_str)
        
        # Remove strike from base
        base = base[:-len(strike_str)]
    else:
        return None
    
    # Extract year and month
    # Pattern: YY + M or YY + MONTHNAME
    year_match = re.search(r'(\d{2})([A-Z]{1,3})$', base)
    if year_match:
        year_str = year_match.group(1)
        month_code = year_match.group(2)
        
        # Convert 2-digit year to 4-digit
        year = 2000 + int(year_str)
        
        # Month codes: J=F, F=G, M=H, A=I, M=J, J=K, J=L, A=M, S=N, O=O, N=P, D=Q
        # Common: JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
        month_map = {
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
            'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
            'J': 1, 'F': 2, 'M': 3, 'A': 4, 'M': 5, 'J': 6,
            'J': 7, 'A': 8, 'S': 9, 'O': 10, 'N': 11, 'D': 12
        }
        
        # Single letter month codes (NSE style)
        single_letter_map = {
            'J': 1, 'F': 2, 'M': 3, 'A': 4, 'M': 5, 'J': 6,
            'J': 7, 'A': 8, 'S': 9, 'O': 10, 'N': 11, 'D': 12
        }
        
        if len(month_code) == 1:
            # Single letter: J, F, M, A, M, J, J, A, S, O, N, D
            month = single_letter_map.get(month_code, 12)
        elif month_code in month_map:
            month = month_map[month_code]
        else:
            # Default to December if can't parse
            month = 12
        
        # Get underlying symbol (everything before year)
        underlying = base[:-len(year_str + month_code)]
        
        # For MCX, keep the underlying as-is (e.g., CRUDEOILM)
        # For NSE, underlying is already correct (e.g., NIFTY)
        
        # For expiry, use last Thursday of the month (standard for NSE)
        # Simplified: use last day of month
        from calendar import monthrange
        last_day = monthrange(year, month)[1]
        expiry_date = datetime(year, month, last_day, 15, 30, 0)  # 3:30 PM IST
        
        return {
            'underlying': underlying,  # e.g., "CRUDEOILM" for MCX or "NIFTY" for NSE
            'expiry_date': expiry_date,
            'strike': strike,
            'option_type': option_type,
            'is_option': True,
            'is_mcx': is_mcx  # Add flag to indicate MCX contract
        }
    
    return None

def get_option_price_from_fyers(option_symbol):
    """Get current option price from Fyers API"""
    try:
        if FyresIntegration.fyers is None:
            return None
        
        response = fyres_quote(option_symbol)
        if response and 'd' in response and len(response['d']) > 0:
            # Get last traded price
            ltp = response['d'][0]['v'].get('lp', None)
            return ltp
    except Exception as e:
        print(f"Error getting option price for {option_symbol}: {e}")
    return None

def get_underlying_price_from_fyers(underlying_symbol):
    """Get current underlying asset price from Fyers API"""
    try:
        if FyresIntegration.fyers is None:
            return None
        
        # Try to get quote for underlying
        response = fyres_quote(underlying_symbol)
        if response and 'd' in response and len(response['d']) > 0:
            ltp = response['d'][0]['v'].get('lp', None)
            return ltp
    except Exception as e:
        print(f"Error getting underlying price for {underlying_symbol}: {e}")
    return None

def get_future_symbol(underlying, expiry_date):
    """
    Construct future symbol based on underlying and expiry date
    For NIFTY -> NSE:NIFTY25NOVFUT
    For BANKNIFTY -> NSE:BANKNIFTY25NOVFUT
    For MCX contracts (e.g., CRUDEOILM) -> MCX:CRUDEOILM25NOVFUT
    """
    if not expiry_date:
        return None
    
    # Remove MCX: prefix if present (in case it was passed with prefix)
    if underlying.startswith('MCX:'):
        underlying = underlying[4:]
    
    # Extract year (last 2 digits) and month from expiry date
    year_2digit = expiry_date.strftime('%y')  # e.g., "25" for 2025
    month_code = expiry_date.strftime('%b').upper()[:3]  # e.g., "NOV" for November
    
    # Use full 3-letter month code (NOV, DEC, etc.)
    # Format: NIFTY25NOVFUT (25 = year, NOV = month, FUT = future)
    
    # Construct future symbol
    if 'NIFTY' in underlying.upper() and 'BANK' not in underlying.upper():
        # NIFTY future: NSE:NIFTY25NOVFUT
        future_symbol = f"NSE:NIFTY{year_2digit}{month_code}FUT"
    elif 'BANKNIFTY' in underlying.upper() or 'BANK' in underlying.upper():
        # BANKNIFTY future: NSE:BANKNIFTY25NOVFUT
        future_symbol = f"NSE:BANKNIFTY{year_2digit}{month_code}FUT"
    else:
        # Check if it's an MCX contract (MCX commodities like CRUDEOIL, GOLD, SILVER, etc.)
        # MCX underlying format: COMMODITY + MONTHCODE (e.g., CRUDEOILM, GOLDM, SILVERM)
        # MCX future format: MCX:COMMODITY + MONTHCODE + YY + MONTH + FUT
        # e.g., MCX:CRUDEOILM25NOVFUT
        mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM']
        underlying_upper = underlying.upper()
        
        # Check if underlying starts with any MCX commodity name
        is_mcx = False
        for commodity in mcx_commodities:
            if underlying_upper.startswith(commodity):
                is_mcx = True
                break
        
        if is_mcx:
            # MCX future: MCX:COMMODITY + MONTHCODE + YY + MONTH + FUT
            # e.g., MCX:CRUDEOILM25NOVFUT
            future_symbol = f"MCX:{underlying}{year_2digit}{month_code}FUT"
        else:
            return None
    
    return future_symbol

def get_future_ltp(future_symbol):
    """Get Last Traded Price (LTP) of future from Fyers API"""
    try:
        if FyresIntegration.fyers is None:
            return None
        
        # Use get_ltp function from FyresIntegration
        from FyresIntegration import get_ltp
        ltp = get_ltp(future_symbol)
        return ltp
    except Exception as e:
        print(f"Error getting future LTP for {future_symbol}: {e}")
        return None

def calculate_atm_strike(future_ltp, strike_distance=50):
    """
    Calculate At-The-Money (ATM) strike price
    For NIFTY, strike distance is 50, so round to nearest 50
    
    Parameters:
    - future_ltp: Last Traded Price of the future
    - strike_distance: Strike interval (50 for NIFTY, 100 for BANKNIFTY)
    
    Returns:
    - Rounded strike price
    """
    if future_ltp is None or future_ltp <= 0:
        return None
    
    # Round to nearest strike_distance
    atm_strike = round(future_ltp / strike_distance) * strike_distance
    return int(atm_strike)

def generate_option_symbol(underlying, expiry_date, strike, option_type, expiry_type='weekly', is_mcx=False):
    """
    Generate option symbol based on underlying, expiry, strike, and option type
    
    Parameters:
    - underlying: Underlying symbol (e.g., "NIFTY", "CRUDEOIL")
    - expiry_date: Expiry date (datetime object)
    - strike: Strike price (integer)
    - option_type: 'c' for Call (CE) or 'p' for Put (PE)
    - expiry_type: 'weekly' or 'monthly'
    - is_mcx: Boolean, True for MCX contracts (adds MCX: prefix)
    
    Returns:
    - Option symbol string (e.g., "NSE:NIFTY25N1824500CE" for weekly or "MCX:CRUDEOIL25NOV25500CE" for MCX monthly)
    """
    try:
        # Extract year (last 2 digits)
        year_2digit = expiry_date.strftime('%y')  # e.g., "25" for 2025
        
        # Determine option type suffix
        option_suffix = 'CE' if option_type.lower() == 'c' else 'PE'
        
        # Determine exchange prefix
        exchange_prefix = 'MCX:' if is_mcx else 'NSE:'
        
        if expiry_type.lower() == 'weekly':
            # Weekly format: NSE:NIFTY25N1824500CE or MCX:CRUDEOIL25N1824500CE
            # Extract month code (single letter: J, F, M, A, M, J, J, A, S, O, N, D)
            month_codes = ['J', 'F', 'M', 'A', 'M', 'J', 'J', 'A', 'S', 'O', 'N', 'D']
            month = expiry_date.month - 1  # 0-indexed
            month_code = month_codes[month]
            
            # Extract day (2 digits)
            day = expiry_date.strftime('%d')  # e.g., "18"
            
            # Format: Exchange:Underlying + year + month_code + day + strike + option_suffix
            option_symbol = f"{exchange_prefix}{underlying}{year_2digit}{month_code}{day}{strike}{option_suffix}"
            
        else:  # monthly
            if is_mcx:
                # MCX monthly format: MCX:CRUDEOILM25NOV5300CE
                # Underlying already includes month code (e.g., CRUDEOILM)
                # Extract month abbreviation (3 letters: JAN, FEB, MAR, etc.)
                month_abbr = expiry_date.strftime('%b').upper()  # e.g., "NOV"
                
                # Format: MCX:UnderlyingWithMonthCode + year + month_abbr + strike + option_suffix
                # e.g., MCX:CRUDEOILM25NOV5300CE
                option_symbol = f"{exchange_prefix}{underlying}{year_2digit}{month_abbr}{strike}{option_suffix}"
            else:
                # NSE monthly format: NSE:NIFTY25NOV25500CE
                # Extract month abbreviation (3 letters: JAN, FEB, MAR, etc.)
                month_abbr = expiry_date.strftime('%b').upper()  # e.g., "NOV"
                
                # Format: Exchange:Underlying + year + month_abbr + strike + option_suffix
                option_symbol = f"{exchange_prefix}{underlying}{year_2digit}{month_abbr}{strike}{option_suffix}"
        
        return option_symbol
    except Exception as e:
        print(f"Error generating option symbol: {e}")
        return None

def calculate_iv_pyvollib(option_price, underlying_price, strike, time_to_expiry, risk_free_rate=0.1, option_type='c'):
    """
    Calculate Implied Volatility using py_vollib Black model (for options on futures)
    
    The Black model is appropriate for options on futures/forwards, which is the case
    for Indian options that are settled on futures prices.
    
    Parameters:
    - option_price: Current option price
    - underlying_price: Current futures/forward price (F) - NOT spot price
    - strike: Strike price (K)
    - time_to_expiry: Time to expiration in years (t)
    - risk_free_rate: Risk-free interest rate (r), default 0.1 (10%)
    - option_type: 'c' for call, 'p' for put
    
    Returns: Implied volatility as decimal (e.g., 0.20 for 20%)
    
    Note: Black model parameter order: (price, F, K, r, t, flag)
    This is different from Black-Scholes: (price, S, K, t, r, flag)
    """
    if not PY_VOLLIB_AVAILABLE:
        return None
    
    if option_price is None or underlying_price is None or strike is None:
        return None
    
    if option_price <= 0 or underlying_price <= 0 or strike <= 0:
        return None
    
    if time_to_expiry <= 0:
        return None
    
    try:
        # py_vollib.black.implied_volatility expects:
        # implied_volatility(price, F, K, r, t, flag)
        # Note: r and t are in different order than Black-Scholes!
        iv = implied_volatility(
            float(option_price),
            float(underlying_price),  # F = futures/forward price
            float(strike),            # K = strike price
            float(risk_free_rate),    # r = risk-free rate
            float(time_to_expiry),    # t = time to expiry in years
            option_type               # flag = 'c' or 'p'
        )
        return iv
    except Exception as e:
        # Suppress expected errors for options priced below intrinsic value
        # This is normal when option price < intrinsic value (no valid IV exists)
        error_msg = str(e).lower()
        if 'intrinsic' in error_msg or 'below' in error_msg:
            # This is expected - option price is below intrinsic value, no IV can be calculated
            return None
        else:
            # Log unexpected errors only
            print(f"Error calculating IV with py_vollib Black model: {e}")
        return None

def safe_fetch_ohlc(symbol, timeframe):
    """
    Safely fetch OHLC data with proper error handling
    """
    try:
        if FyresIntegration.fyers is None:
            return None
        
        # Call the original fetchOHLC function
        df = fetchOHLC(symbol, timeframe)
        return df
    except KeyError as e:
        # Handle case where API response doesn't have 'candles' key
        print(f"API response error for {symbol}: Missing 'candles' key in response")
        print(f"Full error: {e}")
        return None
    except Exception as e:
        print(f"Error fetching OHLC data for {symbol}: {e}")
        return None

def calculate_iv(df, window=20, timeframe='1D', symbol=None, risk_free_rate=0.1, 
                manual_strike=None, manual_expiry=None, manual_option_type=None):
    """
    Calculate Implied Volatility using py_vollib Black model (for options) or Historical Volatility (for underlying)
    
    For Options:
    - Uses py_vollib Black model with option prices (appropriate for options on futures)
    - Requires: option_price, futures_price, strike, time_to_expiry
    
    Parameters:
    - manual_strike: Optional manual strike price (overrides parsed value)
    - manual_expiry: Optional manual expiry datetime string (overrides parsed value)
    - manual_option_type: Optional manual option type 'c' or 'p' (overrides parsed value)
    
    For Underlying Assets (fallback):
    - Uses rolling standard deviation of log returns (Historical Volatility)
    - Formula: IV = std(ln(P_t/P_{t-1})) × √(periods_per_year) × 100
    
    Returns annualized volatility as percentage (%)
    """
    if df is None or len(df) < 2:
        return None
    
    # Check if symbol is an option or if manual option parameters are provided
    option_info = None
    
    # If manual parameters provided, create option_info dict
    if manual_strike is not None or manual_expiry is not None or manual_option_type:
        # Try to parse symbol first, then override with manual values
        parsed_info = parse_option_symbol(symbol) if symbol else None
        
        # Parse expiry datetime if provided
        expiry_date = None
        if manual_expiry:
            try:
                # datetime-local format: "YYYY-MM-DDTHH:mm"
                expiry_date = datetime.strptime(manual_expiry, '%Y-%m-%dT%H:%M')
            except ValueError:
                try:
                    # Try ISO format
                    expiry_date = datetime.fromisoformat(manual_expiry.replace('Z', '+00:00'))
                except:
                    print(f"Could not parse expiry date: {manual_expiry}")
        
        # Extract underlying symbol
        underlying = None
        if parsed_info:
            underlying = parsed_info['underlying']
        elif ':' in symbol:
            # Extract from format like "NSE:RELIANCE-EQ"
            underlying = symbol.split(':')[-1].split('-')[0]
        else:
            # Use symbol as-is, but try to remove option suffixes
            underlying = symbol.replace('CE', '').replace('PE', '').rstrip('0123456789')
        
        option_info = {
            'underlying': underlying if underlying else symbol,
            'strike': manual_strike if manual_strike is not None else (parsed_info['strike'] if parsed_info else None),
            'expiry_date': expiry_date if expiry_date else (parsed_info['expiry_date'] if parsed_info else None),
            'option_type': manual_option_type if manual_option_type else (parsed_info['option_type'] if parsed_info else 'c'),
            'is_option': True
        }
        
        # Validate required fields
        if option_info['strike'] is None or option_info['expiry_date'] is None:
            print("Manual option parameters incomplete, trying to parse symbol...")
            option_info = parse_option_symbol(symbol) if symbol else None
    else:
        # Auto-detect from symbol
        option_info = parse_option_symbol(symbol) if symbol else None
    
    if option_info and PY_VOLLIB_AVAILABLE and option_info.get('strike') and option_info.get('expiry_date'):
        # Calculate IV using py_vollib Black model for options (options on futures)
        print(f"Calculating IV using py_vollib Black model for option: {symbol}")
        print(f"  Strike: {option_info['strike']}, Type: {option_info['option_type']}, Expiry: {option_info['expiry_date']}")
        
        underlying_symbol = option_info['underlying']
        expiry_date = option_info['expiry_date']
        
        # Normalize expiry_date to timezone-naive to avoid timezone mismatch errors
        if expiry_date.tzinfo is not None:
            expiry_date = expiry_date.replace(tzinfo=None)
        
        # Get future symbol for NIFTY or BANKNIFTY
        future_symbol = get_future_symbol(underlying_symbol, expiry_date)
        
        if future_symbol:
            print(f"  Using future symbol: {future_symbol}")
            
            # Fetch historical data for future symbol
            print(f"  Fetching historical data for future: {future_symbol}")
            df_future = safe_fetch_ohlc(future_symbol, timeframe)
            
            if df_future is None or len(df_future) == 0:
                print(f"  Could not fetch historical data for {future_symbol}, falling back to historical volatility")
            else:
                print(f"  Fetched {len(df_future)} rows of future data")
                
                # Prepare option dataframe - keep only date and close
                df_option = df[['date', 'close']].copy()
                df_option['date'] = pd.to_datetime(df_option['date'])
                
                # Prepare future dataframe - keep only date and close, rename close to fclose
                df_future_prep = df_future[['date', 'close']].copy()
                df_future_prep.rename(columns={'close': 'fclose'}, inplace=True)
                df_future_prep['date'] = pd.to_datetime(df_future_prep['date'])
                
                # Merge option and future data by date
                df_merged = pd.merge(df_option, df_future_prep, on='date', how='inner')
                
                if len(df_merged) == 0:
                    print(f"  No matching dates between option and future data, falling back to historical volatility")
                else:
                    print(f"  Merged {len(df_merged)} rows of data")
                    
                    # Calculate IV for each row using historical future prices
                    iv_values = []
                    
                    for idx, row in df_merged.iterrows():
                        option_price = row['close']
                        future_price = row['fclose']
                        
                        if option_price > 0 and future_price > 0:
                            # Calculate time to expiry for this timestamp
                            row_date = row['date'] if isinstance(row['date'], datetime) else pd.to_datetime(row['date'])
                            
                            # Fix timezone mismatch: make row_date timezone-naive if needed
                            if row_date.tzinfo is not None:
                                # If row_date is timezone-aware, convert to naive
                                row_date = row_date.replace(tzinfo=None)
                            
                            # expiry_date is already normalized to naive above, so we can use it directly
                            time_to_expiry = (expiry_date - row_date).total_seconds() / (365.25 * 24 * 3600)
                            
                            if time_to_expiry > 0:
                                # Calculate IV using py_vollib Black model with historical future price
                                iv_decimal = calculate_iv_pyvollib(
                                    option_price=option_price,
                                    underlying_price=future_price,
                                    strike=option_info['strike'],
                                    time_to_expiry=time_to_expiry,
                                    risk_free_rate=risk_free_rate,
                                    option_type=option_info['option_type']
                                )
                                
                                if iv_decimal is not None:
                                    iv_values.append(iv_decimal * 100)  # Convert to percentage
                                else:
                                    iv_values.append(np.nan)
                            else:
                                iv_values.append(np.nan)  # Option expired
                        else:
                            iv_values.append(np.nan)
                    
                    # Add IV column
                    df_merged['iv'] = iv_values
                    
                    # Fill NaN values with forward fill, then backward fill
                    df_merged['iv'] = df_merged['iv'].ffill().bfill().fillna(0)
                    
                    # Add required columns for CSV export
                    df_merged['option_name'] = symbol  # Option symbol name
                    df_merged['underlying_name'] = future_symbol  # Underlying future name
                    
                    # Add metadata columns
                    if option_info.get('strike'):
                        df_merged['strike'] = option_info['strike']
                    if option_info.get('expiry_date'):
                        df_merged['expiry'] = option_info['expiry_date'].strftime('%Y-%m-%d %H:%M:%S')
                    if option_info.get('option_type'):
                        df_merged['option_type'] = option_info['option_type']
                    df_merged['timeframe'] = timeframe
                    
                    # Ensure date is in the right format
                    df_merged['date'] = pd.to_datetime(df_merged['date'])
                    
                    # Debug: Print columns before returning
                    print(f"  DataFrame columns before return: {list(df_merged.columns)}")
                    print(f"  DataFrame shape: {df_merged.shape}")
                    
                    # Get valid IVs for logging
                    valid_ivs = [iv for iv in iv_values if iv is not None and not (isinstance(iv, float) and (np.isnan(iv) or np.isinf(iv)))]
                    if len(valid_ivs) > 0:
                        print(f"Calculated IV using py_vollib Black model with historical future prices (range: {min(valid_ivs):.2f}% - {max(valid_ivs):.2f}%)")
                    
                    return df_merged
        else:
            print(f"Could not construct future symbol for {underlying_symbol}, falling back to historical volatility")
        
        print("Failed to calculate IV with py_vollib Black model, falling back to historical volatility")
    
    # Fallback to Historical Volatility calculation
    print("Using Historical Volatility calculation (rolling standard deviation)")
    
    # Calculate log returns
    df['returns'] = np.log(df['close'] / df['close'].shift(1))
    
    # Determine annualization factor based on timeframe
    timeframe_factors = {
        '1s': 252 * 375 * 60,  # 1 second: 22500 seconds per day * 252 days
        '1': 252 * 375,  # 1 minute: 375 minutes per day * 252 days
        '5': 252 * 75,   # 5 minutes: 75 periods per day
        '15': 252 * 25,  # 15 minutes: 25 periods per day
        '30': 252 * 12,  # 30 minutes: 12 periods per day
        '60': 252 * 6,   # 1 hour: 6 periods per day
        '120': 252 * 3,  # 2 hours: 3 periods per day
        '1D': 252,       # Daily: 252 trading days
    }
    
    # Get the appropriate factor, default to daily if not found
    periods_per_year = timeframe_factors.get(str(timeframe), 252)
    
    # Calculate rolling volatility (annualized)
    if len(df) > window:
        df['iv'] = df['returns'].rolling(window=min(window, len(df))).std() * np.sqrt(periods_per_year) * 100
    else:
        # If not enough data, use all available data
        df['iv'] = df['returns'].std() * np.sqrt(periods_per_year) * 100
    
    # Fill NaN values with forward fill
    df['iv'] = df['iv'].ffill()
    
    # Fill any remaining NaN with 0
    df['iv'] = df['iv'].fillna(0)
    
    # Add timeframe metadata for CSV export
    df['timeframe'] = timeframe
    
    # For historical volatility fallback, still try to add option metadata if it's an option symbol
    # This ensures CSV always has the required columns
    if symbol:
        # Try to parse option symbol and get underlying price
        option_info = parse_option_symbol(symbol)
        if option_info and option_info.get('underlying'):
            underlying_symbol = option_info['underlying']
            expiry_date = option_info.get('expiry_date')
            if expiry_date:
                future_symbol = get_future_symbol(underlying_symbol, expiry_date)
                if future_symbol:
                    # Try to fetch historical future data for fclose column
                    print(f"  Attempting to fetch future data for fallback: {future_symbol}")
                    df_future = safe_fetch_ohlc(future_symbol, timeframe)
                    
                    if df_future is not None and len(df_future) > 0:
                        # Merge with future data to get fclose
                        df_future_prep = df_future[['date', 'close']].copy()
                        df_future_prep.rename(columns={'close': 'fclose'}, inplace=True)
                        df_future_prep['date'] = pd.to_datetime(df_future_prep['date'])
                        df['date'] = pd.to_datetime(df['date'])
                        
                        df = pd.merge(df, df_future_prep, on='date', how='left')
                        print(f"  Merged future data: {df['fclose'].notna().sum()} rows have fclose values")
                    else:
                        # If can't fetch future data, use current LTP for all rows
                        underlying_price = get_future_ltp(future_symbol)
                        if underlying_price:
                            df['fclose'] = underlying_price
                            print(f"  Using current LTP for fclose: {underlying_price}")
                    
                    # Always add required columns
                    df['option_name'] = symbol
                    df['underlying_name'] = future_symbol
                    if option_info.get('strike'):
                        df['strike'] = option_info['strike']
                    if expiry_date:
                        df['expiry'] = expiry_date.strftime('%Y-%m-%d %H:%M:%S')
                    if option_info.get('option_type'):
                        df['option_type'] = option_info['option_type']
        else:
            # Not an option, just add symbol name
            df['symbol_name'] = symbol
    
    return df

def fetch_data_loop_automatic(future_symbol, expiry_date, expiry_type, option_type, timeframe, strike_distance, risk_free_rate=0.1):
    """
    Continuously fetch data in automatic mode:
    1. Get future LTP
    2. Calculate ATM strike
    3. Generate option symbol
    4. Fetch option data and calculate IV
    5. Repeat every 1 second
    """
    global iv_data_store, fetching_status
    
    # Extract underlying from future symbol
    if ':' in future_symbol:
        exchange_part = future_symbol.split(':')[0]  # "NSE" or "MCX"
        underlying_part = future_symbol.split(':')[1]
    else:
        exchange_part = None
        underlying_part = future_symbol
    
    # Detect MCX contracts
    is_mcx = (exchange_part and exchange_part.upper() == 'MCX') or 'MCX:' in future_symbol.upper()
    
    underlying = None
    if is_mcx:
        # MCX contracts: Remove year (2 digits), month (3 letters), and FUT suffix
        # e.g., CRUDEOILM25NOVFUT -> CRUDEOILM
        # Pattern: COMMODITY + MONTHCODE + YY + MONTH + FUT
        if underlying_part.endswith('FUT'):
            # Remove FUT suffix first
            base = underlying_part[:-3]
            # Remove year (2 digits) + month (3 letters) from end
            # Pattern: YY + MONTH (e.g., 25NOV)
            import re
            # Remove pattern: 2 digits + 3 letters from the end
            underlying = re.sub(r'\d{2}[A-Z]{3}$', '', base)
            # If regex didn't match, try simple approach
            if underlying == base and len(base) >= 5:
                # Check if last 5 characters match YY + MONTH pattern (2 digits + 3 letters)
                if base[-5:-3].isdigit() and base[-3:].isalpha():
                    underlying = base[:-5]
                else:
                    # Last resort: assume format is COMMODITY + MONTHCODE, keep as is
                    underlying = base
        else:
            # No FUT suffix, might be just the commodity+monthcode
            underlying = underlying_part
    elif 'NIFTY' in underlying_part:
        if 'BANK' in underlying_part:
            underlying = 'BANKNIFTY'
        else:
            underlying = 'NIFTY'
    
    if not underlying:
        print(f"Error: Could not extract underlying from {future_symbol}")
        return
    
    iteration = 0
    while fetching_status["active"] and fetching_status.get("mode") == "automatic":
        try:
            iteration += 1
            print(f"\n=== Automatic Mode Iteration {iteration} ===")
            
            # Check if fyers is available
            if FyresIntegration.fyers is None:
                print("Fyers not initialized. Waiting...")
                time.sleep(5)
                continue
            
            # Get future LTP
            future_ltp = get_future_ltp(future_symbol)
            if future_ltp is None:
                print(f"Could not fetch LTP for {future_symbol}. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            print(f"Future LTP: {future_ltp}")
            
            # Calculate ATM strike
            # Use the strike_distance passed to the function (from user input or defaults)
            # strike_distance is already set from the function parameter, no need to recalculate
            atm_strike = calculate_atm_strike(future_ltp, strike_distance)
            if atm_strike is None:
                print(f"Could not calculate ATM strike. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            print(f"ATM Strike: {atm_strike}")
            
            # Generate option symbol (with MCX: prefix for MCX contracts)
            symbol = generate_option_symbol(underlying, expiry_date, atm_strike, option_type, expiry_type, is_mcx=is_mcx)
            if not symbol:
                print(f"Could not generate option symbol. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            print(f"Generated Option Symbol: {symbol}")
            
            # Update fetching status with current symbol
            fetching_status["symbol"] = symbol
            fetching_status["strike"] = atm_strike
            
            # Fetch historical data for the option symbol
            df = safe_fetch_ohlc(symbol, timeframe)
            
            if df is None or len(df) == 0:
                print(f"Failed to fetch data for {symbol}. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            # Calculate IV
            df_with_iv = calculate_iv(
                df.copy(),
                window=20,
                timeframe=timeframe,
                symbol=symbol,
                risk_free_rate=risk_free_rate,
                manual_strike=atm_strike,
                manual_expiry=expiry_date.isoformat(),
                manual_option_type=option_type
            )
            
            if df_with_iv is not None and 'iv' in df_with_iv.columns:
                # Filter out zero IV values for charting (but keep in CSV)
                iv_values_for_chart = df_with_iv['iv'].fillna(0).tolist()
                timestamps_for_chart = df_with_iv['date'].astype(str).tolist()
                
                # Store IV data with timestamps
                iv_data_store[symbol] = {
                    "timestamps": timestamps_for_chart,
                    "iv_values": iv_values_for_chart,
                    "close_prices": df_with_iv['close'].tolist(),
                    "fclose_prices": df_with_iv['fclose'].tolist() if 'fclose' in df_with_iv.columns else [],
                    "last_update": datetime.now().isoformat()
                }
                
                # Log IV statistics
                non_zero_ivs = [iv for iv in iv_values_for_chart if iv > 0]
                if non_zero_ivs:
                    print(f"IV data stored: {len(non_zero_ivs)} non-zero values (range: {min(non_zero_ivs):.2f}% - {max(non_zero_ivs):.2f}%)")
                
                # Save IV calculation to CSV file
                save_iv_to_csv(
                    symbol=symbol,
                    df_with_iv=df_with_iv,
                    timeframe=timeframe,
                    strike=atm_strike,
                    expiry=expiry_date.isoformat(),
                    option_type=option_type
                )
            else:
                print(f"Warning: Could not calculate IV for {symbol}")
            
            # Wait 1 second before next iteration
            print(f"Waiting 1 second before next update...")
            time.sleep(1)
            
        except Exception as e:
            print(f"Error in automatic fetch loop: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)  # Wait before retrying on error

def fetch_data_loop(symbol, timeframe, manual_strike=None, manual_expiry=None, manual_option_type=None, risk_free_rate=0.1):
    """Continuously fetch historical data and calculate IV"""
    global iv_data_store, fetching_status
    
    while fetching_status["active"] and fetching_status["symbol"] == symbol and fetching_status["timeframe"] == timeframe:
        try:
            # Check if fyers is available
            if FyresIntegration.fyers is None:
                print("Fyers not initialized. Waiting...")
                time.sleep(5)
                continue
            
            # Fetch historical data using safe wrapper
            df = safe_fetch_ohlc(symbol, timeframe)
            
            if df is None:
                print(f"Failed to fetch data for {symbol}. This might be due to:")
                print("  - Invalid symbol format")
                print("  - Insufficient historical data available")
                print("  - API rate limiting")
                print("  - Symbol not supported for the selected timeframe")
                time.sleep(5)
                continue
            
            if df is not None and len(df) > 0:
                try:
                    # Calculate IV (will use py_vollib Black model for options, historical volatility for underlying)
                    df_with_iv = calculate_iv(
                        df.copy(), 
                        window=20, 
                        timeframe=timeframe, 
                        symbol=symbol, 
                        risk_free_rate=risk_free_rate,
                        manual_strike=manual_strike,
                        manual_expiry=manual_expiry,
                        manual_option_type=manual_option_type
                    )
                    
                    if df_with_iv is not None and 'iv' in df_with_iv.columns:
                        # Filter out zero IV values for charting (but keep in CSV)
                        iv_values_for_chart = df_with_iv['iv'].fillna(0).tolist()
                        timestamps_for_chart = df_with_iv['date'].astype(str).tolist()
                        
                        # Store IV data with timestamps
                        iv_data_store[symbol] = {
                            "timestamps": timestamps_for_chart,
                            "iv_values": iv_values_for_chart,
                            "close_prices": df_with_iv['close'].tolist(),
                            "fclose_prices": df_with_iv['fclose'].tolist() if 'fclose' in df_with_iv.columns else [],
                            "last_update": datetime.now().isoformat()
                        }
                        
                        # Log IV statistics
                        non_zero_ivs = [iv for iv in iv_values_for_chart if iv > 0]
                        if non_zero_ivs:
                            print(f"IV data stored: {len(non_zero_ivs)} non-zero values (range: {min(non_zero_ivs):.2f}% - {max(non_zero_ivs):.2f}%)")
                        else:
                            print(f"Warning: All IV values are zero for {symbol}")
                        
                        # Save IV calculation to CSV file
                        save_iv_to_csv(
                            symbol=symbol,
                            df_with_iv=df_with_iv,
                            timeframe=timeframe,
                            strike=manual_strike,
                            expiry=manual_expiry,
                            option_type=manual_option_type
                        )
                except Exception as e:
                    print(f"Error calculating IV for {symbol}: {e}")
            else:
                print(f"No data received for {symbol}. Retrying...")
            
            # Wait before next fetch (adjust interval as needed)
            time.sleep(1)  # Fetch every 1 second
            
        except Exception as e:
            print(f"Unexpected error in fetch loop: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(1)  # Wait 1 second before retrying on error

@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def login():
    """Handle Fyers API login"""
    try:
        print("Login request received")
        credentials = load_credentials()
        if not credentials:
            print("Failed to load credentials")
            return jsonify({"success": False, "message": "Failed to load credentials. Please check FyersCredentials.csv file."}), 500
        
        # Validate required credentials
        required_fields = ['client_id', 'secret_key', 'FY_ID', 'totpkey', 'PIN', 'redirect_uri']
        missing_fields = [field for field in required_fields if not credentials.get(field)]
        if missing_fields:
            print(f"Missing credentials: {missing_fields}")
            return jsonify({"success": False, "message": f"Missing credentials: {', '.join(missing_fields)}"}), 500
        
        print("Starting automated login...")
        # Perform automated login
        automated_login(
            client_id=credentials.get('client_id'),
            secret_key=credentials.get('secret_key'),
            FY_ID=credentials.get('FY_ID'),
            TOTP_KEY=credentials.get('totpkey'),
            PIN=credentials.get('PIN'),
            redirect_uri=credentials.get('redirect_uri')
        )
        
        print("Login process completed, checking fyers object...")
        # Check if login was successful
        if FyresIntegration.fyers is not None:
            try:
                # Try to get profile to verify login
                profile = FyresIntegration.fyers.get_profile()
                print("Login successful, profile:", profile)
                session['logged_in'] = True
                return jsonify({"success": True, "message": "Login successful"})
            except Exception as e:
                print(f"Error verifying login: {e}")
                return jsonify({"success": False, "message": f"Login completed but verification failed: {str(e)}"}), 401
        else:
            print("Login failed: fyers object is None")
            return jsonify({"success": False, "message": "Login failed: Could not initialize Fyers session"}), 401
            
    except KeyError as e:
        # KeyError could be from accessing credentials or from automated_login function
        # Check if it's from credentials access (which we validate above) or from login process
        error_key = str(e).strip("'\"")
        
        # If KeyError is from automated_login (auth_code, access_token, etc.), 
        # it's a login process error, not a credentials file error
        if error_key in ['access_token', 'refresh_token', 'auth_code', 'Url', 'request_key', 'data']:
            # These come from API responses during login, not from credentials file
            error_msg = f"Login process error: Missing '{error_key}' in API response. This may indicate an issue with the login flow."
            print(error_msg)
            print(f"Full KeyError: {e}")
            import traceback
            traceback.print_exc()
            return jsonify({"success": False, "message": error_msg}), 500
        else:
            # This might be from credentials access
            error_msg = f"Missing required field in credentials: {str(e)}"
            print(error_msg)
            return jsonify({"success": False, "message": error_msg}), 500
    except Exception as e:
        error_msg = f"Login error: {str(e)}"
        print(error_msg)
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": error_msg}), 500

@app.route('/api/check_login', methods=['GET'])
def check_login():
    """Check if user is logged in"""
    if FyresIntegration.fyers is not None:
        try:
            profile = FyresIntegration.fyers.get_profile()
            return jsonify({"logged_in": True, "profile": profile})
        except:
            return jsonify({"logged_in": False})
    return jsonify({"logged_in": False})

@app.route('/api/start_fetching', methods=['POST'])
def start_fetching():
    """Start fetching historical data and calculating IV"""
    global fetching_status, iv_data_store
    
    data = request.json
    mode = data.get('mode', 'manual')  # 'manual' or 'automatic'
    timeframe = data.get('timeframe')
    risk_free_rate = data.get('risk_free_rate', 0.10)  # Default 10% (0.10)
    
    if not timeframe:
        return jsonify({"success": False, "message": "Timeframe is required"}), 400
    
    # Validate risk_free_rate
    try:
        risk_free_rate = float(risk_free_rate)
        if risk_free_rate < 0 or risk_free_rate > 1:
            return jsonify({"success": False, "message": "Risk-free rate must be between 0 and 1 (0% to 100%)"}), 400
    except (ValueError, TypeError):
        return jsonify({"success": False, "message": "Invalid risk-free rate format"}), 400
    
    if not FyresIntegration.fyers:
        return jsonify({"success": False, "message": "Please login first"}), 401
    
    # Stop previous fetching if active
    old_symbol = fetching_status.get("symbol")
    if fetching_status["active"]:
        fetching_status["active"] = False
        time.sleep(1)  # Wait for thread to stop
    
    # Clear old data: delete previous symbol's CSV file and clear iv_data_store
    print("Clearing old data before starting new fetch...")
    if old_symbol:
        deleted_count = delete_csv_files(old_symbol)  # Delete old symbol's CSV file
        if old_symbol in iv_data_store:
            del iv_data_store[old_symbol]
            print(f"Cleared IV data store for old symbol: {old_symbol}")
    
    # Clear any other symbols in the store
    iv_data_store.clear()
    print("Cleared all IV data from memory")
    
    if mode == 'automatic':
        # Automatic mode: Generate option symbol from future symbol
        future_symbol = data.get('future_symbol')
        expiry_type = data.get('expiry_type', 'weekly')  # 'weekly' or 'monthly'
        expiry_date_str = data.get('expiry_date')
        option_type = data.get('option_type', 'c')  # 'c' for Call, 'p' for Put
        
        if not future_symbol or not expiry_date_str:
            return jsonify({"success": False, "message": "Future symbol and expiry date are required for automatic mode"}), 400
        
        try:
            # Parse expiry date
            expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d')
        except:
            try:
                expiry_date = datetime.fromisoformat(expiry_date_str.replace('Z', '+00:00'))
                if expiry_date.tzinfo:
                    expiry_date = expiry_date.replace(tzinfo=None)
            except:
                return jsonify({"success": False, "message": "Invalid expiry date format"}), 400
        
        # Extract underlying from future symbol (e.g., "NSE:NIFTY25NOVFUT" -> "NIFTY", "MCX:CRUDEOILM" -> "CRUDEOIL")
        if ':' in future_symbol:
            exchange_part = future_symbol.split(':')[0]  # "NSE" or "MCX"
            underlying_part = future_symbol.split(':')[1]
        else:
            exchange_part = None
            underlying_part = future_symbol
        
        # Detect MCX contracts
        is_mcx = (exchange_part and exchange_part.upper() == 'MCX') or 'MCX:' in future_symbol.upper()
        
        # Remove FUT suffix and year/month codes to get underlying
        # For NIFTY25NOVFUT, we need to extract "NIFTY"
        # For MCX:CRUDEOILM25NOVFUT, we need to extract "CRUDEOILM" (remove 25NOVFUT)
        underlying = None
        if is_mcx:
            # MCX contracts: Remove year (2 digits), month (3 letters), and FUT suffix
            # e.g., CRUDEOILM25NOVFUT -> CRUDEOILM
            # Pattern: COMMODITY + MONTHCODE + YY + MONTH + FUT
            if underlying_part.endswith('FUT'):
                # Remove FUT suffix first
                base = underlying_part[:-3]
                # Remove year (2 digits) + month (3 letters) from end
                # Pattern: YY + MONTH (e.g., 25NOV)
                import re
                # Remove pattern: 2 digits + 3 letters from the end
                underlying = re.sub(r'\d{2}[A-Z]{3}$', '', base)
                # If regex didn't match, try simple approach
                if underlying == base and len(base) >= 5:
                    # Check if last 5 characters match YY + MONTH pattern (2 digits + 3 letters)
                    if base[-5:-3].isdigit() and base[-3:].isalpha():
                        underlying = base[:-5]
                    else:
                        # Last resort: assume format is COMMODITY + MONTHCODE, keep as is
                        underlying = base
            else:
                # No FUT suffix, might be just the commodity+monthcode
                underlying = underlying_part
        elif 'NIFTY' in underlying_part:
            if 'BANK' in underlying_part:
                underlying = 'BANKNIFTY'
            else:
                underlying = 'NIFTY'
        
        if not underlying:
            return jsonify({"success": False, "message": "Could not extract underlying from future symbol"}), 400
        
        # Get future LTP
        future_ltp = get_future_ltp(future_symbol)
        if future_ltp is None:
            return jsonify({"success": False, "message": f"Could not fetch LTP for {future_symbol}"}), 400
        
        # Calculate ATM strike
        # Get strike_step from request, or use defaults based on symbol type
        strike_step = data.get('strike_step')
        if strike_step is not None:
            try:
                strike_distance = float(strike_step)
                if strike_distance <= 0:
                    return jsonify({"success": False, "message": "Strike step must be greater than 0"}), 400
            except (ValueError, TypeError):
                return jsonify({"success": False, "message": "Invalid strike step format"}), 400
        else:
            # Default strike distance: 50 for NIFTY and MCX (Crude Oil), 100 for BANKNIFTY
            if is_mcx:
                strike_distance = 50  # MCX contracts (Crude Oil) use 50
            elif 'BANK' in underlying:
                strike_distance = 100  # BANKNIFTY uses 100
            else:
                strike_distance = 50  # NIFTY uses 50
        
        atm_strike = calculate_atm_strike(future_ltp, strike_distance)
        
        if atm_strike is None:
            return jsonify({"success": False, "message": "Could not calculate ATM strike"}), 400
        
        # Generate option symbol (with MCX: prefix for MCX contracts)
        symbol = generate_option_symbol(underlying, expiry_date, atm_strike, option_type, expiry_type, is_mcx=is_mcx)
        
        if not symbol:
            return jsonify({"success": False, "message": "Could not generate option symbol"}), 400
        
        print(f"Automatic mode: Generated option symbol {symbol} from future {future_symbol} (LTP: {future_ltp}, ATM Strike: {atm_strike})")
        
        # Delete new symbol's CSV file to ensure fresh data
        if symbol and symbol != old_symbol:
            delete_csv_files(symbol)
            print(f"Deleted CSV file for new symbol: {symbol}")
        
        # Start new fetching with generated symbol
        fetching_status = {
            "active": True,
            "symbol": symbol,
            "timeframe": timeframe,
            "mode": "automatic",
            "future_symbol": future_symbol,
            "expiry_type": expiry_type,
            "expiry_date": expiry_date_str,
            "option_type": option_type,
            "strike": atm_strike,
            "expiry": expiry_date.isoformat()
        }
        
        # Start fetching in background thread with automatic mode
        # Note: strike_distance is calculated above and passed to the thread
        # Pass strike_step (or None) so the loop can use it or calculate defaults
        thread = threading.Thread(target=fetch_data_loop_automatic, args=(future_symbol, expiry_date, expiry_type, option_type, timeframe, strike_distance, risk_free_rate), daemon=True)
        thread.start()
        
        return jsonify({
            "success": True, 
            "message": "Automatic data fetching started",
            "generated_symbol": symbol,
            "future_ltp": future_ltp,
            "atm_strike": atm_strike,
            "strike_step": strike_distance
        })
    
    else:
        # Manual mode: Use provided symbol
        symbol = data.get('symbol')
        strike = data.get('strike')  # Optional manual strike
        expiry = data.get('expiry')  # Optional manual expiry (datetime string)
        option_type = data.get('option_type')  # Optional manual option type ('c' or 'p')
        
        if not symbol:
            return jsonify({"success": False, "message": "Symbol is required for manual mode"}), 400
        
        # Delete new symbol's CSV file to ensure fresh data
        if symbol and symbol != old_symbol:
            delete_csv_files(symbol)
            print(f"Deleted CSV file for new symbol: {symbol}")
        
        # Start new fetching with optional parameters
        fetching_status = {
            "active": True,
            "symbol": symbol,
            "timeframe": timeframe,
            "mode": "manual",
            "strike": strike,
            "expiry": expiry,
            "option_type": option_type
        }
        
        # Start fetching in background thread
        thread = threading.Thread(target=fetch_data_loop, args=(symbol, timeframe, strike, expiry, option_type, risk_free_rate), daemon=True)
        thread.start()
        
        return jsonify({"success": True, "message": "Data fetching started"})

@app.route('/api/stop_fetching', methods=['POST'])
def stop_fetching():
    """Stop fetching historical data"""
    global fetching_status
    fetching_status["active"] = False
    return jsonify({"success": True, "message": "Data fetching stopped"})

@app.route('/api/get_iv_data', methods=['GET'])
def get_iv_data():
    """Get current IV data for charting"""
    symbol = request.args.get('symbol')
    
    if symbol and symbol in iv_data_store:
        data = iv_data_store[symbol]
        # Log data being sent for debugging
        print(f"Returning IV data for {symbol}: {len(data.get('timestamps', []))} timestamps, {len(data.get('iv_values', []))} IV values")
        return jsonify(data)
    else:
        print(f"No IV data found for symbol: {symbol}")
        return jsonify({"timestamps": [], "iv_values": [], "close_prices": [], "fclose_prices": [], "last_update": None})

@app.route('/api/get_status', methods=['GET'])
def get_status():
    """Get current fetching status"""
    return jsonify(fetching_status)

@app.route('/api/load_csv_data', methods=['GET'])
def load_csv_data():
    """Load IV data from CSV files in data folder"""
    try:
        # Get list of CSV files in data folder
        csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]
        
        if not csv_files:
            return jsonify({"success": False, "message": "No CSV files found in data folder"}), 404
        
        # Get the most recent CSV file or a specific one
        symbol = request.args.get('symbol')
        if symbol:
            # Sanitize symbol for filename
            safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
            safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
            filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
        else:
            # Use the most recent CSV file
            csv_files_with_paths = [(os.path.join(DATA_FOLDER, f), os.path.getmtime(os.path.join(DATA_FOLDER, f))) for f in csv_files]
            csv_files_with_paths.sort(key=lambda x: x[1], reverse=True)
            filename = csv_files_with_paths[0][0]
        
        if not os.path.exists(filename):
            return jsonify({"success": False, "message": f"CSV file not found: {filename}"}), 404
        
        # Read CSV file
        df = pd.read_csv(filename)
        
        # Ensure required columns exist
        if 'date' not in df.columns or 'iv' not in df.columns:
            return jsonify({"success": False, "message": "CSV file missing required columns (date, iv)"}), 400
        
        # Convert date column to datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Sort by date
        df = df.sort_values('date')
        
        # Convert to format expected by frontend
        timestamps = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
        iv_values = df['iv'].fillna(0).tolist()
        close_prices = df['close'].tolist() if 'close' in df.columns else []
        fclose_prices = df['fclose'].tolist() if 'fclose' in df.columns else []
        
        # Get symbol name from filename
        symbol_name = os.path.basename(filename).replace('.csv', '')
        
        return jsonify({
            "success": True,
            "timestamps": timestamps,
            "iv_values": iv_values,
            "close_prices": close_prices,
            "fclose_prices": fclose_prices,
            "symbol": symbol_name,
            "data_points": len(timestamps),
            "last_update": timestamps[-1] if timestamps else None
        })
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/list_csv_files', methods=['GET'])
def list_csv_files():
    """List all available CSV files in data folder"""
    try:
        csv_files = [f.replace('.csv', '') for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]
        return jsonify({"success": True, "files": csv_files})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)

