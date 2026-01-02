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

# Import pytz for timezone handling (for market hours)
try:
    import pytz
    PYTZ_AVAILABLE = True
except ImportError:
    PYTZ_AVAILABLE = False
    print("Warning: pytz not installed. Install with: pip install pytz")

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

# Thread management for fetching
fetch_thread = None  # Track the active fetch thread
fetch_lock = threading.Lock()  # Lock to prevent race conditions

# Thread management for fetching
fetch_thread = None  # Track the active fetch thread
fetch_lock = threading.Lock()  # Lock to prevent race conditions

# Global logs storage (max 1000 entries to prevent memory issues)
app_logs = []
MAX_LOGS = 1000

def add_log(level, message, details=None):
    """
    Add a log entry to the application logs
    
    Parameters:
    - level: 'ERROR', 'WARNING', 'INFO', 'DEBUG'
    - message: Main log message
    - details: Optional additional details (dict or string)
    """
    global app_logs
    log_entry = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'level': level,
        'message': str(message),
        'details': str(details) if details else None
    }
    app_logs.append(log_entry)
    
    # Keep only the last MAX_LOGS entries
    if len(app_logs) > MAX_LOGS:
        app_logs = app_logs[-MAX_LOGS:]
    
    # Also print to console for debugging
    print(f"[{log_entry['timestamp']}] [{level}] {message}")
    if details:
        print(f"  Details: {details}")

# Create data folder if it doesn't exist
DATA_FOLDER = 'data'
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
    print(f"Created data folder: {DATA_FOLDER}")

# Market hours configuration
# NSE: 9:15 AM to 3:30 PM IST
# MCX: 9:00 AM to 11:30 PM IST (23:30)
MARKET_HOURS = {
    'NSE': {'open': (9, 15), 'close': (15, 30)},  # 9:15 AM to 3:30 PM IST
    'MCX': {'open': (9, 0), 'close': (23, 30)}     # 9:00 AM to 11:30 PM IST
}

def is_market_open(symbol=None, exchange=None):
    """
    Check if market is currently open based on symbol or exchange
    Returns True if market is open, False otherwise
    
    Market hours:
    - NSE: 9:15 AM to 3:30 PM IST
    - MCX: 9:00 AM to 11:30 PM IST (23:30)
    """
    try:
        # Determine exchange from symbol or use provided exchange
        if not exchange:
            if symbol:
                if symbol.startswith('MCX:'):
                    exchange = 'MCX'
                elif symbol.startswith('NSE:'):
                    exchange = 'NSE'
                else:
                    # Default to NSE if not specified
                    exchange = 'NSE'
            else:
                exchange = 'NSE'  # Default
        
        if exchange not in MARKET_HOURS:
            print(f"  [is_market_open] Exchange {exchange} not recognized, assuming market is open")
            return True  # If exchange not recognized, assume market is open
        
        # Get current IST time
        if PYTZ_AVAILABLE:
            ist = pytz.timezone('Asia/Kolkata')
            current_time = datetime.now(ist)
        else:
            # Fallback: assume IST is UTC+5:30 (not perfect but works)
            current_time = datetime.now()
        
        current_hour = current_time.hour
        current_minute = current_time.minute
        current_weekday = current_time.weekday()  # 0 = Monday, 6 = Sunday
        
        # Check if it's a weekend (Saturday = 5, Sunday = 6)
        if current_weekday >= 5:
            print(f"  [is_market_open] Weekend detected (weekday={current_weekday}), market is closed")
            return False
        
        # Get market hours
        market_open = MARKET_HOURS[exchange]['open']
        market_close = MARKET_HOURS[exchange]['close']
        
        open_hour, open_minute = market_open
        close_hour, close_minute = market_close
        
        # Convert to minutes for easier comparison
        current_minutes = current_hour * 60 + current_minute
        open_minutes = open_hour * 60 + open_minute
        close_minutes = close_hour * 60 + close_minute
        
        # Check if current time is within market hours
        is_open = open_minutes <= current_minutes <= close_minutes
        print(f"  [is_market_open] {exchange}: Current time={current_hour:02d}:{current_minute:02d} ({current_minutes} min), Market hours={open_hour:02d}:{open_minute:02d}-{close_hour:02d}:{close_minute:02d} ({open_minutes}-{close_minutes} min), Result={'OPEN' if is_open else 'CLOSED'}")
        return is_open
    except Exception as e:
        print(f"  [is_market_open] Exception: {e}")
        import traceback
        traceback.print_exc()
        # On error, assume market is open to avoid blocking
        return True

# Clean up CSV files on app start (OPTIONAL - can be bypassed)
# Set CLEANUP_ON_START = False to keep CSV files across sessions
CLEANUP_ON_START = False  # Set to True if you want fresh start on every app launch

def cleanup_csv_files():
    """Delete all CSV files from data folder for fresh start (optional)"""
    if not CLEANUP_ON_START:
        print("CSV cleanup on startup is disabled - keeping existing CSV files")
        return
    
    try:
        if os.path.exists(DATA_FOLDER):
            csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]
            deleted_count = 0
            for csv_file in csv_files:
                filepath = os.path.join(DATA_FOLDER, csv_file)
                try:
                    os.remove(filepath)
                    deleted_count += 1
                except Exception as e:
                    print(f"Warning: Could not delete {csv_file}: {e}")
            if deleted_count > 0:
                print(f"Cleaned up {deleted_count} CSV file(s) from data folder for fresh start")
    except Exception as e:
        print(f"Warning: Could not clean up CSV files: {e}")

# Clean up on app start (only if CLEANUP_ON_START is True)
cleanup_csv_files()

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
        if deleted_count > 0:
            add_log('INFO', f'Deleted {deleted_count} CSV file(s)', {'deleted_count': deleted_count, 'symbol': symbol if symbol else 'all'})
        return deleted_count
    except Exception as e:
        error_msg = f"Error deleting CSV files: {e}"
        print(error_msg)
        add_log('ERROR', error_msg, {'error': str(e), 'symbol': symbol if symbol else 'all'})
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

def load_symbol_settings():
    """
    Load symbol settings from SymbolSetting.csv
    Returns list of dicts with: prefix, symbol, expiry_date, strike_step, option_expiry_date, option_expiry_time
    """
    symbols = []
    try:
        with open('SymbolSetting.csv', 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                try:
                    prefix = row.get('Prefix', '').strip()
                    symbol = row.get('SYMBOL', '').strip()
                    expiry_str = row.get('EXPIERY', '').strip()
                    strike_step_str = row.get('StrikeStep', '').strip()
                    option_expiry_str = row.get('OptionExpiery', '').strip()
                    option_expiry_time_str = row.get('Time', '').strip()
                    
                    if not prefix or not symbol or not expiry_str:
                        continue
                    
                    # Parse future expiry date (format: DD-MM-YYYY)
                    try:
                        expiry_date = datetime.strptime(expiry_str, '%d-%m-%Y')
                    except ValueError:
                        # Try alternative format
                        try:
                            expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                        except ValueError:
                            print(f"Could not parse expiry date: {expiry_str}")
                            continue
                    
                    # Parse option expiry date (format: DD-MM-YYYY)
                    option_expiry_date = None
                    option_expiry_datetime = None
                    if option_expiry_str:
                        try:
                            option_expiry_date = datetime.strptime(option_expiry_str, '%d-%m-%Y')
                            # Combine with time if provided
                            if option_expiry_time_str:
                                try:
                                    # Parse time (format: HH:MM)
                                    time_parts = option_expiry_time_str.split(':')
                                    if len(time_parts) == 2:
                                        hour = int(time_parts[0])
                                        minute = int(time_parts[1])
                                        option_expiry_datetime = option_expiry_date.replace(hour=hour, minute=minute)
                                    else:
                                        option_expiry_datetime = option_expiry_date
                                except ValueError:
                                    option_expiry_datetime = option_expiry_date
                            else:
                                option_expiry_datetime = option_expiry_date
                        except ValueError:
                            try:
                                option_expiry_date = datetime.strptime(option_expiry_str, '%Y-%m-%d')
                                option_expiry_datetime = option_expiry_date
                            except ValueError:
                                print(f"Could not parse option expiry date: {option_expiry_str}")
                    
                    # Parse strike step
                    strike_step = None
                    if strike_step_str:
                        try:
                            strike_step = float(strike_step_str)
                        except ValueError:
                            pass
                    
                    symbols.append({
                        'prefix': prefix,
                        'symbol': symbol,
                        'expiry_date': expiry_date,
                        'strike_step': strike_step,
                        'expiry_str': expiry_str,
                        'option_expiry_date': option_expiry_date,
                        'option_expiry_datetime': option_expiry_datetime,
                        'option_expiry_str': option_expiry_str,
                        'option_expiry_time': option_expiry_time_str
                    })
                except Exception as e:
                    print(f"Error parsing symbol row: {row}, Error: {e}")
                    continue
        
        return symbols
    except FileNotFoundError:
        print("SymbolSetting.csv not found. Creating default file...")
        # Create default file
        default_symbols = [
            {'prefix': 'NSE', 'symbol': 'NIFTY', 'expiry_date': datetime(2025, 11, 25), 'strike_step': 50, 'expiry_str': '25-11-2025', 'option_expiry_date': None, 'option_expiry_datetime': None, 'option_expiry_str': '', 'option_expiry_time': ''}
        ]
        return default_symbols
    except Exception as e:
        print(f"Error loading symbol settings: {e}")
        return []

def generate_future_symbol_from_settings(prefix, symbol, expiry_date):
    """
    Generate future symbol from settings format
    Format: {PREFIX}:{SYMBOL}{YEARFROMDATE}{MONTHFROMDATE}{FUT}
    Example: NSE:NIFTY25NOVFUT
    For MCX: {PREFIX}:{SYMBOL}{YEARFROMDATE}{MONTHFROMDATE}{FUT} (NO month code letter)
    Example: MCX:CRUDEOIL25DECFUT
    
    Note: Symbols like SILVERM and GOLDM are kept as-is (they are different contracts, not month codes)
    """
    try:
        year_2digit = expiry_date.strftime('%y')  # e.g., "25"
        month_abbr = expiry_date.strftime('%b').upper()  # e.g., "NOV"
        
        # Check if it's MCX
        is_mcx = prefix.upper() == 'MCX'
        
        if is_mcx:
            # List of MCX commodities (base names without month codes)
            mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
            # MCX month codes: F, G, H, J, K, M, N, Q, U, V, X, Z
            mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
            
            # Known contract variants that should NOT have month codes removed
            # These are different contracts (e.g., SILVERM = SILVER Mini, GOLDM = GOLD Mini)
            contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
            
            symbol_clean = symbol.upper()
            
            # Check if this is a known contract variant - keep as-is
            if symbol_clean in [v.upper() for v in contract_variants]:
                # Keep the symbol as-is (e.g., SILVERM, GOLDM)
                symbol_clean = symbol
            elif len(symbol) > 0 and symbol[-1].upper() in mcx_month_codes:
                # Get the base symbol without the last character
                base_symbol = symbol[:-1].upper()
                
                # Check if base symbol is a known commodity (and not a variant)
                is_known_commodity = base_symbol in [c.upper() for c in mcx_commodities]
                is_variant = base_symbol in [v.upper() for v in contract_variants]
                
                # Only remove month code if it's a known commodity and NOT a variant
                if is_known_commodity and not is_variant:
                    symbol_clean = base_symbol
                else:
                    # Keep the symbol as-is (might be a variant or unknown contract)
                    symbol_clean = symbol
            else:
                # Symbol doesn't end with a month code, keep as-is
                symbol_clean = symbol
            
            # MCX format: MCX:COMMODITY + YY + MONTH + FUT (NO month code letter)
            # e.g., MCX:CRUDEOIL25DECFUT, MCX:SILVER26FEBFUT, MCX:SILVERM26FEBFUT
            future_symbol = f"{prefix}:{symbol_clean}{year_2digit}{month_abbr}FUT"
        else:
            # NSE format: NSE:SYMBOL + YY + MONTH + FUT
            # e.g., NSE:NIFTY25NOVFUT
            future_symbol = f"{prefix}:{symbol}{year_2digit}{month_abbr}FUT"
        
        return future_symbol
    except Exception as e:
        print(f"Error generating future symbol: {e}")
        import traceback
        traceback.print_exc()
        return None

def save_iv_to_csv(symbol, df_with_iv, timeframe=None, strike=None, expiry=None, option_type=None):
    """
    Save IV calculation results to CSV file in data folder
    File name: symbolname.csv (sanitized)
    
    Appends/merges new data with existing CSV file to preserve historical data.
    Includes: date, option_name, underlying_name, close, fclose, strike, expiry, iv, option_type, timeframe
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
        
        # Define priority columns in order (exactly as user wants)
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
        
        # Additional columns to include if available
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
        
        # Prepare new data with only required columns
        new_data = csv_data[columns_to_save].copy()
        
        # Check if CSV file already exists
        if os.path.exists(filename):
            try:
                # Read existing CSV
                existing_df = pd.read_csv(filename)
                
                # Ensure date column is datetime for comparison
                if 'date' in existing_df.columns:
                    existing_df['date'] = pd.to_datetime(existing_df['date'])
                if 'date' in new_data.columns:
                    new_data['date'] = pd.to_datetime(new_data['date'])
                
                # Merge: Remove duplicates based on date (keep latest)
                # Combine both dataframes
                combined_df = pd.concat([existing_df, new_data], ignore_index=True)
                
                # Remove duplicates based on date, keeping the last occurrence
                combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
                
                # Sort by date
                combined_df = combined_df.sort_values('date')
                
                # Convert date back to string format
                combined_df['date'] = combined_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Save merged data
                combined_df[columns_to_save].to_csv(filename, index=False, float_format='%.4f')
                
                new_rows = len(new_data)
                total_rows = len(combined_df)
                print(f"IV data appended/merged to: {filename}")
                print(f"  Added {new_rows} new rows, Total rows: {total_rows}, Columns: {', '.join(columns_to_save)}")
            except Exception as e:
                print(f"Warning: Could not merge with existing CSV ({e}), overwriting file...")
                # Fallback: overwrite if merge fails
                new_data['date'] = pd.to_datetime(new_data['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
                new_data[columns_to_save].to_csv(filename, index=False, float_format='%.4f')
                print(f"IV data saved to: {filename} (overwritten)")
                print(f"  Saved {len(new_data)} rows with columns: {', '.join(columns_to_save)}")
        else:
            # New file: save directly
            new_data['date'] = pd.to_datetime(new_data['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
            new_data[columns_to_save].to_csv(filename, index=False, float_format='%.4f')
            print(f"IV data saved to: {filename}")
            print(f"  Saved {len(new_data)} rows with columns: {', '.join(columns_to_save)}")
        
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
    
    # Extract year, month, and day
    # Pattern for weekly: YY + M + DD (e.g., 25N18 = 2025 Nov 18) - NIFTY25N1825500CE
    # Pattern for monthly: YY + MONTHNAME (e.g., 25NOV = 2025 Nov) - NIFTY25NOV25500CE
    
    # Try weekly format first: YY + single_letter_month + 2_digit_day
    weekly_match = re.search(r'(\d{2})([A-Z])(\d{2})(\d+)$', base)
    if weekly_match:
        # Weekly option: Extract day from symbol
        year_str = weekly_match.group(1)
        month_code = weekly_match.group(2)
        day_str = weekly_match.group(3)
        # strike_str would be in group(4), but we already extracted it above
        
        # Convert 2-digit year to 4-digit
        year = 2000 + int(year_str)
        day = int(day_str)
        
        # Single letter month codes (NSE style)
        single_letter_map = {
            'J': 1, 'F': 2, 'M': 3, 'A': 4, 'M': 5, 'J': 6,
            'J': 7, 'A': 8, 'S': 9, 'O': 10, 'N': 11, 'D': 12
        }
        month = single_letter_map.get(month_code, 12)
        
        # Get underlying symbol (everything before year+month+day+strike)
        # We need to remove: year_str + month_code + day_str + strike_str
        underlying = base[:-len(year_str + month_code + day_str + strike_str)]
        
        # Weekly options expire on the specified day
        # NSE (NIFTY) options expire at 3:15 PM IST
        # MCX options expire at 11:20 PM IST
        # Determine expiry time based on exchange (NSE vs MCX)
        if is_mcx:
            expiry_date = datetime(year, month, day, 23, 20, 0)  # 11:20 PM IST for MCX
        else:
            expiry_date = datetime(year, month, day, 15, 15, 0)  # 3:15 PM IST for NSE
    else:
        # Try monthly format: YY + MONTHNAME (3 letters)
        year_match = re.search(r'(\d{2})([A-Z]{1,3})$', base)
        if year_match:
            year_str = year_match.group(1)
            month_code = year_match.group(2)
            
            # Convert 2-digit year to 4-digit
            year = 2000 + int(year_str)
            
            # Month codes
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
            
            # Get underlying symbol (everything before year+month)
            underlying = base[:-len(year_str + month_code)]
            
            # For monthly options, use last Thursday of the month (standard for NSE)
            # NIFTY monthly options expire on the last Thursday of the month at 3:30 PM IST
            from calendar import monthrange, weekday
            last_day = monthrange(year, month)[1]
            # Find the last Thursday of the month
            last_thursday = None
            for day in range(last_day, 0, -1):
                if weekday(year, month, day) == 3:  # Thursday = 3
                    last_thursday = day
                    break
            
            if last_thursday:
                # Use last Thursday
                # NSE (NIFTY) monthly options expire at 3:15 PM IST
                # MCX monthly options expire at 11:20 PM IST
                if is_mcx:
                    expiry_date = datetime(year, month, last_thursday, 23, 20, 0)  # 11:20 PM IST for MCX
                else:
                    expiry_date = datetime(year, month, last_thursday, 15, 15, 0)  # 3:15 PM IST for NSE
            else:
                # Fallback: use last day if no Thursday found (shouldn't happen)
                if is_mcx:
                    expiry_date = datetime(year, month, last_day, 23, 20, 0)  # 11:20 PM IST for MCX
                else:
                    expiry_date = datetime(year, month, last_day, 15, 15, 0)  # 3:15 PM IST for NSE
        else:
            return None
        
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
        error_msg = f"Error getting option price for {option_symbol}"
        print(f"{error_msg}: {e}")
        add_log('ERROR', error_msg, {'symbol': option_symbol, 'error': str(e)})
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
        error_msg = f"Error getting underlying price for {underlying_symbol}"
        print(f"{error_msg}: {e}")
        add_log('ERROR', error_msg, {'symbol': underlying_symbol, 'error': str(e)})
        return None

def get_future_symbol(underlying, expiry_date):
    """
    Construct future symbol based on underlying and expiry date
    For NIFTY -> NSE:NIFTY25NOVFUT
    For BANKNIFTY -> NSE:BANKNIFTY25NOVFUT
    For MCX contracts (e.g., CRUDEOIL or CRUDEOILM) -> MCX:CRUDEOIL25NOVFUT (NO month code letter)
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
        # MCX underlying format: COMMODITY (may have month code suffix like CRUDEOILM, GOLDM, SILVERM)
        # MCX future format: MCX:COMMODITY + YY + MONTH + FUT (NO month code letter)
        # e.g., MCX:CRUDEOIL25NOVFUT
        # IMPORTANT: Contract variants (SILVERM, GOLDM) are different contracts, not month codes
        contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
        mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
        underlying_upper = underlying.upper()
        
        # Check for contract variants FIRST (these are separate contracts, not month codes)
        is_contract_variant = False
        matched_variant = None
        for variant in contract_variants:
            if underlying_upper == variant or underlying_upper.startswith(variant):
                is_contract_variant = True
                matched_variant = variant
                break
        
        if is_contract_variant:
            # For contract variants, preserve the full name (e.g., SILVERM -> MCX:SILVERM26JANFUT)
            # Use the matched variant name (e.g., SILVERM, GOLDM)
            underlying_clean = matched_variant
            future_symbol = f"MCX:{underlying_clean}{year_2digit}{month_code}FUT"
        else:
            # Check if underlying starts with any MCX commodity name
            is_mcx = False
            matched_commodity = None
            for commodity in mcx_commodities:
                if underlying_upper.startswith(commodity):
                    is_mcx = True
                    matched_commodity = commodity
                    break
            
            if is_mcx:
                # MCX future: MCX:COMMODITY + YY + MONTH + FUT (NO month code letter)
                # e.g., MCX:CRUDEOIL25NOVFUT
                # Check if underlying already has a month code (single letter at end) and remove it
                # MCX month codes: F, G, H, J, K, M, N, Q, U, V, X, Z
                mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                
                # Remove month code from underlying if it exists
                underlying_clean = underlying
                if matched_commodity and len(underlying) > len(matched_commodity) and underlying[-1].upper() in mcx_month_codes:
                    # Remove the month code, keep only the commodity name
                    underlying_clean = matched_commodity
                elif matched_commodity:
                    # Use the matched commodity name directly
                    underlying_clean = matched_commodity
                
                future_symbol = f"MCX:{underlying_clean}{year_2digit}{month_code}FUT"
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
        error_msg = f"Error getting future LTP for {future_symbol}"
        print(f"{error_msg}: {e}")
        add_log('WARNING', error_msg, {'symbol': future_symbol, 'error': str(e)})
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
        # Ensure underlying is a string and strip any whitespace
        if underlying is None:
            return None
        underlying = str(underlying).strip()
        
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
                # MCX monthly format: MCX:CRUDEOIL25DEC5300CE
                # Format: {Ex}:{Ex_Commodity}{YY}{MMM}{Strike}{Opt_Type}
                # Examples: MCX:CRUDEOIL20OCT4000CE, MCX:GOLD20DEC40000PE
                # 
                # IMPORTANT: For contract variants (SILVERM, GOLDM), check if options exist
                # Some variants may use the base commodity name in option symbols
                underlying_clean = underlying
                
                # List of contract variants (these are separate contracts, NOT month codes)
                contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
                
                # Check if underlying is a contract variant
                is_variant = False
                variant_name = None
                for variant in contract_variants:
                    if underlying_clean.upper() == variant or underlying_clean.upper().startswith(variant):
                        variant_name = variant
                        is_variant = True
                        break
                
                if is_variant:
                    # For contract variants (SILVERM, GOLDM), options use the SAME variant name as the future
                    # If user selected SILVERM future, generate SILVERM options (not SILVER)
                    # If user selected GOLDM future, generate GOLDM options (not GOLD)
                    # This ensures the option symbol matches the selected future symbol setting
                    underlying_clean = variant_name
                else:
                    # Not a contract variant, check if it has a month code suffix
                    # List of MCX commodities (base names without month codes)
                    mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
                    mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                    
                    # Remove month code from underlying if it exists
                    for commodity in mcx_commodities:
                        if underlying_clean.upper().startswith(commodity):
                            # Check if it's longer than commodity AND ends with a month code
                            if len(underlying_clean) > len(commodity):
                                last_char = underlying_clean[-1].upper()
                                # Only remove if it's a month code (not a contract variant)
                                if last_char in mcx_month_codes:
                                    # Double-check it's not a variant
                                    is_variant_check = False
                                    for variant in contract_variants:
                                        if underlying_clean.upper().startswith(variant):
                                            is_variant_check = True
                                            # Keep variant name for options (match the selected future symbol)
                                            underlying_clean = variant
                                            break
                                    if not is_variant_check:
                                        # It's a month code, remove it
                                        underlying_clean = commodity
                            else:
                                # Already clean, use as is
                                underlying_clean = commodity
                            break
                
                # Extract month abbreviation (3 letters: JAN, FEB, MAR, etc.)
                month_abbr = expiry_date.strftime('%b').upper()  # e.g., "DEC"
                
                # Format: MCX:COMMODITY + year + month_abbr + strike + option_suffix
                # Format: {Ex}:{Ex_Commodity}{YY}{MMM}{Strike}{Opt_Type}
                # e.g., MCX:CRUDEOIL25DEC5300CE, MCX:SILVERM26JAN230000CE (for SILVERM future)
                # Ensure underlying_clean doesn't contain year digits (clean it)
                underlying_clean = str(underlying_clean).strip()
                # Remove any trailing digits that might have been incorrectly included
                import re
                underlying_clean = re.sub(r'\d+$', '', underlying_clean)
                option_symbol = f"{exchange_prefix}{underlying_clean}{year_2digit}{month_abbr}{strike}{option_suffix}"
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

def calculate_iv_pyvollib(option_price, underlying_price, strike, time_to_expiry, risk_free_rate=0.06, option_type='c'):
    """
    Calculate Implied Volatility using py_vollib Black model (for options on futures)
    
    The Black model is appropriate for options on futures/forwards, which is the case
    for Indian options that are settled on futures prices.
    
    Parameters:
    - option_price: Current option price
    - underlying_price: Current futures/forward price (F) - NOT spot price
    - strike: Strike price (K)
    - time_to_expiry: Time to expiration in years (t)
    - risk_free_rate: Risk-free interest rate (r), default 0.06 (6% = typical Indian risk-free rate)
    - option_type: 'c' for call, 'p' for put
    
    Returns: Implied volatility as decimal (e.g., 0.20 for 20%)
    
    Note: Black model parameter order: (price, F, K, r, t, flag)
    This is different from Black-Scholes: (price, S, K, t, r, flag)
    """
    if not PY_VOLLIB_AVAILABLE:
        return None
    
    # Validate inputs
    if option_price is None or underlying_price is None or strike is None:
        return None
    
    if option_price <= 0 or underlying_price <= 0 or strike <= 0:
        return None
    
    if time_to_expiry <= 0:
        return None
    
    # Additional validation to prevent invalid calculations
    # Check for reasonable values
    if time_to_expiry > 2.0:  # More than 2 years seems wrong
        return None
    
    if time_to_expiry < 0.0001:  # Less than ~1 hour seems wrong
        return None
    
    # Check if option price is reasonable (should not be more than underlying price for calls)
    if option_type == 'c' and option_price > underlying_price * 1.5:
        # Option price too high relative to underlying - likely data error
        return None
    
    # Check if option price is too low (less than 0.1% of strike) - likely data error
    if option_price < strike * 0.001:
        return None
    
    try:
        # Calculate intrinsic value to validate
        if option_type == 'c':
            intrinsic_value = max(0, underlying_price - strike)
        else:  # put
            intrinsic_value = max(0, strike - underlying_price)
        
        # If option price is significantly below intrinsic value, it's invalid
        if option_price < intrinsic_value * 0.5:
            return None
        
        # py_vollib.black.implied_volatility expects:
        # implied_volatility(price, F, K, r, t, flag)
        # Where: price = option price, F = futures price, K = strike, r = risk-free rate, t = time to expiry, flag = 'c' or 'p'
        # Note: This is the Black model (for options on futures), not Black-Scholes
        iv = implied_volatility(
            float(option_price),
            float(underlying_price),  # F = futures/forward price
            float(strike),            # K = strike price
            float(risk_free_rate),    # r = risk-free rate
            float(time_to_expiry),    # t = time to expiry in years
            option_type               # flag = 'c' or 'p'
        )
        
        # Validate IV result - filter out unreasonable values
        if iv is None:
            return None
        
        # IV should be between 0.01% (0.0001) and 200% (2.0) - anything outside is likely wrong
        if iv < 0.0001 or iv > 2.0:
            return None
        
        # Additional check: if IV is unusually high (>100%), it's likely a calculation error
        # This can happen with bad data (wrong prices, wrong time_to_expiry, etc.)
        if iv > 1.0:  # More than 100% IV is very unusual
            return None
        
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
        # Ensure symbol is a string and strip any whitespace
        symbol = str(symbol).strip() if symbol else None
        if not symbol:
            print(f"❌ ERROR: Invalid symbol provided to safe_fetch_ohlc: {symbol}")
            return None
        
        # Debug: Print the symbol being passed to fetchOHLC
        print(f"DEBUG safe_fetch_ohlc: Symbol being passed: '{symbol}' (type: {type(symbol)}, length: {len(symbol)})")
        
        if FyresIntegration.fyers is None:
            print(f"❌ ERROR: Fyers not initialized. Cannot fetch data for {symbol}")
            return None
        
        # Call the original fetchOHLC function
        df = fetchOHLC(symbol, timeframe)
        return df
        
    except KeyError as e:
        # Handle case where API response doesn't have 'candles' key
        error_msg = f"API response error for {symbol}: Missing 'candles' key in response"
        print(f"❌ {error_msg}\nFull error: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        add_log('ERROR', error_msg, {'symbol': symbol, 'error': str(e), 'error_type': 'KeyError'})
        return None
    except Exception as e:
        error_msg = f"Error fetching OHLC data for {symbol}"
        print(f"❌ {error_msg}: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        add_log('ERROR', error_msg, {'symbol': symbol, 'error': str(e), 'error_type': type(e).__name__})
        return None

def calculate_iv(df, window=20, timeframe='1D', symbol=None, risk_free_rate=0.06, 
                manual_strike=None, manual_expiry=None, manual_option_type=None, manual_future_symbol=None):
    """
    Calculate Implied Volatility using py_vollib Black model (for options) or Historical Volatility (for underlying)
    
    For Options:
    - Uses py_vollib Black model with option prices (appropriate for options on futures)
    - Requires: option_price, futures_price, strike, time_to_expiry
    
    Parameters:
    - manual_strike: Optional manual strike price (overrides parsed value)
    - manual_expiry: Optional manual expiry datetime string (overrides parsed value) - used for option symbol and time_to_expiry calculation
    - manual_option_type: Optional manual option type 'c' or 'p' (overrides parsed value)
    - manual_future_symbol: Optional future symbol (from SymbolSetting.csv). If provided, uses this instead of reconstructing from option expiry
    
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
                    try:
                        # Try date-only format: "YYYY-MM-DD"
                        expiry_date = datetime.strptime(manual_expiry, '%Y-%m-%d')
                    except:
                        print(f"Could not parse expiry date: {manual_expiry}")
        
        # Extract underlying symbol and determine exchange
        underlying = None
        is_mcx_underlying = False
        if parsed_info:
            underlying = parsed_info['underlying']
            is_mcx_underlying = parsed_info.get('is_mcx', False)
        elif ':' in symbol:
            # Extract from format like "NSE:RELIANCE-EQ" or "MCX:CRUDEOILM..."
            exchange_part = symbol.split(':')[0]
            is_mcx_underlying = (exchange_part.upper() == 'MCX')
            underlying = symbol.split(':')[-1].split('-')[0]
        else:
            # Use symbol as-is, but try to remove option suffixes
            underlying = symbol.replace('CE', '').replace('PE', '').rstrip('0123456789')
        
        # If expiry_date was parsed but doesn't have correct time (midnight or not set), adjust it
        if expiry_date and (expiry_date.hour == 0 and expiry_date.minute == 0):
            # Time was not specified, set based on exchange
            if is_mcx_underlying:
                expiry_date = expiry_date.replace(hour=23, minute=20, second=0)  # 11:20 PM IST for MCX
            else:
                expiry_date = expiry_date.replace(hour=15, minute=15, second=0)  # 3:15 PM IST for NSE
        
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
        print(f"Calculating IV for {symbol}...")
        
        underlying_symbol = option_info['underlying']
        expiry_date = option_info['expiry_date']  # Option expiry (used for option symbol and time_to_expiry calculation)
        
        # Normalize expiry_date to timezone-naive to avoid timezone mismatch errors
        if expiry_date.tzinfo is not None:
            expiry_date = expiry_date.replace(tzinfo=None)
        
        # Get future symbol - use manual_future_symbol if provided (from SymbolSetting.csv)
        # Otherwise, reconstruct from underlying and option expiry (may be wrong if option expiry != future expiry)
        if manual_future_symbol:
            future_symbol = manual_future_symbol
        else:
            # Fallback: reconstruct future symbol from underlying and option expiry
            # NOTE: This may be incorrect if option expiry != future expiry
            future_symbol = get_future_symbol(underlying_symbol, expiry_date)
        
        if future_symbol:
            # Fetch historical data for future symbol
            print(f"  Fetching future data for: {future_symbol}")
            df_future = safe_fetch_ohlc(future_symbol, timeframe)
            
            if df_future is None or len(df_future) == 0:
                error_msg = f"Could not fetch historical data for future symbol {future_symbol}"
                print(f"  ❌ {error_msg}")
                add_log('ERROR', error_msg, {
                    'future_symbol': future_symbol,
                    'timeframe': timeframe,
                    'underlying_symbol': underlying_symbol
                })
                print(f"  Falling back to historical volatility")
            else:
                print(f"  ✓ Fetched {len(df_future)} candles for {future_symbol}")
                
                # Prepare option dataframe - keep only date and close
                df_option = df[['date', 'close']].copy()
                df_option['date'] = pd.to_datetime(df_option['date'])
                # Remove timezone info for consistent merging (keep exact timestamp)
                if df_option['date'].dt.tz is not None:
                    df_option['date'] = df_option['date'].dt.tz_localize(None)
                
                # Prepare future dataframe - keep only date and close, rename close to fclose
                df_future_prep = df_future[['date', 'close']].copy()
                df_future_prep.rename(columns={'close': 'fclose'}, inplace=True)
                df_future_prep['date'] = pd.to_datetime(df_future_prep['date'])
                # Remove timezone info for consistent merging (keep exact timestamp)
                if df_future_prep['date'].dt.tz is not None:
                    df_future_prep['date'] = df_future_prep['date'].dt.tz_localize(None)
                
                # Round timestamps to nearest minute to handle slight time differences
                # This ensures proper matching for minute-level data
                df_option['date_rounded'] = df_option['date'].dt.round('1min')
                df_future_prep['date_rounded'] = df_future_prep['date'].dt.round('1min')
                
                # Debug: Print date ranges
                if len(df_option) > 0 and len(df_future_prep) > 0:
                    print(f"  Option date range: {df_option['date'].min()} to {df_option['date'].max()}")
                    print(f"  Future date range: {df_future_prep['date'].min()} to {df_future_prep['date'].max()}")
                
                # Merge option and future data by rounded timestamp (exact minute match)
                # This prevents cartesian products while ensuring proper matching
                print(f"  Merging option data ({len(df_option)} rows) with future data ({len(df_future_prep)} rows)...")
                df_merged = pd.merge(df_option[['date', 'close', 'date_rounded']], 
                                    df_future_prep[['fclose', 'date_rounded']], 
                                    on='date_rounded', 
                                    how='inner')
                
                # Drop the rounded date column and keep original date from option
                df_merged = df_merged.drop(columns=['date_rounded'])
                
                if len(df_merged) == 0:
                    error_msg = f"No matching dates between option and future data. Option dates: {len(df_option)}, Future dates: {len(df_future_prep)}"
                    print(f"  ❌ {error_msg}")
                    add_log('WARNING', error_msg, {
                        'option_symbol': symbol,
                        'future_symbol': future_symbol,
                        'option_rows': len(df_option),
                        'future_rows': len(df_future_prep)
                    })
                    print(f"  Falling back to historical volatility")
                else:
                    print(f"  ✓ Merged data: {len(df_merged)} matching rows")
                    # Check for duplicate dates (shouldn't happen with proper merge)
                    if len(df_merged) != len(df_merged['date'].unique()):
                        print(f"  ⚠ Warning: Found duplicate dates in merged data. Deduplicating...")
                        df_merged = df_merged.drop_duplicates(subset=['date'], keep='first')
                        print(f"  ✓ After deduplication: {len(df_merged)} rows")
                    
                    # Safety check: if merged data is too large, something went wrong
                    if len(df_merged) > len(df_option) * 2:
                        error_msg = f"Merged data has {len(df_merged)} rows, which is suspiciously large (option had {len(df_option)} rows). This suggests a merge issue."
                        print(f"  ❌ {error_msg}")
                        add_log('ERROR', error_msg, {
                            'symbol': symbol,
                            'future_symbol': future_symbol,
                            'option_rows': len(df_option),
                            'future_rows': len(df_future_prep),
                            'merged_rows': len(df_merged)
                        })
                        print(f"  Falling back to historical volatility")
                    else:
                        # Calculate IV for each row using historical future prices
                        print(f"  Starting IV calculation loop for {len(df_merged)} rows...")
                        iv_values = []
                        rows_processed = 0
                        
                        for idx, row in df_merged.iterrows():
                            rows_processed += 1
                            if rows_processed % 1000 == 0:
                                print(f"  Processing IV calculation: {rows_processed}/{len(df_merged)} rows...")
                            
                            option_price = row['close']
                            future_price = row['fclose']
                            
                            # Validate prices are reasonable
                            if pd.isna(option_price) or pd.isna(future_price):
                                iv_values.append(np.nan)
                                continue
                            
                            if option_price <= 0 or future_price <= 0:
                                iv_values.append(np.nan)
                                continue
                            
                            # Additional validation: check if prices are reasonable
                            # Option price should not be more than 50% of strike (for calls) or underlying (for puts)
                            # This filters out obvious data errors
                            strike_price = option_info['strike']
                            if option_info['option_type'] == 'c':
                                if option_price > strike_price * 0.5 or option_price > future_price * 0.5:
                                    iv_values.append(np.nan)
                                    continue
                            else:  # put
                                if option_price > strike_price * 0.5:
                                    iv_values.append(np.nan)
                                    continue
                            
                            # Future price should be reasonable relative to strike (within 50% to 200%)
                            if future_price < strike_price * 0.5 or future_price > strike_price * 2.0:
                                iv_values.append(np.nan)
                                continue
                            
                            if option_price > 0 and future_price > 0:
                                # Calculate time to expiry for this timestamp
                                row_date = row['date'] if isinstance(row['date'], datetime) else pd.to_datetime(row['date'])
                                
                                # Fix timezone mismatch: make row_date timezone-naive if needed
                                if row_date.tzinfo is not None:
                                    # If row_date is timezone-aware, convert to naive
                                    row_date = row_date.replace(tzinfo=None)
                                
                                # expiry_date is already normalized to naive above, so we can use it directly
                                # Calculate time to expiry in years using option expiry
                                # Indian brokers typically use calendar days (365) for time to expiry calculation
                                # However, some use trading days (252). We'll use calendar days as it's more standard.
                                time_diff = expiry_date - row_date
                                total_seconds = time_diff.total_seconds()
                                
                                # Convert to years using calendar days (365 days per year)
                                # This is the standard approach used by most Indian brokers
                                # Note: Using 365.25 accounts for leap years, but 365 is more common in options pricing
                                time_to_expiry = total_seconds / (365.0 * 24 * 3600)
                                
                                # Ensure time to expiry is reasonable (not negative, not too large)
                                if time_to_expiry <= 0:
                                    iv_values.append(np.nan)
                                    continue
                                if time_to_expiry > 2.0:  # More than 2 years seems wrong
                                    print(f"  Warning: Time to expiry seems too large: {time_to_expiry:.4f} years")
                                    iv_values.append(np.nan)
                                    continue
                                
                                # Alternative: Use trading days (more accurate for options)
                                # trading_days_per_year = 252
                                # calendar_days = time_diff.days
                                # trading_days = calendar_days * (trading_days_per_year / 365.25)
                                # time_to_expiry = trading_days / trading_days_per_year
                                
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
                        
                        print(f"  ✓ Completed IV calculation loop. Processed {rows_processed} rows.")
                        
                        # Add IV column
                        df_merged['iv'] = iv_values
                    
                    # Filter out outliers (IV values that are too different from neighbors)
                    # This prevents glitches from bad data
                    iv_series = pd.Series(iv_values)
                    
                    # Calculate rolling median to detect outliers
                    window_size = min(5, max(3, len(iv_series) // 10 + 1))  # Use 5 or 10% of data, whichever is smaller, but at least 3
                    if window_size >= 3 and len(iv_series) > window_size:
                        rolling_median = iv_series.rolling(window=window_size, center=True, min_periods=1).median()
                        rolling_std = iv_series.rolling(window=window_size, center=True, min_periods=1).std()
                        
                        # Replace outliers (values more than 3 standard deviations from rolling median)
                        # Only if the value is significantly different (more than 20% difference)
                        for i in range(len(iv_series)):
                            if pd.notna(iv_series.iloc[i]) and pd.notna(rolling_median.iloc[i]):
                                median_val = rolling_median.iloc[i]
                                std_val = rolling_std.iloc[i] if pd.notna(rolling_std.iloc[i]) else median_val * 0.1
                                
                                # Check if value is an outlier (more than 3 std devs OR more than 20% different)
                                if abs(iv_series.iloc[i] - median_val) > max(3 * std_val, median_val * 0.2):
                                    # Replace with median of neighbors
                                    neighbor_indices = [j for j in range(max(0, i-2), min(len(iv_series), i+3)) if j != i and pd.notna(iv_series.iloc[j])]
                                    if neighbor_indices:
                                        neighbor_median = iv_series.iloc[neighbor_indices].median()
                                        iv_series.iloc[i] = neighbor_median
                    
                    df_merged['iv'] = iv_series.values
                    
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
                    
                    # Get valid IVs for logging
                    valid_ivs = [iv for iv in iv_values if iv is not None and not (isinstance(iv, float) and (np.isnan(iv) or np.isinf(iv)))]
                    if len(valid_ivs) > 0:
                        print(f"  ✓ Calculated IV: {len(valid_ivs)} values (range: {min(valid_ivs):.2f}% - {max(valid_ivs):.2f}%)")
                    else:
                        print(f"  ⚠ Warning: No valid IV values calculated. All {len(iv_values)} values are NaN or invalid.")
                        add_log('WARNING', f'No valid IV values calculated for {symbol}', {
                            'symbol': symbol,
                            'future_symbol': future_symbol,
                            'total_rows': len(iv_values),
                            'valid_ivs': 0
                        })
                    
                    # Even if all IVs are NaN, return the dataframe so we can still display the data
                    # The frontend can handle NaN/zero values
                    return df_merged
        else:
            error_msg = f"Could not construct future symbol for underlying {underlying_symbol}"
            print(f"  ❌ {error_msg}")
            add_log('ERROR', error_msg, {
                'underlying_symbol': underlying_symbol,
                'expiry_date': str(expiry_date),
                'symbol': symbol
            })
            print(f"  Falling back to historical volatility")
        
        print(f"  Failed to calculate IV with py_vollib Black model, falling back to historical volatility")
    
    # Fallback to Historical Volatility calculation
    print(f"  Using Historical Volatility calculation (fallback)")
    
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

def fetch_data_loop_automatic(future_symbol, expiry_date, expiry_type, option_type, timeframe, strike_distance, risk_free_rate=0.07):
    """
    Continuously fetch data in automatic mode:
    1. Get future LTP
    2. Calculate ATM strike
    3. Generate option symbol
    4. Fetch option data and calculate IV
    5. Repeat every 1 second
    
    Only fetches data during market hours (NSE: 9:15-15:30, MCX: 9:00-23:30)
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
    
    # Determine exchange for market hours check
    exchange = 'MCX' if is_mcx else 'NSE'
    
    underlying = None
    if is_mcx:
        # MCX contracts: Remove year (2 digits), month (3 letters), and FUT suffix
        # e.g., CRUDEOIL25DECFUT -> CRUDEOIL
        # Pattern: COMMODITY + YY + MONTH + FUT (NO month code letter)
        if underlying_part.endswith('FUT'):
            # Remove FUT suffix first
            base = underlying_part[:-3]
            # Remove year (2 digits) + month (3 letters) from end
            # Pattern: YY + MONTH (e.g., 25DEC)
            import re
            # Remove pattern: 2 digits + 3 letters from the end
            underlying = re.sub(r'\d{2}[A-Z]{3}$', '', base)
            # If regex didn't match, try simple approach
            if underlying == base and len(base) >= 5:
                # Check if last 5 characters match YY + MONTH pattern (2 digits + 3 letters)
                if base[-5:-3].isdigit() and base[-3:].isalpha():
                    underlying = base[:-5]
                else:
                    # Last resort: assume format is just COMMODITY, keep as is
                    underlying = base
            # IMPORTANT: Check for contract variants FIRST (SILVERM, GOLDM are different contracts, not month codes)
            # Contract variants should be preserved as-is
            contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
            is_contract_variant = False
            for variant in contract_variants:
                if underlying.upper() == variant or underlying.upper().startswith(variant):
                    is_contract_variant = True
                    # Preserve the contract variant name
                    underlying = variant
                    break
            
            # Only check for month codes if it's NOT a contract variant
            if not is_contract_variant:
                mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                if underlying and len(underlying) > 0 and underlying[-1].upper() in mcx_month_codes:
                    # Check if it's a valid commodity name
                    mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
                    for commodity in mcx_commodities:
                        if underlying.upper().startswith(commodity) and len(underlying) > len(commodity):
                            underlying = commodity
                            break
        else:
            # No FUT suffix, might be just the commodity
            underlying = underlying_part
            
            # IMPORTANT: Check for contract variants FIRST (SILVERM, GOLDM are different contracts, not month codes)
            contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
            is_contract_variant = False
            for variant in contract_variants:
                if underlying.upper() == variant or underlying.upper().startswith(variant):
                    is_contract_variant = True
                    # Preserve the contract variant name
                    underlying = variant
                    break
            
            # Only check for month codes if it's NOT a contract variant
            if not is_contract_variant:
                mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                if underlying and len(underlying) > 0 and underlying[-1].upper() in mcx_month_codes:
                    mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
                    for commodity in mcx_commodities:
                        if underlying.upper().startswith(commodity) and len(underlying) > len(commodity):
                            underlying = commodity
                            break
    elif 'NIFTY' in underlying_part:
        if 'BANK' in underlying_part:
            underlying = 'BANKNIFTY'
        else:
            underlying = 'NIFTY'
    
    if not underlying:
        error_msg = f"Could not extract underlying from {future_symbol}"
        print(f"ERROR: {error_msg}")
        add_log('ERROR', error_msg, {'future_symbol': future_symbol, 'underlying_part': underlying_part if 'underlying_part' in locals() else 'unknown'})
        fetching_status["active"] = False
        return
    
    # Determine exchange for market hours check
    exchange = 'MCX' if is_mcx else 'NSE'
    
    iteration = 0
    print(f"Starting automatic fetch loop for future_symbol={future_symbol}, underlying={underlying}", flush=True)
    print(f"Initial fetch status check: active={fetching_status.get('active')}, mode={fetching_status.get('mode')}, future_symbol={fetching_status.get('future_symbol')}", flush=True)
    
    # Store the initial future_symbol to detect if it changed (user restarted with different symbol)
    initial_future_symbol = future_symbol
    
    print(f"Entering while loop. Initial conditions: active={fetching_status.get('active')}, mode={fetching_status.get('mode')}, future_symbol={fetching_status.get('future_symbol')}", flush=True)
    import sys
    sys.stdout.flush()
    
    loop_count = 0
    thread_id = threading.current_thread().ident
    print(f"[Thread {thread_id}] Starting while loop", flush=True)
    
    while fetching_status["active"] and fetching_status.get("mode") == "automatic":
        loop_count += 1
        print(f"[Thread {thread_id}] Loop iteration #{loop_count}", flush=True)
        
        # Check if future_symbol changed (user restarted with different symbol)
        current_future_symbol = fetching_status.get("future_symbol")
        if current_future_symbol and current_future_symbol != initial_future_symbol:
            print(f"[Thread {thread_id}] Future symbol changed from {initial_future_symbol} to {current_future_symbol}. Stopping old thread.", flush=True)
            break
        
        # Debug: Print loop status
        print(f"[Thread {thread_id}] Loop check: active={fetching_status.get('active')}, mode={fetching_status.get('mode')}, future_symbol={fetching_status.get('future_symbol')}", flush=True)
        print(f"[Thread {thread_id}] Entering try block...", flush=True)
        import sys
        sys.stdout.flush()  # Force flush
        
        try:
            # Check if market is open before fetching data
            print(f"  Calling is_market_open(symbol={future_symbol}, exchange={exchange})...", flush=True)
            market_open = is_market_open(symbol=future_symbol, exchange=exchange)
            print(f"Market check for {exchange}: {'OPEN' if market_open else 'CLOSED'}", flush=True)
            if not market_open:
                print(f"Market is closed for {exchange}. Waiting 60 seconds before checking again...")
                time.sleep(60)  # Wait 60 seconds before checking again
                continue
            
            iteration += 1
            print(f"\n=== Automatic Mode Iteration {iteration} ===", flush=True)
            print(f"Fetch status: active={fetching_status.get('active')}, mode={fetching_status.get('mode')}", flush=True)
            
            # Check if fyers is available
            if FyresIntegration.fyers is None:
                print("ERROR: Fyers not initialized. Waiting...", flush=True)
                add_log('WARNING', 'Fyers not initialized in fetch loop', {'iteration': iteration})
                time.sleep(5)
                continue
            
            print(f"Fyers is initialized, proceeding with data fetch...", flush=True)
            
            # Get future LTP
            print(f"Fetching LTP for {future_symbol}...", flush=True)
            future_ltp = get_future_ltp(future_symbol)
            if future_ltp is None:
                print(f"Could not fetch LTP for {future_symbol}. Retrying in 5 seconds...", flush=True)
                time.sleep(5)
                continue
            
            print(f"Future LTP: {future_ltp}", flush=True)
            
            # Calculate ATM strike
            # Use the strike_distance passed to the function (from user input or defaults)
            # strike_distance is already set from the function parameter, no need to recalculate
            atm_strike = calculate_atm_strike(future_ltp, strike_distance)
            if atm_strike is None:
                print(f"Could not calculate ATM strike. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            print(f"ATM Strike: {atm_strike}")
            
            # Generate option symbol using OPTION expiry date (from web input, not future expiry)
            # future_symbol already has the correct future expiry from SymbolSetting.csv
            # expiry_date parameter is the OPTION expiry date from web input
            symbol = generate_option_symbol(underlying, expiry_date, atm_strike, option_type, expiry_type, is_mcx=is_mcx)
            if not symbol:
                print(f"Could not generate option symbol. Retrying in 5 seconds...")
                time.sleep(5)
                continue
            
            print(f"Generated Option Symbol: {symbol}")
            
            # Update fetching status with current symbol (this is what the frontend polls)
            # Only update if we're still fetching the same future_symbol and mode (avoid race conditions)
            current_future_symbol = fetching_status.get("future_symbol")
            current_mode = fetching_status.get("mode")
            if current_future_symbol == future_symbol and current_mode == "automatic":
                fetching_status["symbol"] = symbol
                fetching_status["strike"] = atm_strike
                print(f"Updated fetching_status.symbol to: {symbol}")
            else:
                print(f"Future symbol or mode changed, stopping thread. Current future_symbol: {current_future_symbol}, Expected: {future_symbol}, Mode: {current_mode}")
                break
            
            # Fetch historical data for the option symbol
            print(f"Fetching option data for: {symbol}")
            df = safe_fetch_ohlc(symbol, timeframe)
            
            if df is None or len(df) == 0:
                error_msg = f"Failed to fetch data for {symbol}"
                print(f"❌ {error_msg}. Retrying in 5 seconds...")
                add_log('WARNING', error_msg, {'symbol': symbol, 'iteration': iteration, 'action': 'retrying'})
                
                # Even if fetch failed, try to load existing CSV data for this symbol
                # This ensures chart can display historical data even if current fetch fails
                try:
                    safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
                    safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
                    filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
                    if os.path.exists(filename):
                        print(f"  Attempting to load existing CSV data for {symbol}...")
                        df_csv = pd.read_csv(filename)
                        if 'date' in df_csv.columns and 'iv' in df_csv.columns:
                            df_csv['date'] = pd.to_datetime(df_csv['date'])
                            if df_csv['date'].dt.tz is None:
                                df_csv['date'] = df_csv['date'].dt.tz_localize('Asia/Kolkata')
                            else:
                                df_csv['date'] = df_csv['date'].dt.tz_convert('Asia/Kolkata')
                            df_csv = df_csv.sort_values('date')
                            df_chart_csv = df_csv  # Show all rows, no limit
                            timestamps_csv = df_chart_csv['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                            iv_values_csv = df_chart_csv['iv'].fillna(0).tolist()
                            iv_data_store[symbol] = {
                                "timestamps": timestamps_csv,
                                "iv_values": iv_values_csv,
                                "close_prices": df_chart_csv['close'].tolist() if 'close' in df_chart_csv.columns else [],
                                "fclose_prices": df_chart_csv['fclose'].tolist() if 'fclose' in df_chart_csv.columns else [],
                                "last_update": datetime.now().isoformat()
                            }
                            print(f"  ✓ Loaded {len(timestamps_csv)} data points from CSV for {symbol}")
                except Exception as e:
                    print(f"  Could not load CSV data: {e}")
                
                time.sleep(5)
                continue
            
            print(f"✓ Fetched {len(df)} candles for {symbol}")
            
            # Calculate IV - pass the correct future_symbol from SymbolSetting.csv
            # Option expiry is used for time_to_expiry calculation
            print(f"Starting IV calculation for {symbol} with future_symbol={future_symbol}...")
            try:
                df_with_iv = calculate_iv(
                    df.copy(),
                    window=20,
                    timeframe=timeframe,
                    symbol=symbol,
                    risk_free_rate=risk_free_rate,
                    manual_strike=atm_strike,
                    manual_expiry=expiry_date.isoformat(),  # Option expiry (used for option symbol and time_to_expiry calculation)
                    manual_option_type=option_type,
                    manual_future_symbol=future_symbol  # Pass the correct future symbol from SymbolSetting.csv
                )
                print(f"IV calculation completed. Result: {'None' if df_with_iv is None else f'{len(df_with_iv)} rows'}")
            except Exception as e:
                error_msg = f"Exception during IV calculation for {symbol}: {str(e)}"
                print(f"❌ {error_msg}")
                import traceback
                traceback.print_exc()
                add_log('ERROR', error_msg, {
                    'symbol': symbol,
                    'future_symbol': future_symbol,
                    'error': str(e),
                    'traceback': traceback.format_exc()
                })
                df_with_iv = None
            
            if df_with_iv is not None and 'iv' in df_with_iv.columns:
                print(f"✓ IV calculation successful for {symbol}: {len(df_with_iv)} rows with IV data")
                
                # Ensure dates are in IST timezone before formatting
                if df_with_iv['date'].dt.tz is None:
                    df_with_iv['date'] = df_with_iv['date'].dt.tz_localize('Asia/Kolkata')
                else:
                    df_with_iv['date'] = df_with_iv['date'].dt.tz_convert('Asia/Kolkata')
                
                # Sort by date and get latest 500 records for chart display
                df_with_iv = df_with_iv.sort_values('date')
                df_chart = df_with_iv  # Show all rows, no limit
                
                # Format timestamps with IST timezone info (+05:30) - only latest 500
                timestamps_for_chart = df_chart['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                iv_values_for_chart = df_chart['iv'].fillna(0).tolist()
                
                # Store IV data with timestamps - only latest 500 records for chart
                iv_data_store[symbol] = {
                    "timestamps": timestamps_for_chart,
                    "iv_values": iv_values_for_chart,
                    "close_prices": df_chart['close'].tolist(),
                    "fclose_prices": df_chart['fclose'].tolist() if 'fclose' in df_chart.columns else [],
                    "last_update": datetime.now().isoformat()
                }
                
                print(f"✓ Stored IV data in iv_data_store for symbol: {symbol} ({len(timestamps_for_chart)} data points - all records)")
                print(f"  Debug: iv_data_store keys = {list(iv_data_store.keys())}")
                print(f"  Debug: Current fetching_status.symbol = {fetching_status.get('symbol')}")
                print(f"  Debug: Data stored with {len(timestamps_for_chart)} timestamps and {len(iv_values_for_chart)} IV values")
                
                # Verify data is actually stored and accessible
                if symbol in iv_data_store:
                    stored_data = iv_data_store[symbol]
                    print(f"  ✓ Verification: Data accessible in iv_data_store with {len(stored_data.get('timestamps', []))} timestamps")
                else:
                    print(f"  ❌ ERROR: Data was not stored correctly in iv_data_store!")
                
                # Log IV statistics
                non_zero_ivs = [iv for iv in iv_values_for_chart if iv > 0]
                if non_zero_ivs:
                    print(f"IV data stored: {len(non_zero_ivs)} non-zero values (range: {min(non_zero_ivs):.2f}% - {max(non_zero_ivs):.2f}%)")
                else:
                    print(f"⚠ Warning: All IV values are zero/NaN for {symbol}, but data is stored for display")
                
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
                error_msg = f"Could not calculate IV for {symbol}"
                if df_with_iv is None:
                    error_msg += " - calculate_iv returned None"
                elif 'iv' not in df_with_iv.columns:
                    error_msg += f" - dataframe missing 'iv' column. Available columns: {list(df_with_iv.columns)}"
                print(f"❌ {error_msg}")
                add_log('ERROR', error_msg, {
                    'symbol': symbol,
                    'df_is_none': df_with_iv is None,
                    'columns': list(df_with_iv.columns) if df_with_iv is not None else None
                })
                
                # Even if IV calculation failed, try to store the raw data for debugging
                if df_with_iv is not None and len(df_with_iv) > 0:
                    print(f"  Attempting to store raw data even without IV column...")
                    try:
                        if df_with_iv['date'].dt.tz is None:
                            df_with_iv['date'] = df_with_iv['date'].dt.tz_localize('Asia/Kolkata')
                        else:
                            df_with_iv['date'] = df_with_iv['date'].dt.tz_convert('Asia/Kolkata')
                        
                        timestamps_for_chart = df_with_iv['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                        # Create zero IV values as placeholder
                        iv_values_for_chart = [0] * len(timestamps_for_chart)
                        
                        iv_data_store[symbol] = {
                            "timestamps": timestamps_for_chart,
                            "iv_values": iv_values_for_chart,
                            "close_prices": df_with_iv['close'].tolist() if 'close' in df_with_iv.columns else [],
                            "fclose_prices": df_with_iv['fclose'].tolist() if 'fclose' in df_with_iv.columns else [],
                            "last_update": datetime.now().isoformat()
                        }
                        print(f"  ✓ Stored raw data (without IV) for debugging: {symbol}")
                    except Exception as e:
                        print(f"  ❌ Failed to store raw data: {e}")
            
            # Wait 1 second before next iteration
            print(f"Waiting 1 second before next update...")
            time.sleep(1)
            
        except Exception as e:
            error_msg = f"Error in automatic fetch loop: {str(e)}"
            print(f"❌ {error_msg}")
            print(f"  Exception type: {type(e).__name__}")
            print(f"  Future symbol: {future_symbol}, Underlying: {underlying}, Iteration: {iteration}")
            add_log('ERROR', error_msg, {
                'future_symbol': future_symbol,
                'underlying': underlying,
                'iteration': iteration,
                'error': str(e),
                'error_type': type(e).__name__
            })
            import traceback
            traceback.print_exc()
            print(f"  Waiting 5 seconds before retrying...")
            time.sleep(5)  # Wait before retrying on error

def fetch_data_loop(symbol, timeframe, manual_strike=None, manual_expiry=None, manual_option_type=None, manual_future_symbol=None, risk_free_rate=0.07):
    """
    Continuously fetch historical data and calculate IV
    Only fetches data during market hours (NSE: 9:15-15:30, MCX: 9:00-23:30)
    """
    global iv_data_store, fetching_status
    
    while fetching_status["active"] and fetching_status["symbol"] == symbol and fetching_status["timeframe"] == timeframe:
        try:
            # Check if market is open before fetching data
            if not is_market_open(symbol=symbol):
                print(f"Market is closed for {symbol}. Waiting 60 seconds before checking again...")
                time.sleep(60)  # Wait 60 seconds before checking again
                continue
            
            # Check if fyers is available
            if FyresIntegration.fyers is None:
                print("Fyers not initialized. Waiting...")
                time.sleep(5)
                continue
            
            # Fetch historical data using safe wrapper
            df = safe_fetch_ohlc(symbol, timeframe)
            
            if df is None:
                error_msg = f"Failed to fetch data for {symbol}"
                print(f"{error_msg}. This might be due to:")
                print("  - Invalid symbol format")
                print("  - Insufficient historical data available")
                print("  - API rate limiting")
                print("  - Symbol not supported for the selected timeframe")
                add_log('ERROR', error_msg, {
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'possible_reasons': ['Invalid symbol format', 'Insufficient historical data', 'API rate limiting', 'Symbol not supported']
                })
                time.sleep(5)
                continue
            
            if df is not None and len(df) > 0:
                try:
                    # Use the future symbol provided from SymbolSetting.csv (selected by user in manual mode)
                    # No need to construct it - it's already provided
                    if manual_future_symbol:
                        print(f"Using future symbol from SymbolSetting.csv: {manual_future_symbol}")
                    
                    # Calculate IV (will use py_vollib Black model for options, historical volatility for underlying)
                    # Use the future symbol provided by user (from SymbolSetting.csv dropdown)
                    df_with_iv = calculate_iv(
                        df.copy(), 
                        window=20, 
                        timeframe=timeframe, 
                        symbol=symbol, 
                        risk_free_rate=risk_free_rate,
                        manual_strike=manual_strike,
                        manual_expiry=manual_expiry,
                        manual_option_type=manual_option_type,
                        manual_future_symbol=manual_future_symbol  # Use the future symbol selected by user from dropdown
                    )
                    
                    if df_with_iv is not None and 'iv' in df_with_iv.columns:
                        # Ensure dates are in IST timezone before formatting
                        if df_with_iv['date'].dt.tz is None:
                            df_with_iv['date'] = df_with_iv['date'].dt.tz_localize('Asia/Kolkata')
                        else:
                            df_with_iv['date'] = df_with_iv['date'].dt.tz_convert('Asia/Kolkata')
                        
                        # Sort by date - show all records for chart display
                        df_with_iv = df_with_iv.sort_values('date')
                        df_chart = df_with_iv  # Show all rows, no limit
                        
                        # Format timestamps with IST timezone info (+05:30) - only latest 500
                        timestamps_for_chart = df_chart['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                        iv_values_for_chart = df_chart['iv'].fillna(0).tolist()
                        
                        # Store IV data with timestamps - all records for chart
                        iv_data_store[symbol] = {
                            "timestamps": timestamps_for_chart,
                            "iv_values": iv_values_for_chart,
                            "close_prices": df_chart['close'].tolist(),
                            "fclose_prices": df_chart['fclose'].tolist() if 'fclose' in df_chart.columns else [],
                            "last_update": datetime.now().isoformat()
                        }
                        
                        # Log IV statistics
                        non_zero_ivs = [iv for iv in iv_values_for_chart if iv > 0]
                        if non_zero_ivs:
                            print(f"IV data stored: {len(non_zero_ivs)} non-zero values (range: {min(non_zero_ivs):.2f}% - {max(non_zero_ivs):.2f}%) - all records")
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
        try:
            automated_login(
                client_id=credentials.get('client_id'),
                secret_key=credentials.get('secret_key'),
                FY_ID=credentials.get('FY_ID'),
                TOTP_KEY=credentials.get('totpkey'),
                PIN=credentials.get('PIN'),
                redirect_uri=credentials.get('redirect_uri')
            )
            print("automated_login() completed without exception")
        except Exception as login_error:
            error_msg = f"Error in automated_login(): {login_error}"
            print(error_msg)
            import traceback
            traceback.print_exc()
            # Check if fyers was still set despite the error
            if FyresIntegration.fyers is None:
                return jsonify({"success": False, "message": f"Login failed: {str(login_error)}"}), 500
        
        print("Login process completed, checking fyers object...")
        print(f"FyresIntegration.fyers type: {type(FyresIntegration.fyers)}")
        print(f"FyresIntegration.fyers value: {FyresIntegration.fyers}")
        
        # Check if login was successful
        if FyresIntegration.fyers is not None:
            try:
                # Try to get profile to verify login
                profile = FyresIntegration.fyers.get_profile()
                print("Login successful, profile:", profile)
                session['logged_in'] = True
                
                # NOTE: Symbol download disabled - fyers_symbols folder is only used for symbol search feature,
                # not required for core functionality (option generation, IV calculation, data fetching)
                # Users can manually download symbols via /api/download_symbols endpoint if needed for symbol search
                
                return jsonify({
                    "success": True, 
                    "message": "Login successful",
                    "downloading_symbols": False
                })
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
    global fetching_status, iv_data_store, fetch_thread, fetch_lock
    
    # Acquire lock to prevent race conditions
    if not fetch_lock.acquire(blocking=False):
        return jsonify({"success": False, "message": "Another fetch operation is already in progress. Please wait."}), 400
    
    try:
        data = request.json
        mode = data.get('mode', 'manual')  # 'manual' or 'automatic'
        # Normalize mode to lowercase for case-insensitive comparison
        mode = str(mode).lower().strip() if mode else 'manual'
        timeframe = data.get('timeframe')
        risk_free_rate = data.get('risk_free_rate', 0.07)  # Default 7% (0.07) = 91-day Indian T-Bill yield
        
        print(f"[start_fetching] Received mode: '{data.get('mode')}' -> normalized: '{mode}'")
        print(f"[start_fetching] Request data keys: {list(data.keys()) if data else 'None'}")
        
        if not timeframe:
            fetch_lock.release()
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
        
        # STEP 1: Stop previous fetching completely if active
        print("=" * 60)
        print("STARTING FRESH FETCH - Stopping any existing fetch operations...")
        print("=" * 60)
        
        if fetching_status.get("active"):
            old_symbol = fetching_status.get("symbol")
            old_mode = fetching_status.get("mode")
            print(f"Stopping previous fetch: symbol={old_symbol}, mode={old_mode}")
            
            # Stop the thread
            fetching_status["active"] = False
            fetching_status["mode"] = None
            
            # Wait for thread to finish (with timeout)
            if fetch_thread is not None and fetch_thread.is_alive():
                print(f"Waiting for thread {fetch_thread.ident} to stop...")
                fetch_thread.join(timeout=3)  # Wait up to 3 seconds
                if fetch_thread.is_alive():
                    print(f"Warning: Thread {fetch_thread.ident} did not stop within timeout, but continuing...")
                else:
                    print(f"Thread {fetch_thread.ident} stopped successfully")
            
            fetch_thread = None
            print("Previous fetch stopped")
        
        # Clear in-memory data (CSV files preserved)
        iv_data_store.clear()
        print("Cleared in-memory data (CSV files preserved)")
        
        # Small delay to ensure cleanup is complete
        time.sleep(0.5)
        
        # Helper function to load CSV data into iv_data_store if it exists
        def load_csv_to_store(symbol):
            """Load CSV data into iv_data_store if file exists"""
            try:
                safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
                safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
                filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
                
                if not os.path.exists(filename):
                    print(f"CSV file not found for {symbol}: {filename}")
                    return False
                
                print(f"Loading CSV data from {filename}...")
                df = pd.read_csv(filename)
                
                if 'date' not in df.columns or 'iv' not in df.columns:
                    print(f"CSV file missing required columns. Available: {list(df.columns)}")
                    return False
                
                # Validate symbol matches (but be lenient - use the symbol from request)
                if 'option_name' in df.columns:
                    csv_symbols = df['option_name'].dropna().unique()
                    if len(csv_symbols) > 0:
                        csv_symbol = str(csv_symbols[0]).strip()
                        csv_symbol_normalized = csv_symbol.replace(':', '_')
                        symbol_normalized = symbol.replace(':', '_')
                        if csv_symbol_normalized != symbol_normalized and csv_symbol != symbol:
                            print(f"Symbol mismatch: CSV has '{csv_symbol}' but requested '{symbol}'. Using requested symbol.")
                            # Continue anyway - use the requested symbol
                
                # Convert dates - preserve CSV timestamp exactly as-is
                # CSV timestamps are already correct IST times, so we just parse and format them
                df['date'] = pd.to_datetime(df['date'])
                # Don't apply timezone conversion - CSV timestamps are already correct IST times
                # Just format them with IST timezone indicator for frontend
                df = df.sort_values('date')
                
                # Format for chart - preserve exact CSV timestamp with IST timezone indicator
                # Format as ISO string with IST timezone offset (+05:30) so JavaScript can parse it correctly
                timestamps = df['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                iv_values = df['iv'].fillna(0).tolist()
                close_prices = df['close'].tolist() if 'close' in df.columns else []
                fclose_prices = df['fclose'].tolist() if 'fclose' in df.columns else []
                
                # Store in iv_data_store
                iv_data_store[symbol] = {
                    "timestamps": timestamps,
                    "iv_values": iv_values,
                    "close_prices": close_prices,
                    "fclose_prices": fclose_prices,
                    "last_update": datetime.now().isoformat()
                }
                print(f"✓ Loaded CSV data into iv_data_store for {symbol}: {len(timestamps)} data points")
                print(f"  IV range: {min([v for v in iv_values if v > 0]) if any(v > 0 for v in iv_values) else 0:.2f}% - {max(iv_values):.2f}%")
                return True
            except Exception as e:
                print(f"Warning: Could not load CSV data for {symbol}: {e}")
                import traceback
                traceback.print_exc()
                return False
        
        if mode == 'automatic':
            print("[start_fetching] Processing AUTOMATIC mode")
            # Automatic mode: Generate option symbol from future symbol
            # IMPORTANT: future_symbol comes from SymbolSetting.csv (with future expiry)
            #            expiry_date_str is the OPTION expiry from web input (different from future expiry)
            future_symbol = data.get('future_symbol')
            expiry_type = data.get('expiry_type', 'weekly')  # 'weekly' or 'monthly'
            expiry_date_str = data.get('expiry_date')  # This is OPTION expiry, not future expiry
            option_type = data.get('option_type', 'c')  # 'c' for Call, 'p' for Put
            
            print(f"[start_fetching] Automatic mode params: future_symbol={future_symbol}, expiry_date={expiry_date_str}, expiry_type={expiry_type}, option_type={option_type}")
            
            if not future_symbol:
                fetch_lock.release()
                return jsonify({"success": False, "message": "Future symbol is required for automatic mode"}), 400
            
            # Validate that future_symbol is correctly formatted (should come from SymbolSetting.csv)
            if ':' not in future_symbol or 'FUT' not in future_symbol:
                fetch_lock.release()
                return jsonify({"success": False, "message": f"Invalid future symbol format: {future_symbol}. Should be like MCX:SILVER25DECFUT"}), 400
            
            # Get option expiry from SymbolSetting.csv based on selected future symbol
            option_expiry_date = None
            if expiry_date_str:
                # Use provided expiry date (from auto-filled input)
                try:
                    option_expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%d')
                except:
                    try:
                        option_expiry_date = datetime.strptime(expiry_date_str, '%Y-%m-%dT%H:%M')
                    except:
                        pass
            else:
                # Look up option expiry from SymbolSetting.csv
                symbols = load_symbol_settings()
                for sym in symbols:
                    sym_future = generate_future_symbol_from_settings(sym['prefix'], sym['symbol'], sym['expiry_date'])
                    if sym_future == future_symbol and sym.get('option_expiry_datetime'):
                        option_expiry_date = sym['option_expiry_datetime']
                        print(f"Using option expiry from SymbolSetting.csv: {option_expiry_date}")
                        break
            
            if not option_expiry_date:
                fetch_lock.release()
                return jsonify({"success": False, "message": "Option expiry date is required. Please ensure SymbolSetting.csv has OptionExpiery field."}), 400
            
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
            # For MCX:CRUDEOIL25DECFUT, we need to extract "CRUDEOIL" (remove 25DECFUT)
            underlying = None
            if is_mcx:
                # MCX contracts: Remove year (2 digits), month (3 letters), and FUT suffix
                # e.g., CRUDEOIL25DECFUT -> CRUDEOIL
                # Pattern: COMMODITY + YY + MONTH + FUT (NO month code letter)
                if underlying_part.endswith('FUT'):
                    # Remove FUT suffix first
                    base = underlying_part[:-3]
                    # Remove year (2 digits) + month (3 letters) from end
                    # Pattern: YY + MONTH (e.g., 25DEC)
                    import re
                    # Remove pattern: 2 digits + 3 letters from the end
                    underlying = re.sub(r'\d{2}[A-Z]{3}$', '', base)
                    # If regex didn't match, try simple approach
                    if underlying == base and len(base) >= 5:
                        # Check if last 5 characters match YY + MONTH pattern (2 digits + 3 letters)
                        if base[-5:-3].isdigit() and base[-3:].isalpha():
                            underlying = base[:-5]
                        else:
                            # Last resort: assume format is just COMMODITY, keep as is
                            underlying = base
                    # IMPORTANT: Check for contract variants FIRST (SILVERM, GOLDM are different contracts, not month codes)
                    # These are separate contracts and should be preserved as-is
                    contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
                    underlying_upper = underlying.upper() if underlying else ''
                    is_contract_variant = False
                    # Check if underlying starts with a variant (might have trailing digits/chars)
                    for variant in contract_variants:
                        if underlying_upper == variant or underlying_upper.startswith(variant):
                            # Extract just the variant name (remove any trailing characters)
                            # This ensures we get "SILVERM" not "SILVERM26" or "SILVERM26FEB"
                            underlying = variant
                            is_contract_variant = True
                            break
                    
                    # Only check for month codes if it's NOT a contract variant
                    if not is_contract_variant:
                        mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                        if underlying and len(underlying) > 0 and underlying[-1].upper() in mcx_month_codes:
                            # Check if it's a valid commodity name
                            mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
                            for commodity in mcx_commodities:
                                if underlying.upper().startswith(commodity) and len(underlying) > len(commodity):
                                    # It's a month code, remove it
                                    underlying = commodity
                                    break
                else:
                    # No FUT suffix, might be just the commodity
                    underlying = underlying_part
                    # IMPORTANT: Check for contract variants FIRST (SILVERM, GOLDM are different contracts)
                    contract_variants = ['SILVERM', 'GOLDM', 'SILVERMINI', 'GOLDMINI']
                    underlying_upper = underlying.upper() if underlying else ''
                    is_variant = False
                    for variant in contract_variants:
                        if underlying_upper == variant or underlying_upper.startswith(variant):
                            underlying = variant
                            is_variant = True
                            break
                    
                    if not is_variant:
                        # Not a contract variant, check if it's a month code
                        mcx_month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
                        if underlying and len(underlying) > 0 and underlying[-1].upper() in mcx_month_codes:
                            mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
                            for commodity in mcx_commodities:
                                if underlying.upper().startswith(commodity) and len(underlying) > len(commodity):
                                    underlying = commodity
                                    break
            elif 'NIFTY' in underlying_part:
                if 'BANK' in underlying_part:
                    underlying = 'BANKNIFTY'
                else:
                    underlying = 'NIFTY'
            
            if not underlying:
                error_msg = f"Could not extract underlying from future symbol: {future_symbol}"
                print(f"ERROR: {error_msg}")
                add_log('ERROR', error_msg, {'future_symbol': future_symbol, 'underlying_part': underlying_part})
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 400
            
            print(f"Extracted underlying: {underlying} from future symbol: {future_symbol}")
            
            # Get future LTP
            print(f"Fetching LTP for future symbol: {future_symbol}")
            future_ltp = get_future_ltp(future_symbol)
            if future_ltp is None:
                error_msg = f"Could not fetch LTP for {future_symbol}. Please check if the symbol is valid and market is open."
                print(f"ERROR: {error_msg}")
                add_log('ERROR', error_msg, {'future_symbol': future_symbol})
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 400
            
            print(f"Future LTP fetched successfully: {future_ltp}")
            
            # Calculate ATM strike
            # Get strike_step from request, or use defaults based on symbol type
            strike_step = data.get('strike_step')
            if strike_step is not None:
                try:
                    strike_distance = float(strike_step)
                    if strike_distance <= 0:
                        fetch_lock.release()
                        return jsonify({"success": False, "message": "Strike step must be greater than 0"}), 400
                except (ValueError, TypeError):
                    fetch_lock.release()
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
                fetch_lock.release()
                return jsonify({"success": False, "message": "Could not calculate ATM strike"}), 400
            
            # Generate option symbol using OPTION expiry date (from web input, not future expiry)
            # future_symbol already has the correct future expiry from SymbolSetting.csv
            # option_expiry_date is the OPTION expiry date from web input
            print(f"Generating option symbol: underlying={underlying}, expiry={option_expiry_date}, strike={atm_strike}, type={option_type}, expiry_type={expiry_type}, is_mcx={is_mcx}")
            # Debug: Print the underlying before generating symbol
            print(f"DEBUG: Underlying before symbol generation: '{underlying}' (type: {type(underlying)}, length: {len(underlying) if underlying else 0})")
            symbol = generate_option_symbol(underlying, option_expiry_date, atm_strike, option_type, expiry_type, is_mcx=is_mcx)
            print(f"DEBUG: Generated symbol: '{symbol}'")
            
            if not symbol:
                error_msg = f"Could not generate option symbol for underlying={underlying}, expiry={option_expiry_date}, strike={atm_strike}"
                print(f"ERROR: {error_msg}")
                add_log('ERROR', error_msg, {
                    'underlying': underlying,
                    'expiry': str(option_expiry_date),
                    'strike': atm_strike,
                    'option_type': option_type,
                    'expiry_type': expiry_type
                })
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 400
            
            print(f"Generated option symbol: {symbol}")
            
            print(f"Automatic mode: Future symbol {future_symbol} (from SymbolSetting.csv), Option symbol {symbol} (expiry: {option_expiry_date.strftime('%Y-%m-%d')}), LTP: {future_ltp}, ATM Strike: {atm_strike}")
            
            # STEP 3: Fetch both future and option historical data BEFORE starting continuous loop
            print("=" * 60)
            print("STEP 3: Fetching historical data for option and future...")
            print("=" * 60)
            
            # Fetch option historical data
            print(f"Fetching option historical data for: {symbol}")
            df_option = safe_fetch_ohlc(symbol, timeframe)
            if df_option is None or len(df_option) == 0:
                error_msg = f"Failed to fetch historical data for {symbol}"
                print(f"ERROR: {error_msg}")
                add_log('ERROR', error_msg, {'symbol': symbol})
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 400
            
            print(f"✓ Fetched {len(df_option)} candles for option: {symbol}")
            
            # STEP 4: Calculate IV (this internally fetches future data and merges)
            print("=" * 60)
            print("STEP 4: Calculating IV...")
            print("=" * 60)
            
            try:
                df_with_iv = calculate_iv(
                    df_option.copy(),
                    window=20,
                    timeframe=timeframe,
                    symbol=symbol,
                    risk_free_rate=risk_free_rate,
                    manual_strike=atm_strike,
                    manual_expiry=option_expiry_date.isoformat(),
                    manual_option_type=option_type,
                    manual_future_symbol=future_symbol
                )
                
                if df_with_iv is None or 'iv' not in df_with_iv.columns:
                    error_msg = f"IV calculation failed for {symbol}"
                    print(f"ERROR: {error_msg}")
                    add_log('ERROR', error_msg, {'symbol': symbol})
                    fetch_lock.release()
                    return jsonify({"success": False, "message": error_msg}), 400
                
                print(f"✓ IV calculation successful: {len(df_with_iv)} rows")
                
            except Exception as e:
                error_msg = f"Exception during IV calculation: {str(e)}"
                print(f"ERROR: {error_msg}")
                import traceback
                traceback.print_exc()
                add_log('ERROR', error_msg, {'symbol': symbol, 'error': str(e)})
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 500
            
            # STEP 5: Save to CSV (only required columns)
            print("=" * 60)
            print("STEP 5: Saving to CSV...")
            print("=" * 60)
            
            save_iv_to_csv(
                symbol=symbol,
                df_with_iv=df_with_iv,
                timeframe=timeframe,
                strike=atm_strike,
                expiry=option_expiry_date.isoformat(),
                option_type=option_type
            )
            
            # STEP 6: Store in iv_data_store for chart display (all records)
            print("=" * 60)
            print("STEP 6: Preparing data for chart display...")
            print("=" * 60)
            
            if df_with_iv['date'].dt.tz is None:
                df_with_iv['date'] = df_with_iv['date'].dt.tz_localize('Asia/Kolkata')
            else:
                df_with_iv['date'] = df_with_iv['date'].dt.tz_convert('Asia/Kolkata')
            
            # Sort by date and get latest records
            df_with_iv = df_with_iv.sort_values('date')
            
            # Get all records for chart (no limit)
            df_chart = df_with_iv
            
            timestamps_for_chart = df_chart['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
            iv_values_for_chart = df_chart['iv'].fillna(0).tolist()
            
            iv_data_store[symbol] = {
                "timestamps": timestamps_for_chart,
                "iv_values": iv_values_for_chart,
                "close_prices": df_chart['close'].tolist(),
                "fclose_prices": df_chart['fclose'].tolist() if 'fclose' in df_chart.columns else [],
                "last_update": datetime.now().isoformat()
            }
            
            print(f"✓ Stored {len(timestamps_for_chart)} data points in iv_data_store (all records)")
            print(f"  Debug: iv_data_store keys after initial fetch: {list(iv_data_store.keys())}")
            print(f"  Debug: Symbol stored: {symbol}")
            print(f"  Debug: Data verification - timestamps: {len(timestamps_for_chart)}, IV values: {len(iv_values_for_chart)}")
            print("=" * 60)
            print("Initial fetch complete. Starting continuous updates...")
            print("=" * 60)
            
            # Don't delete CSV files here - let them persist for display
            # CSV files will be deleted when user clicks "Stop Fetching"
            
            # STEP 7: Start continuous fetching in background thread
            # IMPORTANT: Update fetching_status BEFORE starting thread to avoid race conditions
            # Clear old status first, then set new values
            fetching_status.clear()
            fetching_status.update({
                "active": True,
                "symbol": symbol,  # This will be updated by the thread as it generates new symbols
                "timeframe": timeframe,
                "mode": "automatic",
                "future_symbol": future_symbol,  # Future expiry from SymbolSetting.csv - used to detect symbol changes
                "expiry_type": expiry_type,
                "expiry_date": expiry_date_str,  # Option expiry from web input
                "option_type": option_type,
                "strike": atm_strike,
                "expiry": option_expiry_date.isoformat()  # Option expiry from web input
            })
            print(f"Updated fetching_status: symbol={symbol}, mode=automatic, future_symbol={future_symbol}")
            print(f"Full fetching_status: {fetching_status}")
            
            # Start fetching in background thread with automatic mode
            print(f"Starting automatic fetch thread: future_symbol={future_symbol}, option_expiry={option_expiry_date}, option_symbol={symbol}")
            try:
                # Create and start thread
                fetch_thread = threading.Thread(target=fetch_data_loop_automatic, args=(future_symbol, option_expiry_date, expiry_type, option_type, timeframe, strike_distance, risk_free_rate), daemon=True)
                fetch_thread.start()
                print(f"Automatic fetch thread started successfully. Thread ID: {fetch_thread.ident}")
                add_log('INFO', 'Automatic data fetching started', {
                    'future_symbol': future_symbol,
                    'option_symbol': symbol,
                    'future_ltp': future_ltp,
                    'atm_strike': atm_strike,
                    'strike_step': strike_distance
                })
            except Exception as e:
                error_msg = f"Failed to start fetch thread: {str(e)}"
                print(f"ERROR: {error_msg}")
                import traceback
                traceback.print_exc()
                add_log('ERROR', error_msg, {'error': str(e), 'traceback': traceback.format_exc()})
                fetch_thread = None
                fetching_status["active"] = False
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 500
            
            # Release lock after thread is started successfully
            fetch_lock.release()
            
            return jsonify({
                "success": True, 
                "message": "Automatic data fetching started",
                "generated_symbol": symbol,
                "future_ltp": future_ltp,
                "atm_strike": atm_strike,
                "strike_step": strike_distance
            })
        
        elif mode == 'manual':
            print("[start_fetching] Processing MANUAL mode")
            # Manual mode: Use provided symbol, future symbol, and option expiry from CSV
            symbol = data.get('symbol')
            future_symbol = data.get('future_symbol')  # Future symbol from SymbolSetting.csv dropdown
            expiry = data.get('expiry')  # Option expiry from SymbolSetting.csv (auto-filled)
            option_type = data.get('option_type')  # Optional manual option type ('c' or 'p')
            
            print(f"[start_fetching] Manual mode params: symbol={symbol}, future_symbol={future_symbol}, expiry={expiry}")
            
            if not symbol:
                fetch_lock.release()
                return jsonify({"success": False, "message": "Symbol is required for manual mode"}), 400
            
            if not future_symbol:
                fetch_lock.release()
                return jsonify({"success": False, "message": "Future symbol is required for manual mode. Please select from the dropdown."}), 400
            
            # Validate that future_symbol is correctly formatted
            if ':' not in future_symbol or 'FUT' not in future_symbol:
                fetch_lock.release()
                return jsonify({"success": False, "message": f"Invalid future symbol format: {future_symbol}. Should be like MCX:SILVERM26FEBFUT"}), 400
            
            # Get option expiry from SymbolSetting.csv based on selected future symbol
            if not expiry:
                # Look up option expiry from SymbolSetting.csv
                symbols = load_symbol_settings()
                for sym in symbols:
                    sym_future = generate_future_symbol_from_settings(sym['prefix'], sym['symbol'], sym['expiry_date'])
                    if sym_future == future_symbol and sym.get('option_expiry_datetime'):
                        expiry = sym['option_expiry_datetime'].strftime('%Y-%m-%dT%H:%M')
                        print(f"Using option expiry from SymbolSetting.csv: {expiry}")
                        break
        
        # STEP 3: Fetch historical data
        print("=" * 60)
        print("STEP 3: Fetching historical data for symbol...")
        print("=" * 60)
        
        df = safe_fetch_ohlc(symbol, timeframe)
        if df is None or len(df) == 0:
            error_msg = f"Failed to fetch historical data for {symbol}"
            print(f"ERROR: {error_msg}")
            fetch_lock.release()
            return jsonify({"success": False, "message": error_msg}), 400
        
        print(f"✓ Fetched {len(df)} candles for: {symbol}")
        
        # STEP 4: Calculate IV
        print("=" * 60)
        print("STEP 4: Calculating IV...")
        print("=" * 60)
        
        # Use the future symbol directly from SymbolSetting.csv (selected by user)
        # No need to construct it - user has selected it from the dropdown
        print(f"Using future symbol from SymbolSetting.csv: {future_symbol}")
        
        try:
            df_with_iv = calculate_iv(
                df.copy(),
                window=20,
                timeframe=timeframe,
                symbol=symbol,
                risk_free_rate=risk_free_rate,
                manual_strike=None,  # Strike is extracted from symbol, no need for manual input
                manual_expiry=expiry,
                manual_option_type=option_type,
                manual_future_symbol=future_symbol  # Use the future symbol selected by user from dropdown
            )
            
            if df_with_iv is None or 'iv' not in df_with_iv.columns:
                error_msg = f"IV calculation failed for {symbol}"
                print(f"ERROR: {error_msg}")
                fetch_lock.release()
                return jsonify({"success": False, "message": error_msg}), 400
            
            print(f"✓ IV calculation successful: {len(df_with_iv)} rows")
        except Exception as e:
            error_msg = f"Exception during IV calculation: {str(e)}"
            print(f"ERROR: {error_msg}")
            import traceback
            traceback.print_exc()
            fetch_lock.release()
            return jsonify({"success": False, "message": error_msg}), 500
        
        # STEP 5: Save to CSV
        print("=" * 60)
        print("STEP 5: Saving to CSV...")
        print("=" * 60)
        
        # Extract strike from symbol if available
        strike_from_symbol = None
        option_info = parse_option_symbol(symbol)
        if option_info and option_info.get('strike'):
            strike_from_symbol = option_info['strike']
        
        save_iv_to_csv(
            symbol=symbol,
            df_with_iv=df_with_iv,
            timeframe=timeframe,
            strike=strike_from_symbol,
            expiry=expiry,
            option_type=option_type
        )
        
        # STEP 6: Store in iv_data_store for chart (all records)
        print("=" * 60)
        print("STEP 6: Preparing data for chart display...")
        print("=" * 60)
        
        if df_with_iv['date'].dt.tz is None:
            df_with_iv['date'] = df_with_iv['date'].dt.tz_localize('Asia/Kolkata')
        else:
            df_with_iv['date'] = df_with_iv['date'].dt.tz_convert('Asia/Kolkata')
        
        df_with_iv = df_with_iv.sort_values('date')
        df_chart = df_with_iv  # Show all rows, no limit
        
        timestamps_for_chart = df_chart['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
        iv_values_for_chart = df_chart['iv'].fillna(0).tolist()
        
        iv_data_store[symbol] = {
            "timestamps": timestamps_for_chart,
            "iv_values": iv_values_for_chart,
            "close_prices": df_chart['close'].tolist(),
            "fclose_prices": df_chart['fclose'].tolist() if 'fclose' in df_chart.columns else [],
            "last_update": datetime.now().isoformat()
        }
        
        print(f"✓ Stored {len(timestamps_for_chart)} data points in iv_data_store (latest 500 records)")
        print("=" * 60)
        print("Initial fetch complete. Starting continuous updates...")
        print("=" * 60)
        
        # Don't delete CSV files here - let them persist for display
        # CSV files will be deleted when user clicks "Stop Fetching"
        
        # STEP 7: Start continuous fetching in background thread
        fetching_status.clear()
        fetching_status.update({
            "active": True,
            "symbol": symbol,
            "timeframe": timeframe,
            "mode": "manual",
            "expiry": expiry,
            "option_type": option_type,
            "future_symbol": future_symbol  # Store future symbol for reference
        })
        
        # Start fetching in background thread
        try:
            fetch_thread = threading.Thread(target=fetch_data_loop, args=(symbol, timeframe, None, expiry, option_type, future_symbol, risk_free_rate), daemon=True)
            fetch_thread.start()
            print(f"Manual fetch thread started successfully. Thread ID: {fetch_thread.ident}")
        except Exception as e:
            error_msg = f"Failed to start fetch thread: {str(e)}"
            print(f"ERROR: {error_msg}")
            import traceback
            traceback.print_exc()
            fetch_thread = None
            fetching_status["active"] = False
            fetch_lock.release()
            return jsonify({"success": False, "message": error_msg}), 500
        
        # Release lock after thread is started successfully
        fetch_lock.release()
        
        return jsonify({"success": True, "message": "Data fetching started"})
    
    except Exception as e:
        # Ensure lock is always released on any exception
        fetch_lock.release()
        error_msg = f"Unexpected error in start_fetching: {str(e)}"
        print(f"ERROR: {error_msg}")
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": error_msg}), 500

@app.route('/api/stop_fetching', methods=['POST'])
def stop_fetching():
    """Stop fetching historical data (CSV files are preserved)"""
    global fetching_status, iv_data_store, fetch_thread, fetch_lock
    
    print("=" * 60)
    print("STOPPING FETCH OPERATION...")
    print("=" * 60)
    
    # Acquire lock to ensure clean stop
    fetch_lock.acquire()
    
    try:
        # Stop fetching first
        fetching_status["active"] = False
        fetching_status["mode"] = None
        
        # Wait for thread to finish (with timeout)
        if fetch_thread is not None and fetch_thread.is_alive():
            print(f"Waiting for thread {fetch_thread.ident} to stop...")
            fetch_thread.join(timeout=3)  # Wait up to 3 seconds
            if fetch_thread.is_alive():
                print(f"Warning: Thread {fetch_thread.ident} did not stop within timeout")
            else:
                print(f"Thread {fetch_thread.ident} stopped successfully")
        
        fetch_thread = None
        
        # CSV files are NOT deleted - they are preserved in data folder for future reference
        print("Stopping fetch - CSV files will be preserved in data folder")
        add_log('INFO', 'Data fetching stopped - CSV files preserved', {})
        
        # Clear all in-memory data only (CSV files remain on disk)
        iv_data_store.clear()
        print("Stopped fetching - in-memory data cleared, CSV files preserved")
        
        return jsonify({"success": True, "message": "Data fetching stopped. CSV files preserved in data folder."})
    finally:
        # Always release lock
        fetch_lock.release()

@app.route('/api/get_iv_data', methods=['GET'])
def get_iv_data():
    """Get current IV data for charting - loads from CSV if not in memory"""
    symbol = request.args.get('symbol')
    
    if symbol and symbol in iv_data_store:
        data = iv_data_store[symbol]
        # Log data being sent for debugging
        print(f"Returning IV data for {symbol}: {len(data.get('timestamps', []))} timestamps, {len(data.get('iv_values', []))} IV values")
        return jsonify(data)
    else:
        # Debug: Print what symbols are available in iv_data_store
        available_symbols = list(iv_data_store.keys())
        print(f"No IV data found in memory for symbol: {symbol}")
        print(f"Available symbols in iv_data_store: {available_symbols}")
        
        if symbol:
            # Check if there's a similar symbol (case-insensitive or with different formatting)
            symbol_upper = symbol.upper()
            for stored_symbol in available_symbols:
                if stored_symbol.upper() == symbol_upper:
                    print(f"Found case-insensitive match: {stored_symbol} (requested: {symbol})")
                    data = iv_data_store[stored_symbol]
                    return jsonify(data)
            
            # If not in memory, try loading from CSV file
            print(f"Attempting to load IV data from CSV for symbol: {symbol}")
            try:
                safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
                safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
                filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
                
                if os.path.exists(filename):
                    print(f"CSV file found: {filename}, loading data...")
                    df = pd.read_csv(filename)
                    
                    if 'date' not in df.columns or 'iv' not in df.columns:
                        print(f"CSV file missing required columns. Available: {list(df.columns)}")
                        return jsonify({"timestamps": [], "iv_values": [], "close_prices": [], "fclose_prices": [], "last_update": None})
                    
                    # Convert dates
                    df['date'] = pd.to_datetime(df['date'])
                    if df['date'].dt.tz is None:
                        df['date'] = df['date'].dt.tz_localize('Asia/Kolkata')
                    else:
                        df['date'] = df['date'].dt.tz_convert('Asia/Kolkata')
                    
                    # Sort by date - show all records for chart
                    df = df.sort_values('date')
                    df_chart = df  # Show all rows, no limit
                    
                    timestamps = df_chart['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                    iv_values = df_chart['iv'].fillna(0).tolist()
                    close_prices = df_chart['close'].tolist() if 'close' in df_chart.columns else []
                    fclose_prices = df_chart['fclose'].tolist() if 'fclose' in df_chart.columns else []
                    
                    # Store in iv_data_store for future requests
                    iv_data_store[symbol] = {
                        "timestamps": timestamps,
                        "iv_values": iv_values,
                        "close_prices": close_prices,
                        "fclose_prices": fclose_prices,
                        "last_update": datetime.now().isoformat()
                    }
                    
                    print(f"✓ Loaded {len(timestamps)} data points from CSV for {symbol} (all records)")
                    print(f"  Debug: Stored in iv_data_store with key: {symbol}")
                    print(f"  Debug: iv_data_store now has keys: {list(iv_data_store.keys())}")
                    return jsonify({
                        "timestamps": timestamps,
                        "iv_values": iv_values,
                        "close_prices": close_prices,
                        "fclose_prices": fclose_prices,
                        "last_update": datetime.now().isoformat()
                    })
                else:
                    print(f"CSV file not found: {filename}")
                    # Try to find similar CSV files (in case symbol format differs slightly)
                    if os.path.exists(DATA_FOLDER):
                        csv_files = [f for f in os.listdir(DATA_FOLDER) if f.endswith('.csv')]
                        print(f"  Available CSV files in data folder: {csv_files[:10]}...")  # Show first 10
                        # Try to find a match by checking if symbol (without colons) matches filename
                        symbol_no_colon = symbol.replace(':', '_')
                        for csv_file in csv_files:
                            csv_name_no_ext = csv_file.replace('.csv', '')
                            if symbol_no_colon.upper() == csv_name_no_ext.upper():
                                print(f"  Found matching CSV file: {csv_file} (case-insensitive match)")
                                # Try loading this file
                                filename = os.path.join(DATA_FOLDER, csv_file)
                                df = pd.read_csv(filename)
                                if 'date' in df.columns and 'iv' in df.columns:
                                    df['date'] = pd.to_datetime(df['date'])
                                    if df['date'].dt.tz is None:
                                        df['date'] = df['date'].dt.tz_localize('Asia/Kolkata')
                                    else:
                                        df['date'] = df['date'].dt.tz_convert('Asia/Kolkata')
                                    df = df.sort_values('date')
                                    df_chart = df  # Show all rows, no limit
                                    timestamps = df_chart['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
                                    iv_values = df_chart['iv'].fillna(0).tolist()
                                    close_prices = df_chart['close'].tolist() if 'close' in df_chart.columns else []
                                    fclose_prices = df_chart['fclose'].tolist() if 'fclose' in df_chart.columns else []
                                    iv_data_store[symbol] = {
                                        "timestamps": timestamps,
                                        "iv_values": iv_values,
                                        "close_prices": close_prices,
                                        "fclose_prices": fclose_prices,
                                        "last_update": datetime.now().isoformat()
                                    }
                                    print(f"  ✓ Loaded {len(timestamps)} data points from matched CSV file")
                                    return jsonify({
                                        "timestamps": timestamps,
                                        "iv_values": iv_values,
                                        "close_prices": close_prices,
                                        "fclose_prices": fclose_prices,
                                        "last_update": datetime.now().isoformat()
                                    })
                                break
            except Exception as e:
                print(f"Error loading CSV data for {symbol}: {e}")
                import traceback
                traceback.print_exc()
        
        add_log('WARNING', f'IV data not found for symbol: {symbol}', {
            'requested_symbol': symbol,
            'available_symbols': available_symbols
        })
        return jsonify({"timestamps": [], "iv_values": [], "close_prices": [], "fclose_prices": [], "last_update": None})

@app.route('/api/get_status', methods=['GET'])
def get_status():
    """Get current fetching status"""
    return jsonify(fetching_status)

@app.route('/api/load_csv_data', methods=['GET'])
def load_csv_data():
    """Load IV data from CSV files in data folder with strict symbol validation"""
    try:
        # Symbol is REQUIRED - no auto-loading of most recent file
        symbol = request.args.get('symbol')
        if not symbol:
            return jsonify({"success": False, "message": "Symbol parameter is required. Cannot auto-load CSV without explicit symbol."}), 400
        
        # Sanitize symbol for filename matching
        safe_symbol = re.sub(r'[<>:"/\\|?*]', '_', symbol)
        safe_symbol = safe_symbol.replace(':', '_').replace(' ', '_')
        filename = os.path.join(DATA_FOLDER, f"{safe_symbol}.csv")
        
        if not os.path.exists(filename):
            return jsonify({"success": False, "message": f"CSV file not found for symbol: {symbol}"}), 404
        
        # Read CSV file
        df = pd.read_csv(filename)
        
        # Ensure required columns exist
        if 'date' not in df.columns or 'iv' not in df.columns:
            return jsonify({"success": False, "message": "CSV file missing required columns (date, iv)"}), 400
        
        # STRICT VALIDATION: Verify CSV content matches requested symbol
        # Check if CSV has option_name column and verify it matches
        if 'option_name' in df.columns:
            # Get unique option names from CSV (should all be the same)
            csv_symbols = df['option_name'].dropna().unique()
            if len(csv_symbols) > 0:
                csv_symbol = str(csv_symbols[0]).strip()
                # Compare symbols (handle both MCX: and MCX_ formats)
                csv_symbol_normalized = csv_symbol.replace(':', '_')
                symbol_normalized = symbol.replace(':', '_')
                
                if csv_symbol_normalized != symbol_normalized and csv_symbol != symbol:
                    error_msg = f"Symbol mismatch: CSV file contains '{csv_symbol}' but requested '{symbol}'. This prevents data mixing between different symbols."
                    print(f"ERROR: {error_msg}")
                    return jsonify({"success": False, "message": error_msg}), 400
        
        # Also verify filename matches (double-check)
        filename_symbol = os.path.basename(filename).replace('.csv', '')
        if filename_symbol != safe_symbol:
            error_msg = f"Filename mismatch: Expected '{safe_symbol}' but found '{filename_symbol}'"
            print(f"ERROR: {error_msg}")
            return jsonify({"success": False, "message": error_msg}), 400
        
        # Convert date column to datetime - preserve CSV timestamp exactly as-is
        # CSV timestamps are already correct IST times, so we just parse and format them
        df['date'] = pd.to_datetime(df['date'])
        # Don't apply timezone conversion - CSV timestamps are already correct IST times
        # Just format them with IST timezone indicator for frontend
        
        # Sort by date
        df = df.sort_values('date')
        
        # Convert to format expected by frontend
        # Format as ISO string with IST timezone offset (+05:30) so JavaScript can parse it correctly
        # This preserves the exact CSV timestamp
        timestamps = df['date'].dt.strftime('%Y-%m-%dT%H:%M:%S+05:30').tolist()
        iv_values = df['iv'].fillna(0).tolist()
        close_prices = df['close'].tolist() if 'close' in df.columns else []
        fclose_prices = df['fclose'].tolist() if 'fclose' in df.columns else []
        
        # Return the original symbol (not sanitized filename) for consistency
        print(f"Successfully loaded CSV data for symbol: {symbol} ({len(timestamps)} data points)")
        
        return jsonify({
            "success": True,
            "timestamps": timestamps,
            "iv_values": iv_values,
            "close_prices": close_prices,
            "fclose_prices": fclose_prices,
            "symbol": symbol,  # Return original symbol, not filename
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

@app.route('/api/get_symbol_settings', methods=['GET'])
def get_symbol_settings():
    """Get symbol settings from CSV file for dropdown"""
    try:
        symbols = load_symbol_settings()
        print(f"[get_symbol_settings] Loaded {len(symbols)} symbols from CSV")
        
        # Generate future symbols and format for frontend
        symbol_list = []
        for sym in symbols:
            future_symbol = generate_future_symbol_from_settings(
                sym['prefix'], 
                sym['symbol'], 
                sym['expiry_date']
            )
            if future_symbol:
                # Format option expiry datetime for datetime-local input (YYYY-MM-DDTHH:mm)
                option_expiry_datetime_str = None
                if sym.get('option_expiry_datetime'):
                    option_expiry_datetime_str = sym['option_expiry_datetime'].strftime('%Y-%m-%dT%H:%M')
                elif sym.get('option_expiry_date'):
                    option_expiry_datetime_str = sym['option_expiry_date'].strftime('%Y-%m-%dT%H:%M')
                
                symbol_list.append({
                    'future_symbol': future_symbol,
                    'prefix': sym['prefix'],
                    'symbol': sym['symbol'],
                    'expiry_date': sym['expiry_date'].strftime('%Y-%m-%d'),
                    'expiry_str': sym['expiry_str'],
                    'strike_step': sym['strike_step'],
                    'option_expiry_date': sym['option_expiry_date'].strftime('%Y-%m-%d') if sym.get('option_expiry_date') else None,
                    'option_expiry_datetime': option_expiry_datetime_str,
                    'option_expiry_time': sym.get('option_expiry_time', '')
                })
                print(f"[get_symbol_settings] Generated: {sym['prefix']}:{sym['symbol']} -> {future_symbol}")
            else:
                print(f"[get_symbol_settings] Failed to generate future symbol for: {sym['prefix']}:{sym['symbol']}")
        
        print(f"[get_symbol_settings] Returning {len(symbol_list)} symbols to frontend")
        return jsonify({"success": True, "symbols": symbol_list, "count": len(symbol_list)})
    except Exception as e:
        error_msg = f"Error loading symbol settings: {e}"
        add_log('ERROR', error_msg, traceback.format_exc())
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/get_logs', methods=['GET'])
def get_logs():
    """Get application logs"""
    try:
        level_filter = request.args.get('level', None)  # Optional filter by level
        limit = request.args.get('limit', 100, type=int)  # Limit number of logs
        
        logs = app_logs.copy()
        
        # Filter by level if specified
        if level_filter:
            logs = [log for log in logs if log['level'] == level_filter.upper()]
        
        # Return most recent logs first, limit the count
        logs = logs[-limit:]
        logs.reverse()  # Most recent first
        
        return jsonify({"success": True, "logs": logs, "total": len(app_logs)})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/clear_logs', methods=['POST'])
def clear_logs():
    """Clear all application logs"""
    global app_logs
    app_logs = []
    add_log('INFO', 'Logs cleared by user')
    return jsonify({"success": True, "message": "Logs cleared"})

@app.route('/api/download_symbols', methods=['POST'])
def download_symbols():
    """Download all Fyers symbols from Symbol Master JSON files
    
    Exchange options:
    - None or 'ALL': Download all exchanges
    - 'NSE': Download all NSE segments (NSE_CM, NSE_FO, NSE_CD, NSE_COM)
    - 'MCX': Download MCX_COM
    - 'BSE': Download all BSE segments (BSE_CM, BSE_FO)
    - Specific segment: 'NSE_CM', 'NSE_FO', 'NSE_CD', 'NSE_COM', 'BSE_CM', 'BSE_FO', 'MCX_COM'
    - List of segments: ['MCX_COM', 'NSE_FO'] - Download multiple specific segments
    """
    try:
        from FyresIntegration import download_fyers_symbols
        
        data = request.json or {}
        exchange = data.get('exchange')  # Optional: 'NSE', 'MCX', 'BSE', specific segment, or list like ['MCX_COM', 'NSE_FO']
        save_path = data.get('save_path', 'fyers_symbols')
        
        exchange_display = exchange if exchange else 'ALL'
        if isinstance(exchange, list):
            exchange_display = ', '.join(exchange)
        
        print(f"Downloading Fyers symbols (exchange: {exchange_display})...")
        add_log('INFO', f'Downloading Fyers symbols', {'exchange': exchange_display, 'save_path': save_path})
        
        downloaded_files = download_fyers_symbols(exchange=exchange, save_path=save_path)
        
        # Count total symbols downloaded
        total_symbols = sum(f.get('symbol_count', 0) for f in downloaded_files.values() if f.get('file_path'))
        
        return jsonify({
            "success": True,
            "message": f"Downloaded symbols from {len([f for f in downloaded_files.values() if f.get('file_path')])} exchange(s)",
            "files": downloaded_files,
            "total_symbols": total_symbols
        })
    except Exception as e:
        error_msg = f"Error downloading symbols: {str(e)}"
        print(error_msg)
        add_log('ERROR', error_msg, {'error': str(e)})
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": error_msg}), 500

@app.route('/api/search_symbols', methods=['GET'])
def search_symbols_endpoint():
    """Search for symbols in downloaded Symbol Master files"""
    try:
        from FyresIntegration import search_symbols
        
        query = request.args.get('query', '')
        exchange = request.args.get('exchange')  # Optional exchange filter
        symbols_dir = request.args.get('symbols_dir', 'fyers_symbols')
        
        if not query:
            return jsonify({"success": False, "message": "Query parameter is required"}), 400
        
        print(f"Searching symbols: query='{query}', exchange={exchange or 'ALL'}")
        
        results = search_symbols(query=query, exchange=exchange, symbols_dir=symbols_dir)
        
        return jsonify({
            "success": True,
            "query": query,
            "exchange": exchange,
            "results": results,
            "count": len(results)
        })
    except Exception as e:
        error_msg = f"Error searching symbols: {str(e)}"
        print(error_msg)
        add_log('ERROR', error_msg, {'error': str(e), 'query': query})
        return jsonify({"success": False, "message": error_msg}), 500

@app.route('/api/list_symbol_files', methods=['GET'])
def list_symbol_files():
    """List all downloaded symbol CSV files"""
    try:
        import os
        
        symbols_dir = request.args.get('symbols_dir', 'fyers_symbols')
        
        if not os.path.exists(symbols_dir):
            return jsonify({
                "success": True,
                "message": "Symbols directory does not exist. Download symbols first.",
                "files": [],
                "count": 0,
                "master_file": None
            })
        
        csv_files = [f for f in os.listdir(symbols_dir) if f.endswith('_symbols.csv')]
        
        file_info = []
        master_file_info = None
        
        for csv_file in csv_files:
            file_path = os.path.join(symbols_dir, csv_file)
            
            # Check if it's the master file
            if csv_file == 'fyers_master_symbols.csv':
                exchange = 'MASTER'
                is_master = True
            else:
                exchange = csv_file.replace('_symbols.csv', '')
                is_master = False
            
            # Count symbols in file
            symbol_count = 0
            try:
                import csv
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    symbol_count = sum(1 for row in reader) - 1  # Exclude header
            except:
                pass
            
            file_data = {
                'filename': csv_file,
                'exchange': exchange,
                'file_path': file_path,
                'symbol_count': symbol_count,
                'is_master': is_master
            }
            
            if is_master:
                master_file_info = file_data
            else:
                file_info.append(file_data)
        
        return jsonify({
            "success": True,
            "files": file_info,
            "count": len(file_info),
            "total_symbols": sum(f['symbol_count'] for f in file_info),
            "master_file": master_file_info
        })
    except Exception as e:
        error_msg = f"Error listing symbol files: {str(e)}"
        print(error_msg)
        return jsonify({"success": False, "message": error_msg}), 500

@app.route('/api/create_master_symbols', methods=['POST'])
def create_master_symbols():
    """Create or update the master symbols CSV file"""
    try:
        from FyresIntegration import create_master_symbols_csv
        
        data = request.json or {}
        symbols_dir = data.get('symbols_dir', 'fyers_symbols')
        master_file = data.get('master_file', 'fyers_master_symbols.csv')
        
        print(f"Creating master symbols file from {symbols_dir}...")
        add_log('INFO', 'Creating master symbols CSV file', {'symbols_dir': symbols_dir, 'master_file': master_file})
        
        master_path = create_master_symbols_csv(symbols_dir=symbols_dir, master_file=master_file)
        
        if master_path:
            # Count symbols in master file
            import csv
            symbol_count = 0
            try:
                with open(master_path, 'r', encoding='utf-8') as f:
                    reader = csv.reader(f)
                    symbol_count = sum(1 for row in reader) - 1  # Exclude header
            except:
                pass
            
            return jsonify({
                "success": True,
                "message": f"Master symbols file created successfully",
                "master_file": master_path,
                "symbol_count": symbol_count
            })
        else:
            return jsonify({
                "success": False,
                "message": "Failed to create master symbols file. Check if symbol files exist."
            }), 500
            
    except Exception as e:
        error_msg = f"Error creating master symbols file: {str(e)}"
        print(error_msg)
        add_log('ERROR', error_msg, {'error': str(e)})
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "message": error_msg}), 500

if __name__ == '__main__':
    import webbrowser
    import threading
    import sys
    
    # Prevent Cursor from auto-detecting and opening preview
    # Set environment variable to disable auto-preview
    os.environ['BROWSER'] = 'none'  # Disable auto-browser in some IDEs
    
    # Open browser after a short delay to allow server to start
    def open_browser():
        time.sleep(1.5)  # Wait for server to start
        try:
            # Open in default system browser
            webbrowser.open('http://127.0.0.1:3000')
        except Exception as e:
            print(f"Could not open browser automatically: {e}")
            print("Please manually open: http://127.0.0.1:3000")
    
    # Start browser in a separate thread
    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()
    
    print("\n" + "="*60)
    print("IV Charts Web Application")
    print("="*60)
    print(f"Server starting on http://127.0.0.1:3000")
    print(f"Opening in your default browser...")
    print("\nTo disable Cursor's auto-preview:")
    print("1. Go to Cursor Settings (Ctrl+,)")
    print("2. Search for 'preview' or 'browser'")
    print("3. Disable 'Auto Open Preview' or similar setting")
    print("="*60 + "\n")
    
    # Run with use_reloader=False to prevent multiple browser opens
    # and to reduce Cursor's auto-detection
    app.run(debug=True, host='127.0.0.1', port=3000, use_reloader=False)

