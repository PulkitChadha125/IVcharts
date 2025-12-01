"""
Utility functions for IV Charts application
Helper functions extracted from main.py for better code organization
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os
import csv
import re
from FyresIntegration import automated_login, fetchOHLC, fyres_quote
import FyresIntegration

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

# Constants
DATA_FOLDER = 'data'

# Market hours configuration
# NSE: 9:15 AM to 3:30 PM IST
# MCX: 9:00 AM to 11:30 PM IST (23:30)
MARKET_HOURS = {
    'NSE': {'open': (9, 15), 'close': (15, 30)},  # 9:15 AM to 3:30 PM IST
    'MCX': {'open': (9, 0), 'close': (23, 30)}     # 9:00 AM to 11:30 PM IST
}

# Create data folder if it doesn't exist
if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)
    print(f"Created data folder: {DATA_FOLDER}")


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


def cleanup_csv_files(cleanup_on_start=False):
    """Delete all CSV files from data folder for fresh start (optional)"""
    if not cleanup_on_start:
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


def delete_csv_files(symbol=None, add_log_func=None):
    """
    Delete CSV files from data folder
    If symbol is provided, delete only that symbol's CSV file
    If symbol is None, delete all CSV files
    
    Args:
        symbol: Optional symbol name to delete specific CSV file
        add_log_func: Optional logging function to call
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
        if deleted_count > 0 and add_log_func:
            add_log_func('INFO', f'Deleted {deleted_count} CSV file(s)', {'deleted_count': deleted_count, 'symbol': symbol if symbol else 'all'})
        return deleted_count
    except Exception as e:
        error_msg = f"Error deleting CSV files: {e}"
        print(error_msg)
        if add_log_func:
            add_log_func('ERROR', error_msg, {'error': str(e), 'symbol': symbol if symbol else 'all'})
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
    Returns list of dicts with: prefix, symbol, expiry_date, strike_step
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
                    
                    if not prefix or not symbol or not expiry_str:
                        continue
                    
                    # Parse expiry date (format: DD-MM-YYYY)
                    try:
                        expiry_date = datetime.strptime(expiry_str, '%d-%m-%Y')
                    except ValueError:
                        # Try alternative format
                        try:
                            expiry_date = datetime.strptime(expiry_str, '%Y-%m-%d')
                        except ValueError:
                            print(f"Could not parse expiry date: {expiry_str}")
                            continue
                    
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
                        'expiry_str': expiry_str
                    })
                except Exception as e:
                    print(f"Error parsing symbol row: {row}, Error: {e}")
                    continue
        
        return symbols
    except FileNotFoundError:
        print("SymbolSetting.csv not found. Creating default file...")
        # Create default file
        default_symbols = [
            {'prefix': 'NSE', 'symbol': 'NIFTY', 'expiry_date': datetime(2025, 11, 25), 'strike_step': 50, 'expiry_str': '25-11-2025'}
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


def get_option_price_from_fyers(option_symbol, add_log_func=None):
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
        if add_log_func:
            add_log_func('ERROR', error_msg, {'symbol': option_symbol, 'error': str(e)})
        return None


def get_underlying_price_from_fyers(underlying_symbol, add_log_func=None):
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
        if add_log_func:
            add_log_func('ERROR', error_msg, {'symbol': underlying_symbol, 'error': str(e)})
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
        mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
        underlying_upper = underlying.upper()
        
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


def get_future_ltp(future_symbol, add_log_func=None):
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
        if add_log_func:
            add_log_func('WARNING', error_msg, {'symbol': future_symbol, 'error': str(e)})
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
                # MCX monthly format: MCX:CRUDEOIL25DEC5300CE
                # Remove month code from underlying (e.g., CRUDEOILM -> CRUDEOIL)
                # MCX futures have month code (CRUDEOILM), but options don't use it
                underlying_clean = underlying
                
                # List of MCX commodities (base names without month codes)
                mcx_commodities = ['CRUDEOIL', 'GOLD', 'SILVER', 'COPPER', 'ZINC', 'LEAD', 'NICKEL', 'ALUMINIUM', 'NATURALGAS']
                
                # Remove month code from underlying if it exists
                # Check if underlying starts with any commodity name
                for commodity in mcx_commodities:
                    if underlying_clean.upper().startswith(commodity):
                        # If underlying is longer than commodity, it likely has a month code suffix
                        if len(underlying_clean) > len(commodity):
                            # Use just the commodity name (remove month code)
                            underlying_clean = commodity
                        else:
                            # Already clean, use as is
                            underlying_clean = commodity
                        break
                
                # Extract month abbreviation (3 letters: JAN, FEB, MAR, etc.)
                month_abbr = expiry_date.strftime('%b').upper()  # e.g., "DEC"
                
                # Format: MCX:COMMODITY + year + month_abbr + strike + option_suffix
                # e.g., MCX:CRUDEOIL25DEC5300CE
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


def safe_fetch_ohlc(symbol, timeframe, add_log_func=None):
    """
    Safely fetch OHLC data with proper error handling
    """
    try:
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
        if add_log_func:
            add_log_func('ERROR', error_msg, {'symbol': symbol, 'error': str(e), 'error_type': 'KeyError'})
        return None
    except Exception as e:
        error_msg = f"Error fetching OHLC data for {symbol}"
        print(f"❌ {error_msg}: {e}")
        print(f"Error type: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        if add_log_func:
            add_log_func('ERROR', error_msg, {'symbol': symbol, 'error': str(e), 'error_type': type(e).__name__})
        return None

