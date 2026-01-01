from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
import webbrowser
from datetime import datetime, timedelta, date
from time import sleep
import os
import pyotp
import requests
import json
import math
import pytz
from urllib.parse import parse_qs, urlparse
import warnings
import pandas as pd
access_token=None
fyers=None
shared_data = {}
shared_data_2 = {}
# Lock to ensure thread-safe access to the shared data
def apiactivation(client_id, redirect_uri, response_type, state, secret_key, grant_type):
    from fyers_apiv3 import fyersModel
    import webbrowser

    appSession = fyersModel.SessionModel(
        client_id=client_id,
        redirect_uri=redirect_uri,
        response_type=response_type,
        state=state,
        secret_key=secret_key,
        grant_type=grant_type
    )

    try:
        generateTokenUrl = appSession.generate_authcode()
        print("generateTokenUrl:", generateTokenUrl)

        # If it's a full URL, open browser (manual login)
        if generateTokenUrl.startswith("https://"):
            print("Opening browser for manual login...")
            webbrowser.open(generateTokenUrl, new=1)
            return generateTokenUrl

        # Else, assume it's an auth code directly
        elif isinstance(generateTokenUrl, dict) and "data" in generateTokenUrl and "auth" in generateTokenUrl["data"]:
            print("Auth code obtained directly:", generateTokenUrl["data"]["auth"])
            return generateTokenUrl["data"]["auth"]

        else:
            print("Unexpected response format:", generateTokenUrl)
            return None

    except Exception as e:
        print("Error during auth code generation:", e)
        return None


def automated_login(client_id,secret_key,FY_ID,TOTP_KEY,PIN,redirect_uri):

    pd.set_option('display.max_columns', None)
    warnings.filterwarnings('ignore')

    import base64


    def getEncodedString(string):
        string = str(string)
        base64_bytes = base64.b64encode(string.encode("ascii"))
        return base64_bytes.decode("ascii")

    global fyers,access_token

    URL_SEND_LOGIN_OTP = "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2"
    response = requests.post(url=URL_SEND_LOGIN_OTP, json={"fy_id": getEncodedString(FY_ID), "app_id": "2"})
    print("Status code:", response.status_code)
    print("Raw text:", response.text)
    res = response.json()

    if datetime.now().second % 30 > 27: sleep(5)
    URL_VERIFY_OTP = "https://api-t2.fyers.in/vagator/v2/verify_otp"
    otp_response = requests.post(url=URL_VERIFY_OTP,
                         json={"request_key": res["request_key"], "otp": pyotp.TOTP(TOTP_KEY).now()})
    
    if otp_response.status_code != 200:
        raise Exception(f"Failed to verify OTP. Status: {otp_response.status_code}, Response: {otp_response.text}")
    
    res2 = otp_response.json()
    print("OTP verification response:", res2)
    
    if 'request_key' not in res2:
        raise Exception(f"Missing 'request_key' in OTP verification response: {res2}")

    ses = requests.Session()
    URL_VERIFY_OTP2 = "https://api-t2.fyers.in/vagator/v2/verify_pin_v2"
    payload2 = {"request_key": res2["request_key"], "identity_type": "pin", "identifier": getEncodedString(PIN)}
    pin_response = ses.post(url=URL_VERIFY_OTP2, json=payload2)
    
    if pin_response.status_code != 200:
        raise Exception(f"Failed to verify PIN. Status: {pin_response.status_code}, Response: {pin_response.text}")
    
    res3 = pin_response.json()
    print("PIN verification response:", res3)
    
    if 'data' not in res3 or 'access_token' not in res3['data']:
        raise Exception(f"Missing 'access_token' in PIN verification response: {res3}")

    ses.headers.update({
        'authorization': f"Bearer {res3['data']['access_token']}"
    })

    TOKENURL = "https://api-t1.fyers.in/api/v3/token"
    payload3 = {"fyers_id": FY_ID,
                "app_id": client_id[:-4],
                "redirect_uri": redirect_uri,
                "appType": "100", "code_challenge": "",
                "state": "None", "scope": "", "nonce": "", "response_type": "code", "create_cookie": True}

    token_response = ses.post(url=TOKENURL, json=payload3, allow_redirects=False)
    
    # Handle both 200 (OK) and 308 (Permanent Redirect) status codes
    # 308 is a redirect, but the response body still contains the JSON with the URL
    if token_response.status_code not in [200, 308]:
        raise Exception(f"Failed to get token URL. Status: {token_response.status_code}, Response: {token_response.text}")
    
    # Parse JSON response (works for both 200 and 308)
    try:
        res3 = token_response.json()
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON response from token URL request: {token_response.text}")
    
    print("Token URL response:", res3)
    
    if 'Url' not in res3:
        raise Exception(f"Missing 'Url' in token response: {res3}")
    
    url = res3['Url']
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    
    if 'auth_code' not in query_params or len(query_params['auth_code']) == 0:
        raise Exception(f"Missing 'auth_code' in URL: {url}")
    
    auth_code = query_params['auth_code'][0]
    grant_type = "authorization_code"

    response_type = "code"

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type=response_type,
        grant_type=grant_type
    )
    session.set_token(auth_code)
    response = session.generate_token()
    
    # Debug: Print the actual response structure
    print(f"generate_token() response type: {type(response)}")
    print(f"generate_token() response: {response}")
    
    # Check if response is valid and contains access_token
    if not response:
        raise Exception("generate_token() returned None or empty response")
    
    # Check response structure - access_token might be nested or at root level
    if isinstance(response, dict):
        if 'access_token' in response:
            access_token = response['access_token']
        elif 'data' in response and isinstance(response['data'], dict) and 'access_token' in response['data']:
            access_token = response['data']['access_token']
        elif 's' in response and response.get('s') == 'ok' and 'access_token' in response:
            access_token = response['access_token']
        else:
            # Print the actual response structure for debugging
            print(f"Unexpected response structure from generate_token(): {response}")
            print(f"Response keys: {list(response.keys()) if isinstance(response, dict) else 'not a dict'}")
            raise KeyError(f"access_token not found in response. Response keys: {list(response.keys()) if isinstance(response, dict) else 'not a dict'}")
    else:
        raise Exception(f"Unexpected response type from generate_token(): {type(response)}")
    
    print("access_token: ",access_token)
    fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path=os.getcwd())
    
    # Verify fyers object was created successfully
    if fyers is None:
        raise Exception("Failed to create FyersModel object")
    
    # Test the connection by getting profile
    try:
        profile = fyers.get_profile()
        print("Profile retrieved successfully:", profile)
    except Exception as e:
        print(f"Warning: Could not get profile after login: {e}")
        # Don't raise - fyers object might still be valid
    
    print("automated_login completed successfully")

def get_ltp(SYMBOL):
    global fyers
    data={"symbols":f"{SYMBOL}"}
    res=fyers.quotes(data)
    if 'd' in res and len(res['d']) > 0:
        lp = res['d'][0]['v']['lp']
        return lp

    else:
        print("Last Price (lp) not found in the response.")




def get_position():
    global fyers
      ## This will provide all the trade related information
    res=fyers.positions()
    return res

def get_orderbook():
    global fyers
    res = fyers.orderbook()
    return res
      ## This will provide the user with all the order realted information

def get_tradebook():
    global fyers
    res = fyers.tradebook()
    return res


def fetchOHLC_Scanner(symbol):
    dat =str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(5)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat ,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)

    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date']=df['date'].apply(pd.Timestamp,unit='s',tzinfo=pytz.timezone('Asia/Kolkata'))
    return df.tail(5)

def fetchOHLC_Weekly(symbol):
    from datetime import datetime, timedelta
    import pandas as pd
    import numpy as np

    # Extended range for full candle history
    today = datetime.now()
    dat = str((today + timedelta(days=1)).date())
    dat1 = str((today - timedelta(days=160)).date())

    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }

    response = fyers.history(data=data)

    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)

    # Convert timestamp to datetime in IST
    df['date'] = pd.to_datetime(df['date'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
    df.set_index('date', inplace=True)


    # ============ Weekly OHLC ============
    df_weekly = df.resample('W-FRI').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()

    # ============ Monthly OHLC with actual last available dates ============

    df['year'] = df.index.year
    df['month'] = df.index.month

    # Group by (year, month)
    grouped = df.groupby(['year', 'month'])

    records = []
    index_dates = []

    for (y, m), group in grouped:
        open_price = group['open'].iloc[0]
        high_price = group['high'].max()
        low_price = group['low'].min()
        close_price = group['close'].iloc[-1]
        volume_sum = group['volume'].sum()

        # Use the actual last trading day in the group as index
        last_date = group.index[-1]
        index_dates.append(last_date)

        records.append([open_price, high_price, low_price, close_price, volume_sum])

    df_monthly = pd.DataFrame(records, columns=['open', 'high', 'low', 'close', 'volume'], index=index_dates)

    # Ensure index is sorted
    df_monthly.sort_index(inplace=True)



    return df_weekly, df_monthly





# def fetchOHLC_Weekly(symbol):
#     # Approx 140 days for 20 weeks of daily data
#     dat = str(datetime.now().date())
#     dat1 = str((datetime.now() - timedelta(days=140)).date())

#     data = {
#         "symbol": symbol,
#         "resolution": "1D",
#         "date_format": "1",
#         "range_from": dat1,
#         "range_to": dat,
#         "cont_flag": "1"
#     }

#     response = fyers.history(data=data)
#     # print("response weekly:", response)

#     cl = ['date', 'open', 'high', 'low', 'close', 'volume']
#     df = pd.DataFrame(response['candles'], columns=cl)

#     # Convert Unix timestamp to datetime in IST
#     df['date'] = pd.to_datetime(df['date'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
#     df.set_index('date', inplace=True)

#     # Resample to weekly candles, week ending on Friday
#     df_weekly = df.resample('W-FRI').agg({
#         'open': 'first',
#         'high': 'max',
#         'low': 'min',
#         'close': 'last',
#         'volume': 'sum'
#     })

#     # Drop incomplete weeks
#     df_weekly.dropna(inplace=True)

#     return df_weekly  # Return last 20 weeks

def fetchOHLC(symbol,tf):
    # Ensure symbol is a string and strip any whitespace
    symbol = str(symbol).strip() if symbol else None
    if not symbol:
        print(f"❌ ERROR: Invalid symbol provided to fetchOHLC: {symbol}")
        return None
    
    # Debug: Print the symbol being sent to API
    print(f"DEBUG fetchOHLC: Symbol received: '{symbol}' (type: {type(symbol)}, length: {len(symbol)})")
    
    dat =str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(90)).date())
    
    # Ensure symbol is clean before creating data dict
    clean_symbol = str(symbol).strip()
    
    # Debug: Verify symbol before creating data dict
    print(f"DEBUG fetchOHLC: Clean symbol before data dict: '{clean_symbol}' (length: {len(clean_symbol)})")
    
    data = {
        "symbol": clean_symbol,  # Use the cleaned symbol
        "resolution":str(tf),
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }

    print("data: ",data)
    
    # Debug: Verify symbol in data dict after creation
    data_symbol = str(data.get('symbol', '')).strip()
    print(f"DEBUG fetchOHLC: Symbol in data dict: '{data_symbol}' (length: {len(data_symbol)})")
    
    # Verify the symbol is correct (should be exactly 24 characters for MCX:SILVERM26JAN233000CE)
    if len(data_symbol) != 24:
        print(f"⚠️ WARNING: Symbol length is {len(data_symbol)}, expected 24. Symbol: '{data_symbol}'")
    
    try:
        # Store the original symbol for error reporting (use the cleaned one)
        # Keep a local copy to avoid any variable modification issues
        symbol_for_logging = str(clean_symbol).strip()
        
        # Debug: Final verification before API call
        print(f"DEBUG fetchOHLC: About to call API with symbol: '{symbol_for_logging}'")
        
        response = fyers.history(data=data)
        
        # Check response structure (minimal logging)
        if isinstance(response, dict):
            if 's' in response:
                status = response['s']
                if status != 'ok':
                    print(f"⚠️ API Status: {status}")
            if 'candles' in response:
                candle_count = len(response['candles']) if isinstance(response['candles'], list) else 0
                print(f"✓ Fetched {candle_count} candles for {symbol_for_logging}")
            else:
                # Always use the stored symbol for error reporting to avoid corruption
                print(f"⚠️ WARNING: 'candles' key not found in response for {symbol_for_logging}")
                print(f"Response keys: {list(response.keys())}")
                # Debug: Check if response contains symbol info
                if 'message' in response:
                    print(f"DEBUG fetchOHLC: API error message: {response.get('message')}")
                if 'symbol' in response:
                    print(f"DEBUG fetchOHLC: Response contains symbol: {response.get('symbol')}")
        else:
            print(f"⚠️ WARNING: Response is not a dict, type: {type(response)}")
        
        # Check if response has error
        if isinstance(response, dict) and response.get('s') != 'ok':
            error_msg = response.get('message', 'Unknown error')
            print(f"❌ API Error: {error_msg}")
            print(f"Response: {response}")
            return None
        
        # Check if candles exist
        if 'candles' not in response:
            print("❌ ERROR: 'candles' key missing in response")
            return None
        
        if not isinstance(response['candles'], list):
            print(f"❌ ERROR: 'candles' is not a list, type: {type(response['candles'])}")
            return None
        
        if len(response['candles']) == 0:
            print("⚠️ WARNING: Empty candles list returned")
            return pd.DataFrame()  # Return empty DataFrame
        
        cl = ['date', 'open', 'high', 'low', 'close', 'volume']
        df = pd.DataFrame(response['candles'], columns=cl)
        df['date']=df['date'].apply(pd.Timestamp,unit='s',tzinfo=pytz.timezone('Asia/Kolkata'))
        
        return df
        
    except Exception as e:
        print(f"\n❌ EXCEPTION in fetchOHLC:")
        print(f"Error type: {type(e).__name__}")
        print(f"Error message: {str(e)}")
        import traceback
        traceback.print_exc()
        print("="*80 + "\n")
        raise


def fetchOHLC_get_selected_price(symbol, date):

    print("option symbol :",symbol)
    print("option symbol date :", date)
    dat = str(datetime.now().date())
    dat1 = str((datetime.now() - timedelta(25)).date())
    data = {
        "symbol": symbol,
        "resolution": "1D",
        "date_format": "1",
        "range_from": dat1,
        "range_to": dat,
        "cont_flag": "1"
    }
    response = fyers.history(data=data)
    cl = ['date', 'open', 'high', 'low', 'close', 'volume']
    df = pd.DataFrame(response['candles'], columns=cl)
    df['date'] = pd.to_datetime(df['date'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata').dt.date
    target_date = pd.to_datetime(date).date()
    matching_row = df[df['date'] == target_date]
    if matching_row.empty:
        return 0
    else:
        close_price = matching_row.iloc[0]['close']
        return close_price
    



def fyres_websocket(symbollist):
    from fyers_apiv3.FyersWebsocket import data_ws
    global access_token

    def onmessage(message):
        """
        Callback function to handle incoming messages from the FyersDataSocket WebSocket.

        Parameters:
            message (dict): The received message from the WebSocket.

        """
        # print("Response:", message)
        if 'symbol' in message and 'ltp' in message:
            shared_data[message['symbol']] = message['ltp']
            # print("shared_data: ",shared_data)




    def onerror(message):
        """
        Callback function to handle WebSocket errors.

        Parameters:
            message (dict): The error message received from the WebSocket.


        """
        print("Error:", message)


    def onclose(message):
        """
        Callback function to handle WebSocket connection close events.
        """
        print("Connection closed:", message)


    def onopen():
        """
        Callback function to subscribe to data type and symbols upon WebSocket connection.

        """
        # Specify the data type and symbols you want to subscribe to
        data_type = "SymbolUpdate"

        # Subscribe to the specified symbols and data type
        symbols = symbollist
        # ['NSE:LTIM24JULFUT', 'NSE:BHARTIARTL24JULFUT']
        fyers.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        fyers.keep_running()


    # Replace the sample access token with your actual access token obtained from Fyers
    # access_token = "XC4XXXXXXM-100:eXXXXXXXXXXXXfZNSBoLo"

    # Create a FyersDataSocket instance with the provided parameters
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,  # Access token in the format "appid:accesstoken"
        log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=True,  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,  # Save response in a log file instead of printing it.
        reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=onopen,  # Callback function to subscribe to data upon connection.
        on_close=onclose,  # Callback function to handle WebSocket connection close events.
        on_error=onerror,  # Callback function to handle WebSocket errors.
        on_message=onmessage  # Callback function to handle incoming messages from the WebSocket.
    )

    # Establish a connection to the Fyers WebSocket
    fyers.connect()

def fyres_quote(symbol):
    data = {
        "symbols": f"{symbol}"
    }

    response = fyers.quotes(data=data)
    return response

def download_fyers_symbols(exchange=None, save_path="fyers_symbols"):
    """
    Download all tradable symbols from Fyers Symbol Master JSON files
    
    Parameters:
    - exchange: Optional exchange filter. Can be:
                - None: downloads all exchanges
                - String: single exchange (e.g., 'NSE_CM', 'NSE_FO', 'NSE_CD', 'NSE_COM', 
                  'BSE_CM', 'BSE_FO', 'MCX_COM')
                - List: multiple exchanges (e.g., ['MCX_COM', 'NSE_FO'])
                - Base exchange: 'NSE', 'MCX', 'BSE' (downloads all segments for that exchange)
    - save_path: Directory path to save the CSV files (default: "fyers_symbols")
    
    Returns:
    - Dictionary with exchange names as keys and file paths as values
    """
    import os
    import csv
    import json
    
    # Fyers Symbol Master JSON file URLs
    symbol_master_urls = {
        'NSE_CD': {
            'url': 'https://public.fyers.in/sym_details/NSE_CD_sym_master.json',
            'name': 'NSE Currency Derivatives',
            'exchange': 'NSE'
        },
        'NSE_FO': {
            'url': 'https://public.fyers.in/sym_details/NSE_FO_sym_master.json',
            'name': 'NSE Equity Derivatives',
            'exchange': 'NSE'
        },
        'NSE_COM': {
            'url': 'https://public.fyers.in/sym_details/NSE_COM_sym_master.json',
            'name': 'NSE Commodity',
            'exchange': 'NSE'
        },
        'NSE_CM': {
            'url': 'https://public.fyers.in/sym_details/NSE_CM_sym_master.json',
            'name': 'NSE Capital Market',
            'exchange': 'NSE'
        },
        'BSE_CM': {
            'url': 'https://public.fyers.in/sym_details/BSE_CM_sym_master.json',
            'name': 'BSE Capital Market',
            'exchange': 'BSE'
        },
        'BSE_FO': {
            'url': 'https://public.fyers.in/sym_details/BSE_FO_sym_master.json',
            'name': 'BSE Equity Derivatives',
            'exchange': 'BSE'
        },
        'MCX_COM': {
            'url': 'https://public.fyers.in/sym_details/MCX_COM_sym_master.json',
            'name': 'MCX Commodity',
            'exchange': 'MCX'
        }
    }
    
    # Create save directory if it doesn't exist
    if not os.path.exists(save_path):
        os.makedirs(save_path)
        print(f"Created directory: {save_path}")
    
    downloaded_files = {}
    
    # Filter exchanges if specified
    if exchange:
        # Handle list of exchanges
        if isinstance(exchange, list):
            exchanges_to_download = [e for e in exchange if e in symbol_master_urls]
        # Support both old format (NSE, MCX) and new format (NSE_CM, NSE_FO, etc.)
        elif exchange in symbol_master_urls:
            exchanges_to_download = [exchange]
        elif exchange == 'NSE':
            # Download all NSE segments
            exchanges_to_download = [k for k in symbol_master_urls.keys() if k.startswith('NSE_')]
        elif exchange == 'MCX':
            exchanges_to_download = [k for k in symbol_master_urls.keys() if k.startswith('MCX_')]
        elif exchange == 'BSE':
            exchanges_to_download = [k for k in symbol_master_urls.keys() if k.startswith('BSE_')]
        else:
            exchanges_to_download = [exchange] if exchange in symbol_master_urls else []
    else:
        exchanges_to_download = list(symbol_master_urls.keys())
    
    for exch_key in exchanges_to_download:
        if exch_key not in symbol_master_urls:
            print(f"Warning: Exchange '{exch_key}' not found in available exchanges")
            continue
        
        exch_info = symbol_master_urls[exch_key]
        url = exch_info['url']
        file_path = os.path.join(save_path, f"{exch_key}_symbols.csv")
        
        try:
            print(f"Downloading {exch_info['name']} symbols from {url}...")
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse JSON response
            json_data = response.json()
            
            # Convert JSON to CSV
            if isinstance(json_data, dict):
                # If JSON is a dict, check if it has a list of symbols
                if 'symbols' in json_data:
                    symbols_list = json_data['symbols']
                elif 'data' in json_data:
                    symbols_list = json_data['data']
                else:
                    # Try to find the first list value
                    symbols_list = None
                    for key, value in json_data.items():
                        if isinstance(value, list) and len(value) > 0:
                            symbols_list = value
                            break
                    
                    if symbols_list is None:
                        # If no list found, treat the dict itself as a single symbol
                        symbols_list = [json_data]
            elif isinstance(json_data, list):
                symbols_list = json_data
            else:
                raise ValueError(f"Unexpected JSON format: {type(json_data)}")
            
            if not symbols_list or len(symbols_list) == 0:
                print(f"  ⚠ No symbols found in {exch_key}")
                downloaded_files[exch_key] = {
                    'file_path': None,
                    'symbol_count': 0,
                    'url': url,
                    'error': 'No symbols found in JSON'
                }
                continue
            
            # Get all unique keys from all symbols to create CSV columns
            all_keys = set()
            for symbol in symbols_list:
                if isinstance(symbol, dict):
                    all_keys.update(symbol.keys())
            
            # Ensure common columns are first
            priority_keys = ['Fytoken', 'fytoken', 'Symbol', 'symbol', 'Exch', 'exch', 
                           'InstrumentType', 'instrument_type', 'LotSize', 'lot_size',
                           'TickSize', 'tick_size', 'ISIN', 'isin']
            ordered_keys = []
            
            # Add priority keys first
            for key in priority_keys:
                if key in all_keys:
                    ordered_keys.append(key)
                    all_keys.remove(key)
            
            # Add remaining keys alphabetically
            ordered_keys.extend(sorted(all_keys))
            
            # Add Exchange column
            if 'Exchange' not in ordered_keys:
                ordered_keys.insert(0, 'Exchange')
            
            # Write to CSV
            with open(file_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=ordered_keys, extrasaction='ignore')
                writer.writeheader()
                
                for symbol in symbols_list:
                    if isinstance(symbol, dict):
                        # Add Exchange column
                        symbol['Exchange'] = exch_info['exchange']
                        writer.writerow(symbol)
            
            symbol_count = len(symbols_list)
            
            downloaded_files[exch_key] = {
                'file_path': file_path,
                'symbol_count': symbol_count,
                'url': url,
                'name': exch_info['name']
            }
            
            print(f"✓ Downloaded {exch_info['name']}: {symbol_count:,} symbols saved to {file_path}")
            
        except requests.exceptions.RequestException as e:
            print(f"✗ Error downloading {exch_info['name']} symbols: {e}")
            downloaded_files[exch_key] = {
                'file_path': None,
                'error': str(e),
                'url': url,
                'name': exch_info['name']
            }
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing JSON for {exch_info['name']}: {e}")
            downloaded_files[exch_key] = {
                'file_path': None,
                'error': f"JSON parse error: {str(e)}",
                'url': url,
                'name': exch_info['name']
            }
        except Exception as e:
            print(f"✗ Unexpected error downloading {exch_info['name']} symbols: {e}")
            import traceback
            traceback.print_exc()
            downloaded_files[exch_key] = {
                'file_path': None,
                'error': str(e),
                'url': url,
                'name': exch_info['name']
            }
    
    # Create master CSV file after downloading (if at least one file was downloaded)
    successful_downloads = [f for f in downloaded_files.values() if f.get('file_path')]
    if successful_downloads:
        print("\nCreating master symbols CSV file...")
        master_path = create_master_symbols_csv(save_path, "fyers_master_symbols.csv")
        if master_path:
            downloaded_files['_master'] = {
                'file_path': master_path,
                'symbol_count': sum(f.get('symbol_count', 0) for f in successful_downloads),
                'type': 'master_consolidated'
            }
    
    return downloaded_files

def create_master_symbols_csv(symbols_dir="fyers_symbols", master_file="fyers_master_symbols.csv"):
    """
    Create a consolidated master CSV file from all downloaded symbol files
    
    Parameters:
    - symbols_dir: Directory where individual exchange symbol CSV files are stored
    - master_file: Output filename for the master CSV file
    
    Returns:
    - Path to the created master CSV file, or None if failed
    """
    import os
    import csv
    from datetime import datetime
    
    if not os.path.exists(symbols_dir):
        print(f"Symbols directory '{symbols_dir}' does not exist")
        return None
    
    # Find all symbol CSV files (exclude master file itself)
    symbol_files = [f for f in os.listdir(symbols_dir) 
                    if f.endswith('_symbols.csv') and f != master_file]
    
    if not symbol_files:
        print(f"No symbol files found in '{symbols_dir}'")
        return None
    
    master_path = os.path.join(symbols_dir, master_file)
    all_symbols = []
    exchange_counts = {}
    
    print(f"Creating master symbols file from {len(symbol_files)} exchange(s)...")
    
    # Read all symbol files and combine them
    for symbol_file in symbol_files:
        exchange_segment = symbol_file.replace('_symbols.csv', '')
        file_path = os.path.join(symbols_dir, symbol_file)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                
                # Add exchange segment column to each row if not present
                for row in rows:
                    # Exchange column should already exist from download, but add ExchangeSegment
                    if 'ExchangeSegment' not in row and 'exchange_segment' not in row:
                        row['ExchangeSegment'] = exchange_segment
                    # Ensure Exchange column exists (base exchange like NSE, MCX, BSE)
                    if 'Exchange' not in row and 'exchange' not in row:
                        # Extract base exchange from segment (NSE_CM -> NSE)
                        base_exchange = exchange_segment.split('_')[0] if '_' in exchange_segment else exchange_segment
                        row['Exchange'] = base_exchange
                    all_symbols.append(row)
                
                exchange_counts[exchange_segment] = len(rows)
                print(f"  ✓ Added {len(rows)} symbols from {exchange_segment}")
                
        except Exception as e:
            print(f"  ✗ Error reading {symbol_file}: {e}")
            continue
    
    if not all_symbols:
        print("No symbols found to consolidate")
        return None
    
    # Get all unique column names from all rows
    all_columns = set()
    for row in all_symbols:
        all_columns.update(row.keys())
    
    # Ensure Exchange and ExchangeSegment are first, then Symbol, then others alphabetically
    priority_columns = ['Exchange', 'ExchangeSegment', 'Symbol', 'symbol', 'Fytoken', 'fytoken']
    ordered_columns = []
    
    # Add priority columns first
    for col in priority_columns:
        if col in all_columns:
            ordered_columns.append(col)
            all_columns.remove(col)
    
    # Add remaining columns alphabetically
    ordered_columns.extend(sorted(all_columns))
    
    # Write master CSV file
    try:
        with open(master_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=ordered_columns, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(all_symbols)
        
        total_symbols = len(all_symbols)
        print(f"✓ Master symbols file created: {master_path}")
        print(f"  Total symbols: {total_symbols:,}")
        print(f"  Exchanges: {', '.join(exchange_counts.keys())}")
        print(f"  Breakdown: {', '.join([f'{exch}: {count:,}' for exch, count in exchange_counts.items()])}")
        
        return master_path
        
    except Exception as e:
        print(f"✗ Error writing master CSV file: {e}")
        import traceback
        traceback.print_exc()
        return None

def search_symbols(query, exchange=None, symbols_dir="fyers_symbols", use_master=True):
    """
    Search for symbols in downloaded Symbol Master files
    
    Parameters:
    - query: Search query (symbol name or part of it)
    - exchange: Optional exchange filter (e.g., 'NSE', 'MCX')
    - symbols_dir: Directory where symbol CSV files are stored
    - use_master: If True, use master file if available (faster). If False, search individual files.
    
    Returns:
    - List of matching symbols with their details
    """
    import os
    import csv
    import re
    
    results = []
    query_lower = query.lower()
    
    # Try to use master file first if available and use_master is True
    master_file = os.path.join(symbols_dir, 'fyers_master_symbols.csv')
    if use_master and os.path.exists(master_file) and not exchange:
        # Use master file for faster search (all exchanges)
        try:
            with open(master_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Search in symbol name (case-insensitive)
                    symbol = row.get('Symbol', row.get('symbol', ''))
                    if query_lower in symbol.lower():
                        results.append(row)
            return results
        except Exception as e:
            print(f"Error reading master file, falling back to individual files: {e}")
    
    # Fallback: search individual exchange files
    if exchange:
        # Support both old format (NSE, MCX) and new format (NSE_CM, NSE_FO, etc.)
        if exchange in ['NSE', 'MCX', 'BSE']:
            # Search all files for that exchange
            csv_files = [os.path.join(symbols_dir, f) for f in os.listdir(symbols_dir) 
                        if f.endswith('_symbols.csv') and f.startswith(f"{exchange}_") 
                        and f != 'fyers_master_symbols.csv'] if os.path.exists(symbols_dir) else []
        else:
            # Specific exchange segment (e.g., NSE_CM, NSE_FO)
            csv_files = [os.path.join(symbols_dir, f"{exchange}_symbols.csv")]
    else:
        csv_files = [os.path.join(symbols_dir, f) for f in os.listdir(symbols_dir) 
                     if f.endswith('_symbols.csv') and f != 'fyers_master_symbols.csv'] if os.path.exists(symbols_dir) else []
    
    for csv_file in csv_files:
        if not os.path.exists(csv_file):
            continue
        
        exchange_name = os.path.basename(csv_file).replace('_symbols.csv', '')
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Search in symbol name (case-insensitive)
                    symbol = row.get('Symbol', row.get('symbol', ''))
                    if query_lower in symbol.lower():
                        # Add exchange segment name if not already present
                        if 'ExchangeSegment' not in row and 'exchange_segment' not in row:
                            row['ExchangeSegment'] = exchange_name
                        # Ensure Exchange column exists (should already be there from download)
                        if 'Exchange' not in row and 'exchange' not in row:
                            # Extract base exchange from segment (NSE_CM -> NSE)
                            base_exchange = exchange_name.split('_')[0] if '_' in exchange_name else exchange_name
                            row['Exchange'] = base_exchange
                        results.append(row)
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
    
    return results





def fyres_websocket_option(symbollist):
    from fyers_apiv3.FyersWebsocket import data_ws
    global access_token

    def onmessage(message):
        """
        Callback function to handle incoming messages from the FyersDataSocket WebSocket.

        Parameters:
            message (dict): The received message from the WebSocket.

        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{timestamp} - {message}\n")
        if 'symbol' in message and 'ltp' in message:
            shared_data_2[message['symbol']] = message['ltp']




    def onerror(message):
        """
        Callback function to handle WebSocket errors.

        Parameters:
            message (dict): The error message received from the WebSocket.


        """
        print("Error:", message)


    def onclose(message):
        """
        Callback function to handle WebSocket connection close events.
        """
        print("Connection closed:", message)


    def onopen():
        """
        Callback function to subscribe to data type and symbols upon WebSocket connection.

        """
        # Specify the data type and symbols you want to subscribe to
        data_type = "SymbolUpdate"

        # Subscribe to the specified symbols and data type
        symbols = symbollist
        # ['NSE:LTIM24JULFUT', 'NSE:BHARTIARTL24JULFUT']
        fyers.subscribe(symbols=symbols, data_type=data_type)

        # Keep the socket running to receive real-time data
        fyers.keep_running()


    # Replace the sample access token with your actual access token obtained from Fyers
    # access_token = "XC4XXXXXXM-100:eXXXXXXXXXXXXfZNSBoLo"

    # Create a FyersDataSocket instance with the provided parameters
    fyers = data_ws.FyersDataSocket(
        access_token=access_token,  # Access token in the format "appid:accesstoken"
        log_path="",  # Path to save logs. Leave empty to auto-create logs in the current directory.
        litemode=True,  # Lite mode disabled. Set to True if you want a lite response.
        write_to_file=False,  # Save response in a log file instead of printing it.
        reconnect=True,  # Enable auto-reconnection to WebSocket on disconnection.
        on_connect=onopen,  # Callback function to subscribe to data upon connection.
        on_close=onclose,  # Callback function to handle WebSocket connection close events.
        on_error=onerror,  # Callback function to handle WebSocket errors.
        on_message=onmessage  # Callback function to handle incoming messages from the WebSocket.
    )

    # Establish a connection to the Fyers WebSocket
    fyers.connect()



def place_order(symbol,quantity,type,side,price):
    # Set quantity to 1 by default if not provided
    if quantity is None or quantity == 0:
        quantity = 1
    quantity = int(quantity)
    price = float(price)
    
    # Keep type as integer (1=Limit, 2=Market)
    order_type = int(type)
    
    # Keep side as integer (1=Buy, -1=Sell)
    order_side = int(side)
    
    print("quantity: ",quantity)
    print("price: ",price)
    print("type: ",order_type)
    print("side: ",order_side)
    
    # For market orders (type=2), set limitPrice to 0
    limit_price = 0 if order_type == 2 else price
    
    # Use the exact field names and data types from Fyers API documentation
    data = {
        "symbol": symbol,
        "qty": quantity,
        "type": order_type,
        "side": order_side,
        "productType": "INTRADAY",
        "limitPrice": limit_price,
        "stopPrice": 0,
        "validity": "DAY",
        "disclosedQty": 0,
        "offlineOrder": False,
        "stopLoss": 0,
        "takeProfit": 0,
        "orderTag": "tag1"
    }
    
    print("Order data: ", data)
    response = fyers.place_order(data=data)
    print("response: ",response)
    return response

