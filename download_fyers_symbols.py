"""
Standalone script to download all Fyers tradable symbols
This script downloads Symbol Master JSON files from Fyers and converts them to CSV format
"""

import os
import sys
from FyresIntegration import download_fyers_symbols, search_symbols

def main():
    print("="*60)
    print("Fyers Symbol Downloader")
    print("="*60)
    print()
    
    # Default save path
    save_path = "fyers_symbols"
    
    # Ask user which exchanges to download
    print("Available exchanges:")
    print("  1. All exchanges (all segments)")
    print("  2. NSE (all segments: CM, FO, CD, COM)")
    print("  3. MCX (Commodity)")
    print("  4. BSE (all segments: CM, FO)")
    print("  5. NSE_FO (NSE Equity Derivatives)")
    print("  6. NSE_CM (NSE Capital Market)")
    print("  7. NSE_CD (NSE Currency Derivatives)")
    print("  8. NSE_COM (NSE Commodity)")
    print("  9. Custom exchange segment")
    print()
    
    choice = input("Select option (1-9) [default: 1]: ").strip() or "1"
    
    exchange = None
    if choice == "2":
        exchange = "NSE"
    elif choice == "3":
        exchange = "MCX"
    elif choice == "4":
        exchange = "BSE"
    elif choice == "5":
        exchange = "NSE_FO"
    elif choice == "6":
        exchange = "NSE_CM"
    elif choice == "7":
        exchange = "NSE_CD"
    elif choice == "8":
        exchange = "NSE_COM"
    elif choice == "9":
        exchange = input("Enter exchange segment (NSE_CM, NSE_FO, NSE_CD, NSE_COM, BSE_CM, BSE_FO, MCX_COM): ").strip()
        if not exchange:
            exchange = None
    
    print()
    print(f"Downloading symbols to: {save_path}/")
    print(f"Exchange filter: {exchange or 'ALL'}")
    print()
    
    # Download symbols
    try:
        downloaded_files = download_fyers_symbols(exchange=exchange, save_path=save_path)
        
        print()
        print("="*60)
        print("Download Summary")
        print("="*60)
        
        total_symbols = 0
        successful_downloads = 0
        
        for exch, file_info in downloaded_files.items():
            if file_info.get('file_path'):
                symbol_count = file_info.get('symbol_count', 0)
                total_symbols += symbol_count
                successful_downloads += 1
                print(f"✓ {exch}: {symbol_count:,} symbols -> {file_info['file_path']}")
            else:
                error = file_info.get('error', 'Unknown error')
                print(f"✗ {exch}: Failed - {error}")
        
        print()
        print(f"Total: {successful_downloads} exchange(s) downloaded, {total_symbols:,} symbols")
        print()
        print(f"Symbol files saved in: {os.path.abspath(save_path)}/")
        print()
        
        # Ask if user wants to search
        search_query = input("Enter a symbol to search (or press Enter to skip): ").strip()
        if search_query:
            print()
            print(f"Searching for '{search_query}'...")
            results = search_symbols(query=search_query, exchange=None, symbols_dir=save_path)
            
            if results:
                print(f"\nFound {len(results)} matching symbol(s):\n")
                for i, result in enumerate(results[:20], 1):  # Show first 20 results
                    symbol = result.get('Symbol', result.get('symbol', 'N/A'))
                    exch = result.get('Exchange', result.get('exchange', 'N/A'))
                    exch_seg = result.get('ExchangeSegment', result.get('exchange_segment', ''))
                    if exch_seg:
                        print(f"  {i}. {symbol} ({exch} - {exch_seg})")
                    else:
                        print(f"  {i}. {symbol} ({exch})")
                
                if len(results) > 20:
                    print(f"\n  ... and {len(results) - 20} more results")
            else:
                print(f"No symbols found matching '{search_query}'")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

