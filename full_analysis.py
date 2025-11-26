import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import re
import warnings
warnings.filterwarnings('ignore')

MASTER_CSV = "GFDLNFO_BACKADJUSTED_31102025.csv"
SAMPLE_FEATHER = "ACC_2024-05-07.feather"
OUTPUT_DIR = "nifty50_analysis"

NIFTY50_SYMBOLS = [
    'ADANIENT', 'ADANIPORTS', 'APOLLOHOSP', 'ASIANPAINT', 'AXISBANK',
    'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BHARTIARTL', 'BEL',
    'BPCL', 'BRITANNIA', 'CIPLA', 'COALINDIA', 'DIVISLAB',
    'DRREDDY', 'EICHERMOT', 'GRASIM', 'HCLTECH', 'HDFCBANK',
    'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDUNILVR', 'ICICIBANK',
    'INDIGO', 'INFY', 'ITC', 'JIOFIN', 'JSWSTEEL',
    'KOTAKBANK', 'LT', 'M&M', 'MARUTI', 'NESTLEIND',
    'NTPC', 'ONGC', 'POWERGRID', 'RELIANCE', 'SBILIFE',
    'SBIN', 'SUNPHARMA', 'TATACONSUM', 'TATAPOWER', 'TATASTEEL',  
    'TCS', 'TITAN', 'ULTRACEMCO', 'UPL', 'WIPRO'
]


def parse_ticker(ticker_str):
    """
    Parse NSE derivative ticker into components
    
    Examples:
        'RELIANCE25NOV252000CE.NFO' -> ('RELIANCE', '25NOV25', 2000, 'CE', 'OPTION')
        'RELIANCE28NOV25FUT.NFO' -> ('RELIANCE', '28NOV25', None, 'FUT', 'FUTURE')
    """
    ticker = str(ticker_str).replace('.NFO', '')
    
    # match futures: SYMBOL-I/II/III
    
    fut_pattern = r'^([A-Z&-]+?)-(I|II|III)$'
    fut_match = re.match(fut_pattern, ticker)
    if fut_match:
        symbol = fut_match.group(1)
        bucket_suffix = fut_match.group(2) # I, II, or III
        bucket = f"FUT_{bucket_suffix}"
        return symbol, bucket, None, 'FUT', 'FUTURE'
    
    # match options: SYMBOL + EXPIRY + STRIKE + CE/PE
    opt_pattern = r'^([A-Z&-]+?)(\d{2}[A-Z]{3}\d{2})(\d+)(CE|PE)$'
    opt_match = re.match(opt_pattern, ticker)
    if opt_match:
        symbol = opt_match.group(1)
        expiry = opt_match.group(2)
        strike = int(opt_match.group(3))
        opt_type = opt_match.group(4)
        return symbol, expiry, strike, opt_type, 'OPTION'
    
    return None, None, None, None, None


def determine_future_bucket(expiry_str, reference_date='31/10/2025'):
    """
    Determine if future is FUT_I (near), FUT_II (mid), or FUT_III (far)
    Based on expiry month relative to data date
    """
    month_map = {
        'NOV': 1, 'DEC': 2, 'JAN': 3, 'FEB': 4,
        'MAR': 5, 'APR': 6, 'MAY': 7, 'JUN': 8,
        'JUL': 9, 'AUG': 10, 'SEP': 11, 'OCT': 12
    }
    
    month_match = re.search(r'([A-Z]{3})', expiry_str)
    if not month_match:
        return 'FUT_I'
    
    month = month_match.group(1)
    
    # for Oct 31 data: NOV=near, DEC=mid, JAN=far
    if month == 'NOV':
        return 'FUT_I'
    elif month == 'DEC':
        return 'FUT_II'
    elif month == 'JAN':
        return 'FUT_III'
    else:
        return 'FUT_I'  # Default


def create_wide_dataframe(symbol, df_master, sample_df, file_date):
    """
    Convert long-format data to wide-format with DYNAMIC column generation
    """
   
    print(f"Processing: {symbol}")

    
    # filter data for this symbol
    # match tickers starting with symbol followed by digit (options) or hyphen (futures)
    mask = df_master['Ticker'].str.contains(f'^{re.escape(symbol)}[\\d-]', na=False, regex=True)
    df_symbol = df_master[mask].copy()
    
    if len(df_symbol) == 0:
        print(f"No data found")
        return None
    
    print(f"Found {len(df_symbol):,} rows, {df_symbol['Ticker'].nunique()} unique contracts")
    
    # parse all tickers
    ticker_info = []
    for ticker in df_symbol['Ticker'].unique():
        sym, expiry, strike, opt_type, instrument = parse_ticker(ticker)
        if sym == symbol:  # ensure of symbol matched 
            ticker_info.append({
                'Ticker': ticker,
                'Symbol': sym,
                'Expiry': expiry,
                'Strike': strike,
                'OptType': opt_type,
                'Instrument': instrument
            })
    
    if not ticker_info:
        print(f"No valid tickers parsed")
        return None
    
    df_tickers = pd.DataFrame(ticker_info)
    df_symbol = df_symbol.merge(df_tickers, on='Ticker')
    
    print(f"Parsed {len(df_tickers)} contracts:")
    print(f"Futures: {len(df_tickers[df_tickers['Instrument'] == 'FUTURE'])}")
    print(f"Options: {len(df_tickers[df_tickers['Instrument'] == 'OPTION'])}")
    
    # create datetime and get unique timestamps
    df_symbol['Datetime'] = pd.to_datetime(
        df_symbol['Date'] + ' ' + df_symbol['Time'],
        format='%d/%m/%Y %H:%M:%S',
        errors='coerce'
    )
    
    unique_times = sorted(df_symbol['Datetime'].dropna().unique())
    print(f"  Time range: {len(unique_times)} timestamps")
    
    # initialize result DataFrame
    result = pd.DataFrame({
        'Datetime': unique_times,
        'FileDate': pd.to_datetime(file_date, format='%d/%m/%Y'),
        'Date': pd.to_datetime(file_date, format='%d/%m/%Y'),
        'Time': [dt.strftime('%H:%M:%S') for dt in unique_times]
    })
    
    # field mapping
    fields_map = {
        'Close': 'Close',
        'High': 'High',
        'Low': 'Low',
        'Open': 'Open',
        'Open Interest': 'Open_Interest',
        'Volume': 'Volume'
    }
    
    # dynamically create columns for each contract
    contracts_processed = 0
    all_columns = set(['FileDate', 'Date', 'Time'])  # base columns
    
    for _, contract in df_tickers.iterrows():
        ticker = contract['Ticker']
        instrument = contract['Instrument']
        strike = contract['Strike']
        opt_type = contract['OptType']
        expiry = contract['Expiry']
        
        # get data for this contract
        contract_data = df_symbol[df_symbol['Ticker'] == ticker].copy()
        contract_data = contract_data.set_index('Datetime')
        
        # determine column prefix
        if instrument == 'FUTURE':
            # expiry is already 'FUT_I', 'FUT_II', etc. from parse_ticker
            col_prefix = expiry
        elif instrument == 'OPTION':
            col_prefix = f"{strike}{opt_type}"
        else:
            continue
        
        # map each field
        for csv_field, col_suffix in fields_map.items():
            col_name = f"{col_prefix}_{col_suffix}"
            all_columns.add(col_name)
            
            # create column if it doesn't exist
            if col_name not in result.columns:
                result[col_name] = np.nan
            
            # populate data
            values = contract_data[csv_field]
            for idx, val in values.items():
                result.loc[result['Datetime'] == idx, col_name] = val
        
        contracts_processed += 1
    
    print(f"Processed {contracts_processed} contracts")
    print(f"Created {len(all_columns)} total columns")
    
    # remove datetime helper column
    if 'Datetime' in result.columns:
        result = result.drop(columns=['Datetime'])
        
    # sort columns: metadata first, then numerical strike order
    metadata_cols = ['FileDate', 'Date', 'Time']
    metadata_cols = [c for c in metadata_cols if c in result.columns]
    
    def get_col_sort_key(col_name):
        """sort key for option columns: (strike, type, metric)"""
        # regex to parse: 1200CE_Close -> (1200, 'CE', 'Close')
        match = re.match(r'(\d+)(CE|PE)_(.+)', col_name)
        if match:
            strike = int(match.group(1))
            opt_type = match.group(2)
            metric = match.group(3)
            # sort order: strike (int), type (CE<PE), metric (str)
            return (strike, opt_type, metric)
        # fallback for non-matching columns (futures etc)
        return (float('inf'), col_name, '')

    # get remaining columns and sort them numerically
    other_cols = [c for c in result.columns if c not in metadata_cols]
    other_cols.sort(key=get_col_sort_key)
    
    # reorder DataFrame
    result = result[metadata_cols + other_cols]
    
    print(f"  Final shape: {result.shape}")
    non_null = result.notna().sum().sum()
    total = result.shape[0] * result.shape[1]
    print(f"  Data density: {non_null:,}/{total:,} cells ({100*non_null/total:.1f}%)")
    
    return result


def main():
    """Main processing function"""
    
    print("Data Loading Started")   
    
    # create output directory
    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # load sample feather (for reference only)
    print("Loading Sample Feather")
    sample_df = pd.read_feather(SAMPLE_FEATHER)
    print(f"  Structure: {len(sample_df)} rows × {len(sample_df.columns)} columns")
    
    # load master csv
    print("Loading Master CSV")
    df_master = pd.read_csv(
        MASTER_CSV,
        dtype={
            'Ticker': 'str',
            'Date': 'str',
            'Time': 'str',
            'Open': 'float64',
            'High': 'float64',
            'Low': 'float64',
            'Close': 'float64',
            'Volume': 'float64',
            'Open Interest': 'float64'
        }
    )
    print(f"Loaded: {len(df_master):,} rows × {len(df_master.columns)} columns")
    
    # get file date
    file_date = df_master['Date'].iloc[0]
    date_obj = datetime.strptime(file_date, '%d/%m/%Y')
    date_str = date_obj.strftime('%Y-%m-%d')
    print(f"Date: {date_str}")
    
    # process each symbol
    print(f"Processing {len(NIFTY50_SYMBOLS)} NIFTY 50 symbols...")
    
    success_count = 0
    failed = []
    
    for symbol in NIFTY50_SYMBOLS:
        try:
            result_df = create_wide_dataframe(symbol, df_master, sample_df, file_date)
            
            if result_df is not None and len(result_df) > 0:
                # save as feather
                output_file = output_path / f"{symbol}_{date_str}.feather"
                result_df.reset_index(drop=True).to_feather(output_file)
                print(f"  Saved: {output_file.name}")
                success_count += 1
            else:
                print(f"  Failed: No data generated")
                failed.append(symbol)
                
        except Exception as e:
            print(f"  Error: {e}")
            import traceback
            traceback.print_exc()
            failed.append(symbol)
    
    
   
    print(f"Successfully processed: {success_count}/{len(NIFTY50_SYMBOLS)} symbols")
    if failed:
        print(f"Failed ({len(failed)}): {', '.join(failed)}")
    print(f"\nOutput directory: {output_path.absolute()}")
   


if __name__ == "__main__":
    main()