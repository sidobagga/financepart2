import requests
import pandas as pd
import os
from datetime import datetime
import time
import json
import sqlite3
from pathlib import Path

# API configuration
API_KEY = "fjRDKKnsRnVNMfFepDM6ox31u9RlPklv"
BASE_URL = "https://financialmodelingprep.com/stable"

# Parameters
SYMBOLS = ["IBM", "RPD"]
YEARS = list(range(2020, 2026))  # 2020 to 2025
QUARTERS = [1, 2, 3, 4]

# Create output directory
OUTPUT_DIR = "financial_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Database setup
DB_PATH = os.path.join(OUTPUT_DIR, "financial_data.db")

# Create subdirectories for each data type
for subdir in ["raw", "csv", "consolidated"]:
    os.makedirs(os.path.join(OUTPUT_DIR, subdir), exist_ok=True)

# API endpoints to fetch
ENDPOINTS = {
    "earning_call_transcript": {
        "url": "/earning-call-transcript",
        "params": ["symbol", "year", "quarter"],
        "quarterly": True
    },
    "cash_flow_statement": {
        "url": "/cash-flow-statement",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period"
    },
    "balance_sheet_statement": {
        "url": "/balance-sheet-statement",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period"
    },
    "income_statement": {
        "url": "/income-statement",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period"
    },
    "ratios": {
        "url": "/ratios",
        "params": ["symbol"],
        "quarterly": True,
        "period_param": "period"
    },
    "analyst_estimates": {
        "url": "/analyst-estimates",
        "params": ["symbol", "period"],
        "additional_params": {"page": 0, "limit": 10},
        "quarterly": True
    },
    "news_press_releases": {
        "url": "/news/press-releases",
        "params": ["symbols"],
        "quarterly": False
    }
}

def fetch_api_data(endpoint_name, endpoint_config, symbol, year=None, quarter=None, period=None):
    """Fetch data from the FMP API for a specific endpoint, symbol, year, and quarter"""
    url = f"{BASE_URL}{endpoint_config['url']}"
    
    # Build parameters
    params = {"apikey": API_KEY}
    
    if "symbol" in endpoint_config["params"]:
        params["symbol"] = symbol
    if "symbols" in endpoint_config["params"]:
        params["symbols"] = symbol
    
    if year is not None and "year" in endpoint_config["params"]:
        params["year"] = year
    
    if quarter is not None and "quarter" in endpoint_config["params"]:
        params["quarter"] = quarter
    
    # Handle period parameter (annual vs quarterly)
    if period and "period_param" in endpoint_config:
        params[endpoint_config["period_param"]] = period
    elif "period" in endpoint_config["params"]:
        params["period"] = period if period else "annual"
            
    # Add any additional parameters
    if "additional_params" in endpoint_config:
        params.update(endpoint_config["additional_params"])
    
    try:
        print(f"Fetching {endpoint_name} for {symbol}" + 
              (f" for year {year}" if year else "") + 
              (f" quarter {quarter}" if quarter else "") +
              (f" period {period}" if period else ""))
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Save raw JSON for debugging purposes
            save_raw_json(data, endpoint_name, symbol, year, quarter, period)
            
            return data
        else:
            print(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def save_raw_json(data, endpoint_name, symbol, year=None, quarter=None, period=None):
    """Save raw JSON data for debugging"""
    if not data:
        return
    
    # Create a filename
    filename_parts = [endpoint_name, symbol]
    if year:
        filename_parts.append(f"Y{year}")
    if quarter:
        filename_parts.append(f"Q{quarter}")
    if period:
        filename_parts.append(period)
    
    filename = "_".join(filename_parts) + ".json"
    filepath = os.path.join(OUTPUT_DIR, "raw", filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)

def save_to_csv(data, endpoint_name, symbol, year=None, quarter=None, period=None):
    """Save API response data to CSV"""
    if not data:
        return None
    
    # Create a filename based on the endpoint, symbol, year, and quarter
    filename_parts = [endpoint_name, symbol]
    if year:
        filename_parts.append(f"Y{year}")
    if quarter:
        filename_parts.append(f"Q{quarter}")
    if period:
        filename_parts.append(period)
    
    filename = "_".join(filename_parts) + ".csv"
    filepath = os.path.join(OUTPUT_DIR, "csv", filename)
    
    # Convert to DataFrame and save
    try:
        # Handle different data structures
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # If it's a single record, convert to a list with one item
            df = pd.DataFrame([data])
        else:
            print(f"Unrecognized data format for {endpoint_name}")
            return None
        
        if not df.empty:
            # Add metadata columns if not present
            if "symbol" not in df.columns:
                df["symbol"] = symbol
            if year is not None and "year" not in df.columns:
                df["year"] = year
            if quarter is not None and "quarter" not in df.columns:
                df["quarter"] = quarter
            if period is not None and "period" not in df.columns:
                df["period"] = period
            
            # Add data source column
            df["data_source"] = endpoint_name
            
            df.to_csv(filepath, index=False)
            print(f"Saved to {filepath}")
            return df
        else:
            print(f"No data to save for {endpoint_name}")
            return None
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        return None

def create_consolidated_files():
    """Consolidate individual CSV files into endpoint-specific consolidated files"""
    consolidated_dir = os.path.join(OUTPUT_DIR, "consolidated")
    csv_dir = os.path.join(OUTPUT_DIR, "csv")
    
    # Group files by endpoint
    endpoints = {}
    for file in os.listdir(csv_dir):
        if file.endswith(".csv"):
            endpoint_name = file.split("_")[0]
            if endpoint_name not in endpoints:
                endpoints[endpoint_name] = []
            endpoints[endpoint_name].append(os.path.join(csv_dir, file))
    
    # Consolidate each endpoint's files
    for endpoint, files in endpoints.items():
        if not files:
            continue
            
        print(f"Consolidating {len(files)} files for {endpoint}")
        
        dfs = []
        for file in files:
            try:
                df = pd.read_csv(file)
                dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")
        
        if dfs:
            consolidated = pd.concat(dfs, ignore_index=True)
            output_path = os.path.join(consolidated_dir, f"{endpoint}_all_data.csv")
            consolidated.to_csv(output_path, index=False)
            print(f"Created consolidated file: {output_path}")

def create_master_csv():
    """Create a single CSV file with data from all sources"""
    print("Creating master CSV file with all data...")
    
    csv_dir = os.path.join(OUTPUT_DIR, "csv")
    all_dfs = []
    
    # Read all CSV files
    for file in os.listdir(csv_dir):
        if file.endswith(".csv"):
            try:
                file_path = os.path.join(csv_dir, file)
                df = pd.read_csv(file_path)
                
                # Extract metadata from filename
                parts = file.replace(".csv", "").split("_")
                endpoint = parts[0]
                
                # Add endpoint if not already present
                if "data_source" not in df.columns:
                    df["data_source"] = endpoint
                
                all_dfs.append(df)
            except Exception as e:
                print(f"Error reading {file}: {e}")
    
    if all_dfs:
        # Create master dataframe
        master_df = pd.concat(all_dfs, ignore_index=True)
        
        # Standardize column names to lowercase with underscores
        master_df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in master_df.columns]
        
        # Common identifier columns to move to the front
        id_columns = ["data_source", "symbol", "year", "quarter", "period", "date"]
        front_cols = [col for col in id_columns if col in master_df.columns]
        other_cols = [col for col in master_df.columns if col not in front_cols]
        master_df = master_df[front_cols + other_cols]
        
        # Save master CSV
        master_path = os.path.join(OUTPUT_DIR, "master_financial_data.csv")
        master_df.to_csv(master_path, index=False)
        print(f"Created master CSV file with {len(master_df)} rows at {master_path}")
        
        # Generate a sample with a subset of columns for preview
        sample_cols = front_cols + other_cols[:min(10, len(other_cols))]
        sample_df = master_df[sample_cols].head(100)
        sample_path = os.path.join(OUTPUT_DIR, "sample_financial_data.csv")
        sample_df.to_csv(sample_path, index=False)
        print(f"Created sample CSV file for preview at {sample_path}")
        
        return master_df
    else:
        print("No data to consolidate into master CSV")
        return None

def init_database():
    """Initialize SQLite database with schema for financial data"""
    print(f"Initializing SQLite database at {DB_PATH}")
    
    # Create connection to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create tables for each data type
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS companies (
        symbol TEXT PRIMARY KEY,
        name TEXT,
        added_date TIMESTAMP
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS income_statements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        date TEXT,
        period TEXT,
        year INTEGER,
        quarter INTEGER,
        revenue REAL,
        cost_of_revenue REAL,
        gross_profit REAL,
        gross_profit_ratio REAL,
        research_and_development_expenses REAL,
        general_and_administrative_expenses REAL,
        selling_and_marketing_expenses REAL,
        selling_general_and_administrative_expenses REAL,
        operating_expenses REAL,
        operating_income REAL,
        operating_income_ratio REAL,
        interest_expense REAL,
        ebitda REAL,
        ebitda_ratio REAL,
        net_income REAL,
        net_income_ratio REAL,
        eps REAL,
        eps_diluted REAL,
        weighted_average_shares_outstanding REAL,
        weighted_average_shares_outstanding_diluted REAL,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS balance_sheets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        date TEXT,
        period TEXT,
        year INTEGER,
        quarter INTEGER,
        cash_and_cash_equivalents REAL,
        short_term_investments REAL,
        cash_and_short_term_investments REAL,
        net_receivables REAL,
        inventory REAL,
        total_current_assets REAL,
        property_plant_equipment REAL,
        goodwill REAL,
        intangible_assets REAL,
        total_assets REAL,
        accounts_payable REAL,
        short_term_debt REAL,
        total_current_liabilities REAL,
        long_term_debt REAL,
        total_liabilities REAL,
        total_stockholders_equity REAL,
        total_debt REAL,
        net_debt REAL,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cash_flow_statements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        date TEXT,
        period TEXT,
        year INTEGER,
        quarter INTEGER,
        net_income REAL,
        depreciation_and_amortization REAL,
        stock_based_compensation REAL,
        change_in_working_capital REAL,
        cash_from_operations REAL,
        capital_expenditure REAL,
        acquisitions REAL,
        cash_from_investing REAL,
        debt_repayment REAL,
        common_stock_issued REAL,
        common_stock_repurchased REAL,
        dividends_paid REAL,
        cash_from_financing REAL,
        free_cash_flow REAL,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS financial_ratios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        date TEXT,
        period TEXT,
        year INTEGER,
        quarter INTEGER,
        pe_ratio REAL,
        price_to_sales_ratio REAL,
        pb_ratio REAL,
        debt_to_equity REAL,
        roa REAL,
        roe REAL,
        current_ratio REAL,
        quick_ratio REAL,
        dividend_yield REAL,
        dividend_payout_ratio REAL,
        gross_margin REAL,
        operating_margin REAL,
        net_margin REAL,
        fcf_margin REAL,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS analyst_estimates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        date TEXT,
        period TEXT,
        year INTEGER,
        quarter INTEGER,
        estimated_revenue_low REAL,
        estimated_revenue_avg REAL,
        estimated_revenue_high REAL,
        estimated_ebitda_low REAL,
        estimated_ebitda_avg REAL,
        estimated_ebitda_high REAL,
        estimated_eps_low REAL,
        estimated_eps_avg REAL,
        estimated_eps_high REAL,
        number_of_analysts INTEGER,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS earning_call_transcripts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        year INTEGER,
        quarter INTEGER,
        date TEXT,
        title TEXT,
        content TEXT,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS news_press_releases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT,
        date TEXT,
        title TEXT,
        content TEXT,
        url TEXT,
        FOREIGN KEY (symbol) REFERENCES companies(symbol)
    )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_income_symbol ON income_statements(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance_symbol ON balance_sheets(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cashflow_symbol ON cash_flow_statements(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratios_symbol ON financial_ratios(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_estimates_symbol ON analyst_estimates(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transcripts_symbol ON earning_call_transcripts(symbol)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_news_symbol ON news_press_releases(symbol)')
    
    conn.commit()
    conn.close()
    
    print("Database schema created successfully")

def insert_data_to_db(master_df):
    """Insert consolidated dataframes into SQLite database tables"""
    if master_df is None or master_df.empty:
        print("No data to insert into database")
        return
        
    print("Inserting data into SQLite database...")
    conn = sqlite3.connect(DB_PATH)
    
    # Add companies
    companies_df = pd.DataFrame({
        'symbol': SYMBOLS,
        'name': SYMBOLS,  # Replace with actual company names if available
        'added_date': pd.Timestamp.now()
    })
    companies_df.to_sql('companies', conn, if_exists='replace', index=False)
    
    # Process each consolidated file by endpoint type
    consolidated_dir = os.path.join(OUTPUT_DIR, "consolidated")
    
    # Map consolidated file prefixes to database tables
    endpoint_to_table = {
        'income': 'income_statements',
        'balance': 'balance_sheets',
        'cash': 'cash_flow_statements',
        'ratios': 'financial_ratios',
        'analyst': 'analyst_estimates',
        'earning': 'earning_call_transcripts',
        'news': 'news_press_releases'
    }
    
    # Loop through each endpoint's consolidated file
    for file_prefix, table_name in endpoint_to_table.items():
        file_path = os.path.join(consolidated_dir, f"{file_prefix}_all_data.csv")
        
        if os.path.exists(file_path):
            print(f"Processing {file_prefix} data for database insertion")
            try:
                df = pd.read_csv(file_path)
                
                # Convert column names to lowercase and replace spaces/hyphens with underscores
                df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]
                
                # Drop duplicates if any
                if 'symbol' in df.columns and 'date' in df.columns:
                    df = df.drop_duplicates(subset=['symbol', 'date'], keep='first')
                
                # Insert to database
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                print(f"Inserted {len(df)} rows into {table_name}")
            except Exception as e:
                print(f"Error inserting {file_prefix} data: {e}")
        else:
            print(f"No consolidated file found for {file_prefix}")
    
    conn.close()
    print("Database population complete")

def main():
    """Main function to fetch and save all data"""
    for symbol in SYMBOLS:
        # Process earnings call transcripts separately - only Q1 2025
        transcript_endpoint = "earning_call_transcript"
        transcript_config = ENDPOINTS[transcript_endpoint]
        data = fetch_api_data(transcript_endpoint, transcript_config, symbol, 2025, 1)
        save_to_csv(data, transcript_endpoint, symbol, 2025, 1)
        time.sleep(1)
        
        # Process all other endpoints normally
        for endpoint_name, endpoint_config in ENDPOINTS.items():
            if endpoint_name != "earning_call_transcript":
                # Handle period-based endpoints
                if "period_param" in endpoint_config:
                    # Annual data
                    data = fetch_api_data(endpoint_name, endpoint_config, symbol, period="annual")
                    save_to_csv(data, endpoint_name, symbol, period="annual")
                    time.sleep(1)
                    
                    # Quarterly data
                    data = fetch_api_data(endpoint_name, endpoint_config, symbol, period="quarter")
                    save_to_csv(data, endpoint_name, symbol, period="quarter")
                    time.sleep(1)
    
    # Create consolidated files
    create_consolidated_files()
    
    # Create master CSV with all data
    master_df = create_master_csv()
    
    # Initialize SQLite database
    init_database()
    
    # Insert data into database
    insert_data_to_db(master_df)
    
    print("Data collection and database creation complete!")
    print(f"All data has been saved to the '{OUTPUT_DIR}' directory")
    print(f"- Raw JSON files: {OUTPUT_DIR}/raw/")
    print(f"- Individual CSV files: {OUTPUT_DIR}/csv/")
    print(f"- Consolidated CSV files: {OUTPUT_DIR}/consolidated/")
    print(f"- SQLite database: {DB_PATH}")
    if master_df is not None:
        print(f"- Master CSV file: {os.path.join(OUTPUT_DIR, 'master_financial_data.csv')}")
        print(f"- Sample CSV preview: {os.path.join(OUTPUT_DIR, 'sample_financial_data.csv')}")

if __name__ == "__main__":
    main() 