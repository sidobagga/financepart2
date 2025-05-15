# Financial Data SQLite Database

This repository contains a system to collect financial data for companies from the Financial Modeling Prep API and store it in a SQLite database for analysis.

## Important Note

**This repository contains ONLY the code needed to fetch and analyze financial data. No data files are included.**

You must run the data collection script to generate the data files and SQLite database locally.

## Features

- Download financial data from Financial Modeling Prep API
- Create a SQLite database with appropriate schema for financial data
- Store income statements, balance sheets, cash flow statements, financial ratios, and earnings call transcripts
- Query the database with pre-built or custom SQL queries
- Visualize financial metrics for comparison

## Prerequisites

- Python 3.6+
- Required Python packages (install via `pip install -r requirements.txt`):
  - requests
  - pandas
  - matplotlib
  - tabulate
  - sqlite3 (included in Python standard library)

## Setup

1. Clone this repository:
   ```
   git clone https://github.com/sidobagga/financepart2.git
   cd financepart2
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Create the financial_data directory:
   ```
   mkdir -p financial_data/raw financial_data/csv financial_data/consolidated
   ```

4. (Optional) Configure API key and symbols:
   - Edit `fmp_data_collector.py` to update the `API_KEY` variable with your Financial Modeling Prep API key
   - Edit the `SYMBOLS` list to include the stock symbols you want to analyze

## Usage

### Collecting Data and Creating the Database

Run the data collector script to fetch data and create the SQLite database:

```
python3 fmp_data_collector.py
```

This will:
- Fetch financial data for the configured symbols
- Save raw JSON and CSV files in the `financial_data` directory
- Create a SQLite database at `financial_data/financial_data.db`
- Populate the database with the collected data

### Querying the Database

Run the query script to view sample data from the database:

```
python3 query_financial_db.py
```

This will:
- Display information about the tables in the database
- Show sample data from each table
- Create a revenue comparison chart at `financial_data/revenue_comparison.png`

### Exploring Database Schema

To check the schema of all tables in the database:

```
python3 check_schema.py
```

## Database Schema

The SQLite database contains the following tables:

1. `companies` - Company information (symbol, name)
2. `income_statements` - Revenue, expenses, profit, EPS
3. `balance_sheets` - Assets, liabilities, equity
4. `cash_flow_statements` - Cash flows from operations, investing, financing
5. `financial_ratios` - Key financial ratios like P/E, debt-to-equity
6. `earning_call_transcripts` - Earnings call transcripts
7. `news_press_releases` - News and press releases

## Customization

- Modify the `SYMBOLS` list in `fmp_data_collector.py` to add or remove companies
- Edit the `YEARS` and `QUARTERS` variables to change the time period for data collection
- Customize the queries in `query_financial_db.py` to run different analyses

## Files

- `fmp_data_collector.py` - Main script to collect data and create the database
- `query_financial_db.py` - Script to query and visualize data from the database
- `check_schema.py` - Script to check the database schema
- `requirements.txt` - List of required Python packages

## Output Directory Structure

All the following files and directories will be generated when you run the data collection script:

```
financial_data/
├── raw/                 # Raw JSON responses from the API (generated)
├── csv/                 # Individual CSV files for each data point (generated)
├── consolidated/        # Consolidated CSV files by data type (generated)
├── financial_data.db    # SQLite database (generated)
├── master_financial_data.csv       # Master CSV with all data (generated)
├── sample_financial_data.csv       # Sample of the master data (generated)
└── revenue_comparison.png          # Generated chart (generated)
```

## License

MIT 