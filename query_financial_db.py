import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt
from tabulate import tabulate

# Database path
DB_PATH = os.path.join("financial_data", "financial_data.db")

def check_database():
    """Check if database exists and has data"""
    if not os.path.exists(DB_PATH):
        print(f"Database not found at {DB_PATH}")
        return False
    
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        if not tables:
            print("Database exists but contains no tables.")
            conn.close()
            return False
        
        print("Database contains the following tables:")
        for i, table in enumerate(tables, 1):
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"{i}. {table[0]} - {count} rows")
        
        conn.close()
        return True
    except Exception as e:
        print(f"Error accessing database: {e}")
        return False

def query_financial_data():
    """Query and display sample data from the financial database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Get company information
        company_df = pd.read_sql("SELECT * FROM companies", conn)
        print("\nCompanies in the database:")
        print(tabulate(company_df, headers='keys', tablefmt='psql', showindex=False))
        
        # Sample queries for each table
        sample_queries = {
            "Latest Income Statements": """
                SELECT symbol, date, period, revenue, grossprofit, netincome, eps
                FROM income_statements
                ORDER BY date DESC
                LIMIT 5
            """,
            "Latest Balance Sheets": """
                SELECT symbol, date, period, totalassets, totalliabilities, totalstockholdersequity
                FROM balance_sheets
                ORDER BY date DESC
                LIMIT 5
            """,
            "Latest Cash Flow Statements": """
                SELECT symbol, date, period, netcashprovidedbyoperatingactivities, capitalexpenditure, freecashflow
                FROM cash_flow_statements
                ORDER BY date DESC
                LIMIT 5
            """,
            "Financial Ratios": """
                SELECT symbol, date, period, pricetoearningsratio, pricetosalesratio, debttoequityratio, dividendyield
                FROM financial_ratios
                ORDER BY date DESC
                LIMIT 5
            """,
            "Latest Earnings Call Transcripts": """
                SELECT symbol, date, year, quarter, substr(content, 1, 100) || '...' as content_preview
                FROM earning_call_transcripts
                ORDER BY date DESC
                LIMIT 5
            """
        }
        
        # Execute and display each query
        for title, query in sample_queries.items():
            print(f"\n{title}:")
            df = pd.read_sql(query, conn)
            if not df.empty:
                print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))
            else:
                print("No data available")
        
        # Count records by company and table
        print("\nRecord counts by company and table:")
        tables = ["income_statements", "balance_sheets", "cash_flow_statements", 
                  "financial_ratios", "earning_call_transcripts"]
        
        count_data = []
        for symbol in company_df['symbol']:
            row = [symbol]
            for table in tables:
                count_query = f"SELECT COUNT(*) FROM {table} WHERE symbol = ?"
                cursor = conn.cursor()
                cursor.execute(count_query, (symbol,))
                count = cursor.fetchone()[0]
                row.append(count)
            count_data.append(row)
        
        count_df = pd.DataFrame(count_data, columns=['Symbol'] + [t.replace('_', ' ').title() for t in tables])
        print(tabulate(count_df, headers='keys', tablefmt='psql', showindex=False))
        
        # Plot revenue over time for comparison
        try:
            plt.figure(figsize=(12, 6))
            
            for symbol in company_df['symbol']:
                revenue_query = """
                    SELECT date, revenue 
                    FROM income_statements 
                    WHERE symbol = ? AND period = 'FY'
                    ORDER BY date
                """
                revenue_df = pd.read_sql(revenue_query, conn, params=(symbol,))
                
                if not revenue_df.empty:
                    revenue_df['date'] = pd.to_datetime(revenue_df['date'])
                    plt.plot(revenue_df['date'], revenue_df['revenue'] / 1e9, marker='o', label=symbol)
            
            plt.title('Annual Revenue Comparison (in Billions)')
            plt.xlabel('Year')
            plt.ylabel('Revenue ($ Billions)')
            plt.legend()
            plt.grid(True, linestyle='--', alpha=0.7)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Save plot
            plot_path = os.path.join("financial_data", "revenue_comparison.png")
            plt.savefig(plot_path)
            print(f"\nRevenue comparison plot saved to {plot_path}")
            
        except Exception as e:
            print(f"Error creating plot: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error querying database: {e}")

if __name__ == "__main__":
    print(f"Checking financial database at {DB_PATH}")
    if check_database():
        query_financial_data()
    else:
        print("Please run fmp_data_collector.py first to create and populate the database.") 