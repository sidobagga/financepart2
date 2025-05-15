import sqlite3
import os
import pandas as pd

# Database path
DB_PATH = os.path.join("financial_data", "financial_data.db")

def check_table_schema(table_name):
    """Check the schema of a specific table"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        print(f"\nSchema for table '{table_name}':")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        sample = cursor.fetchone()
        if sample:
            print(f"\nSample data for '{table_name}':")
            for i, col in enumerate(columns):
                value = sample[i]
                # Truncate long values
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"  {col[1]}: {value}")
        
        conn.close()
    except Exception as e:
        print(f"Error checking schema: {e}")

def check_all_tables():
    """Check schema for all tables in the database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        conn.close()
        
        for table in tables:
            if table[0] != 'sqlite_sequence':  # Skip internal SQLite table
                check_table_schema(table[0])
                print("-" * 80)
        
    except Exception as e:
        print(f"Error listing tables: {e}")

if __name__ == "__main__":
    print(f"Checking database schema at {DB_PATH}")
    check_all_tables() 