import pandas as pd
import numpy as np
import logging
import sys
import os

# Boilerplate to fix imports from project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# NOTE: log_anomaly import removed (Separation of Concerns)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def run_null_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Null Injection (Data Quality) ---")

        # 1. READ: Get a batch of products (Source)
        # We take 1000 products to simulate a daily update
        query = "SELECT * FROM products LIMIT 1000"
        df = pd.read_sql(query, conn)

        if df.empty:
            logging.error("Source table empty.")
            return

        # 2. CHAOS: Corrupt the data (The 'Null' Logic)
        # We want ~40% of rows to have missing Category Names
        # This simulates an upstream mapping failure
        mask = np.random.random(len(df)) < 0.4  # Creates a True/False mask for 40% of rows
        
        # Apply NULLs (None in Python becomes NULL in SQL)
        df.loc[mask, 'product_category_name'] = None

        # Calculate metrics just for console logging (not db logging)
        total_rows = len(df)
        null_count = df['product_category_name'].isna().sum()
        
        logging.info(f"Corrupted Data: Injected {null_count} NULLs out of {total_rows} rows.")

        # 3. LOAD: Append to Bronze
        df.to_sql('bronze_products', conn, if_exists='append', index=False)
        logging.info("Appended batch to 'bronze_products'.")

        # 4. REFLECTION:
        # Manual logging removed. The Detector will catch this data quality issue.

    except Exception as e:
        logging.error(f"Null Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_null_injection()