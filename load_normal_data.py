import pandas as pd
import uuid
import logging
import sys
import os

# Fix imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_ids(df):
    """Refreshes Order IDs to simulate new organic traffic."""
    df['order_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_normal_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Normal Data Injection (The Control Group) ---")

        # 1. READ: Get a standard sample
        query = "SELECT * FROM order_items LIMIT 100"
        df_source = pd.read_sql(query, conn)
        
        if df_source.empty:
            logging.error("Source table is empty!")
            return

        # 2. TRANSFORM: Just anonymize, NO Chaos
        # We process exactly 100 rows. 
        # This is > 10 (Drop Threshold) and < 200 (Shift Threshold).
        # It is the "Goldilocks" zone.
        df_normal = generate_new_ids(df_source.copy())

        logging.info(f"Generated {len(df_normal)} rows (Standard Traffic).")

        # 3. LOAD: Append to Bronze
        df_normal.to_sql('bronze_order_items', conn, if_exists='append', index=False)
        
        logging.info("Successfully APPENDED Normal Data to 'bronze_order_items'.")

    except Exception as e:
        logging.error(f"Normal Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_normal_injection()