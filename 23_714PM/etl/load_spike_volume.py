import pandas as pd
import uuid
import logging
import sys
import os

# Add project root to system path to allow imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# NOTE: log_anomaly import removed (Separation of Concerns)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_ids(df):
    """
    Helps simulate 'new' data by generating fresh Order IDs.
    Otherwise, we just keep inserting the same old IDs.
    """
    # Generate a unique ID for every row
    df['order_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_spike_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Volume Spike Injection ---")

        # 1. READ: Get a sample of 'normal' data from the Source
        # We read from the original clean table
        query = "SELECT * FROM order_items LIMIT 100"
        df_source = pd.read_sql(query, conn)
        
        if df_source.empty:
            logging.error("Source table is empty! Cannot run injection.")
            return

        # 2. CHAOS: Multiply the data to create a SPIKE
        # Replicating the data 50 times (100 * 50 = 5000 rows)
        df_spike = pd.concat([df_source] * 50, ignore_index=True)
        
        # 3. TRANSFORM: Make it look like NEW data (New IDs)
        df_spike = generate_new_ids(df_spike)

        logging.info(f"Generated {len(df_spike)} rows (Simulating a Traffic Spike).")

        # 4. LOAD (APPEND): Write to the Bronze Layer
        # We use 'if_exists="append"' to keep history!
        df_spike.to_sql('bronze_order_items', conn, if_exists='append', index=False)
        
        logging.info("Successfully APPENDED data to 'bronze_order_items'.")

        # 5. REFLECTION:
        # Manual logging removed. The Detector will catch this spike based on rules.py.

    except Exception as e:
        logging.error(f"ETL Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_spike_injection()