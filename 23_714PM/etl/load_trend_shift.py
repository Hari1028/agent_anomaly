import pandas as pd
import uuid
import logging
import sys
import os

# Fix imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# No log_anomaly import (Detector handles the observation)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_ids(df):
    """
    Refreshes Order IDs to simulate new organic traffic.
    """
    df['order_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_trend_shift_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Trend Shift Injection (Step Change) ---")

        # 1. READ: Get a sample of 'normal' data
        query = "SELECT * FROM order_items LIMIT 100"
        df_source = pd.read_sql(query, conn)
        
        if df_source.empty:
            logging.error("Source table is empty! Cannot run injection.")
            return

        # 2. CHAOS: Simulate a "Step Change" (New Normal)
        # Unlike the 50x Spike, this is a moderate, sustained increase (e.g., 5x).
        # Scenario: A new marketing campaign permanently increased daily users.
        multiplier = 5 
        df_trend = pd.concat([df_source] * multiplier, ignore_index=True)
        
        # 3. TRANSFORM: Anonymize
        df_trend = generate_new_ids(df_trend)

        logging.info(f"Generated {len(df_trend)} rows. (Volume Multiplier: {multiplier}x).")
        logging.warning("Simulating a permanent Trend Shift (Step Change) in transaction volume.")

        # 4. LOAD: Append to Bronze
        df_trend.to_sql('bronze_order_items', conn, if_exists='append', index=False)
        
        logging.info("Successfully APPENDED data to 'bronze_order_items'.")

        # 5. REFLECTION:
        # No manual logging.
        # The Detector will compare this batch against the 'baseline_avg_high' rule 
        # and flag it as a 'sustained_volume_shift' if it persists.

    except Exception as e:
        logging.error(f"Trend Shift Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_trend_shift_injection()