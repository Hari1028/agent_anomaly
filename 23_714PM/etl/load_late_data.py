import pandas as pd
import uuid
import logging
import sys
import os
from datetime import datetime, timedelta

# Fix imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# NOTE: log_anomaly import removed (Separation of Concerns)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_order_ids(df):
    """
    Generates new Order IDs so it looks like new incoming traffic.
    """
    df['order_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_latency_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting SLA Latency Injection (Stale Data) ---")

        # 1. READ: Get a batch of orders
        query = "SELECT * FROM orders LIMIT 100"
        df = pd.read_sql(query, conn)

        if df.empty:
            logging.error("Source table empty.")
            return

        # 2. TRANSFORM: Make them look like 'new' orders first
        df = generate_new_order_ids(df)

        # 3. CHAOS: The "Time Travel" Hack
        # We modify 'order_purchase_timestamp' to be 72 hours (3 days) in the past.
        # First, convert to datetime objects to do math
        df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
        
        # Subtract 3 days
        df['order_purchase_timestamp'] = df['order_purchase_timestamp'] - timedelta(days=3)
        
        # Convert back to string for SQLite storage
        df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype(str)

        # Calculate the "Lag" just for console logging
        max_lag_minutes = 3 * 24 * 60  # 4320 minutes

        logging.warning(f"Data Timestamp pushed back by 3 days. Lag is ~{max_lag_minutes} mins.")

        # 4. LOAD: Append to Bronze
        df.to_sql('bronze_orders', conn, if_exists='append', index=False)
        logging.info("Appended 'stale' batch to 'bronze_orders'.")

        # 5. REFLECTION:
        # Manual logging removed. The Detector will catch this SLA violation based on the 60 min threshold.

    except Exception as e:
        logging.error(f"Latency Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_latency_injection()