import pandas as pd
import uuid
import logging
import sys
import os

# Fix imports to find 'db' folder
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# NOTE: log_anomaly import removed (Separation of Concerns)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_customer_ids(df):
    """
    Assigns new UUIDs to the customer_id column so they look like new users.
    """
    df['customer_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    # Olist has 'customer_unique_id' too, let's randomize that to be safe
    df['customer_unique_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_drop_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Volume DROP Injection (Missing Data) ---")

        # 1. READ: Attempt to read the standard batch size (e.g., 1000 rows)
        query = "SELECT * FROM customers LIMIT 1000"
        df_source = pd.read_sql(query, conn)

        if df_source.empty:
            logging.error("Source table empty.")
            return

        expected_volume = len(df_source) # We expected 1000

        # 2. CHAOS: Simulate the Drop
        # "Network failed after receiving only 5 packets"
        df_drop = df_source.head(5).copy()
        
        # 3. TRANSFORM: Anonymize IDs to look like new traffic
        df_drop = generate_new_customer_ids(df_drop)
        
        actual_volume = len(df_drop)
        logging.warning(f"Traffic Drop Injected! Expected {expected_volume} rows, but processing {actual_volume}.")

        # 4. LOAD: Append the tiny batch to Bronze
        df_drop.to_sql('bronze_customers', conn, if_exists='append', index=False)
        logging.info("Appended partial batch to 'bronze_customers'.")

        # 5. REFLECTION:
        # Manual logging removed. The Detector will catch this based on min_rows rule.

    except Exception as e:
        logging.error(f"Drop Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_drop_injection()