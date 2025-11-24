import pandas as pd
import uuid
import logging
import sys
import os

# Fix imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# NOTE: log_anomaly import removed (Separation of Concerns)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_payment_ids(df):
    """
    Generates new Order IDs so the batch looks like NEW transactions.
    """
    # We assume 1 payment per order for simplicity in this UUID generation
    df['order_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_duplicate_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Duplicate Injection (Double Payment) ---")

        # 1. READ: Get a normal batch of payments
        query = "SELECT * FROM order_payments LIMIT 500"
        df_source = pd.read_sql(query, conn)

        if df_source.empty:
            logging.error("Source table empty.")
            return

        # 2. TRANSFORM: Prepare a valid "new" batch first
        df_clean = generate_new_payment_ids(df_source.copy())

        # 3. CHAOS: The "Retry Error"
        # We concatenate the clean data with ITSELF.
        # Result: 500 unique IDs, but 1000 total rows. Every row has a twin.
        df_duped_batch = pd.concat([df_clean, df_clean], ignore_index=True)

        # Calculate metrics just for console logging
        total_rows = len(df_duped_batch)
        duplicate_count = total_rows - df_duped_batch['order_id'].nunique()
        
        logging.warning(f"Injection Prepared: {total_rows} rows, containing {duplicate_count} duplicates.")

        # 4. LOAD: Append the messy batch to Bronze
        df_duped_batch.to_sql('bronze_order_payments', conn, if_exists='append', index=False)
        logging.info("Appended duplicate batch to 'bronze_order_payments'.")

        # 5. REFLECTION:
        # Manual logging removed. The Detector will catch this using the uniqueness_check.

    except Exception as e:
        logging.error(f"Duplicate Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_duplicate_injection()