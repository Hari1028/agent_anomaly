import sqlite3
import logging
import sys
import os

# Fix imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.connection import get_db_connection
# No log_anomaly import (Detector handles the observation)

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def run_deletion_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Deletion Injection (Accidental Data Loss) ---")

        cursor = conn.cursor()

        # 1. CHECK: Assess the current state of bronze_order_payments
        # We need to ensure the table exists and has data before we try to delete it.
        try:
            cursor.execute("SELECT COUNT(*) FROM bronze_order_payments")
            current_count = cursor.fetchone()[0]
        except sqlite3.OperationalError:
            logging.error("Table 'bronze_order_payments' does not exist. Run the other scripts first to populate data.")
            return

        if current_count == 0:
            logging.warning("Table 'bronze_order_payments' is already empty. Nothing to delete.")
            return

        # 2. CHAOS: The "Fat Finger" Delete
        # We will delete 40% of the existing data to simulate a catastrophic accidental deletion.
        rows_to_delete = int(current_count * 0.40)
        
        # Safety check: Ensure we delete at least 1 row if data exists
        if rows_to_delete < 1: 
            rows_to_delete = 1

        logging.info(f"Preparing to delete {rows_to_delete} rows (Current Total: {current_count}).")

        # Execute DELETE. 
        # In SQLite, we use the 'rowid' to target the most recently added rows (simulating a bad rollback or cleanup).
        delete_query = f"""
            DELETE FROM bronze_order_payments 
            WHERE rowid IN (
                SELECT rowid FROM bronze_order_payments 
                ORDER BY rowid DESC 
                LIMIT {rows_to_delete}
            )
        """
        
        cursor.execute(delete_query)
        conn.commit()
        
        # 3. VERIFY: Check the new count
        cursor.execute("SELECT COUNT(*) FROM bronze_order_payments")
        new_count = cursor.fetchone()[0]
        
        logging.warning(f"DATA LOSS EVENT SUCCESSFUL! Row count dropped from {current_count} to {new_count}.")

        # 4. REFLECTION:
        # No manual logging. 
        # The Detector will catch this because the total row count will likely fall below the expected threshold 
        # or (in a more advanced version) it detects the negative growth rate.

    except Exception as e:
        logging.error(f"Deletion Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_deletion_injection()