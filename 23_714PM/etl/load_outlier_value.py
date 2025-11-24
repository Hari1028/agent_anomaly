import pandas as pd
import uuid
import logging
import sys
import os

# Fix imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import connection only. We remove the import for log_anomaly.
from db.connection import get_db_connection
# NOTE: The import 'from db.utils import log_anomaly' has been removed

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def generate_new_ids(df):
    # Function to anonymize/refresh IDs, ensuring the batch is unique.
    df['order_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
    return df

def run_outlier_injection():
    conn = get_db_connection()
    if not conn:
        return

    try:
        logging.info("--- Starting Outlier Injection (Pricing Glitch with JOIN) ---")
        
        # --- 1. READ & JOIN: Get and combine source data ---
        
        # Query 1: Get raw order items data
        query_items = "SELECT * FROM order_items LIMIT 100"
        df_items = pd.read_sql(query_items, conn)
        
        # Query 2: Get product category data for the join condition
        # We need the product_id and product_category_name
        query_products = "SELECT product_id, product_category_name FROM products"
        df_products = pd.read_sql(query_products, conn)
        
        if df_items.empty or df_products.empty:
            logging.error("One or both source tables are empty.")
            return

        # Perform the JOIN: Combine items with product categories
        df_merged = pd.merge(
            df_items, 
            df_products, 
            on='product_id', 
            how='left' # Left join to keep all items, even if product info is missing
        )

        # --- 2. TRANSFORM: Anonymize and Inject Conditional Chaos ---
        
        # Anonymize (as in the original script)
        df_merged = generate_new_ids(df_merged)

        # Define the conditional injection.
        # We will inject the outlier ONLY for products in a specific category (e.g., 'beleza_saude')
        target_category = 'beleza_saude' 
        outlier_value = 1000000.00
        
        # Find the index of the first row matching the target category
        target_index = df_merged[df_merged['product_category_name'] == target_category].index
        
        if not target_index.empty:
            # Inject the outlier price into the identified row
            df_merged.at[target_index[0], 'price'] = outlier_value
            logging.warning(f"Injected Conditional Outlier: Price set to ${outlier_value} for category '{target_category}'.")
        else:
            logging.warning(f"No items found for category '{target_category}'. Skipping outlier injection.")

        # --- 3. LOAD: Write ONLY the required columns to Bronze ---
        
        # Define columns that belong in the bronze_order_items table (Exclude product_category_name)
        bronze_columns = [col for col in df_items.columns if col in df_merged.columns]
        
        # Write the cleaned batch to Bronze
        df_merged[bronze_columns].to_sql('bronze_order_items', conn, if_exists='append', index=False)
        logging.info("Appended batch with JOIN-based conditional outliers to 'bronze_order_items'.")

        # --- 4. REFLECTION: Anomaly Logging is REMOVED ---
        # The original log_anomaly() call is intentionally removed here.
        # The responsibility for detection and logging is now with anomaly/detector.py.

    except Exception as e:
        logging.error(f"Outlier Injection Failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_outlier_injection()