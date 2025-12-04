import sqlite3
import logging
import json
from datetime import datetime, timedelta

# Import the necessary modules from your project structure
from db.connection import get_db_connection 
from db.utils import log_anomaly 
from anomaly.rules import ANOMALY_RULES 

# Configure Logging (Ensure it's set up for the script)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- HELPER FUNCTION (The Fix) ---
def table_exists(conn, table_name):
    """Checks if a table exists in the DB to avoid crashes on first run."""
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        return cursor.fetchone() is not None
    except sqlite3.Error:
        return False

# --- 1. DETECTION FUNCTIONS ---

def check_volume_anomalies(conn):
    """Checks for row count spikes, drops, deletions, and trend shifts."""
    logging.info("--- Running Volume Checks ---")
    
    # --- Check 1: Volume issues in bronze_order_items (Spike vs Trend Shift vs Normal) ---
    table = "bronze_order_items"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        
        # Fetch current count once for this table
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        current_row_count = cursor.fetchone()[0]
        
        spike_rule = rules.get("row_count_spike")
        shift_rule = rules.get("sustained_volume_shift")
        success_rule = rules.get("data_ingestion_success") # <--- NEW
        
        # Logic: Priority check. If it's a massive Spike, log Critical. 
        if spike_rule and current_row_count > spike_rule["max_rows"]:
            log_anomaly(
                conn, 
                source_table=table, 
                category="Volume", 
                check_name="row_count_spike", 
                severity=spike_rule["severity"], 
                metric_value=current_row_count, 
                threshold_value=spike_rule["max_rows"], 
                meta_data={"note": "CRITICAL: Batch exceeded max row count threshold."}
            )
        elif shift_rule and current_row_count > shift_rule["max_batch_rows"]:
            log_anomaly(
                conn, 
                source_table=table, 
                category="Volume", 
                check_name="sustained_volume_shift", 
                severity=shift_rule["severity"], 
                metric_value=current_row_count, 
                threshold_value=shift_rule["max_batch_rows"], 
                meta_data={"note": "WARNING: Volume is elevated (Trend Shift detected)."}
            )
        # NEW LOGIC: Check for Health/Success (Heartbeat)
        elif success_rule and (success_rule["min_safe_rows"] <= current_row_count <= success_rule["max_safe_rows"]):
             log_anomaly(
                conn, 
                source_table=table, 
                category="Volume", 
                check_name="data_ingestion_success", 
                severity=success_rule["severity"], 
                metric_value=current_row_count, 
                threshold_value=0, 
                meta_data={"note": "Healthy batch detected (Heartbeat)."}
            )
    else:
        logging.warning(f"Skipping check for {table}: Table not created yet.")

    # --- Check 2: Drop in bronze_customers ---
    table = "bronze_customers"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        drop_rule = rules.get("row_count_drop")

        if drop_rule:
            min_rows = drop_rule["min_rows"]
            severity = drop_rule["severity"]
            
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            current_row_count = cursor.fetchone()[0]
            
            if current_row_count < min_rows:
                log_anomaly(
                    conn, 
                    source_table=table, 
                    category="Volume", 
                    check_name="row_count_drop", 
                    severity=severity, 
                    metric_value=current_row_count, 
                    threshold_value=min_rows, 
                    meta_data={"note": "Batch dropped below min row count threshold."}
                )

    # --- Check 3: Deletion in bronze_order_payments (Data Loss) ---
    table = "bronze_order_payments"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        deletion_rule = rules.get("row_count_deletion")

        if deletion_rule:
            min_total = deletion_rule["min_total_rows"]
            severity = deletion_rule["severity"]

            cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
            current_row_count = cursor.fetchone()[0]

            if current_row_count < min_total:
                log_anomaly(
                    conn,
                    source_table=table,
                    category="Volume", 
                    check_name="row_count_deletion",
                    severity=severity,
                    metric_value=current_row_count,
                    threshold_value=min_total,
                    meta_data={"note": "CRITICAL: Significant data loss detected."}
                )


def check_data_quality_anomalies(conn):
    """Checks for nulls, duplicates, and outlier values."""
    logging.info("--- Running Data Quality Checks ---")

    # Check 4: Null Injection in bronze_products
    table = "bronze_products"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        null_rule = rules.get("null_injection")

        if null_rule:
            column = null_rule["column"]
            max_null_pct = null_rule["max_null_percentage"]
            severity = null_rule["severity"]
            
            # Calculate the null percentage
            query = f"""
                SELECT 
                    CAST(SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS REAL) * 100 / COUNT(*),
                    COUNT(*)
                FROM {table}
            """
            cursor = conn.execute(query)
            result = cursor.fetchone()
            
            if result and result[1] > 0:
                null_percent = result[0] if result[0] is not None else 0.0
                total_rows = result[1]
                null_percent_as_ratio = null_percent / 100 
                
                if null_percent_as_ratio > max_null_pct:
                    log_anomaly(
                        conn, 
                        source_table=table, 
                        category="Data_Quality", 
                        check_name="null_injection_check", 
                        severity=severity, 
                        metric_value=null_percent_as_ratio, 
                        threshold_value=max_null_pct, 
                        meta_data={"column": column, "total_rows": total_rows}
                    )

    # Check 5: Duplicates in bronze_order_payments
    table = "bronze_order_payments"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        dup_rule = rules.get("duplicates")

        if dup_rule:
            max_dups = dup_rule["max_duplicate_count"]
            severity = dup_rule["severity"]

            query = f"SELECT COUNT(order_id) - COUNT(DISTINCT order_id) FROM {table}"
            cursor = conn.execute(query)
            duplicate_count = cursor.fetchone()[0]
            
            if duplicate_count > max_dups:
                log_anomaly(
                    conn, 
                    source_table=table, 
                    category="Data_Quality", 
                    check_name="duplicate_payments_check", 
                    severity=severity, 
                    metric_value=duplicate_count, 
                    threshold_value=max_dups, 
                    meta_data={"note": "Detected excess duplicates based on order_id key."}
                )

    # Check 6: Outlier Value in bronze_order_items
    table = "bronze_order_items"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        outlier_rule = rules.get("price_outlier")

        if outlier_rule:
            column = outlier_rule["column"]
            max_value = outlier_rule["max_value"]
            severity = outlier_rule["severity"]

            query = f"SELECT COUNT(*) FROM {table} WHERE {column} > {max_value}"
            cursor = conn.execute(query)
            outlier_count = cursor.fetchone()[0]
            
            if outlier_count > 0:
                log_anomaly(
                    conn, 
                    source_table=table, 
                    category="Data_Quality", 
                    check_name="price_outlier_check", 
                    severity=severity, 
                    metric_value=outlier_count, 
                    threshold_value=max_value, 
                    meta_data={"column": column, "note": f"Found {outlier_count} records above ${max_value}."}
                )

# --- 2. PIPELINE SLA CHECK ---

def check_sla_anomalies(conn):
    """Checks for data latency/staleness."""
    logging.info("--- Running SLA Checks ---")
    
    # Check 7: Latency/Staleness in bronze_orders
    table = "bronze_orders"
    if table_exists(conn, table):
        rules = ANOMALY_RULES.get(table, {})
        latency_rule = rules.get("data_latency")
        
        if latency_rule:
            column = latency_rule["column"]
            max_latency_minutes = latency_rule["max_latency_minutes"]
            severity = latency_rule["severity"]
            
            current_time = datetime.now()
            latency_threshold = current_time - timedelta(minutes=max_latency_minutes)
            
            query = f"SELECT MIN({column}) FROM {table}"
            cursor = conn.execute(query)
            result = cursor.fetchone()
            
            if result and result[0]:
                try:
                    clean_ts = result[0].split('.')[0]
                    oldest_data_time = datetime.strptime(clean_ts, '%Y-%m-%d %H:%M:%S')
                    
                    if oldest_data_time < latency_threshold:
                        actual_latency_minutes = (current_time - oldest_data_time).total_seconds() / 60
                        
                        log_anomaly(
                            conn, 
                            source_table=table, 
                            category="SLA", 
                            check_name="data_latency_check", 
                            severity=severity, 
                            metric_value=actual_latency_minutes, 
                            threshold_value=max_latency_minutes, 
                            meta_data={"oldest_data_timestamp": result[0]}
                        )
                except ValueError as e:
                    logging.warning(f"Could not parse timestamp '{result[0]}' for SLA check: {e}")

# --- 3. MAIN EXECUTION ---

def run_detector():
    """Main function to execute all anomaly checks."""
    logging.info("Starting Anomaly Detector Run...")
    
    conn = get_db_connection()
    if conn is None:
        logging.error("Detector failed to run: Database connection is unavailable.")
        return

    try:
        # Run all check groups
        check_volume_anomalies(conn)
        check_data_quality_anomalies(conn)
        check_sla_anomalies(conn)
        
    except Exception as e:
        logging.critical(f"A major error occurred during detection: {e}")
        
    finally:
        conn.close()
        logging.info("Anomaly Detector Run Complete.")

# Entry point for the script
if __name__ == "__main__":
    run_detector()