import json
import logging
import sqlite3

# Initialize logging if it hasn't been done in the main script
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def log_anomaly(conn, source_table: str, category: str, check_name: str, 
                severity: str, metric_value: float, threshold_value: float, 
                meta_data: dict | str | None = None):
    """
    Inserts a record into the centralized anomaly_audit_log table.
    
    Args:
        conn: The active SQLite database connection object.
        source_table: The Bronze table where the issue occurred (e.g., 'bronze_orders').
        category: The anomaly type ('Volume', 'Data_Quality', 'SLA').
        check_name: Specific check name (e.g., 'row_count_spike').
        severity: Impact level ('INFO', 'WARNING', 'CRITICAL').
        metric_value: The measured value that triggered the alert.
        threshold_value: The defined limit that was violated.
        meta_data: Optional dictionary or JSON string for LLM context.
    """
    try:
        # 1. Ensure meta_data is a string (JSON) or None
        if isinstance(meta_data, dict):
            meta_data_str = json.dumps(meta_data)
        elif meta_data is None:
            meta_data_str = None
        else:
            # Assume it's already a string if not a dict or None
            meta_data_str = meta_data
            
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO anomaly_audit_log 
        (source_table, anomaly_category, check_name, severity, metric_value, threshold_value, meta_data)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        # 2. Execute with parameters
        cursor.execute(sql, (
            source_table, 
            category, 
            check_name, 
            severity, 
            metric_value, 
            threshold_value, 
            meta_data_str
        ))
        conn.commit()
        
        # 3. Enhanced Logging (using schema names)
        logging.info(f"Anomaly Logged | Table: {source_table} | Check: {check_name} | Value: {metric_value}")

    except sqlite3.Error as e:
        # Log the failure but allow the detector script to continue if possible
        logging.error(f"FATAL LOGGING ERROR: Failed to insert anomaly into audit table. Error: {e}")
    except Exception as e:
        # Catch unexpected errors like JSON serialization issues
        logging.error(f"UNEXPECTED ERROR in log_anomaly: {e}")