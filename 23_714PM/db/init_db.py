from connection import get_db_connection
import logging
import sqlite3

# Define the SQL for the Audit Table (Generic Structure - Option A)
CREATE_AUDIT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS anomaly_audit_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    source_table TEXT NOT NULL,       -- Which table had the issue (e.g. 'olist_orders')
    anomaly_category TEXT NOT NULL,   -- 'Volume', 'Data_Quality', 'Pipeline_SLA'
    check_name TEXT,                  -- e.g. 'null_check', 'row_count_spike'
    severity TEXT DEFAULT 'INFO',     -- 'INFO', 'WARNING', 'CRITICAL'
    metric_value REAL,                -- The actual number we measured (e.g., 500)
    threshold_value REAL,             -- The limit allowed (e.g., 50)
    meta_data TEXT,                   -- JSON string for the Agent (context)
    run_id TEXT                       -- Optional: to group logs by execution
);
"""

def init_tables():
    """
    Runs the DDL to create the audit table.
    """
    conn = get_db_connection()

    if conn is None:
        logging.error("Skipping table initialization due to connection failure.")
        return

    try:
        cursor = conn.cursor()
        
        # Execute the creation query
        logging.info("Attempting to create 'anomaly_audit_log' table...")
        cursor.execute(CREATE_AUDIT_TABLE_SQL)
        
        # Commit the changes (Save them)
        conn.commit()
        logging.info("Table 'anomaly_audit_log' created (or verified) successfully.")

    except sqlite3.Error as e:
        logging.error(f"Error creating table: {e}")
    
    finally:
        # Always close the connection, even if there is an error
        conn.close()
        logging.info("Database connection closed.")

# Allow running this file directly
if __name__ == "__main__":
    init_tables()