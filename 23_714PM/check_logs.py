import pandas as pd
from db.connection import get_db_connection

def view_logs():
    conn = get_db_connection()
    if not conn:
        print("❌ Could not connect to DB.")
        return

    print("\n--- 🔎 INSPECTING ANOMALY AUDIT LOG ---")
    
    # Read the last 10 logs
    query = """
    SELECT event_timestamp, source_table, anomaly_category, check_name, metric_value, severity 
    FROM anomaly_audit_log 
    ORDER BY log_id DESC 
    LIMIT 10
    """
    
    try:
        df = pd.read_sql(query, conn)
        if df.empty:
            print("⚠️ The log table is EMPTY. No anomalies detected yet.")
        else:
            print(df.to_string(index=False))
            print("\n✅ Success! Data found in logs.")
    except Exception as e:
        print(f"Error reading DB: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    view_logs()