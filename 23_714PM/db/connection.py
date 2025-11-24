import sqlite3
import logging
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables (should be at the very top of the execution flow)
load_dotenv()

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- Define Project Root and DB Path ---
def get_db_path() -> Path | None:
    """
    Calculates the absolute path to the SQLite database file.
    Assumes:
    1. This script (connection.py) is inside a 'db' directory.
    2. The database file (e.g., olist.sqlite) is in the parent directory (project root).
    """
    # 1. Get the path object for THIS file
    current_file_path = Path(__file__).resolve()
    
    # 2. Go up one level to the project root directory
    project_root = current_file_path.parent.parent
    
    # 3. Get filename from .env
    db_filename = os.getenv("DB_PATH")
    
    if not db_filename:
        logging.error("CRITICAL: DB_PATH not set in .env")
        return None
    
    # 4. Join the root and filename
    full_db_path = project_root / db_filename
    
    return full_db_path

# --- Database Connection Function ---
def get_db_connection():
    """Establishes and returns a SQLite connection."""
    
    full_db_path = get_db_path()
    
    if not full_db_path:
        return None
    
    # Debug print (Use .as_posix() for clean path string)
    print(f"\n--- DEBUG: Calculated DB Path: {full_db_path.as_posix()} ---\n")

    # Check existence
    if not full_db_path.exists():
        logging.error(f"CRITICAL: Database file not found at: {full_db_path.as_posix()}")
        return None

    try:
        # Use str(full_db_path) for sqlite3.connect
        conn = sqlite3.connect(str(full_db_path))
        conn.row_factory = sqlite3.Row
        logging.info("Successfully connected to Olist DB.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Connection failed: {e}")
        return None
    
# --- Test Block ---
# This block runs ONLY when you execute this script directly (e.g., python db/connection.py)
if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        print("\n✅ CONNECTION TEST SUCCESSFUL: Connection object received.")
        conn.close()
        print("Connection closed.")
    else:
        print("\n❌ CONNECTION TEST FAILED. Check logs for CRITICAL errors.")