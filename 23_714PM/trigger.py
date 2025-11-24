import argparse
import sys
import os
import logging

# --- 1. Import Project Modules ---

# Add project root to path (This is correct for execution reliability)
# The project root is one level up from 'scripts'
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# Import ETL Injection functions (The 'Chaos' step)
from etl.load_spike_volume import run_spike_injection
from etl.load_drop_volume import run_drop_injection
from etl.load_null_injection import run_null_injection
from etl.load_duplicates import run_duplicate_injection
from etl.load_late_data import run_latency_injection
from etl.load_outlier_value import run_outlier_injection

# **NEW IMPORTS for Phase 1 Expansion**
from etl.load_deletion import run_deletion_injection
from etl.load_trend_shift import run_trend_shift_injection

# Import the Anomaly Detector (The 'Sidecar Observability' step)
from anomaly.detector import run_detector 

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main():
    parser = argparse.ArgumentParser(description="Chaos Injection Trigger")
    
    parser.add_argument(
        "--scenario", 
        type=str, 
        required=True,
        # Updated choices to include the new scenarios
        choices=[
            "spike", "drop", "null", "duplicate", "late", "outlier", 
            "deletion", "trend_shift", "all"
        ], 
        help="Choose which anomaly to inject into the pipeline."
    )

    args = parser.parse_args()

    print(f"\n--- TRIGGERING SCENARIO: {args.scenario.upper()} ---")

    # --- 2. ETL (Injection) Step ---
    if args.scenario == "spike":
        run_spike_injection()
    elif args.scenario == "drop":
        run_drop_injection()
    elif args.scenario == "null":
        run_null_injection()
    elif args.scenario == "duplicate":
        run_duplicate_injection()
    elif args.scenario == "late":
        run_latency_injection()
    elif args.scenario == "outlier":
        run_outlier_injection()
    elif args.scenario == "deletion":
        run_deletion_injection()
    elif args.scenario == "trend_shift":
        run_trend_shift_injection()
    elif args.scenario == "all":
        # Run all injections (Great for populating historical logs)
        run_spike_injection()
        run_null_injection()
        run_drop_injection()
        run_duplicate_injection()
        run_latency_injection()
        run_outlier_injection()
        run_deletion_injection()
        run_trend_shift_injection()
        
    print("--- INJECTION COMPLETE. STARTING DETECTION. ---")

    # --- 3. Detection Step ---
    # After the ETL injects the data, the detector immediately checks the Bronze layer
    run_detector()
    
    print("\n--- END-TO-END RUN COMPLETE. CHECK ANOMALY_AUDIT_LOG. ---\n")

if __name__ == "__main__":
    main()