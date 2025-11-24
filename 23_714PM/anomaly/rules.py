# anomaly/rules.py
# Centralized configuration for all anomaly detection thresholds.

# Define a dictionary for each table that needs a rule.
# Each key represents the rule or metric being checked.

ANOMALY_RULES = {
    # --- 1. VOLUME CHECKS ---
    "bronze_order_items": {
        # Check: load_spike_volume.py injects a 50x spike
        "row_count_spike": {
            "max_rows": 2000,         # Max acceptable rows per batch (assuming normal is ~40)
            "severity": "CRITICAL"
        },
        # NEW RULE: Check for load_trend_shift.py (5x increase)
        # Detects if volume is elevated but not yet a massive spike
        "sustained_volume_shift": {
            "max_batch_rows": 200,    # If batch > 200, it's a shift (Normal ~100, Spike ~5000)
            "severity": "WARNING"
        },
        # Check: load_outlier_value.py injects a $1,000,000 price
        "price_outlier": {
            "column": "price",
            "max_value": 50000,       # Max acceptable price in BRL (e.g., R$50,000)
            "severity": "CRITICAL"
        }
    },
    
    "bronze_customers": {
        # Check: load_drop_volume.py injects only 5 rows
        "row_count_drop": {
            "min_rows": 10,           # Min acceptable rows per batch (assuming normal is ~40)
            "severity": "CRITICAL"
        }
    },
    
    # --- 2. DATA QUALITY CHECKS ---
    "bronze_products": {
        # Check: load_null_injection.py injects 40% NULLs in category
        "null_injection": {
            "column": "product_category_name",
            "max_null_percentage": 0.05,  # 5% maximum allowed NULLs
            "severity": "WARNING"
        }
    },
    
    "bronze_order_payments": {
        # Check: load_duplicates.py injects double payments
        "duplicates": {
            # Assumes a composite key check on ['order_id', 'payment_sequential']
            "max_duplicate_count": 0,
            "severity": "CRITICAL"
        },
        # NEW RULE: Check for load_deletion.py (Accidental Data Loss)
        "row_count_deletion": {
            "min_total_rows": 1000,   # If total table count drops below this, flag as data loss
            "severity": "CRITICAL"
        }
    },

    # --- 3. SLA / LATENCY CHECKS ---
    "bronze_orders": {
        # Check: load_late_data.py injects data from 3 days ago
        "data_latency": {
            "column": "order_purchase_timestamp",
            "max_latency_minutes": 60,  # Max acceptable delay between data time and load time
            "severity": "CRITICAL"
        }
    }
}