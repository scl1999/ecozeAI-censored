import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timezone
import sys
import os

# --- Configuration ---
SERVICE_ACCOUNT_KEY_PATH = "~/..."
OUTPUT_FILE_PATH = "~/ecoze-firebase/apcfSDCFReview2.xlsx"
COLLECTION_NAME = "products_new"
# --------------------

def initialize_firebase():
    """Initializes Firebase connection."""
    try:
        if not firebase_admin._apps:
            if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
                print(f"❌ Error: Service Account Key not found at {SERVICE_ACCOUNT_KEY_PATH}")
                sys.exit(1)
                
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        sys.exit(1)

def format_list_field(value):
    """Joins a list of strings with comma separation, handles non-lists gracefully."""
    if isinstance(value, list):
        return ", ".join([str(v) for v in value if v])
    return str(value) if value is not None else ""

def main():
    print("=== APCF & SDCF Review Export ===\n")

    # 1. Setup
    db = initialize_firebase()
    
    # Define Date Threshold: 17th November 2025, 00:00 UTC
    target_date = datetime(2025, 11, 17, 0, 0, 0, tzinfo=timezone.utc)
    
    print(f"📅 Filtering documents created on or after: {target_date} (UTC)")
    print(f"🔍 Scanning '{COLLECTION_NAME}' collection...")

    # 2. Query Firestore
    # We query by date first. We do NOT filter by supplier_cf != 0 in the query 
    # to avoid 'Inequality on different fields' index errors.
    docs_stream = db.collection(COLLECTION_NAME)\
        .where(filter=FieldFilter("createdAt", ">=", target_date))\
        .stream()

    export_data = []
    scanned_count = 0

    # 3. Process Documents
    for doc in tqdm(docs_stream, desc="Scanning Docs", unit="docs"):
        data = doc.to_dict()
        scanned_count += 1
        
        # Check supplier_cf logic:
        # Must exist AND be not equal to 0
        supplier_cf = data.get("supplier_cf")
        
        if supplier_cf is None or supplier_cf == 0:
            continue
            
        # Extract fields for Set 1
        name = data.get("name")
        oscf = data.get("oscf")
        socf_life = data.get("socf_lifecycle_stages")
        extra_info = data.get("extra_information")
        sdcf_stds = format_list_field(data.get("sdcf_standards"))
        sdcf_iso = data.get("sdcf_iso_aligned")
        
        # Extract fields for Set 2
        supplier_cf2 = data.get("supplier_cf2")
        oscf2 = data.get("oscf2")
        socf_life2 = data.get("socf_lifecycle_stages2")
        extra_info2 = data.get("extra_information2")
        sdcf_stds2 = format_list_field(data.get("sdcf_standards2"))
        sdcf_iso2 = data.get("sdcf_iso_aligned2")

        # Build Row
        row = {
            "name": name,
            "supplier_cf": supplier_cf,
            "oscf": oscf,
            "socf_lifecycle_stages": socf_life,
            "extra_information": extra_info,
            "sdcf_standards": sdcf_stds,
            "sdcf_iso_aligned": sdcf_iso,
            "supplier_cf2": supplier_cf2,
            "oscf2": oscf2,
            "socf_lifecycle_stages2": socf_life2,
            "extra_information2": extra_info2,
            "sdcf_standards2": sdcf_stds2,
            "sdcf_iso_aligned2": sdcf_iso2
        }
        
        export_data.append(row)

    print("\n" + "-"*40)
    print(f"Total Scanned (Date Match): {scanned_count}")
    print(f"Total Exported (Supplier CF Match): {len(export_data)}")
    print("-"*40 + "\n")

    # 4. Save to Excel
    if export_data:
        # Ensure directory exists
        output_dir = os.path.dirname(OUTPUT_FILE_PATH)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        print(f"💾 Saving to: {OUTPUT_FILE_PATH}...")
        df = pd.DataFrame(export_data)
        
        # Enforce column order as requested
        columns_order = [
            "name", "supplier_cf", "oscf", "socf_lifecycle_stages", 
            "extra_information", "sdcf_standards", "sdcf_iso_aligned",
            "supplier_cf2", "oscf2", "socf_lifecycle_stages2", 
            "extra_information2", "sdcf_standards2", "sdcf_iso_aligned2"
        ]
        
        # Reorder columns (safety check in case keys were added in different order)
        df = df[columns_order]
        
        df.to_excel(OUTPUT_FILE_PATH, index=False)
        print("✅ Export successful.")
    else:
        print("⚠️ No documents matched criteria. Excel file not created.")

if __name__ == "__main__":
    main()
