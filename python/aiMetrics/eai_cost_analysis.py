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
OUTPUT_FILE_PATH = "~/ecoze-firebase/cost-analysis.xlsx"
# --------------------

def initialize_firebase():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        sys.exit(1)

def fetch_and_analyze_costs(db):
    try:
        print("\n--- Starting Cost Analysis (Field: cfName) ---")
        
        # 1. Date Filter (Strict UTC)
        target_date = datetime(2025, 11, 21, 0, 0, 0, tzinfo=timezone.utc)
        print(f"Filtering documents created on or after: {target_date} (UTC)")

        products_ref = db.collection("products_new")
        materials_ref = db.collection("materials")

        # 2. Query Parents
        print("Querying parent documents...")
        # Using FieldFilter to avoid the UserWarning
        p_query = products_ref.where(filter=FieldFilter("createdAt", ">=", target_date)).stream()
        m_query = materials_ref.where(filter=FieldFilter("createdAt", ">=", target_date)).stream()

        parent_tasks = []
        
        # Collect Products
        for doc in p_query:
            parent_tasks.append((doc, "pn_tokens", "Product"))
            
        # Collect Materials
        for doc in m_query:
            parent_tasks.append((doc, "m_tokens", "Material"))

        total_parents = len(parent_tasks)
        print(f"Found {total_parents} matching parent documents.")

        if total_parents == 0:
            print("No parents found matching the date criteria. Exiting.")
            return

        # --- DIAGNOSTIC: Check Sub-collections ---
        # Checks the first document to ensure 'pn_tokens' or 'm_tokens' actually exists
        first_doc, expected_sub, doc_type = parent_tasks[0]
        print(f"\n--- DIAGNOSTIC: Inspecting first {doc_type} ({first_doc.id}) ---")
        actual_sub_cols = [c.id for c in first_doc.reference.collections()]
        
        if expected_sub not in actual_sub_cols:
            print(f"⚠️  WARNING: Expected sub-collection '{expected_sub}' NOT found.")
            print(f"   Actual sub-collections: {actual_sub_cols}")
        else:
            print(f"✅ Verified sub-collection '{expected_sub}' exists.")
        print("----------------------------------------------------------------\n")
        # -----------------------------------------

        all_token_data = []
        skipped_count = 0
        raw_token_count = 0

        print("Fetching sub-collection tokens...")
        
        # 3. Fetch Tokens
        for parent_doc, sub_col_name, _ in tqdm(parent_tasks, desc="Processing Parents"):
            tokens_ref = parent_doc.reference.collection(sub_col_name)
            tokens = tokens_ref.stream()

            for token in tokens:
                raw_token_count += 1
                token_data = token.to_dict()
                
                # --- CHANGE IS HERE ---
                # We now get 'cfName' from DB, but map it to 'cloudfunction' for the report
                c_name = token_data.get("cfName")
                t_cost = token_data.get("totalCost")

                if c_name is not None and t_cost is not None:
                    all_token_data.append({
                        "cloudfunction": str(c_name), # Map 'cfName' -> 'cloudfunction' column
                        "totalCost": float(t_cost)
                    })
                else:
                    skipped_count += 1

        print(f"\nDiagnostics:")
        print(f"Total tokens scanned: {raw_token_count}")
        print(f"Tokens skipped (missing 'cfName' or 'totalCost'): {skipped_count}")
        print(f"Valid tokens collected: {len(all_token_data)}")

        if not all_token_data:
            print("No valid tokens found. Please check if 'cfName' and 'totalCost' fields exist.")
            return

        # 4. Grouping & Averaging
        print("Calculating averages...")
        df = pd.DataFrame(all_token_data)

        # Group by the 'cloudfunction' column (which holds cfName data)
        grouped_df = df.groupby('cloudfunction', as_index=False)['totalCost'].mean()

        # Rename 'totalCost' to 'average_cost'
        grouped_df.rename(columns={'totalCost': 'average_cost'}, inplace=True)

        # 5. Export to Excel
        output_dir = os.path.dirname(OUTPUT_FILE_PATH)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        grouped_df.to_excel(OUTPUT_FILE_PATH, index=False)

        print("\n✅ Analysis Complete.")
        print(f"File created at: {OUTPUT_FILE_PATH}")
        print("-" * 30)
        print(grouped_df) # Preview results

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def main():
    db = initialize_firebase()
    fetch_and_analyze_costs(db)

if __name__ == "__main__":
    main()
