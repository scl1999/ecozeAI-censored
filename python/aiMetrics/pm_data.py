import firebase_admin
from firebase_admin import credentials, firestore
import sys
import pandas as pd
from tqdm import tqdm

# --- Configuration ---
# TODO: Replace this with the actual path to your Firebase service account key file.
SERVICE_ACCOUNT_KEY_PATH = "~/..."

PRODUCT_DATA_OUTPUT_PATH = "~/product_data.xlsx"
MATERIAL_DATA_OUTPUT_PATH = "~/materials_data.xlsx"
# --------------------

def export_pn_data(pDoc_ref):
    """Exports the /pn_data subcollection for a given product."""
    print(f"\nExporting data from /products_new/{pDoc_ref.id}/pn_data...")
    
    pn_data_ref = pDoc_ref.collection("pn_data")
    pdDocs = list(pn_data_ref.stream())

    if not pdDocs:
        print("No documents found in /pn_data subcollection. Skipping product data export.")
        return

    data_for_excel = []
    for doc in pdDocs:
        doc_data = doc.to_dict()
        row = {
            'type': doc_data.get('type'),
            'url': doc_data.get('url')
        }
        data_for_excel.append(row)
        
    df = pd.DataFrame(data_for_excel, columns=['type', 'url'])
    df.to_excel(PRODUCT_DATA_OUTPUT_PATH, index=False, engine='openpyxl')
    print(f"✅ Success! {len(pdDocs)} rows exported to {PRODUCT_DATA_OUTPUT_PATH}")


def export_m_data(db, pDoc_ref):
    """Finds all linked materials and exports their /m_data subcollections."""
    print(f"\nFinding materials linked to product {pDoc_ref.id}...")
    
    materials_query = db.collection("materials").where("linked_product", "==", pDoc_ref)
    mDocs = list(materials_query.stream())

    if not mDocs:
        print("No materials found linked to this product. Skipping material data export.")
        return
        
    print(f"Found {len(mDocs)} linked materials. Exporting their /m_data...")

    all_materials_data = []
    # Loop through each parent material document
    for doc in tqdm(mDocs, desc="Processing Materials"):
        mDoc_data = doc.to_dict()
        material_name = mDoc_data.get('name', 'Unnamed Material')
        
        # Get all documents from its 'm_data' subcollection
        m_data_ref = doc.reference.collection("m_data")
        mdDocs = list(m_data_ref.stream())

        for sub_doc in mdDocs:
            sub_doc_data = sub_doc.to_dict()
            row = {
                'material_name': material_name,
                'type': sub_doc_data.get('type'),
                'url': sub_doc_data.get('url')
            }
            all_materials_data.append(row)

    if not all_materials_data:
        print("No data found in any /m_data subcollections. Skipping material data export.")
        return

    df = pd.DataFrame(all_materials_data, columns=['material_name', 'type', 'url'])
    df.to_excel(MATERIAL_DATA_OUTPUT_PATH, index=False, engine='openpyxl')
    print(f"✅ Success! {len(all_materials_data)} rows exported to {MATERIAL_DATA_OUTPUT_PATH}")


def main():
    """Main function to initialize Firebase and run the exports."""
    print("Initializing Firebase...")
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        sys.exit(1)

    product_id_input = input("Product ID: ")
    if not product_id_input:
        print("Error: Product ID cannot be empty.")
        sys.exit(1)
        
    # Get product reference and check existence
    pDoc_ref = db.collection("products_new").document(product_id_input)
    if not pDoc_ref.get().exists:
        print(f"Error: Product with ID '{product_id_input}' not found.")
        return
        
    # Run both export functions
    export_pn_data(pDoc_ref)
    export_m_data(db, pDoc_ref)

if __name__ == "__main__":
    main()
