import firebase_admin
from firebase_admin import credentials, firestore
import sys

# --- Configuration ---
# TODO: Replace this with the actual path to your service account key file.
SERVICE_ACCOUNT_KEY_PATH = "~/..."
PRODUCTS_COLLECTION = "products_new"
MATERIALS_COLLECTION = "materials"
# --------------------

def initialize_firebase():
    """Initializes the Firebase Admin SDK."""
    print("Initializing Firebase...")
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firebase initialized successfully.")
        return db
    except Exception as e:
        print(f"❌ Error initializing Firebase: {e}")
        print(f"Please check your service account key path: '{SERVICE_ACCOUNT_KEY_PATH}'")
        sys.exit(1)

def main():
    """Main function to run the data retrieval."""
    db = initialize_firebase()

    # 1. Ask the user for the Product Name
    try:
        uInput = input("Product Name: ").strip()
        if not uInput:
            print("Error: Product name cannot be empty.")
            return
    except KeyboardInterrupt:
        print("\nOperation cancelled.")
        sys.exit(0)

    # 2. Find the product document (pDoc)
    print(f"\nSearching for product: '{uInput}'...")
    product_query = db.collection(PRODUCTS_COLLECTION).where("name", "==", uInput).limit(1)
    pDocs = list(product_query.stream())

    if not pDocs:
        print(f"❌ Error: Product not found with name '{uInput}'.")
        return
    
    pDoc = pDocs[0]
    pDoc_data = pDoc.to_dict()
    pDoc_ref = pDoc.reference # Get the document reference for the next query

    # 3. Find all Tier 1 materials (mDocs) linked to this product
    materials_query = db.collection(MATERIALS_COLLECTION).where(
        "tier", "==", 1
    ).where(
        "linked_product", "==", pDoc_ref
    )
    mDocs = list(materials_query.stream())

    # 4. Print the results to the terminal
    print("\n" + "="*40)
    print(f"Product Name: {pDoc_data.get('name', 'N/A')}")

    if not mDocs:
        print("\nNo Tier 1 materials found for this product.")
    else:
        print(f"\nFound {len(mDocs)} Tier 1 material(s):")
        
        for i, doc in enumerate(mDocs, 1):
            data = doc.to_dict()
            
            # Safely get each field value, defaulting to 'N/A' if missing
            name = data.get('name', 'N/A')
            supplier = data.get('supplier_name', 'N/A')
            address = data.get('supplier_address', 'N/A')
            country = data.get('country_of_origin', 'N/A')

            print("\n" + "-"*20)
            print(f"Material_{i}: {name}")
            print(f"Supplier: {supplier}")
            print(f"Supplier Address: {address}")
            print(f"Country of Origin: {country}")
            print("-" * 20)
            
    print("="*40)


if __name__ == "__main__":
    main()
