import firebase_admin
from firebase_admin import credentials, firestore
import sys

# --- Configuration ---
# TODO: Replace this with the actual path to your Firebase service account key file.
SERVICE_ACCOUNT_KEY_PATH = "~/..."
# --------------------

def trim_reasoning(text: str) -> str:
    """
    Trims a reasoning string to only include text after 'Response:'.
    Returns the original text if 'Response:' is not found.
    """
    if not isinstance(text, str) or "Response:" not in text:
        return text
    
    try:
        # Split the string at the first occurrence of 'Response:' and take the second part
        return text.split("Response:", 1)[1].strip()
    except IndexError:
        return "" # Return empty string if split fails unexpectedly

def generate_product_report(db, product_id):
    """
    Fetches a product, its materials, and their reasoning docs, and prints a detailed report.

    Args:
        db: The Firestore database client.
        product_id (str): The document ID of the product to report on.
    """
    try:
        # --- 1. Get the Product Document (pDoc) ---
        pDoc_ref = db.collection("products_new").document(product_id)
        pDoc = pDoc_ref.get()

        if not pDoc.exists:
            print(f"Error: Product with ID '{product_id}' not found.")
            return

        pDoc_data = pDoc.to_dict()

        # --- 2. Get Product Reasoning Documents (prDocs) ---
        pn_reasoning_ref = pDoc_ref.collection("pn_reasoning")
        
        reasoning_functions = ["apcfMPCFProcessing", "apcfMPCFFull", "apcfCFReview"]
        product_reasonings = {}
        for func in reasoning_functions:
            query_result = list(pn_reasoning_ref.where("cloudfunction", "==", func).limit(1).stream())
            if query_result:
                product_reasonings[func] = query_result[0].to_dict().get("reasoningOriginal", "")

        # --- 3. Find and Sort all Material Documents (mDocs) ---
        materials_query = db.collection("materials").where(
            "linked_product", "==", pDoc_ref
        )
        mDocs_snapshots = list(materials_query.stream())
        
        sorted_mDocs = sorted(
            mDocs_snapshots,
            key=lambda doc: doc.to_dict().get("estimated_cf", 0.0) or 0.0,
            reverse=True
        )

        # --- 4. Print the Report ---
        
        # --- Product Section ---
        print("\n")
        print(f"Product Name: {pDoc_data.get('name', '(not set)')}")
        print(f"Product Estimated CF (...): {pDoc_data.get('estimated_cf', '(not set)')}")
        print(f"Product CF Full (...): {pDoc_data.get('cf_full', '(not set)')}")
        print(f"Product Transport CF: {pDoc_data.get('transport_cf', '(not set)')}")
        print(f"Product Processing CF: {pDoc_data.get('cf_processing', '(not set)')}")

        # --- Product Reasoning Section ---
        for func in reasoning_functions:
            if func in product_reasonings:
                print(f"\n--- Reasoning for {func} ---")
                print(trim_reasoning(product_reasonings[func]))
        
        print("\n===============\n")

        # --- Materials Section ---
        if not sorted_mDocs:
            print("No materials are linked to this product.")
        else:
            for i, doc in enumerate(sorted_mDocs, 1):
                mDoc_data = doc.to_dict()
                
                print(f"Material {i} Name: {mDoc_data.get('name', '(not set)')}")
                print(f"Material {i} Tier: {mDoc_data.get('tier', '(not set)')}")
                print(f"Material {i} Estimated CF: {mDoc_data.get('estimated_cf', '(not set)')}")
                print(f"Material {i} CF Full: {mDoc_data.get('cf_full', '(not set)')}")

                # Get and print the reasoning for this material
                m_reasoning_ref = doc.reference.collection("m_reasoning")
                reasoning_result = list(m_reasoning_ref.where("cloudfunction", "==", "apcfMPCFFull").limit(1).stream())
                if reasoning_result:
                    reasoning_text = reasoning_result[0].to_dict().get("reasoningOriginal", "")
                    print("\nReasoning:")
                    print(trim_reasoning(reasoning_text))

                if i < len(sorted_mDocs):
                    print("\n-----\n")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        sys.exit(1)


def main():
    """
    Main function to initialize Firebase and run the report generator.
    """
    print("Initializing Firebase...")
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("Firebase initialized successfully.")
    except Exception as e:
        print(f"Error initializing Firebase: {e}")
        print(f"Please check your service account key path in the script.")
        sys.exit(1)

    product_id_input = input("Product ID: ")
    if not product_id_input:
        print("Error: Product ID cannot be empty.")
        sys.exit(1)
        
    generate_product_report(db, product_id_input)


if __name__ == "__main__":
    main()
