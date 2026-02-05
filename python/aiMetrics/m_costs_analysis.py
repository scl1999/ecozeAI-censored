import firebase_admin
from firebase_admin import credentials, firestore
import sys
from datetime import datetime, timezone
from tqdm import tqdm

# --- Configuration ---
# TODO: Replace this with the actual path to your Firebase service account key file.
SERVICE_ACCOUNT_KEY_PATH = "~/..."
MATERIALS_COLLECTION = "materials"
# --------------------

def main():
    """
    Main function to initialize Firebase and run the calculation.
    """
    print("Initializing Firebase...")
    try:
        # Initialize the Firebase Admin SDK
        if not firebase_admin._apps:
            cred = credentials.Certificate(SERVICE_ACCOUNT_KEY_PATH)
            firebase_admin.initialize_app(cred)
        db = firestore.client()
        print("✅ Firebase initialized successfully.")
    except Exception as e:
        print(f"❌ Error initializing Firebase: {e}")
        print(f"Please check your service account key path: '{SERVICE_ACCOUNT_KEY_PATH}'")
        sys.exit(1)

    # --- Calculation Logic ---

    # 1. Define the query parameters
    # The start date is set to 11th September 2025, 00:00 UTC
    start_date = datetime(2025, 9, 11, 0, 0, 0, tzinfo=timezone.utc)

    print(f"\nFinding materials created after {start_date.strftime('%Y-%m-%d')} with 'apcfMaterials2_done' != True...")

    # Construct the Firestore query
    mDocs_query = db.collection(MATERIALS_COLLECTION).where(
        "createdAt", ">", start_date
    ).where(
        "apcfMaterials2_done", "!=", True
    )

    # Execute the query and convert to a list to show progress
    mDocs = list(mDocs_query.stream())

    if not mDocs:
        print("⚠️ No matching materials found. Exiting.")
        sys.exit(0)

    print(f"Found {len(mDocs)} matching materials.")

    # 2. Calculate the average of the 'totalCost' field
    costs = []
    # Use tqdm for a progress bar
    for doc in tqdm(mDocs, desc="Calculating costs"):
        # Use .get() to safely access the field, defaulting to 0 if it's missing
        cost = doc.to_dict().get("totalCost", 0)
        costs.append(cost)

    # Calculate the average, handling the case of an empty list
    averageCost = sum(costs) / len(costs) if costs else 0

    # 3. Print the final result
    print("\n--- Calculation Complete ---")
    # The :.4f formats the number to 4 decimal places
    print(f"Average Material Cost: $ {averageCost:.4f}")


if __name__ == "__main__":
    main()
