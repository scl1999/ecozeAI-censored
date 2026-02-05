#!/usr/bin/env python3
"""
Compute stats for a given product’s materials and transport records,
showing progress with tqdm.

1. Prompts for a Product ID (document in /products_new/).
2. Counts:
   - Total materials linked to that product.
   - Total materials_transport sub-docs across those materials.
   - Of those transports, how many have emissions_kgco2e set.
   - Of those materials, how many have supplier_name set (not "Unknown").
   - Of those materials, how many have supplier_address set (not "Unknown").
   - Of those materials, how many have estimated_cf set (not null).
3. Prints the results.
"""

import sys
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, firestore
from tqdm import tqdm

# ─── Configuration ─────────────────────────────────────────────────────────────

SERVICE_ACCOUNT_JSON = "/home/ecoze/..."
PRODUCTS_COLL        = "products_new"
MATERIALS_COLL       = "materials"
TRANSPORT_SUBCOLL    = "materials_transport"

# ─── Initialization ────────────────────────────────────────────────────────────

def init_firestore():
    if not firebase_admin._apps:
        cred = credentials.Certificate(SERVICE_ACCOUNT_JSON)
        firebase_admin.initialize_app(cred)
    return firestore.client()

# ─── Main Logic ─────────────────────────────────────────────────────────────────

def main():
    db = init_firestore()

    # Prompt for the product ID
    product_id = input("Product ID: ").strip()
    if not product_id:
        print("ERROR: No Product ID provided.", file=sys.stderr)
        sys.exit(1)
    p_ref = db.collection(PRODUCTS_COLL).document(product_id)

    # 1. Materials linked to this product
    print("Querying materials linked to product...")
    m_snaps = list(
        db.collection(MATERIALS_COLL)
          .where("linked_product", "==", p_ref)
          .stream()
    )
    t = len(m_snaps)

    # Initialize counters
    mt  = 0    # total materials_transport docs
    mtc = 0    # those with emissions_kgco2e
    msn = 0    # materials with supplier_name set
    msa = 0    # materials with supplier_address set
    mc  = 0    # materials with estimated_cf set

    # 2-6. Iterate materials with progress bar
    for m_snap in tqdm(m_snaps, desc="Materials", unit="doc"):
        m_data = m_snap.to_dict() or {}

        # supplier_name
        name = str(m_data.get("supplier_name") or "").strip()
        if name and name.lower() != "unknown":
            msn += 1

        # supplier_address
        addr = str(m_data.get("supplier_address") or "").strip()
        if addr and addr.lower() != "unknown":
            msa += 1

        # estimated_cf
        if m_data.get("estimated_cf") is not None:
            mc += 1

        # materials_transport subcollection
        t_docs = list(m_snap.reference
                          .collection(TRANSPORT_SUBCOLL)
                          .stream())
        mt += len(t_docs)

        # Count emissions_kgco2e within transport docs, with inner progress
        for t_snap in tqdm(
                t_docs,
                desc=f"Transports of {m_snap.id}",
                unit="subdoc",
                leave=False
        ):
            t_data = t_snap.to_dict() or {}
            if t_data.get("emissions_kgco2e") is not None:
                mtc += 1

    # 7. Print results
    print("\n====== Stats ======\n")
    print(f"Total # Materials Docs: {t}\n")
    print("-" * 80 + "\n")
    print(f"# Materials Transport Docs: {mt}")
    print(f"# MT Calcs: {mtc}")
    print(f"# MT SN: {msn}")
    print(f"# MT SA: {msa}")
    print(f"# MT MPCF: {mc}")

if __name__ == "__main__":
    main()
