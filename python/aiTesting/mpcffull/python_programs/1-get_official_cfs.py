import os
import glob
import base64
import re
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from google import genai
from google.genai import types

from dotenv import load_dotenv

load_dotenv()

# Retrieve the API key
API_KEY = os.getenv("API_KEY")

# Optional: Check if the key was loaded
if not API_KEY:
    print("⚠️  Warning: 'API_KEY' not found in .env file.")

# --- CONFIGURATION ---
INPUT_DIR = "~/ecoze-firebase/ai-testing/mpcffull/documents/"
OUTPUT_FILE = "~/ecoze-firebase/ai-testing/mpcffull/results.xlsx"
MODEL_NAME = "aiModelPlaceholder"

def parse_ai_response(response_text):
    """
    Parses the structured text output from the AI into a list of dictionaries.
    Each dictionary represents one product configuration (one row in Excel).
    """
    products = []
    current_product = {}
    
    # Split text into lines and process
    lines = response_text.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Regex to find lines starting with "/" and containing "="
        # Matches: /key (Type) = value
        match = re.match(r'^/(\w+)(?:_\d+)?\s*(?:\(.*\))?\s*=\s*(.*)', line)
        
        if match:
            raw_key = match.group(1) # e.g., product_name (removes _1, _2 suffix handling)
            value = match.group(2).strip()
            
            # If we see a 'product_name' key and current_product is not empty, 
            # it implies the start of a new product block. Push the old one.
            if "product_name" in raw_key and current_product:
                products.append(current_product)
                current_product = {}
            
            # clean key: remove suffixes like _1, _2 if regex didn't catch specific numbering patterns
            # The regex `(\w+)` combined with `(?:_\d+)?` attempts to separate "product_name" from "_1"
            # So raw_key should be clean (e.g. "product_name")
            
            current_product[raw_key] = value

    # Append the last product found
    if current_product:
        products.append(current_product)
        
    return products

def process_document(file_path):
    """
    Sends a single document to Gemini and extracts data.
    """
    try:
        # Initialize Client
        client = genai.Client(api_key=API_KEY)

        # Read file bytes
        with open(file_path, "rb") as f:
            file_bytes = f.read()

        # Define the Prompt (System instructions + few-shot example included in config below)
        # Note: We structure the user prompt to mimic the requested example structure
        
        contents = [
            types.Content(
                role="user",
                parts=[
                    types.Part.from_bytes(
                        mime_type="application/pdf",
                        data=file_bytes 
                    ),
                    types.Part.from_text(text="Extract the carbon footprint data for all configurations in this document."),
                ],
            ),
        ]

        # Use the specific system instruction provided in your prompt
        generate_content_config = types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(
                thinking_level="HIGH",
            ),
            system_instruction=[
                types.Part.from_text(text="""Your job is to scan a document given to you and extract information. Output your answer in the exact following format and no other text (note, a document may contain multiple configurations for the product and you must give these separately):

/product_name_1 (String) = the name of the product (including brand name)
/total_cf_kg (Double) = the official total cradle-to-grave carbon footprint of the product which has been disclosed by the manufacturer (kgCO2e)
/materials_manufacturing_cf_percentage (Double) = the percentage of the products carbon footprint stemming from materials and manufacturing
/transportation_cf_percentage (Double) =  the percentage of the products carbon footprint stemming from transportation
/use_phase_cf_percentage (Double) = ...
/end_of_life_cf_percentage (Double) = ...
/url (String) = the url of the EPD where an AI got this information from
/cradle_to_gate_cf = the official cradle-to-gate carbon footprint of the product (kgCO2e)

[repeat for all other configurations if any]

/product_name_N (String) = the name of the product (including brand name)
/total_cf_kg (Double) = ...
[... same fields ...]
"""),
            ],
            temperature=1 # Low temperature for more deterministic data extraction
        )

        # Non-streaming call is easier for data extraction tasks
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=contents,
            config=generate_content_config,
        )

        # Extract text from response (concatenating parts if necessary)
        if response.text:
            return parse_ai_response(response.text)
        else:
            return []

    except Exception as e:
        print(f"\nError processing {os.path.basename(file_path)}: {e}")
        return []

def main():
    # 1. Find all PDF documents
    search_pattern = os.path.join(INPUT_DIR, "*.pdf")
    files = glob.glob(search_pattern)
    
    if not files:
        print(f"No PDF files found in {INPUT_DIR}")
        return

    print(f"Found {len(files)} documents. Processing...")

    all_extracted_rows = []

    # 2. Process in Parallel with tqdm progress bar
    # Adjust max_workers based on your rate limits (usually 5-10 is safe for standard keys)
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        future_to_file = {executor.submit(process_document, f): f for f in files}
        
        # Process as they complete
        for future in tqdm(as_completed(future_to_file), total=len(files), unit="doc"):
            data = future.result()
            if data:
                # Add source filename to data for traceability
                file_name = os.path.basename(future_to_file[future])
                for row in data:
                    row['source_file'] = file_name
                    all_extracted_rows.append(row)

    # 3. Write to Excel
    if all_extracted_rows:
        df_new = pd.DataFrame(all_extracted_rows)
        
        # Clean up column order if desired, or leave as is
        # Ensure numeric columns are actually numeric
        cols_to_numeric = [
            'total_cf_kg', 'materials_manufacturing_cf_percentage', 
            'transportation_cf_percentage', 'use_phase_cf_percentage', 
            'end_of_life_cf_percentage', 'cradle_to_gate_cf'
        ]
        for col in cols_to_numeric:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors='coerce')

        # Check if Excel exists to append, otherwise create new
        if os.path.exists(OUTPUT_FILE):
            print(f"Appending to existing file: {OUTPUT_FILE}")
            # Load existing
            try:
                df_existing = pd.read_excel(OUTPUT_FILE)
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            except Exception as e:
                print(f"Could not read existing Excel file (might be corrupt or empty). Creating new. Error: {e}")
                df_combined = df_new
        else:
            print(f"Creating new file: {OUTPUT_FILE}")
            df_combined = df_new
            
        # Save
        df_combined.to_excel(OUTPUT_FILE, index=False)
        print("Done successfully.")
    else:
        print("No data was extracted from any documents.")

if __name__ == "__main__":
    main()
