import os
import shutil
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
SOURCE_FILE = "~/…"
TARGET_FILE = "~/…"
MODEL_NAME = "..."

# --- SYSTEM INSTRUCTION ---
SYS_MSG = """..."""

def parse_ai_response(text):
    """
    Parses the structured response into a dictionary for the dataframe.
    """
    if not text:
        return {}
    
    data = {
        'spend_based_ef_cf': None,
        'ai_reasoning_sef': None,
        'activity_based_ef_cf': None,
        'ai_resoning_aef': None
    }
    
    # Regex to find values after the keys
    # We use re.DOTALL to capture multi-line methodology descriptions
    
    # 1. sb_cf (capture numbers only)
    sb_cf_match = re.search(r'\*sb_cf:\s*([0-9.,]+)', text)
    if sb_cf_match:
        try:
            # Clean number string (remove commas)
            clean_num = sb_cf_match.group(1).replace(',', '')
            data['spend_based_ef_cf'] = float(clean_num)
        except ValueError:
            pass

    # 2. sb_methodology_used
    # Capture everything until the next asterisk or end of string
    sb_meth_match = re.search(r'\*sb_methodology_used:\s*(.*?)(?=\*ab_cf|\Z)', text, re.DOTALL)
    if sb_meth_match:
        data['ai_reasoning_sef'] = sb_meth_match.group(1).strip()

    # 3. ab_cf
    ab_cf_match = re.search(r'\*ab_cf:\s*([0-9.,]+)', text)
    if ab_cf_match:
        try:
            clean_num = ab_cf_match.group(1).replace(',', '')
            data['activity_based_ef_cf'] = float(clean_num)
        except ValueError:
            pass

    # 4. ab_methodology_used
    ab_meth_match = re.search(r'\*ab_methodology_used:\s*(.*?)(?=$|\Z)', text, re.DOTALL)
    if ab_meth_match:
        data['ai_resoning_aef'] = ab_meth_match.group(1).strip()
        
    return data

def process_product(product_name):
    """
    Calls Gemini API for a single product.
    """
    if not product_name or pd.isna(product_name):
        return {}

    try:
        client = genai.Client(api_key=API_KEY)
        
        # Tools configuration
        tools = [
            types.Tool(google_search=types.GoogleSearch()),
            types.Tool(url_context=types.UrlContext())
        ]

        generate_content_config = types.GenerateContentConfig(
            temperature=0.7,
            system_instruction=[types.Part.from_text(text=SYS_MSG)],
            tools=tools,
            thinking_config=types.ThinkingConfig(thinking_level="HIGH") # Using reasoning to find factors
        )

        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[product_name],
            config=generate_content_config,
        )

        if response.text:
            return parse_ai_response(response.text)
        else:
            return {}

    except Exception as e:
        print(f"Error processing '{product_name}': {e}")
        return {}

def main():
    # 1. Duplicate File
    if not os.path.exists(SOURCE_FILE):
        print(f"Source file not found: {SOURCE_FILE}")
        return

    print(f"Duplicating {SOURCE_FILE} to {TARGET_FILE}...")
    shutil.copyfile(SOURCE_FILE, TARGET_FILE)

    # 2. Load the duplicated file
    df = pd.read_excel(TARGET_FILE)
    
    if "product_name" not in df.columns:
        print("Error: 'product_name' column not found in Excel file.")
        return

    print(f"Processing {len(df)} rows (10 at a time)...")

    # Storage for results {index: data_dict}
    results_map = {}

    # 3. Parallel Processing
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all tasks
        future_to_index = {
            executor.submit(process_product, row['product_name']): index 
            for index, row in df.iterrows()
        }

        # Process as they complete with progress bar
        for future in tqdm(as_completed(future_to_index), total=len(df), unit="product"):
            index = future_to_index[future]
            try:
                data = future.result()
                results_map[index] = data
            except Exception as e:
                print(f"Thread error on index {index}: {e}")
                results_map[index] = {}

    # 4. Update DataFrame
    # Initialize empty columns if they don't exist
    new_cols = ['spend_based_ef_cf', 'ai_reasoning_sef', 'activity_based_ef_cf', 'ai_resoning_aef']
    for col in new_cols:
        if col not in df.columns:
            df[col] = None

    # Fill data
    for index, data in results_map.items():
        if data:
            df.at[index, 'spend_based_ef_cf'] = data.get('spend_based_ef_cf')
            df.at[index, 'ai_reasoning_sef'] = data.get('ai_reasoning_sef')
            df.at[index, 'activity_based_ef_cf'] = data.get('activity_based_ef_cf')
            df.at[index, 'ai_resoning_aef'] = data.get('ai_resoning_aef')

    # 5. Save Result
    print(f"Saving results to {TARGET_FILE}...")
    df.to_excel(TARGET_FILE, index=False)
    print("Done.")

if __name__ == "__main__":
    main()
