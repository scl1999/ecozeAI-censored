import pandas as pd
import requests
import json
import os
import sys
import time
from pathlib import Path
import concurrent.futures
from dotenv import load_dotenv

load_dotenv()

# Retrieve the API key
API_KEY = os.getenv("API_KEY")

# Optional: Check if the key was loaded
if not API_KEY:
    print("⚠️  Warning: 'API_KEY' not found in .env file.")
    
INPUT_FILE = "~/ecoze-firebase/eai-testing/mpcffull/pcf_testing2.xlsx"
MODEL_NAME = "aiModelPlaceholder" 
BATCH_SIZE = 10

# Endpoint
URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_NAME}:generateContent?key={API_KEY}"

def call_gemini(prompt, system_instruction):
    """
    Calls the Gemini API with the specified parameters.
    """
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "systemInstruction": {
            "parts": [{"text": system_instruction}]
        },
        "tools": [
            {"google_search": {}}  # Enable Google Search Grounding
        ],
        "generationConfig": {
            "temperature": 1,
            "maxOutputTokens": 65535,
            # Note: 'thinking_config' is experimental. 
            # We map "Thinking Level = high" to include thoughts if supported by the model version.
            # If the model rejects this param, remove the next line.
            # "thinkingConfig": {"include_thoughts": True} 
        }
    }
    
    try:
        response = requests.post(URL, headers=headers, json=payload)
        
        if response.status_code != 200:
            print(f"   ❌ API Error ({response.status_code}): {response.text[:200]}")
            return None
            
        result = response.json()
        
        # Extract text from the first candidate
        try:
            return result["candidates"][0]["content"]["parts"][0]["text"]
        except (KeyError, IndexError):
            print(f"   ❌ Unexpected response format: {str(result)[:200]}")
            return None
            
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
        return None

def parse_step_1_output(text):
    """Parses: Description: [text]"""
    if not text:
        return None
    
    marker = "Description:"
    idx = text.find(marker)
    if idx != -1:
        clean_text = text[idx + len(marker):].strip()
        # Remove quotes if the AI added them unnecessarily
        if clean_text.startswith('"') and clean_text.endswith('"'):
            clean_text = clean_text[1:-1].strip()
        return clean_text
    return text.strip()

def parse_step_2_output(text):
    """
    Parses:
    *pass_or_fail: [Pass/Fail]
    *description: [text]
    Returns: (status, description)
    """
    if not text:
        return None, None
        
    status = None
    description = None
    
    # Simple line parsing
    lines = text.split('\n')
    for line in lines:
        line = line.strip()
        if line.lower().startswith("*pass_or_fail:"):
            val = line.split(":", 1)[1].strip().lower()
            if "pass" in val:
                status = "Pass"
            else:
                status = "Fail"
        elif line.lower().startswith("*description:"):
            description = line.split(":", 1)[1].strip()
            # Handle quotes
            if description.startswith('"') and description.endswith('"'):
                description = description[1:-1].strip()
                
    return status, description

def process_row(index, row):
    """
    Processes a single row: Step 1 (Research) -> Step 2 (Fact Check).
    Returns (index, final_description)
    """
    product_name = str(row["product_name"])
    
    # Skip if empty name
    if not product_name or product_name.lower() == "nan":
        return index, None

    print(f"   ⏳ Processing: {product_name}...")
    
    # --- Step 1: Research ---
    prompt_1 = product_name
    sys_1 = """Your job is to take in a product name and research the product. You will be constructing a detailed description of what the product is and does, including what it is made from.

!! You MUST use your google search and url context tools to ground your answer on the most up to date information. !!

Output your answer in the exact following format and no other text:
"
Description: 
[Set to "..." if the description passed. If the description didnt pass, output the complete new description here.]
"
""" 
    
    desc_raw = call_gemini(prompt_1, sys_1)
    desc_1 = parse_step_1_output(desc_raw)
    
    if not desc_1:
        print(f"   ⚠️  [{product_name}] Step 1 produced no output.")
        return index, None

    # --- Step 2: Fact Check ---
    prompt_2 = f"""
Product Name:
{product_name}

Description given by the AI:
{desc_1}
"""
    sys_2 = """Your job is to take in a product name and a description of the product. You must fact check the description and create a new description where necessary.

!! You MUST use your google search and url context tools to ground your answer on the most up to date information. !!

Output your answer in the exact following format and no other text:
"
*pass_or_fail: [Set as "Pass" and no other text if the description doesnt need changing, set as "Fail" if the description needs changing.]
*description: [Set to "..." if the description passed. If the description didnt pass, output the complete new description here.]
"
"""
    check_raw = call_gemini(prompt_2, sys_2)
    status, desc_2 = parse_step_2_output(check_raw)
    
    final_desc = ""
    
    if status == "Pass":
        print(f"   ✅ [{product_name}] Validated (Pass).")
        final_desc = desc_1
    elif status == "Fail":
        print(f"   🔄 [{product_name}] Correction needed (Fail). Updating.")
        # If desc_2 is "..." or empty, we fall back to desc_1, otherwise use desc_2
        if desc_2 and desc_2 != "...":
            final_desc = desc_2
        else:
            final_desc = desc_1
    else:
        print(f"   ⚠️  [{product_name}] Unclear status '{status}'. Defaulting to original.")
        final_desc = desc_1
        
    return index, final_desc

def main():
    print(f"=== PCF Description Generator ({MODEL_NAME}) ===\n")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Error: File not found at {INPUT_FILE}")
        sys.exit(1)
        
    print(f"📖 Reading {INPUT_FILE}...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"❌ Error reading Excel: {e}")
        sys.exit(1)
        
    if "product_name" not in df.columns:
        print("❌ Error: 'product_name' column missing.")
        sys.exit(1)
        
    # Ensure output column exists
    if "product_description" not in df.columns:
        df["product_description"] = ""

    total_rows = len(df)
    print(f"✅ Loaded {total_rows} rows.")
    
    # Process in batches using ThreadPoolExecutor
    # We use BATCH_SIZE workers to run them in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
        
        # Iterate over the dataframe in chunks of BATCH_SIZE
        for start_idx in range(0, total_rows, BATCH_SIZE):
            end_idx = min(start_idx + BATCH_SIZE, total_rows)
            print(f"\n🚀 Starting batch: Rows {start_idx + 1} to {end_idx}...")
            
            # Create a list of futures for the current batch
            futures = []
            
            # We iterate by index to ensure we can update the correct row in the DF later
            # Note: df.index might not be 0..N if filtered, but iloc works by position
            # To be safe, we'll iterate over the actual indices in this slice
            batch_indices = df.index[start_idx:end_idx]
            
            for idx in batch_indices:
                row = df.loc[idx]
                futures.append(executor.submit(process_row, idx, row))
            
            # Wait for all futures in the batch to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    res_idx, res_desc = future.result()
                    if res_desc is not None:
                        df.at[res_idx, "product_description"] = res_desc
                except Exception as exc:
                    print(f"   ❌ A thread generated an exception: {exc}")
            
            # Save after each batch
            print(f"   💾 Saving batch progress to {INPUT_FILE}...")
            df.to_excel(INPUT_FILE, index=False)

    # Final Save (redundant but safe)
    print(f"\n💾 Saving final changes to {INPUT_FILE}...")
    df.to_excel(INPUT_FILE, index=False)
    print("✅ Process Complete.")

if __name__ == "__main__":
    main()