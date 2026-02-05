import os
import re
import time
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

OUTPUT_FILE = "~/..."
API_KEY = "..."  # Hardcoded as requested

# Model Definitions
MODEL_GUIDANCE = "..."
MODEL_MAIN = "..."

# Limits
MAX_AUDIT_LOOPS = 2
PARALLEL_WORKERS = 10  # Process 10 rows at a time

# --- SYSTEM PROMPTS (CONSTANTS) ---

SYS_MSG_MPCFFULL_CORE = """..."""

SYS_MSG_GUIDANCE = """..."""

SYS_MSG_FLASH_AUDITOR = """..."""

# --- HELPER FUNCTIONS ---

def call_gemini(client, model, contents, config):
    """
    Helper to call the API and aggregate stream response with up to 3 retries.
    """
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        response_text = ""
        try:
            # Use stream as per examples
            for chunk in client.models.generate_content_stream(
                model=model,
                contents=contents,
                config=config,
            ):
                if chunk.text:
                    response_text += chunk.text
            return response_text
        except Exception as e:
            print(f"\n[Attempt {attempt} - Error calling {model}]: {e}")
            if attempt < max_retries:
                time.sleep(2)  # Short delay before retry
            else:
                print(f"Max retries reached for {model}. Returning None.")
                return None

def extract_cf_value(text):
    if not text:
        return None
    # 1. Sanitize (replace non-breaking space)
    sanitized_txt = text.replace('\u00A0', ' ')
    # 2. Regex Match: Handle optional asterisk and spaces more robustly
    match = re.search(r'(?:\*?\s*)cf_value\s*[:=]\s*([^\s\n\r]+)', sanitized_txt, re.IGNORECASE)
    if match:
        val_str = match.group(1)
        # Remove non-numeric chars except . e E - 
        clean_val = re.sub(r'[^\d.eE-]', '', val_str)
        try:
            return float(clean_val)
        except ValueError:
            return None
    return None

def process_product_logic(product_name, product_description=""):
    """
    Replicates the 'Guidance -> Pro-Flash-Pro auditor loop' logic.
    Tracks all reasoning history.
    """
    tqdm.write(f"\n>>> Starting processing for: {product_name}")
    client = genai.Client(api_key=API_KEY)
    history_log = []
    
    # ---------------------------------------------------------
    # STEP 0: Guidance AI (aiModelPlaceholder)
    # ---------------------------------------------------------
    guidance_user_msg = f"""Product Name: {product_name}
Product Description: {product_description}
"""
    
    tools_def = [
        types.Tool(url_context=types.UrlContext()),
        types.Tool(google_search=types.GoogleSearch()),
    ]
    
    guidance_config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_level="HIGH"),
        tools=tools_def,
        system_instruction=[types.Part.from_text(text=SYS_MSG_GUIDANCE)],
        temperature=1,
        max_output_tokens=65535,
    )
    
    guidance_response = call_gemini(client, MODEL_GUIDANCE, [types.Content(role="user", parts=[types.Part.from_text(text=guidance_user_msg)])], guidance_config)
    if not guidance_response: return "Error in Guidance step", None

    history_log.append("--- [STEP 0: GUIDANCE] ---\n" + guidance_response)

    user_msg = f"""Product Name: {product_name}
Product Description:
{product_description}

Guidance given:
{guidance_response}"""
    
    # ---------------------------------------------------------
    # STEP 1: Analyst (aiModelPlaceholder)
    # ---------------------------------------------------------
    
    pro_config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_level="HIGH"),
        tools=tools_def,
        system_instruction=[types.Part.from_text(text=SYS_MSG_MPCFFULL_CORE)],
        temperature=1,
        max_output_tokens=65535,
    )
    
    auditor_config = types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_level="HIGH"),
        tools=tools_def,
        system_instruction=[types.Part.from_text(text=SYS_MSG_FLASH_AUDITOR)],
        temperature=1,
        max_output_tokens=65535,
    )

    # 1a. Initial Call
    chat_history = [
        types.Content(role="user", parts=[types.Part.from_text(text=user_msg)])
    ]
    
    response_1 = call_gemini(client, MODEL_MAIN, chat_history, pro_config)
    if not response_1: return "Error in Step 1a", None
    
    history_log.append("--- [STEP 1a: ANALYST INITIAL] ---\n" + response_1)
    chat_history.append(types.Content(role="model", parts=[types.Part.from_text(text=response_1)]))

    # 1b. "Go Again" Prompt
    follow_up_prompt = '...'
    
    chat_history.append(types.Content(role="user", parts=[types.Part.from_text(text=follow_up_prompt)]))
    
    response_2 = call_gemini(client, MODEL_MAIN, chat_history, pro_config)
    if not response_2: return "Error in Step 1b", None
    
    history_log.append("--- [STEP 1b: ANALYST FOLLOW-UP] ---\n" + response_2)
    chat_history.append(types.Content(role="model", parts=[types.Part.from_text(text=response_2)]))
    
    current_answer = response_2 if "cf_value" in response_2 else response_1

    # ---------------------------------------------------------
    # STEP 2: The Auditor Loop (Flash checks Analyst)
    # ---------------------------------------------------------
    
    loop_count = 0
    stop_loop = False
    auditor_feedback = ""
    
    while loop_count < MAX_AUDIT_LOOPS and not stop_loop:
        loop_count += 1
        
        if loop_count > 1 and auditor_feedback:
            refine_prompt = f"User Feedback: {auditor_feedback}\n\n..."
            
            chat_history.append(types.Content(role="user", parts=[types.Part.from_text(text=refine_prompt)]))
            current_answer = call_gemini(client, MODEL_MAIN, chat_history, pro_config)
            history_log.append(f"--- [STEP 2: ANALYST REFINEMENT LOOP {loop_count}] ---\n" + current_answer)
            chat_history.append(types.Content(role="model", parts=[types.Part.from_text(text=current_answer)]))

        auditor_user_prompt = f"""
System Instructions given to the AI:
{SYS_MSG_MPCFFULL_CORE}

User Prompt:
{user_msg}

AI's Answer:
{current_answer}
"""
        if loop_count > 1:
            auditor_user_prompt = "The AI has had another go. Shown below is its response.\n" + auditor_user_prompt

        auditor_contents = [types.Content(role="user", parts=[types.Part.from_text(text=auditor_user_prompt)])]
        auditor_response = call_gemini(client, MODEL_MAIN, auditor_contents, auditor_config)
        
        history_log.append(f"--- [STEP 2: AUDITOR FEEDBACK LOOP {loop_count}] ---\n" + (auditor_response or "No response"))

        rating_match = re.search(r'\*rating:\s*(Pass|Refine)', auditor_response or "", re.IGNORECASE)
        rating = rating_match.group(1) if rating_match else "Pass"
        
        reasoning_match = re.search(r'\*rating_reasoning:\s*([\s\S]+)', auditor_response or "", re.IGNORECASE)
        reasoning = reasoning_match.group(1) if reasoning_match else "No reasoning."

        if rating.lower() == "pass":
            stop_loop = True
        else:
            auditor_feedback = reasoning

    # ---------------------------------------------------------
    # STEP 3: Final Refinement
    # ---------------------------------------------------------
    if not stop_loop and auditor_feedback:
        final_prompt = f"AI Auditor Feedback: {auditor_feedback}\n\n..."
        chat_history.append(types.Content(role="user", parts=[types.Part.from_text(text=final_prompt)]))
        current_answer = call_gemini(client, MODEL_MAIN, chat_history, pro_config)
        history_log.append("--- [STEP 3: FINAL ANALYST REFINEMENT] ---\n" + current_answer)

    # Use the extracted_val logic on current_answer
    extracted_val = extract_cf_value(current_answer)
    
    full_history_text = "\n\n".join(history_log)
    
    tqdm.write(f"<<< Finished processing for: {product_name} (Extracted: {extracted_val})")
    return full_history_text, extracted_val

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"File not found: {INPUT_FILE}")
        return

    df = pd.read_excel(INPUT_FILE)
    if "product_name" not in df.columns:
        print("Column 'product_name' missing.")
        return

    # Initialize columns if they don't exist
    if 'cf_ecozeAI' not in df.columns:
        df['cf_ecozeAI'] = ""
    if 'cf_value_extracted' not in df.columns:
        df['cf_value_extracted'] = None

    # Filter rows that need processing
    rows_to_process = []
    for idx, row in df.iterrows():
        # Skip if cf_value_extracted is already present (not NaN/None)
        if pd.notna(row.get('cf_value_extracted')):
            continue
        rows_to_process.append((idx, row))

    skipped = len(df) - len(rows_to_process)
    if skipped > 0:
        print(f"Skipping {skipped} already processed rows.")

    if not rows_to_process:
        print("All rows already processed. Nothing to do.")
        return

    print(f"Processing {len(rows_to_process)} rows with {PARALLEL_WORKERS} threads...")
    
    results = {} # index -> (text, val)

    with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
        future_map = {
            executor.submit(process_product_logic, row.get('product_name', ''), row.get('product_description', '')): idx
            for idx, row in rows_to_process
        }

        completed = 0
        total_to_process = len(rows_to_process)
        for future in tqdm(as_completed(future_map), total=total_to_process):
            idx = future_map[future]
            try:
                full_text, val = future.result()
                results[idx] = (full_text, val)
                # Update DF immediately for this row
                df.at[idx, 'cf_ecozeAI'] = full_text
                df.at[idx, 'cf_value_extracted'] = val
            except Exception as e:
                df.at[idx, 'cf_ecozeAI'] = f"Error: {e}"
                df.at[idx, 'cf_value_extracted'] = None

            completed += 1
            if completed % PARALLEL_WORKERS == 0 or completed == total_to_process:
                df.to_excel(OUTPUT_FILE, index=False)
                # print(f"\nSaved checkpoint at {completed} rows.")

    print(f"Done. Final results saved to {OUTPUT_FILE}.")

if __name__ == "__main__":
    main()
