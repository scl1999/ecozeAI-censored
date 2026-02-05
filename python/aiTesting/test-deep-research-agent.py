import os
import json
import requests
import sys
from dotenv import load_dotenv

load_dotenv()

# Retrieve the API key
API_KEY = os.getenv("API_KEY")

# Optional: Check if the key was loaded
if not API_KEY:
    print("⚠️  Warning: 'API_KEY' not found in .env file.")

PROMPT_FILE_PATH = "~/ecoze-firebase/deep-research-agent-prompt.txt"
API_URL = "https://generativelanguage.googleapis.com/v1beta/interactions?alt=sse"
AGENT_NAME = "deep-research-pro-preview-12-2025"
# ---------------------

def get_prompt():
    """Reads the prompt from the specified file path."""
    try:
        with open(PROMPT_FILE_PATH, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"Error: File not found at {PROMPT_FILE_PATH}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file: {e}")
        sys.exit(1)

def run_deep_research():
    prompt_text = get_prompt()
    print(f"--- Loaded Prompt ({len(prompt_text)} chars) ---\n")

    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": API_KEY
    }

    payload = {
        "input": prompt_text,
        "agent": AGENT_NAME,
        "background": True,  
        "stream": True,      
        "agent_config": {
            "type": "deep-research",
            "thinking_summaries": "auto" 
        }
    }

    print(">>> Initiating Deep Research Agent (Streaming Mode)...")
    print(">>> You will see every event object below.\n")
    
    try:
        response = requests.post(API_URL, headers=headers, json=payload, stream=True)
        response.raise_for_status()

        interaction_id = None
        
        # Process the stream line by line
        for line in response.iter_lines():
            if line:
                decoded_line = line.decode('utf-8')
                
                # Server-Sent Events always start with "data: "
                if decoded_line.startswith("data: "):
                    json_str = decoded_line[6:] 
                    
                    try:
                        data = json.loads(json_str)

                        # --- 1. PRINT RAW EVENT (Per your request) ---
                        # We print the compact JSON so you see exactly what is happening
                        print(f"[STREAM EVENT] {json.dumps(data)}")

                        # --- 2. EXTRACT INTERACTION ID ---
                        # Usually found in the 'interaction' object in the start event
                        if 'interaction' in data and 'id' in data['interaction']:
                            current_id = data['interaction']['id']
                            if interaction_id != current_id:
                                interaction_id = current_id
                                print(f"\n\033[94m[!] INTERACTION STARTED. ID: {interaction_id}\033[0m\n")

                        # --- 3. SIGNAL FINAL OUTPUT ---
                        # Check for the final text field indicating completion
                        # Note: The schema might nest this or send it as a distinct event type.
                        # We look for 'text' output or status 'completed'.
                        if 'text' in data and data['text']:
                            print("\n" + "="*20 + " \033[92mFINAL AGENT OUTPUT\033[0m " + "="*20 + "\n")
                            print(data['text'])
                            print("\n" + "="*60 + "\n")

                    except json.JSONDecodeError:
                        # Sometimes keep-alive signals or empty data lines occur
                        print(f"[RAW LINE] {decoded_line}")
                        
    except KeyboardInterrupt:
        print("\n\n[!] User interrupted the stream.")
    except Exception as e:
        print(f"\n[!] An error occurred: {e}")

if __name__ == "__main__":
    run_deep_research()
