import requests
import json
import os
import sys
from dotenv import load_dotenv

def trigger_deep_research():
    url = "https://generativelanguage.googleapis.com/v1beta/interactions"
    prompt_file_path = "~/ecoze-firebase/dr-agent-prompt.txt"

    load_dotenv()

    # Retrieve the API key
    API_KEY = os.getenv("API_KEY")

    # Optional: Check if the key was loaded
    if not API_KEY:
        print("⚠️  Warning: 'API_KEY' not found in .env file.")

    
    # Read input from file
    if not os.path.exists(prompt_file_path):
        print(f"❌ Error: Prompt file not found at {prompt_file_path}")
        sys.exit(1)
        
    try:
        with open(prompt_file_path, 'r', encoding='utf-8') as f:
            prompt_input = f.read().strip()
            
        if not prompt_input:
            print(f"⚠️ Warning: Prompt file is empty.")
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)

    headers = {
        "x-goog-api-key": API_KEY,
        "Content-Type": "application/json"
    }
    
    payload = {
        "agent": "deep-research-pro-preview-12-2025",
        "input": prompt_input,
        "background": True
    }
    
    print(f"🚀 Sending POST request to: {url}")
    print(f"   Agent: {payload['agent']}")
    print(f"   Input Source: {prompt_file_path}")
    print(f"   Input Preview: {prompt_input[:100]}..." if len(prompt_input) > 100 else f"   Input: {prompt_input}")
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        
        print("\n--- Response ---")
        print(f"Status Code: {response.status_code}")
        
        try:
            # Try to print pretty JSON
            print(json.dumps(response.json(), indent=2))
        except json.JSONDecodeError:
            # Fallback for non-JSON response
            print(response.text)
            
    except Exception as e:
        print(f"❌ Error occurred: {e}")

if __name__ == "__main__":
    trigger_deep_research()
