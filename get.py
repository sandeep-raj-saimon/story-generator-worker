import requests
import time
import os

FAL_KEY = 'c3d960c4-6b6f-4013-9808-60c0f659c8b0:0dce40b689a7ff59db807b4c5a4eb9e8'
request_id = "abb752dd-ee8e-4f35-80c4-ba0df412babd"  # Replace this with your actual request_id

# Endpoint for checking status
status_url = f"https://queue.fal.run/fal-ai/flux-1/requests/{request_id}"

# Headers
headers = {
    "Authorization": f"Key {FAL_KEY}"
}

# Polling loop
while True:
    response = requests.get(status_url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        status = data.get("status")
        print(f"Status: {status}")
        
        if status == "succeeded":
            image_url = data.get("outputs", {}).get("images", [{}])[0].get("url")
            print("Image generated:", image_url)
            break
        elif status in ["failed", "cancelled"]:
            print("Generation failed or cancelled.")
            break
        else:
            time.sleep(5)
    else:
        print("Error:", response.status_code, response.text)
        break
