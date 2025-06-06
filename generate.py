import os
from dotenv import load_dotenv
import fal_client

# Load environment variables from .env file
load_dotenv()

# Verify FAL_KEY is set
if not os.getenv('FAL_KEY'):
    raise ValueError("FAL_KEY environment variable is not set")

def on_queue_update(update):
    if isinstance(update, fal_client.InProgress):
        for log in update.logs:
           print(log["message"])

result = fal_client.subscribe(
    "fal-ai/flux-1/schnell",
    arguments={
        "prompt": "From that day on, Luna became the guardian of the enchanted forest, protecting its magic and beauty for all time."
    },
    with_logs=True,
    on_queue_update=on_queue_update,
)
print(result)