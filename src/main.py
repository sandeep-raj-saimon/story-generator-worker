import os
from pathlib import Path
from handlers.pdf_generation_handler import PDFGenerationHandler
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).resolve().parent.parent
import sys
sys.path.append(str(project_root))

def main():
    load_dotenv()
    queue_url = os.getenv('STORY_GENERATION_QUEUE_URL')
    
    try:
        handler = PDFGenerationHandler()
        handler.start_listening(queue_url)
    except KeyboardInterrupt:
        print("Worker stopped by user")
    except Exception as e:
        print(f"Worker error: {str(e)}")

if __name__ == "__main__":
    main() 