# WhisprTales Worker

A worker service that processes PDF generation requests from SQS and generates PDFs for stories.

## Setup

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

## Configuration

Update the `.env` file with your AWS credentials and configuration:

- `AWS_ACCESS_KEY_ID`: Your AWS access key
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret key
- `AWS_S3_REGION_NAME`: AWS region (e.g., ap-south-1)
- `AWS_STORAGE_BUCKET_NAME`: S3 bucket name for PDF storage
- `WHISPR_TALES_QUEUE_URL`: SQS queue URL
- `API_BASE_URL`: Base URL of your API
- `API_TOKEN`: API authentication token

## Running the Worker

```bash
python src/main.py
```

The worker will:
1. Listen for messages on the SQS queue
2. Process PDF generation requests
3. Generate PDFs using story data
4. Upload PDFs to S3
5. Send notifications when complete

## Architecture

The worker consists of:

1. **PDFGenerationHandler**: Main class that handles:
   - SQS message processing
   - Story data fetching
   - PDF generation
   - S3 upload
   - Notification sending

2. **Main Entry Point**: Sets up logging and runs the handler

## Error Handling

- Failed PDF generations are logged
- SQS messages are only deleted after successful processing
- Errors are caught and logged at each step

## Monitoring

The worker logs:
- Start/stop events
- Message processing status
- PDF generation results
- Errors and exceptions

## Development

To add new features:

1. Create new handlers in `src/handlers/`
2. Update the main entry point if needed
3. Add new environment variables if required
4. Update the README with new features 