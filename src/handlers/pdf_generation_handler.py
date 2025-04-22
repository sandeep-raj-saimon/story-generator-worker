import json
import boto3
import os
import requests
from datetime import datetime
from io import BytesIO
import tempfile
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import Paragraph, Spacer
from PIL import Image
import traceback
from dotenv import load_dotenv
from .base_handler import BaseHandler

load_dotenv()

class PDFGenerationHandler(BaseHandler):
    def __init__(self):
        try:
            super().__init__()
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_S3_REGION_NAME')
            )
            self.sqs_client = boto3.client(
                'sqs',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_S3_REGION_NAME')
            )
            self.bucket_name = os.getenv('AWS_STORAGE_BUCKET_NAME')
            self.api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000/api')
        except Exception as e:
            print(f"Error initializing PDFGenerationHandler: {str(e)}")
            print("Traceback:")
            print(traceback.format_exc())
            raise

    def fetch_story_data(self, story_id, user_id):
        """Fetch story and scenes data from database."""
        try:
            return super().fetch_story_data(story_id, user_id)
        except Exception as e:
            error_msg = f"Failed to fetch story data: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def generate_pdf(self, story_data):
        """Generate PDF from story data."""
        try:
            print("Starting PDF generation")
            buffer = BytesIO()
            pdf = canvas.Canvas(buffer, pagesize=letter)
            width, height = letter
            
            # Set up styles
            styles = getSampleStyleSheet()
            content_style = ParagraphStyle(
                'Content',
                parent=styles['Normal'],
                fontSize=14,
                leading=20,
                alignment=4  # Justified text
            )
            
            # Add scenes in order
            for i, scene in enumerate(story_data['scenes']):
                # Create new page for all scenes except the first one
                if i > 0:
                    pdf.showPage()
                
                # Add scene content at the top third of the page
                scene_content = Paragraph(scene['content'], content_style)
                text_height = height * 0.3  # Reserve top 30% for text
                scene_content.wrapOn(pdf, width - 100, text_height)
                scene_content.drawOn(pdf, 50, height - text_height)
                
                # Add scene's image if present
                if scene.get('media'):
                    for media in scene['media']:
                        if media['media_type'] == 'image':
                            try:
                                print(f"Downloading image from: {media['url']}")
                                response = requests.get(media['url'])
                                if response.status_code == 200:
                                    # Create a temporary file
                                    with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
                                        # Save the image to the temporary file
                                        img = Image.open(BytesIO(response.content))
                                        if img.mode == 'RGBA':
                                            img = img.convert('RGB')
                                        img.save(tmp_file.name, 'JPEG')
                                        
                                        # Calculate image dimensions for bottom 60% of page
                                        img_width = width - 100  # Leave margins
                                        img_height = height * 0.6  # Use 60% of page height
                                        
                                        # Draw image in bottom portion
                                        pdf.drawImage(tmp_file.name, 50, 50, width=img_width, height=img_height)
                                        print(f"Successfully added image to PDF")
                                        
                                        # Clean up the temporary file
                                        os.unlink(tmp_file.name)
                            except Exception as e:
                                print(f"Failed to add image {media['url']}: {str(e)}")
                                print(traceback.format_exc())
                                continue
            
            print("Completed PDF generation")
            pdf.save()
            return buffer.getvalue()
            
        except Exception as e:
            error_msg = f"Failed to generate PDF: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def update_revision(self, revision_id, pdf_url):
        """Update revision with PDF URL."""
        try:
            self.conn.cursor().execute("UPDATE core_revision SET url = %s WHERE id = %s", (pdf_url, revision_id))
            self.conn.commit()
        except Exception as e:
            error_msg = f"Failed to update revision: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)
    
    def upload_to_s3(self, pdf_data, story_id, revision_id):
        """Upload generated PDF to S3."""
        try:
            filename = f"story_{story_id}/preview_{revision_id}.pdf"
            
            print(f"Attempting to upload PDF to S3: {filename}")
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=pdf_data,
                ContentType='application/pdf'
            )
            print(f"Successfully uploaded PDF to S3")
            
            return f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        except Exception as e:
            error_msg = f"Failed to upload PDF to S3: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def send_notification(self, story_id, user_id, pdf_url, revision_id):
        """Send notification about completed PDF generation."""
        # Here you would implement your notification system
        # This could be a WebSocket message, SNS topic, or another SQS queue
        pass

    def process_message(self, message):
        """Process SQS message for PDF generation."""
        try:
            body = json.loads(message['Body'])
            story_id = body['story_id']
            user_id = body['user_id']
            
            print(f"Processing message for story_id: {story_id}, user_id: {user_id}")
            
            # Fetch story data
            story_data = self.fetch_story_data(story_id, user_id)
            print(f"Successfully fetched story data", story_data)
            
            # Generate PDF
            pdf_data = self.generate_pdf(story_data)
            print(f"Successfully generated PDF")
            
            revision = self.create_revision(story_id, 'pdf')
            print(f"Successfully created revision", revision)

            # Upload to S3
            pdf_url = self.upload_to_s3(pdf_data, story_id, revision['id'])
            print(f"Successfully uploaded PDF to S3: {pdf_url}")
                
            # update revision
            self.update_revision(revision['id'], pdf_url)
            print(f"Successfully updated revision", revision)

            # Send notification
            self.send_notification(story_id, user_id, pdf_url, revision['id'])
            print(f"Successfully sent notification")
            
            return {
                'status': 'success',
                'pdf_url': pdf_url
            }
        except Exception as e:
            error_msg = f"Error processing message: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }

    def start_listening(self, queue_url):
        """Start listening for SQS messages."""
        print(f"Starting to listen on queue: {queue_url}")
        queue_url = os.getenv('STORY_GENERATION_QUEUE_URL')
        
        while True:
            try:
                # Receive message from SQS
                response = self.sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )
                
                if 'Messages' in response:
                    for message in response['Messages']:
                        print(f"Received message: {message['MessageId']}")
                        # Process message
                        result = self.process_message(message)
                        
                        # Delete message from queue if successful
                        if result['status'] == 'success':
                            self.sqs_client.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                            print(f"Successfully processed and deleted message: {message['MessageId']}")
                        else:
                            print(f"Failed to process message: {message['MessageId']}")
                            print(f"Error: {result['error']}")
                
            except Exception as e:
                error_msg = f"Error in message processing loop: {str(e)}\nTraceback:\n{traceback.format_exc()}"
                print(error_msg)
                continue

    def handle_pdf_generation(self, story_id, user_id):
        """Handle PDF generation request."""
        try:
            print(f"Starting PDF generation for story_id: {story_id}, user_id: {user_id}")
            
            # Fetch story data
            story_data = self.fetch_story_data(story_id, user_id)
            print("Successfully fetched story data")
            
            # Generate PDF
            pdf_data = self.generate_pdf(story_data)
            print("Successfully generated PDF")
            
            # Upload to S3
            pdf_url = self.upload_to_s3(pdf_data, story_id, user_id)
            print(f"Successfully uploaded PDF to S3: {pdf_url}")
            
            # Send notification
            self.send_notification(story_id, user_id, pdf_url)
            print("Successfully sent notification")
            
            return {
                'status': 'success',
                'pdf_url': pdf_url
            }
        except Exception as e:
            error_msg = f"Error handling PDF generation: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }

    def handle_audio_generation(self, story_id, user_id):
        """Handle audio generation request."""
        return {
            'status': 'error',
            'error': 'Audio generation not implemented'
        }

    def handle_video_generation(self, story_id, user_id):
        """Handle video generation request."""
        return {
            'status': 'error',
            'error': 'Video generation not implemented'
        } 