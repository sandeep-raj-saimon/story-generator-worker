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
from reportlab.platypus import Paragraph, Spacer, Image, PageBreak, SimpleDocTemplate
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from PIL import Image as PILImage
import traceback
from dotenv import load_dotenv
from .base_handler import BaseHandler
from openai import OpenAI
import redis
import fal_client
import subprocess
from moviepy import ImageClip, AudioFileClip, concatenate_videoclips, CompositeVideoClip
from moviepy.video.VideoClip import VideoClip

load_dotenv()

class MediaGenerationHandler(BaseHandler):
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
                region_name=os.getenv('AWS_S3_REGION_NAME'),
            )
            self.bucket_name = os.getenv('AWS_STORAGE_BUCKET_NAME')
            self.api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000/api')
            self.openai_api_key = os.getenv('CHATGPT_OPENAI_API_KEY')
            
            # redis connection
            self.redis_client = redis.Redis(
                host=os.getenv('REDISHOST'),
                port=os.getenv('REDISPORT'),
                password=os.getenv('REDISPASSWORD')
            )

            # Register custom fonts
            self._register_fonts()
            
            # Set up styles
            self.styles = self._setup_styles()
        except Exception as e:
            print(f"Error initializing PDFGenerationHandler: {str(e)}")
            print("Traceback:")
            print(traceback.format_exc())
            raise

    def _register_fonts(self):
        """Register custom fonts for PDF generation."""
        try:
            font_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'fonts')
            pdfmetrics.registerFont(TTFont('PlayfairDisplay', os.path.join(font_dir, 'PlayfairDisplay-Regular.ttf')))
            pdfmetrics.registerFont(TTFont('DancingScript', os.path.join(font_dir, 'DancingScript.ttf')))
            pdfmetrics.registerFont(TTFont('Cinzel', os.path.join(font_dir, 'Cinzel-Regular.ttf')))
        except Exception as e:
            print(f"Warning: Could not register custom fonts: {str(e)}")
            # Fallback to default fonts
            pass

    def _setup_styles(self):
        """Setup custom paragraph styles for different elements."""
        styles = getSampleStyleSheet()
        
        styles.add(ParagraphStyle(
            name='CoverTitle',
            fontName='PlayfairDisplay',
            fontSize=32,
            alignment=TA_CENTER,
            spaceAfter=0,
            leading=40,
            textColor=colors.HexColor('#1a365d')
        ))
        
        styles.add(ParagraphStyle(
            name='CoverSubtitle',
            fontName='PlayfairDisplay',
            fontSize=18,
            alignment=TA_CENTER,
            spaceAfter=20,
            textColor=colors.HexColor('#4a5568')
        ))
        
        styles.add(ParagraphStyle(
            name='SceneTitle',
            fontName='Cinzel',
            fontSize=24,
            alignment=TA_CENTER,
            spaceBefore=20,
            spaceAfter=20,
            textColor=colors.HexColor('#2d3748')
        ))
        
        styles.add(ParagraphStyle(
            name='SceneContent',
            fontName='DancingScript',
            fontSize=20,
            alignment=TA_JUSTIFY,
            leading=30,
            spaceAfter=12
        ))
        
        styles.add(ParagraphStyle(
            name='Footer',
            fontName='Poppins',
            fontSize=10,
            alignment=TA_CENTER,
            textColor=colors.HexColor('#718096')
        ))
        
        return styles

    def _create_cover_page(self, canvas, doc, story_data, user_data):
        """Create a simple cover page with just the story title."""
        canvas.saveState()
        
        # Calculate center position with more space for wrapping
        center_y = doc.height / 2
        
        # Add title with proper wrapping and positioning
        title = Paragraph(story_data['title'], self.styles['CoverTitle'])
        title_width, title_height = title.wrap(doc.width - 3*inch, doc.height)
        
        # Calculate position to ensure title fits within page bounds
        max_height = doc.height - 4*inch  # Leave 2 inches margin top and bottom
        if title_height > max_height:
            # If title is too tall, scale down the font
            scale_factor = max_height / title_height
            self.styles['CoverTitle'].fontSize = int(32 * scale_factor)  # Scale down from base size
            title = Paragraph(story_data['title'], self.styles['CoverTitle'])
            title_width, title_height = title.wrap(doc.width - 3*inch, doc.height)
        
        # Center the title vertically, accounting for its height
        title_y = center_y - (title_height / 2)
        
        # Ensure title doesn't go below bottom margin
        if title_y < 2*inch:
            title_y = 2*inch
        
        # Draw the title
        title.drawOn(canvas, 1.5*inch, title_y)  # Increased left margin for better centering
        
        canvas.restoreState()

    def _create_footer(self, canvas, doc):
        """Create footer with page numbers."""
        canvas.saveState()
        
        # Add page number
        canvas.setFont('Cinzel', 10)
        canvas.setFillColor(colors.HexColor('#718096'))
        page_number = f"Page {doc.page} of {doc.page_count}"
        canvas.drawString(doc.width/2 - 1*inch, 0.5*inch, page_number)
        
        # Add logo if exists
        try:
            if os.getenv('PDF_LOGO'):
                logo = Image(os.getenv('PDF_LOGO'))
                logo.drawHeight = 0.3*inch
                logo.drawWidth = 0.3*inch
                logo.drawOn(canvas, 0.5*inch, 0.3*inch)
        except:
            pass
        
        canvas.restoreState()

    def _process_image(self, image_url):
        """Process and optimize image for PDF."""
        try:
            response = requests.get(image_url)
            img = PILImage.open(BytesIO(response.content))
            
            # Resize image if too large
            max_width = 6*inch
            max_height = 4*inch
            ratio = min(max_width/img.width, max_height/img.height)
            new_size = (int(img.width * ratio), int(img.height * ratio))
            
            img = img.resize(new_size, PILImage.Resampling.LANCZOS)
            
            # Save to BytesIO
            img_byte_arr = BytesIO()
            img.save(img_byte_arr, format='JPEG', quality=85)
            img_byte_arr.seek(0)
            
            return Image(img_byte_arr)
        except Exception as e:
            print(f"Error processing image: {e}")
            return None
    
    def generate_pdf(self, story_data, user_data):
        print('story_data is ', story_data)
        """Generate PDF from story data with improved styling."""
        try:
            print("Starting PDF generation")
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=72
            )
            
            # Build the story content
            story_content = []
            story_content.append(PageBreak())
            # Add cover page
            doc.build([], onFirstPage=lambda c, d: self._create_cover_page(c, d, story_data, user_data))
            
            # Add scenes
            for i, scene in enumerate(story_data['scenes'], 1):
                # Add scene title
                scene_title = Paragraph(
                    f"Scene {i}: {scene['title']}",
                    self.styles['SceneTitle']
                )
                story_content.append(scene_title)
                print('scene is ', scene)
                # Add scene content
                scene_content = Paragraph(
                    scene['content'],
                    self.styles['SceneContent']
                )
                story_content.append(scene_content)
                
                # Add scene image if present
                if scene.get('media'):
                    for media in scene['media']:
                        if media['media_type'] == 'image':
                            img = self._process_image(media['url'])
                            if img:
                                story_content.append(Spacer(1, 0.5*inch))
                                story_content.append(img)
                                story_content.append(Spacer(1, 0.5*inch))
                
                # Add page break between scenes
                if i < len(story_data['scenes']):
                    story_content.append(PageBreak())
            
            # Build the PDF with footer
            doc.build(
                story_content,
                onFirstPage=lambda c, d: self._create_cover_page(c, d, story_data, user_data),
                onLaterPages=self._create_footer
            )
            
            print("Completed PDF generation")
            return buffer.getvalue()
            
        except Exception as e:
            error_msg = f"Failed to generate PDF: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def handle_pdf_generation(self, body):
        story_id, user_id = body.get('story_id'), body.get('user_id')
        """Handle PDF generation request."""
        try:
            # Fetch story and user data
            story_data = self.fetch_story_data(story_id, user_id, 'image')
            user_data = self.fetch_user_data(user_id)
            
            # Generate PDF
            pdf_data = self.generate_pdf(story_data, user_data)
            
            # Create revision record
            revision_id = self.create_revision(story_id, 'pdf')
            
            # Upload to S3
            pdf_url = self.upload_to_s3(pdf_data, story_id, revision_id['id'], 'pdf')
            
            # Update revision with URL
            self.update_revision(revision_id['id'], pdf_url, 'pdf', story_id)
            
            # Send notification
            self.send_notification(story_id, user_id, pdf_url, revision_id['id'])
            
            return {
                'status': 'success',
                'message': 'PDF generated successfully',
                'url': pdf_url
            }
            
        except Exception as e:
            error_msg = f"Failed to generate PDF: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }

    def fetch_story_data(self, story_id, user_id, format=None):
        """Fetch story and scenes data from database."""
        try:
            return super().fetch_story_data(story_id, user_id, format)
        except Exception as e:
            error_msg = f"Failed to fetch story data: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def update_revision(self, revision_id, revision_url, revision_type, story_id):
        """Update revision with PDF URL."""
        print('media_url is ', revision_url)
        print('revision_id is ', revision_id)
        try:
            # First update old revision to not current
            self.conn.cursor().execute(
                "UPDATE core_revision SET is_current = false WHERE story_id = %s AND format = %s AND is_current = true",
                (story_id, revision_type)
            )
            # Then update new revision with URL and set as current
            self.conn.cursor().execute(
                "UPDATE core_revision SET url = %s, is_current = true WHERE id = %s",
                (revision_url, revision_id)
            )
            self.conn.commit()
        except Exception as e:
            error_msg = f"Failed to update revision: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)
    
    def upload_to_s3(self, data, story_id, revision_id, format):
        """Upload generated file to S3."""
        try:
            filename = f"story_{story_id}/preview_{revision_id}.{format}"
            
            print(f"Attempting to upload {format} to S3: {filename}")
            content_type = {
                'pdf': 'application/pdf',
                'mp3': 'audio/mpeg',
                'mp4': 'video/mp4'
            }.get(format, 'application/octet-stream')
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=data,
                ContentType=content_type
            )
            print(f"Successfully uploaded {format} to S3")
            
            return f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        except Exception as e:
            error_msg = f"Failed to upload {format} to S3: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def upload_media_to_s3(self, media_data, story_id, scene_id, media_type):
        """Upload generated media to S3."""
        try:
            filename = f"story_{story_id}/scene_{scene_id}/media_{media_type}.png"
            print(f"Attempting to upload media to S3: {filename}")
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=media_data,
                ContentType='image/png'
            )
            print(f"Successfully uploaded media to S3") 
            return f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        except Exception as e:
            error_msg = f"Failed to upload media to S3: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)

    def send_notification(self, story_id, user_id, pdf_url, revision_id):
        """Send notification about completed PDF generation."""
        # Here you would implement your notification system
        # This could be a WebSocket message, SNS topic, or another SQS queue
        pass

    def process_message(self, body):
        """Process SQS message for PDF generation."""
        try:
            # raise Exception('testing')
            action = body.get('action')
            if action == 'generate_pdf_preview':
                return self.handle_pdf_generation(body)
            elif action == 'generate_audio_preview':
                return self.handle_audio_preview_generation(body)
            elif action == 'generate_video_preview':
                return self.handle_video_generation(body)
            elif action == 'generate_media':
                return self.handle_media_generation(body)
            elif action == 'generate_entire_audio':
                return self.handle_entire_audio_generation(body)
            else:
                return {'status': 'error', 'error': f'Unknown action: {action}'}
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
        queue_url = os.getenv('WHISPR_TALES_QUEUE_URL')
        
        while True:
            try:
                # Receive message from SQS
                response = self.sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=3,
                    WaitTimeSeconds=10
                )
                # {'story_id': 1, 'scene_id': 2, 'media_type': 'image', 'action': 'generate_media', 'credit_cost': 100, 'job_id': '4'}
                if 'Messages' in response:
                    for message in response['Messages']:
                        print(f"Received message: {message['MessageId']}")
                        body = json.loads(message['Body'])
                        try:
                            message_id = message['MessageId']
                            job_id = body.get('job_id')
                            # Set key with 5 minute expiration (300 seconds)
                            is_set = self.redis_client.set(message_id, 1, ex=300, nx=True)
                            print(f"is_set for message_id: {message_id} is: {is_set}")
                            if is_set:
                                try:
                                    with self.conn.cursor() as cursor:
                                        cursor.execute(
                                            """
                                            UPDATE core_job 
                                            SET status = 'processing', 
                                                started_at = NOW() 
                                            WHERE id = %s
                                            """,
                                            [job_id]
                                        )
                                    print(f"Updated job {job_id} status to processing")
                                    # Process message
                                    result = self.process_message(body)
                                    
                                    # Delete message from queue if successful
                                    if result['status'] == 'success':
                                        self.sqs_client.delete_message(
                                            QueueUrl=queue_url,
                                            ReceiptHandle=message['ReceiptHandle']
                                        )
                                        # Update job status to completed
                                        with self.conn.cursor() as cursor:
                                            cursor.execute(
                                                """
                                                UPDATE core_job 
                                                SET status = 'completed',
                                                    completed_at = NOW()
                                                WHERE id = %s
                                                """,
                                                [job_id]
                                            )
                                        print(f"Successfully processed and deleted message: {message['MessageId']}")
                                        print('<-------------------------GENERATION COMPLETE------------------------->')
                                        print()
                                    else:
                                        # Update job status to failed
                                        with self.conn.cursor() as cursor:
                                            cursor.execute(
                                                """
                                                UPDATE core_job 
                                                SET status = 'failed',
                                                    error_message = %s,
                                                    completed_at = NOW()
                                                WHERE id = %s
                                                """,
                                                [result['error'], job_id]
                                            )

                                        # Get credit cost and user id for the failed job
                                        with self.conn.cursor() as cursor:
                                            cursor.execute(
                                                """
                                                SELECT credit_cost, user_id 
                                                FROM core_job
                                                WHERE id = %s
                                                """, 
                                                [job_id]
                                            )
                                            job_info = cursor.fetchone()
                                        credit_cost = job_info[0]
                                        user_id = job_info[1]

                                        # Refund credits to user
                                        with self.conn.cursor() as cursor:
                                            cursor.execute(
                                                """
                                                UPDATE core_credits
                                                SET credits_remaining = credits_remaining + %s
                                                WHERE user_id = %s
                                                """,
                                                [credit_cost, user_id]
                                            )

                                            # Create credit transaction record for refund
                                            cursor.execute(
                                                """
                                                INSERT INTO core_credittransaction
                                                (user_id, credits_used, transaction_type, created_at, updated_at)
                                                VALUES (%s, %s, 'credit', NOW(), NOW())
                                                """,
                                                [user_id, credit_cost]
                                            )
                                        print(f"Updated job {job_id} status to failed")
                                        print(f"Failed to process message: {message['MessageId']}")
                                        print(f"Error: {result['error']}")
                                finally:
                                    # Always delete the Redis key after processing, regardless of success/failure
                                    self.redis_client.delete(message_id)
                            else:
                                print(f"Message already being processed by another worker: {message['MessageId']}")
                        except Exception as e:
                            # Only delete Redis key if we managed to set it
                            if 'message_id' in locals():
                                self.redis_client.delete(message_id)
                            error_msg = f"Error in message processing loop: {str(e)}\nTraceback:\n{traceback.format_exc()}"
                            print(error_msg)
                            continue
                
            except Exception as e:
                error_msg = f"Error in message processing loop: {str(e)}\nTraceback:\n{traceback.format_exc()}"
                print(error_msg)
                continue

    def handle_media_generation(self, body):
        story_id, scene_id, media_type, voice_id, previous_request_ids, next_request_ids, media_id = (
            body.get('story_id'),
            body.get('scene_id'),
            body.get('media_type'),
            body.get('voice_id'),
            body.get('previous_request_ids'),
            body.get('next_request_ids'),
            body.get('media_id')
        )
        """Handle media generation request."""
        try:
            if media_type == 'image':
                # Initialize OpenAI client
                s3_url = self.handle_image_generation(story_id, scene_id)
                self.update_old_media(story_id, scene_id, media_id)
                lock_key = f"scene_{scene_id}_{media_type}_lock"
                self.redis_client.delete(lock_key)
                return {
                    'status': 'success',
                    'media_url': s3_url
                }
                
            elif media_type == 'audio':
                if not voice_id:
                    raise Exception("voice_id is required for audio generation")
                
                # Generate audio using elevenlabs's api for generating audio stream
                s3_url = self.handle_audio_generation(story_id, media_type, scene_id, voice_id)
                self.update_old_media(story_id, scene_id, media_id)
                lock_key = f"scene_{scene_id}_{media_type}_lock"
                self.redis_client.delete(lock_key)
                return {
                    'status': 'success',
                    'media_url': s3_url
                }
            
            return {
                'status': 'error',
                'error': f'Unsupported media type: {media_type}'
            }
            
        except Exception as e:
            # If media_id is provided, update it as active
            if media_id:
                with self.conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE core_media 
                        SET is_active = true
                        WHERE id = %s
                        """,
                        [media_id]
                    )
                print(f"Updated media {media_id} as active")
            error_msg = f"Error handling media generation: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }

    def handle_entire_audio_generation(self, body):
        story_id, voice_id, previous_request_ids, next_request_ids = (
            body.get('story_id'),
            body.get('voice_id'),
            body.get('previous_request_ids') or [],
            body.get('next_request_ids') or []
        )
        try:
            scenes_data = self.fetch_scenes_data(story_id)
            for scene in scenes_data:
                _, request_id = self.handle_audio_generation(story_id, 'audio', scene['id'], voice_id, previous_request_ids, next_request_ids, scene)
                previous_request_ids.append(request_id)
            print(f"Successfully generated audio for all scenes for story_id: {story_id}")
            return {
                    'status': 'success',
                }
        except Exception as e:
            error_msg = f"Error handling entire audio generation for story_id: {story_id} {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }

    def handle_image_generation(self, story_id, scene_id):
        scene = self.fetch_scene_data(scene_id, story_id)
        print(f"Successfully fetched scene data for scene_id: {scene_id}")
        if not os.getenv('FAL_KEY'):
            raise ValueError("FAL_KEY environment variable is not set")

        def on_queue_update(update):
            if isinstance(update, fal_client.InProgress):
                if update.logs:
                    for log in update.logs:
                        print(log["message"])

        result = fal_client.subscribe(
            "fal-ai/flux-1/schnell",
            arguments={
                "prompt": f"""
                        generate a beautiful image for the following scene: {scene['scene_description']}
                        """
            },
            on_queue_update=on_queue_update,
        )
        
        # Get the image URL from FAL
        image_url = result['images'][0]['url']
        
        # Download the image
        image_response = requests.get(image_url)
        image_data = BytesIO(image_response.content)
        
        # Generate a unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"story_{story_id}/scene_{scene_id}/image_{timestamp}.png"
        
        # Upload to S3
        self.s3_client.upload_fileobj(
            image_data,
            self.bucket_name,
            filename,
            ExtraArgs={'ContentType': 'image/png'}
        )
        
        # Create S3 URL
        s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        
        # Insert media record
        self.insert_media(story_id, scene_id, 'image', s3_url, f"AI-generated image for scene")
        print(f"Successfully created media image record")
        
        return s3_url

    def handle_image_generation_openAI(self, story_id, scene_id):
        client = OpenAI(api_key=self.openai_api_key)

        scene = self.fetch_scene_data(scene_id, story_id)
        print(f"Successfully fetched scene data for scene_id: {scene_id}")
        # Generate image using OpenAI's DALL-E
        is_dall_e_2 = self.redis_client.get("is_dall_e_2")
        if is_dall_e_2:
            response = client.images.generate(
                model="dall-e-2",
                prompt=f"""
            generate a beautiful image for the following scene: {scene['content']}
            """,
            size="256x256",
            n=1,
            )
        else:
            response = client.images.generate(
                model="dall-e-3",
                prompt=f"""
                You are a professional visual artist illustrating a key scene from a story.

                Draw the following scene with high emotional impact, based purely on the description:  
                {scene['scene_description']}

                Use bold colors, cinematic lighting, and dramatic composition. Focus on mood, environment, and character emotion.  
                Do not include any text, speech bubbles, labels, or written elements in the image — it should feel like a painting, not a comic book panel.  

                The artwork should visually express the moment, using dynamic angles, detailed backgrounds, and atmospheric depth — like a powerful storybook illustration, not a graphic novel page.
                """,
                size="1024x1024",
                n=1,
            )
        
        # Get the image URL from OpenAI
        image_url = response.data[0].url
        
        # Download the image
        image_response = requests.get(image_url)
        image_data = BytesIO(image_response.content)
        
        # Generate a unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"story_{story_id}/scene_{scene_id}/image_{timestamp}.png"
        
        # Upload to S3
        self.s3_client.upload_fileobj(
            image_data,
            self.bucket_name,
            filename
        )
        
        # Create S3 URL
        s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        self.insert_media(story_id, scene_id, 'image', s3_url, f"AI-generated image for scene: {scene['title']}")
        print(f"Successfully created media image record")
        return s3_url

    def handle_audio_generation(self, story_id, media_type, scene_id=None, voice_id = None, scene_data= None):
        scene = scene_data if scene_data else self.fetch_scene_data(scene_id, story_id)
        print(f"Successfully fetched scene data for scene_id: {scene_id}")

        url = "https://api.play.ht/api/v2/tts/stream"

        payload = {
            "text": f'{scene['content']}',
            "voice": voice_id,
            "output_format": "mp3",
            "voice_engine": "PlayHT2.0"
        }
        headers = {
            "accept": "*/*",
            "content-type": "application/json",
            "AUTHORIZATION": os.getenv('PLAY_HT_KEY'),
            "X-USER-ID": os.getenv('PLAY_HT_USERID')
        }
        response = requests.post(url, json=payload, headers=headers)
        print(f"Successfully generated audio for scene_id: {scene_id}", response)
        
        # Convert generator to bytes
        # audio_bytes = b"".join(response.content)
        audio_data = BytesIO(response.content)
        
        # Generate a unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"story_{story_id}/scene_{scene_id}/audio_{timestamp}.mp3"
        
        # Upload to S3
        self.s3_client.upload_fileobj(
            audio_data,
            self.bucket_name,
            filename
        )
        
        # Create S3 URL
        s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        
        # Update old media to inactive, only one media can be active at a time
        # self.update_old_media(story_id, scene_id)
        # Create Media record
        self.insert_media(story_id, scene_id, media_type, s3_url, f"AI-generated audio for scene: {scene['title']}")
        print(f"Successfully created media audio record")
        return s3_url

    def handle_audio_generation_old(self, story_id, media_type, scene_id=None, voice_id=None, previous_request_ids=None, next_request_ids=None, scene_data= None):
        scene = scene_data if scene_data else self.fetch_scene_data(scene_id, story_id)
        print(f"Successfully fetched scene data for scene_id: {scene_id}")
        # Generate audio using elevenlabs's api for generating audio stream
        response = self.generate_audio(scene['content'], scene_id, voice_id, previous_request_ids, next_request_ids)
        request_id = response.headers["request-id"]
        
        print(f"Successfully generated audio for scene_id: {scene_id}", response)
        
        # Convert generator to bytes
        # audio_bytes = b"".join(response.content)
        audio_data = BytesIO(response.content)
        
        # Generate a unique filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"story_{story_id}/scene_{scene_id}/audio_{timestamp}.mp3"
        
        # Upload to S3
        self.s3_client.upload_fileobj(
            audio_data,
            self.bucket_name,
            filename
        )
        
        # Create S3 URL
        s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        
        # Update old media to inactive, only one media can be active at a time
        # self.update_old_media(story_id, scene_id)
        # Create Media record
        self.insert_media(story_id, scene_id, media_type, s3_url, f"AI-generated audio for scene: {scene['title']}", request_id)
        print(f"Successfully created media audio record")
        return s3_url, request_id


    def handle_audio_preview_generation(self, body):
        story_id, user_id = body.get('story_id'), body.get('user_id')
        """Handle audio generation request."""
        # fetch all audios of the story
        try:
            print(f"Starting audio generation for story_id: {story_id}, user_id: {user_id}")
            
            # Fetch story data
            story_data = self.fetch_story_data(story_id, user_id, 'audio')
            print("Successfully fetched story data")

            # Create revision for tracking
            revision = self.create_revision(story_id, 'audio')
            print(f"Successfully created revision", revision)

            # Get all audio files from story scenes
            audio_files = []
            print(f"Story data", story_data)
            for scene in story_data['scenes']:
                audio_media = [m for m in scene.get('media', []) if m['media_type'] == 'audio']
                if audio_media:
                    audio_files.extend(audio_media)
            print(f"Successfully fetched audio files for story_id: {story_id}, revision_id: {revision['id']}")

            if not audio_files:
                raise Exception("No audio files found in story")
            # we need to merge all audio files and then upload it to s3
            audio_files = self.merge_audio_files(audio_files, story_id, revision['id'])
            print(f"Successfully merged audio files for story_id: {story_id}, revision_id: {revision['id']}")
            # upload the merged audio to s3
            audio_url = self.upload_to_s3(audio_files, story_id, revision['id'], 'mp3')
            print(f"Successfully uploaded audio to S3: {audio_url}")


            # TODO: Implement audio concatenation logic here
            # For now just return the first audio URL
            
            self.update_revision(revision['id'], audio_url, 'audio', story_id)
            print(f"Successfully updated revision with audio URL")

            # Send notification
            self.send_notification(story_id, user_id, audio_url, revision['id'])
            print("Successfully sent notification")

            return {
                'status': 'success',
                'audio_url': audio_url
            }

        except Exception as e:
            error_msg = f"Error handling audio generation: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            return {
                'status': 'error',
                'error': error_msg
            }

    def handle_video_generation(self, body):
        story_id = body.get('story_id')
        user_id = body.get('user_id')
        
        try:
            print(f"Starting video generation for story_id={story_id}, user_id={user_id}")
            
            # 1) Fetch story + media metadata
            story_data = self.fetch_story_data(story_id, user_id, format=['image', 'audio'])
            revision = self.create_revision(story_id, 'video')
            print(f"Fetched story and created revision {revision['id']}")
            
            video_clips = []
            
            # 2) Work in a temp dir
            with tempfile.TemporaryDirectory() as temp_dir:
                for scene in story_data['scenes']:
                    sid = scene['id']
                    print(f"\n-- Scene {sid} --")
                    
                    # find image + audio
                    img_meta = next((m for m in scene['media'] if m['media_type']=='image'), None)
                    aud_meta = next((m for m in scene['media'] if m['media_type']=='audio'), None)
                    if not img_meta or not aud_meta:
                        print(f"Skipping scene {sid}: missing media")
                        continue
                    
                    img_path = os.path.join(temp_dir, f"scene_{sid}.png")
                    aud_path = os.path.join(temp_dir, f"scene_{sid}.mp3")
                    
                    # download image
                    r = requests.get(img_meta['url']); r.raise_for_status()
                    with open(img_path, 'wb') as f: f.write(r.content)
                    PILImage.open(img_path).verify()
                    
                    # download audio
                    r = requests.get(aud_meta['url']); r.raise_for_status()
                    with open(aud_path, 'wb') as f: f.write(r.content)
                    
                    # build clip
                    audio = AudioFileClip(aud_path)
                    image = ImageClip(img_path, duration=audio.duration)
                    clip  = image.with_audio(audio)
                    
                    video_clips.append(clip)
                    print(f"Added scene {sid} (duration {audio.duration:.2f}s)")
                
                if not video_clips:
                    raise RuntimeError("No valid clips generated")
                
                # 3) concatenate + write file
                final = concatenate_videoclips(video_clips, method="compose")
                out_path = os.path.join(temp_dir, f"story_{story_id}.mp4")
                final.write_videofile(out_path, codec='libx264', audio_codec='aac', fps=24)
                
                # 4) upload + notify
                with open(out_path,'rb') as f:
                    url = self.upload_to_s3(f.read(), story_id, revision['id'], 'mp4')
                self.update_revision(revision['id'], url, 'video', story_id)
                self.send_notification(story_id, user_id, url, revision['id'])
                
                return {'status':'success','video_url':url}
        
        except Exception as e:
            tb = traceback.format_exc()
            print(f"Error: {e}\n{tb}")
            return {'status':'error','error':f"{e}\n{tb}"}