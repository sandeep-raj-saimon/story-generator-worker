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
            self.openai_api_key = os.getenv('CHATGPT_OPENAI_API_KEY')
            
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
            fontSize=36,
            alignment=TA_CENTER,
            spaceAfter=30,
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
        """Create a decorative cover page."""
        canvas.saveState()
        
        # Add background texture or image
        # try:
        #     if os.getenv('PDF_COVER_BACKGROUND'):
        #         img = Image(os.getenv('PDF_COVER_BACKGROUND'))
        #         img.drawHeight = doc.height
        #         img.drawWidth = doc.width
        #         img.drawOn(canvas, 0, 0)
        # except:
        #     # Fallback to a gradient background
        #     canvas.setFillColor(colors.HexColor('#f7fafc'))
        #     canvas.rect(0, 0, doc.width, doc.height, fill=1)
        
        # Add title
        title = Paragraph(story_data['title'], self.styles['CoverTitle'])
        title.wrapOn(canvas, doc.width - 2*inch, doc.height)
        title.drawOn(canvas, inch, doc.height - 3*inch)
        
        # Add subtitle if exists
        if story_data.get('subtitle'):
            subtitle = Paragraph(story_data['subtitle'], self.styles['CoverSubtitle'])
            subtitle.wrapOn(canvas, doc.width - 2*inch, doc.height)
            subtitle.drawOn(canvas, inch, doc.height - 4*inch)
        
        # Add author and narrator info
        author_info = Paragraph(
            f"Written by {user_data.get('full_name', user_data['username'])}<br/>Narrated by AI",
            self.styles['CoverSubtitle']
        )
        author_info.wrapOn(canvas, doc.width - 2*inch, doc.height)
        author_info.drawOn(canvas, inch, 2*inch)
        
        # Add watermark
        canvas.setFont('DancingScript', 8)
        canvas.setFillColor(colors.HexColor('#cbd5e0'))
        canvas.drawString(doc.width - 2*inch, 0.5*inch, "Generated by StoryScape AI")
        
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

    def handle_pdf_generation(self, story_id, user_id):
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
            self.update_revision(revision_id['id'], pdf_url)
            
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

    def update_revision(self, revision_id, pdf_url):
        """Update revision with PDF URL."""
        print('pdf_url is ', pdf_url)
        print('revision_id is ', revision_id)
        try:
            self.conn.cursor().execute("UPDATE core_revision SET url = %s WHERE id = %s", (pdf_url, revision_id))
            self.conn.commit()
        except Exception as e:
            error_msg = f"Failed to update revision: {str(e)}\nTraceback:\n{traceback.format_exc()}"
            print(error_msg)
            raise Exception(error_msg)
    
    def upload_to_s3(self, pdf_data, story_id, revision_id, format):
        """Upload generated PDF to S3."""
        try:
            filename = f"story_{story_id}/preview_{revision_id}.{format}"
            
            print(f"Attempting to upload PDF to S3: {filename}")
            content_type = 'audio/mpeg' if format == 'mp3' else 'application/pdf'
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=filename,
                Body=pdf_data,
                ContentType=content_type
            )
            print(f"Successfully uploaded {format} to S3")
            
            return f"https://{self.bucket_name}.s3.amazonaws.com/{filename}"
        except Exception as e:
            error_msg = f"Failed to upload PDF to S3: {str(e)}\nTraceback:\n{traceback.format_exc()}"
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

    def process_message(self, message):
        """Process SQS message for PDF generation."""
        try:
            body = json.loads(message['Body'])
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

    def handle_media_generation(self, body):
        story_id, scene_id, media_type, voice_id, previous_request_ids, next_request_ids = (
            body.get('story_id'),
            body.get('scene_id'),
            body.get('media_type'),
            body.get('voice_id'),
            body.get('previous_request_ids'),
            body.get('next_request_ids')
        )
        """Handle media generation request."""
        try:
            if media_type == 'image':
                # Initialize OpenAI client
                s3_url = self.handle_image_generation(story_id, scene_id)
                return {
                    'status': 'success',
                    'media_url': s3_url
                }
                
            elif media_type == 'audio':
                if not voice_id:
                    raise Exception("voice_id is required for audio generation")
                
                # Generate audio using elevenlabs's api for generating audio stream
                s3_url, _ = self.handle_audio_generation(story_id, media_type, scene_id, voice_id, previous_request_ids, next_request_ids)
                return {
                    'status': 'success',
                    'media_url': s3_url
                }
            
            return {
                'status': 'error',
                'error': f'Unsupported media type: {media_type}'
            }
            
        except Exception as e:
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
        client = OpenAI(api_key=self.openai_api_key)

        scene = self.fetch_scene_data(scene_id, story_id)
        print(f"Successfully fetched scene data for scene_id: {scene_id}")
        # Generate image using OpenAI's DALL-E
        response = client.images.generate(
            model="dall-e-3",
            prompt=f"Create a dynamic comic book panel that brings this scene to life: {scene['scene_description']}. Draw inspiration from classic comic masters - think Jack Kirby's dramatic angles, Todd McFarlane's moody atmospherics, and Jim Lee's detailed character work. Use bold, vibrant colors with high contrast lighting to create visual drama. Frame the composition to maximize emotional impact - if it's an action scene, use dynamic diagonal lines and extreme perspectives; for emotional moments, focus on expressive character close-ups. Include rich background details that enhance the story's setting. Layer in atmospheric effects like speed lines, impact bursts, or mood lighting to amplify the scene's energy. The art style should be professional comic book quality with clean, confident line work, detailed cross-hatching for depth, and strategic use of shadows to create volume. Make every element serve the story - from the character poses to the smallest environmental details. This needs to be a show-stopping panel that makes readers feel the emotion and drama of the moment.",
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

    def handle_audio_generation(self, story_id, media_type, scene_id=None, voice_id=None, previous_request_ids=None, next_request_ids=None, scene_data= None):
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
            
            self.update_revision(revision['id'], audio_url)
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
        story_id, user_id = body.get('story_id'), body.get('user_id')
        """Handle video generation request."""
        return {
            'status': 'error',
            'error': 'Video generation not implemented'
        } 