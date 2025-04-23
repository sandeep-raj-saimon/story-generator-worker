import boto3
import os
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from elevenlabs.client import ElevenLabs
import ffmpeg
import subprocess
import requests
from io import BytesIO

load_dotenv()

class BaseHandler:
    def __init__(self):
        # Initialize database connection
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'story_generator'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        self.conn.autocommit = True

        # Initialize AWS clients with region
        aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.sqs_client = boto3.client('sqs', region_name=aws_region)
        self.bucket_name = os.getenv('AWS_STORAGE_BUCKET_NAME')
        self.elevenlabs_client = ElevenLabs(
            api_key=os.getenv("ELEVENLABS_API_KEY"),
        )

    def create_revision(self, story_id, format, url=None, sub_format=None):
        """Create a new revision for a story."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                INSERT INTO core_revision (story_id, format, url, sub_format, created_at, is_current)
                VALUES (%s, %s, %s, %s, NOW(), TRUE)
                RETURNING id
            """, (story_id, format, url, sub_format))
            return dict(cursor.fetchone())
    
    def fetch_scene_data(self, scene_id, story_id):
        """Fetch scene data from database."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT id, title, content, scene_description, "order" FROM core_scene WHERE id = %s AND story_id = %s
            """, (scene_id, story_id))
            return dict(cursor.fetchone())

    def insert_media(self, story_id, scene_id, media_type, url, description=None):
        """Insert media into database."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                INSERT INTO core_media (story_id, scene_id, media_type, url, created_at, description)
                VALUES (%s, %s, %s, %s, NOW(), %s)
                RETURNING id
            """, (story_id, scene_id, media_type, url, description))
            return dict(cursor.fetchone())

    def merge_audio_files(self, audio_list, story_id, revision_id):
        """[{'id': 101, 'media_type': 'audio', 'url': 'https://story-generation-pdf.s3.amazonaws.com/story_6/scene_93/audio_20250422_190005.mp3', 'description': 'AI-generated audio for scene: The Lantern Post'}, {'id': 109, 'media_type': 'audio', 'url': 'https://story-generation-pdf.s3.amazonaws.com/story_6/scene_94/audio_20250423_053837.mp3', 'description': 'AI-generated audio for scene: The Winter Storm'}, {'id': 103, 'media_type': 'audio', 'url': 'https://story-generation-pdf.s3.amazonaws.com/story_6/scene_95/audio_20250422_190413.mp3', 'description': 'AI-generated audio for scene: Guiding Light'}]"""
        temp_files = []
        for i, audio in enumerate(audio_list):
            response = requests.get(audio['url'])
            temp_file = f"temp_{i}.mp3"
            with open(temp_file, 'wb') as f:
                f.write(response.content)
            temp_files.append(temp_file)
        
        # Create a file with list of inputs
        with open('input.txt', 'w') as f:
            for file in temp_files:
                f.write(f"file '{file}'\n")
        
        # Merge using ffmpeg command line with -y flag to overwrite without asking
        output_path = f"combined_audio_{story_id}_{revision_id}.mp3"
        subprocess.run([
            'ffmpeg',
            '-y',
            '-f', 'concat',
            '-safe', '0',
            '-i', 'input.txt',
            '-c', 'copy',
            output_path
        ])
        
        # Read the merged file content
        with open(output_path, 'rb') as f:
            merged_content = f.read()
        
        # Clean up temporary files
        for file in temp_files:
            os.remove(file)
        os.remove('input.txt')
        os.remove(output_path)  # Also remove the output file after reading
        
        return merged_content  # Return the actual file content instead of path
        
        
    def fetch_story_data(self, story_id, user_id, format=None):
        """Fetch story and scenes data from database."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # Fetch story with scenes in a single query
            cursor.execute("""
                WITH story AS (
                    SELECT id, title, content 
                    FROM core_story 
                    WHERE id = %s AND author_id = %s
                ),
                scene_with_media AS (
                    SELECT 
                        sc.id,
                        sc.title,
                        sc.content,
                        sc.scene_description,
                        sc."order",
                        json_agg(
                            CASE WHEN m.id IS NOT NULL THEN
                                json_build_object(
                                    'id', m.id,
                                    'media_type', m.media_type,
                                    'url', m.url,
                                    'description', m.description
                                )
                            ELSE NULL END
                        ) FILTER (WHERE m.id IS NOT NULL) as media
                    FROM core_scene sc
                    LEFT JOIN core_media m ON m.scene_id = sc.id
                    WHERE sc.story_id IN (SELECT id FROM story)
                    and m.media_type = %s
                    GROUP BY sc.id, sc.title, sc.content, sc.scene_description, sc."order"
                )
                SELECT 
                    s.id, s.title, s.content,
                    json_agg(
                        json_build_object(
                            'id', sc.id,
                            'title', sc.title,
                            'content', sc.content,
                            'scene_description', sc.scene_description,
                            'order', sc."order",
                            'media', COALESCE(sc.media, '[]'::json)
                        ) ORDER BY sc."order"
                    ) as scenes
                FROM story s
                LEFT JOIN scene_with_media sc ON sc.id IN (
                    SELECT id FROM core_scene WHERE story_id = s.id
                )
                GROUP BY s.id, s.title, s.content
            """, (story_id, user_id, format))
            
            result = cursor.fetchone()
            if not result:
                raise Exception(f"Story not found with id {story_id}")
            
            return dict(result)

    def save_media(self, scene_id, media_type, url, description=None):
        """Save media to database."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                INSERT INTO core_media (scene_id, media_type, url, description, created_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                RETURNING id, scene_id, media_type, url, description
            """, (scene_id, media_type, url, description))
            return dict(cursor.fetchone())

    def process_message(self, message):
        """Process SQS message."""
        try:
            body = json.loads(message['Body'])
            story_id = body.get('story_id')
            user_id = body.get('user_id')
            action = body.get('action')
            scene_id = body.get('scene_id')
            media_type = body.get('media_type')
            if action == 'generate_pdf_preview':
                return self.handle_pdf_generation(story_id, user_id)
            elif action == 'generate_media':
                return self.handle_media_generation(story_id, scene_id, media_type)
            else:
                return {'status': 'error', 'error': f'Unknown action: {action}'}
        except Exception as e:
            return {'status': 'error', 'error': str(e)}

    def start_listening(self, queue_url):
        """Listen for SQS messages."""
        while True:
            try:
                response = self.sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )
                
                if 'Messages' in response:
                    for message in response['Messages']:
                        result = self.process_message(message)
                        if result['status'] == 'success':
                            self.sqs_client.delete_message(
                                QueueUrl=queue_url,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                        else:
                            print(f"Error processing message: {result['error']}")
            except Exception as e:
                print(f"Error in message processing: {str(e)}")

    def __del__(self):
        """Cleanup."""
        if hasattr(self, 'conn'):
            self.conn.close() 