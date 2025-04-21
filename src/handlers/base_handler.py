import boto3
import os
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

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

    def fetch_story_data(self, story_id, user_id):
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
            """, (story_id, user_id))
            
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
            story_id = body['story_id']
            user_id = body['user_id']
            action = body['action']

            if action == 'generate_pdf_preview':
                return self.handle_pdf_generation(story_id, user_id)
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