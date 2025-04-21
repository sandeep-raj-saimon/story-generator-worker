import psycopg2
from psycopg2.extras import RealDictCursor
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv('DB_NAME', 'story_generator'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD', 'postgres'),
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432')
        )
        self.conn.autocommit = True

    def fetch_story_data(self, story_id, user_id):
        """Fetch story and scenes data directly from the database."""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Fetch story
                cursor.execute("""
                    SELECT id, title, content 
                    FROM core_story 
                    WHERE id = %s AND author_id = %s
                """, (story_id, user_id))
                story = cursor.fetchone()
                
                if not story:
                    raise Exception(f"Story not found with id {story_id}")
                
                # Fetch scenes
                cursor.execute("""
                    SELECT id, title, content, scene_description, "order"
                    FROM core_scene 
                    WHERE story_id = %s 
                    ORDER BY "order" ASC
                """, (story_id,))
                scenes = cursor.fetchall()
                
                # Convert to dictionary format
                story_data = dict(story)
                story_data['scenes'] = [dict(scene) for scene in scenes]
                
                return story_data
                
        except Exception as e:
            raise Exception(f"Error fetching story data: {str(e)}")

    def save_media(self, scene_id, media_type, url, description=None):
        """Save media information to the database."""
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    INSERT INTO core_media 
                    (scene_id, media_type, url, description, created_at, updated_at) 
                    VALUES 
                    (%s, %s, %s, %s, NOW(), NOW())
                    RETURNING id, scene_id, media_type, url, description
                """, (scene_id, media_type, url, description))
                return dict(cursor.fetchone())
                
        except Exception as e:
            raise Exception(f"Error saving media: {str(e)}")

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close() 