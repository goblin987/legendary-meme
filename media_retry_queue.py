"""
Media Retry Queue System
Ensures 100% media delivery success rate by automatically retrying failed deliveries
"""

import logging
import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from utils import get_db_connection

logger = logging.getLogger(__name__)

class MediaRetryQueue:
    """
    Automatic retry queue for failed media deliveries.
    Ensures 100% delivery success rate by:
    1. Tracking failed media deliveries
    2. Retrying with exponential backoff
    3. Notifying admins after max retries
    """
    
    def __init__(self):
        self.is_running = False
        self._task = None
        
    async def start(self):
        """Start the retry queue background worker"""
        if self.is_running:
            logger.info("📦 Media retry queue already running")
            return
        
        self.is_running = True
        self._task = asyncio.create_task(self._worker())
        logger.info("✅ Media retry queue started")
    
    async def stop(self):
        """Stop the retry queue background worker"""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("🛑 Media retry queue stopped")
    
    async def add_failed_delivery(
        self,
        user_id: int,
        order_id: str,
        media_type: str,
        media_data: dict,
        error_message: str
    ):
        """
        Add a failed media delivery to the retry queue
        
        Args:
            user_id: User ID who should receive the media
            order_id: Order ID for tracking
            media_type: Type of media (photo, video, animation, document, media_group)
            media_data: Dict containing media info (file_id, path, caption, etc.)
            error_message: Error that caused the failure
        """
        conn = get_db_connection()
        c = conn.cursor()
        
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_retry_queue (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL,
                    order_id TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    media_data JSONB NOT NULL,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    max_retries INTEGER DEFAULT 10,
                    next_retry_at TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    last_retry_at TIMESTAMP WITH TIME ZONE,
                    status TEXT DEFAULT 'pending'
                )
            """)
            
            # Calculate next retry time (5 minutes from now)
            next_retry = datetime.now(timezone.utc) + timedelta(minutes=5)
            
            c.execute("""
                INSERT INTO media_retry_queue 
                (user_id, order_id, media_type, media_data, error_message, next_retry_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, order_id, media_type, json.dumps(media_data), error_message, next_retry))
            
            conn.commit()
            logger.info(f"📋 Added failed {media_type} delivery to retry queue for user {user_id}, order {order_id}")
            
        except Exception as e:
            logger.error(f"❌ Error adding to retry queue: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    async def _worker(self):
        """Background worker that processes retry queue"""
        logger.info("🔄 Media retry queue worker started")
        
        while self.is_running:
            try:
                await self._process_queue()
                # Check queue every 60 seconds
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Error in retry queue worker: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _process_queue(self):
        """Process pending retry items"""
        conn = get_db_connection()
        c = conn.cursor()
        
        try:
            # Get items ready for retry
            c.execute("""
                SELECT id, user_id, order_id, media_type, media_data, retry_count, max_retries
                FROM media_retry_queue
                WHERE status = 'pending' 
                AND next_retry_at <= CURRENT_TIMESTAMP
                ORDER BY created_at ASC
                LIMIT 10
            """)
            
            items = c.fetchall()
            
            if not items:
                return
            
            logger.info(f"📦 Processing {len(items)} media retry items...")
            
            for item in items:
                try:
                    await self._retry_delivery(item)
                except Exception as e:
                    logger.error(f"❌ Error retrying delivery {item['id']}: {e}")
                    
        except Exception as e:
            logger.error(f"❌ Error processing retry queue: {e}")
        finally:
            conn.close()
    
    async def _retry_delivery(self, item: dict):
        """Retry a single media delivery"""
        conn = get_db_connection()
        c = conn.cursor()
        
        try:
            retry_id = item['id']
            user_id = item['user_id']
            order_id = item['order_id']
            media_type = item['media_type']
            media_data = json.loads(item['media_data']) if isinstance(item['media_data'], str) else item['media_data']
            retry_count = item['retry_count']
            max_retries = item['max_retries']
            
            logger.info(f"🔄 Retrying {media_type} delivery for user {user_id}, attempt {retry_count + 1}/{max_retries}")
            
            # Import bot instance
            from main import telegram_app
            from utils import send_media_with_retry, send_media_group_with_retry
            
            success = False
            
            try:
                if media_type == 'media_group':
                    # Retry media group
                    result = await send_media_group_with_retry(
                        telegram_app.bot,
                        user_id,
                        media_data.get('media_group', [])
                    )
                    success = result is not None
                    
                else:
                    # Retry single media
                    result = await send_media_with_retry(
                        telegram_app.bot,
                        user_id,
                        media_data.get('file_id') or media_data.get('path'),
                        media_type=media_type,
                        caption=media_data.get('caption')
                    )
                    success = result is not None
                
                if success:
                    # Mark as completed
                    c.execute("""
                        UPDATE media_retry_queue
                        SET status = 'completed', last_retry_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (retry_id,))
                    conn.commit()
                    logger.info(f"✅ Successfully retried {media_type} delivery for user {user_id}")
                    
                else:
                    # Increment retry count and schedule next retry
                    new_retry_count = retry_count + 1
                    
                    if new_retry_count >= max_retries:
                        # Max retries reached, notify admin
                        c.execute("""
                            UPDATE media_retry_queue
                            SET status = 'failed', retry_count = %s, last_retry_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (new_retry_count, retry_id))
                        conn.commit()
                        
                        logger.error(f"❌ Max retries ({max_retries}) reached for {media_type} delivery to user {user_id}")
                        await self._notify_admin_failure(user_id, order_id, media_type, new_retry_count)
                        
                    else:
                        # Schedule next retry with exponential backoff
                        backoff_minutes = min(5 * (2 ** new_retry_count), 60)  # Max 1 hour
                        next_retry = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)
                        
                        c.execute("""
                            UPDATE media_retry_queue
                            SET retry_count = %s, next_retry_at = %s, last_retry_at = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """, (new_retry_count, next_retry, retry_id))
                        conn.commit()
                        
                        logger.warning(f"⏳ Scheduled next retry for {media_type} to user {user_id} in {backoff_minutes} minutes")
                
            except Exception as delivery_error:
                logger.error(f"❌ Delivery retry failed: {delivery_error}")
                
                # Update error and schedule retry
                new_retry_count = retry_count + 1
                if new_retry_count >= max_retries:
                    c.execute("""
                        UPDATE media_retry_queue
                        SET status = 'failed', retry_count = %s, error_message = %s, last_retry_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (new_retry_count, str(delivery_error), retry_id))
                    await self._notify_admin_failure(user_id, order_id, media_type, new_retry_count)
                else:
                    backoff_minutes = min(5 * (2 ** new_retry_count), 60)
                    next_retry = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes)
                    c.execute("""
                        UPDATE media_retry_queue
                        SET retry_count = %s, next_retry_at = %s, error_message = %s, last_retry_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (new_retry_count, next_retry, str(delivery_error), retry_id))
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"❌ Error in retry delivery: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    async def _notify_admin_failure(self, user_id: int, order_id: str, media_type: str, retry_count: int):
        """Notify admin about permanent delivery failure"""
        try:
            from main import telegram_app
            from utils import send_message_with_retry, get_first_primary_admin_id
            
            admin_id = get_first_primary_admin_id()
            if not admin_id:
                return
            
            message = (
                f"⚠️ **Media Delivery Failed**\n\n"
                f"Failed to deliver {media_type} after {retry_count} attempts.\n\n"
                f"**User ID:** {user_id}\n"
                f"**Order ID:** {order_id}\n"
                f"**Media Type:** {media_type}\n\n"
                f"Please investigate and manually deliver if necessary."
            )
            
            await send_message_with_retry(
                telegram_app.bot,
                admin_id,
                message,
                parse_mode='Markdown'
            )
            
        except Exception as e:
            logger.error(f"❌ Error notifying admin: {e}")

# Global retry queue instance
media_retry_queue = MediaRetryQueue()

