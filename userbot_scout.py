"""
Scout Userbot System - Keyword Detection & Auto-Reply
Monitors group messages and auto-replies when keywords are detected
"""

import logging
import re
import asyncio
from datetime import datetime
from typing import List, Dict, Optional

try:
    from pyrogram import Client, filters
    from pyrogram.types import Message
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False

from utils import get_db_connection

logger = logging.getLogger(__name__)

class ScoutSystem:
    """Manages keyword detection and auto-replies for scout userbots"""
    
    def __init__(self):
        self.keywords_cache = []
        self.last_cache_update = None
        self.cache_ttl_seconds = 60  # Refresh cache every 60 seconds
        
    async def load_keywords(self) -> List[Dict]:
        """Load active keywords from database"""
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            c.execute("""
                SELECT id, keyword, match_type, case_sensitive, 
                       response_text, response_delay_seconds
                FROM scout_keywords 
                WHERE is_active = TRUE
                ORDER BY keyword
            """)
            keywords = c.fetchall()
            self.keywords_cache = [dict(k) for k in keywords]
            self.last_cache_update = datetime.now()
            logger.info(f"✅ Loaded {len(self.keywords_cache)} active scout keywords")
            return self.keywords_cache
        except Exception as e:
            logger.error(f"Error loading scout keywords: {e}")
            return []
        finally:
            if conn:
                conn.close()
    
    async def check_message(self, message_text: str) -> Optional[Dict]:
        """Check if message contains any keywords. Returns matched keyword."""
        # Refresh cache if needed
        if not self.keywords_cache or \
           (self.last_cache_update and 
            (datetime.now() - self.last_cache_update).seconds > self.cache_ttl_seconds):
            await self.load_keywords()
        
        if not message_text:
            return None
        
        logger.info(f"🔍 Checking message against {len(self.keywords_cache)} active keywords: '{message_text[:100]}'")
        
        for kw in self.keywords_cache:
            keyword = kw['keyword']
            match_type = kw['match_type']
            case_sensitive = kw['case_sensitive']
            
            # Prepare texts for comparison
            text_to_check = message_text if case_sensitive else message_text.lower()
            kw_to_check = keyword if case_sensitive else keyword.lower()
            
            matched = False
            
            if match_type == 'exact':
                matched = text_to_check == kw_to_check
            elif match_type == 'starts_with':
                matched = text_to_check.startswith(kw_to_check)
            elif match_type == 'contains':
                matched = kw_to_check in text_to_check
            elif match_type == 'regex':
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    matched = bool(re.search(keyword, message_text, flags))
                except re.error:
                    logger.error(f"Invalid regex pattern: {keyword}")
                    continue
            
            if matched:
                logger.info(f"✅ KEYWORD MATCHED! Keyword: '{keyword}' | Match type: {match_type} | Message: '{message_text[:100]}'")
                return kw
        
        logger.info(f"❌ No keyword matched in message: '{message_text[:100]}'")
        return None
    
    async def log_trigger(self, userbot_id: int, keyword_id: int, 
                         message: 'Message', response_sent: bool = False,
                         response_message_id: int = None, error: str = None):
        """Log a keyword trigger event"""
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            # Get message details safely
            chat_id = message.chat.id if message.chat else None
            chat_title = message.chat.title if (message.chat and hasattr(message.chat, 'title')) else None
            user_id = message.from_user.id if message.from_user else None
            user_username = message.from_user.username if message.from_user else None
            message_text = message.text[:500] if message.text else None  # Truncate long messages
            
            c.execute("""
                INSERT INTO scout_triggers 
                (userbot_id, keyword_id, chat_id, chat_title, message_id,
                 user_id, user_username, detected_text, response_sent,
                 response_message_id, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                userbot_id,
                keyword_id,
                chat_id,
                chat_title,
                message.id,
                user_id,
                user_username,
                message_text,
                response_sent,
                response_message_id,
                error
            ))
            
            # Update keyword usage count
            c.execute("""
                UPDATE scout_keywords 
                SET uses_count = uses_count + 1, last_used_at = NOW()
                WHERE id = %s
            """, (keyword_id,))
            
            conn.commit()
            logger.info(f"✅ Scout trigger logged for keyword {keyword_id}")
        except Exception as e:
            logger.error(f"Error logging scout trigger: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
    
    async def send_auto_reply(self, client: Client, userbot_id: int,
                             message: 'Message', keyword_data: Dict) -> bool:
        """Send auto-reply to detected keyword"""
        try:
            # Delay to look more human
            delay = keyword_data.get('response_delay_seconds', 3)
            await asyncio.sleep(delay)
            
            # Send reply
            response_text = keyword_data['response_text']
            sent_message = await message.reply_text(response_text)
            
            # Log successful trigger
            await self.log_trigger(
                userbot_id=userbot_id,
                keyword_id=keyword_data['id'],
                message=message,
                response_sent=True,
                response_message_id=sent_message.id
            )
            
            logger.info(f"✅ Scout replied to keyword '{keyword_data['keyword']}' in chat {message.chat.id}")
            return True
            
        except Exception as e:
            logger.error(f"Error sending scout auto-reply: {e}")
            # Log failed trigger
            await self.log_trigger(
                userbot_id=userbot_id,
                keyword_id=keyword_data['id'],
                message=message,
                response_sent=False,
                error=str(e)
            )
            return False

# Global scout system instance
scout_system = ScoutSystem()


def setup_scout_handlers(client: Client, userbot_id: int):
    """Setup message handlers for scout mode on a userbot client"""
    
    if not PYROGRAM_AVAILABLE:
        logger.error("❌ Pyrogram not available - cannot setup scout handlers")
        return {'success': False, 'error': 'Pyrogram not available'}
    
    try:
        logger.info(f"🔍 Setting up scout handlers for userbot ID {userbot_id}")
        
        @client.on_message(filters.group & filters.text & ~filters.bot & ~filters.me)
        async def handle_group_message(client: Client, message: Message):
            """Monitor all group messages for keywords"""
            try:
                # Check if this userbot has scout mode enabled
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT scout_mode_enabled FROM userbots WHERE id = %s", (userbot_id,))
                result = c.fetchone()
                conn.close()
                
                if not result or not result['scout_mode_enabled']:
                    return  # Scout mode disabled for this userbot
                
                # Check message for keywords
                matched_keyword = await scout_system.check_message(message.text)
                
                if matched_keyword:
                    logger.info(f"🔍 Keyword '{matched_keyword['keyword']}' detected in chat {message.chat.id} (userbot {userbot_id})")
                    # Send auto-reply
                    await scout_system.send_auto_reply(client, userbot_id, message, matched_keyword)
            
            except Exception as e:
                logger.error(f"Error in scout message handler (userbot {userbot_id}): {e}", exc_info=True)
        
        logger.info(f"✅ Scout handlers registered successfully for userbot {userbot_id}")
        logger.info(f"🎯 Userbot {userbot_id} is now monitoring group messages for keywords")
        return {'success': True, 'userbot_id': userbot_id}
        
    except Exception as e:
        logger.error(f"❌ Failed to setup scout handlers for userbot {userbot_id}: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


# === SCOUT DATABASE FUNCTIONS ===

def add_keyword(keyword: str, response_text: str, match_type: str = 'contains',
                case_sensitive: bool = False, response_delay: int = 3,
                created_by: int = None) -> Optional[int]:
    """Add a new scout keyword"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
            INSERT INTO scout_keywords 
            (keyword, response_text, match_type, case_sensitive, response_delay_seconds, created_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (keyword, response_text, match_type, case_sensitive, response_delay, created_by))
        keyword_id = c.fetchone()['id']
        conn.commit()
        logger.info(f"✅ Scout keyword added: {keyword} (ID: {keyword_id})")
        return keyword_id
    except Exception as e:
        logger.error(f"Error adding scout keyword: {e}")
        if conn:
            conn.rollback()
        return None
    finally:
        if conn:
            conn.close()


def toggle_keyword(keyword_id: int, is_active: bool) -> bool:
    """Enable/disable a keyword"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE scout_keywords SET is_active = %s WHERE id = %s", (is_active, keyword_id))
        conn.commit()
        logger.info(f"✅ Keyword {keyword_id} {'enabled' if is_active else 'disabled'}")
        return True
    except Exception as e:
        logger.error(f"Error toggling keyword: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def delete_keyword(keyword_id: int) -> bool:
    """Delete a keyword"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("DELETE FROM scout_keywords WHERE id = %s", (keyword_id,))
        conn.commit()
        logger.info(f"✅ Keyword {keyword_id} deleted")
        return True
    except Exception as e:
        logger.error(f"Error deleting keyword: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def toggle_scout_mode(userbot_id: int, enabled: bool) -> bool:
    """Enable/disable scout mode for a userbot"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE userbots SET scout_mode_enabled = %s WHERE id = %s", (enabled, userbot_id))
        conn.commit()
        logger.info(f"✅ Scout mode {'enabled' if enabled else 'disabled'} for userbot {userbot_id}")
        return True
    except Exception as e:
        logger.error(f"Error toggling scout mode: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()


def test_scout_mode() -> Dict:
    """Test scout mode status and configuration"""
    status = {
        'pyrogram_available': PYROGRAM_AVAILABLE,
        'userbots_configured': 0,
        'userbots_connected': 0,
        'userbots_with_scout': 0,
        'active_keywords': 0,
        'errors': [],
        'warnings': []
    }
    
    # Check Pyrogram
    if not PYROGRAM_AVAILABLE:
        status['errors'].append("Pyrogram library not installed")
        return status
    
    # Check database
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Count userbots
        c.execute("SELECT COUNT(*) as count FROM userbots")
        status['userbots_configured'] = c.fetchone()['count']
        
        # Count connected userbots
        c.execute("SELECT COUNT(*) as count FROM userbots WHERE is_connected = TRUE")
        status['userbots_connected'] = c.fetchone()['count']
        
        # Count userbots with scout mode
        c.execute("SELECT COUNT(*) as count FROM userbots WHERE scout_mode_enabled = TRUE")
        status['userbots_with_scout'] = c.fetchone()['count']
        
        # Count active keywords
        c.execute("SELECT COUNT(*) as count FROM scout_keywords WHERE is_active = TRUE")
        status['active_keywords'] = c.fetchone()['count']
        
        # Check for issues
        if status['userbots_configured'] == 0:
            status['warnings'].append("No userbots configured")
        elif status['userbots_connected'] == 0:
            status['warnings'].append("No userbots connected")
        elif status['userbots_with_scout'] == 0:
            status['warnings'].append("Scout mode not enabled on any userbot")
        elif status['active_keywords'] == 0:
            status['warnings'].append("No active keywords configured")
        
    except Exception as e:
        status['errors'].append(f"Database error: {e}")
        logger.error(f"Error testing scout mode: {e}")
    finally:
        if conn:
            conn.close()
    
    return status

