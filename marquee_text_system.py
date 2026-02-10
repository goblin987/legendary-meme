"""
Marquee Text System - Running Text Animation for Buttons
Creates animated scrolling text that can be added to any menu button
"""

import logging
import asyncio
from typing import Dict, Optional
from utils import get_db_connection

logger = logging.getLogger(__name__)

# Animation speeds (seconds per frame)
SPEEDS = {
    'slow': 1.0,
    'medium': 0.5,
    'fast': 0.2
}

def init_marquee_tables():
    """Initialize marquee text settings table"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        # Create marquee settings table
        c.execute('''
            CREATE TABLE IF NOT EXISTS marquee_settings (
                id SERIAL PRIMARY KEY,
                text TEXT NOT NULL,
                enabled BOOLEAN DEFAULT true,
                speed VARCHAR(20) DEFAULT 'medium',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert default marquee if none exists
        c.execute('SELECT COUNT(*) as count FROM marquee_settings')
        result = c.fetchone()
        count = result['count'] if result else 0
        
        if count == 0:
            c.execute('''
                INSERT INTO marquee_settings (text, enabled, speed)
                VALUES (%s, %s, %s)
            ''', ('🎉 WELCOME TO OUR SHOP! 🎉', True, 'medium'))
            logger.info("✅ Created default marquee text")
        
        conn.commit()
        logger.info("✅ Marquee tables initialized")
        
    except Exception as e:
        logger.error(f"❌ Error initializing marquee tables: {e}", exc_info=True)
        conn.rollback()
    finally:
        conn.close()

def get_marquee_settings() -> Optional[Dict]:
    """Get current marquee settings"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            SELECT id, text, enabled, speed
            FROM marquee_settings
            ORDER BY id DESC
            LIMIT 1
        ''')
        
        result = c.fetchone()
        
        if result:
            return {
                'id': result['id'],
                'text': result['text'],
                'enabled': result['enabled'],
                'speed': result['speed']
            }
        
        return None
        
    finally:
        conn.close()

def update_marquee_text(text: str) -> bool:
    """Update marquee text"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE marquee_settings
            SET text = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = (SELECT id FROM marquee_settings ORDER BY id DESC LIMIT 1)
        ''', (text,))
        
        conn.commit()
        logger.info(f"✅ Updated marquee text: {text}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating marquee text: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        conn.close()

def update_marquee_enabled(enabled: bool) -> bool:
    """Enable/disable marquee animation"""
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE marquee_settings
            SET enabled = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = (SELECT id FROM marquee_settings ORDER BY id DESC LIMIT 1)
        ''', (enabled,))
        
        conn.commit()
        logger.info(f"✅ Marquee {'enabled' if enabled else 'disabled'}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating marquee enabled: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        conn.close()

def update_marquee_speed(speed: str) -> bool:
    """Update marquee animation speed"""
    if speed not in SPEEDS:
        logger.error(f"❌ Invalid speed: {speed}")
        return False
    
    conn = get_db_connection()
    c = conn.cursor()
    
    try:
        c.execute('''
            UPDATE marquee_settings
            SET speed = %s, updated_at = CURRENT_TIMESTAMP
            WHERE id = (SELECT id FROM marquee_settings ORDER BY id DESC LIMIT 1)
        ''', (speed,))
        
        conn.commit()
        logger.info(f"✅ Updated marquee speed: {speed}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error updating marquee speed: {e}", exc_info=True)
        conn.rollback()
        return False
    finally:
        conn.close()

def generate_marquee_frame(text: str, position: int, width: int = 20) -> str:
    """
    Generate a single frame of marquee animation
    
    Args:
        text: The text to animate
        position: Current scroll position
        width: Width of the display window
    
    Returns:
        Animated frame string
    """
    # Add padding for smooth loop
    padded_text = text + "   " + text
    
    # Calculate visible portion
    start = position % len(text)
    end = start + width
    
    # Handle wrapping
    if end <= len(padded_text):
        frame = padded_text[start:end]
    else:
        frame = padded_text[start:] + padded_text[:end - len(padded_text)]
    
    return frame[:width]

def get_current_marquee_frame(position: int) -> str:
    """Get current marquee frame based on settings"""
    settings = get_marquee_settings()
    
    if not settings:
        return "📢 MARQUEE TEXT"
    
    if not settings['enabled']:
        # If disabled, return static text (centered)
        text = settings['text']
        if len(text) > 20:
            return text[:20]
        return text.center(20)
    
    # Generate animated frame
    return generate_marquee_frame(settings['text'], position, width=20)

def get_marquee_speed() -> float:
    """Get current animation speed in seconds"""
    settings = get_marquee_settings()
    
    if not settings:
        return SPEEDS['medium']
    
    return SPEEDS.get(settings['speed'], SPEEDS['medium'])

# Global position tracker for animation
_marquee_position = 0

def advance_marquee_position():
    """Advance marquee position by 1"""
    global _marquee_position
    _marquee_position += 1
    return _marquee_position

def get_marquee_position() -> int:
    """Get current marquee position"""
    return _marquee_position

