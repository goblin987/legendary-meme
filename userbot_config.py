"""
Userbot Configuration Management
Loads configuration from PostgreSQL database (not environment variables)
"""

import logging
from typing import Optional, Dict
from userbot_database import (
    get_userbot_config,
    is_userbot_configured,
    is_userbot_enabled,
    save_userbot_config,
    update_userbot_setting
)

logger = logging.getLogger(__name__)

class UserbotConfig:
    """Userbot configuration manager - loads from PostgreSQL"""
    
    def __init__(self):
        self._config: Optional[Dict] = None
        self.load_from_database()
    
    def load_from_database(self) -> bool:
        """Load configuration from PostgreSQL database"""
        try:
            self._config = get_userbot_config()
            if self._config:
                logger.info("✅ Userbot configuration loaded from database")
                return True
            logger.info("ℹ️ No userbot configuration found in database")
            return False
        except Exception as e:
            logger.error(f"❌ Error loading userbot config: {e}", exc_info=True)
            return False
    
    def reload(self) -> bool:
        """Reload configuration from database"""
        return self.load_from_database()
    
    def is_configured(self) -> bool:
        """Check if userbot is configured"""
        return is_userbot_configured()
    
    def is_enabled(self) -> bool:
        """Check if userbot delivery is enabled"""
        return is_userbot_enabled()
    
    @property
    def api_id(self) -> Optional[str]:
        """Get API ID"""
        if not self._config:
            self.load_from_database()
        return self._config.get('api_id') if self._config else None
    
    @property
    def api_hash(self) -> Optional[str]:
        """Get API Hash"""
        if not self._config:
            self.load_from_database()
        return self._config.get('api_hash') if self._config else None
    
    @property
    def phone_number(self) -> Optional[str]:
        """Get phone number"""
        if not self._config:
            self.load_from_database()
        return self._config.get('phone_number') if self._config else None
    
    @property
    def enabled(self) -> bool:
        """Check if userbot is enabled"""
        if not self._config:
            self.load_from_database()
        return self._config.get('enabled', False) if self._config else False
    
    @property
    def auto_reconnect(self) -> bool:
        """Check if auto-reconnect is enabled"""
        if not self._config:
            self.load_from_database()
        return self._config.get('auto_reconnect', True) if self._config else True
    
    @property
    def send_notifications(self) -> bool:
        """Check if admin notifications are enabled"""
        if not self._config:
            self.load_from_database()
        return self._config.get('send_notifications', True) if self._config else True
    
    @property
    def max_retries(self) -> int:
        """Get max retries for delivery"""
        if not self._config:
            self.load_from_database()
        return self._config.get('max_retries', 3) if self._config else 3
    
    @property
    def retry_delay(self) -> int:
        """Get retry delay in seconds"""
        if not self._config:
            self.load_from_database()
        return self._config.get('retry_delay', 5) if self._config else 5
    
    @property
    def secret_chat_ttl(self) -> int:
        """Get secret chat message TTL in seconds"""
        if not self._config:
            self.load_from_database()
        return self._config.get('secret_chat_ttl', 86400) if self._config else 86400
    
    def save(self, api_id: str, api_hash: str, phone_number: str) -> bool:
        """Save configuration to database"""
        success = save_userbot_config(api_id, api_hash, phone_number)
        if success:
            self.reload()
        return success
    
    def set_enabled(self, enabled: bool) -> bool:
        """Enable or disable userbot"""
        success = update_userbot_setting('enabled', enabled)
        if success:
            self.reload()
        return success
    
    def set_auto_reconnect(self, auto_reconnect: bool) -> bool:
        """Enable or disable auto-reconnect"""
        success = update_userbot_setting('auto_reconnect', auto_reconnect)
        if success:
            self.reload()
        return success
    
    def set_notifications(self, send_notifications: bool) -> bool:
        """Enable or disable admin notifications"""
        success = update_userbot_setting('send_notifications', send_notifications)
        if success:
            self.reload()
        return success
    
    def set_max_retries(self, max_retries: int) -> bool:
        """Set max retries"""
        if not 1 <= max_retries <= 5:
            logger.error("Max retries must be between 1 and 5")
            return False
        success = update_userbot_setting('max_retries', max_retries)
        if success:
            self.reload()
        return success
    
    def set_retry_delay(self, retry_delay: int) -> bool:
        """Set retry delay"""
        if not 1 <= retry_delay <= 30:
            logger.error("Retry delay must be between 1 and 30 seconds")
            return False
        success = update_userbot_setting('retry_delay', retry_delay)
        if success:
            self.reload()
        return success
    
    def set_secret_chat_ttl(self, ttl_hours: int) -> bool:
        """Set secret chat TTL in hours"""
        if not 1 <= ttl_hours <= 48:
            logger.error("TTL must be between 1 and 48 hours")
            return False
        ttl_seconds = ttl_hours * 3600
        success = update_userbot_setting('secret_chat_ttl', ttl_seconds)
        if success:
            self.reload()
        return success
    
    def get_dict(self, force_fresh: bool = False) -> Dict:
        """Get configuration as dictionary
        
        Args:
            force_fresh: If True, bypasses cache and reads directly from DB
        """
        if force_fresh:
            # 🚀  Force fresh read from DB, bypass cache completely
            fresh_config = get_userbot_config()
            if fresh_config:
                self._config = fresh_config  # Update cache for next time
                return fresh_config
            return {}
        
        if not self._config:
            self.load_from_database()
        return self._config if self._config else {}

# Global configuration instance
userbot_config = UserbotConfig()

