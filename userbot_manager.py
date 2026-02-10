"""
Userbot Manager - Pyrogram Client Wrapper
Handles Telegram userbot connection, session management, and auto-reconnect
"""

import logging
import asyncio
from typing import Optional
from datetime import datetime, timezone

try:
    from pyrogram import Client
    from pyrogram.errors import (
        SessionPasswordNeeded, PhoneCodeInvalid, PhoneCodeExpired,
        PhoneNumberInvalid, ApiIdInvalid, AuthKeyUnregistered,
        UserDeactivated, UserDeactivatedBan, FloodWait
    )
    from pyrogram.types import User
    PYROGRAM_AVAILABLE = True
except ImportError:
    PYROGRAM_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.error("❌ Pyrogram not installed. Install with: pip install pyrogram")

from userbot_config import userbot_config
from userbot_database import (
    save_session_string,
    get_session_string,
    update_connection_status,
    get_connection_status
)

logger = logging.getLogger(__name__)

class UserbotManager:
    """Manages Pyrogram userbot client and connections"""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self.is_connected: bool = False
        self.is_initializing: bool = False
        self.reconnect_task: Optional[asyncio.Task] = None
        self._setup_phone_code_hash: Optional[str] = None
        
    async def initialize(self) -> bool:
        """Initialize userbot client from database config"""
        if not PYROGRAM_AVAILABLE:
            logger.error("❌ Pyrogram not available")
            await asyncio.to_thread(update_connection_status, False, "Pyrogram not installed")
            return False
        
        if self.is_initializing:
            logger.warning("⚠️ Userbot is already initializing")
            return False
        
        if not userbot_config.is_configured():
            logger.warning("⚠️ Userbot not configured")
            await asyncio.to_thread(update_connection_status, False, "Not configured")
            return False
        
        self.is_initializing = True
        
        try:
            logger.info("🔧 Initializing userbot...")
            
            api_id = userbot_config.api_id
            api_hash = userbot_config.api_hash
            
            if not api_id or not api_hash:
                logger.error("❌ Missing API credentials")
                await asyncio.to_thread(update_connection_status, False, "Missing API credentials")
                return False
            
            # Get session string from database
            session_string = get_session_string()
            
            # 🚀 MODE: Load session file from PostgreSQL for persistent peer cache!
            from userbot_database import get_session_file, save_session_file
            import os
            
            # Create workdir if it doesn't exist
            os.makedirs("./userbot_data", exist_ok=True)
            
            # Try to load session file from PostgreSQL
            session_file_data = get_session_file(1)  # Userbot ID #1 (legacy)
            if session_file_data:
                logger.info(f"✅ Loading session file from PostgreSQL ({len(session_file_data)} bytes)")
                # Write it to disk for Pyrogram to use
                with open("./userbot_data/userbot_session.session", "wb") as f:
                    f.write(session_file_data)
                logger.info("✅ Session file written to disk from PostgreSQL")
            else:
                logger.info("ℹ️ No session file in PostgreSQL, starting fresh")
            
            # Create Pyrogram client
            # 🚀  FIX: in_memory=False to persist peer cache!
            self.client = Client(
                name="userbot_session",
                api_id=int(api_id),
                api_hash=api_hash,
                session_string=session_string,
                workdir="./userbot_data",
                in_memory=False  # Must be False to save peers to disk!
            )
            
            # Connect to Telegram
            await self.client.start()
            
            # Verify connection
            me = await self.client.get_me()
            logger.info(f"✅ Userbot connected as @{me.username or me.first_name} (ID: {me.id})")
            
            self.is_connected = True
            await asyncio.to_thread(update_connection_status, True, f"Connected as @{me.username or me.first_name}")
            
            # 🚀 MODE: Save session file to PostgreSQL for persistence!
            try:
                session_file_path = "./userbot_data/userbot_session.session"
                if os.path.exists(session_file_path):
                    with open(session_file_path, "rb") as f:
                        session_file_data = f.read()
                    await asyncio.to_thread(save_session_file, 1, session_file_data)  # Userbot ID #1
                    logger.info(f"✅ Session file saved to PostgreSQL ({len(session_file_data)} bytes)")
            except Exception as save_err:
                logger.warning(f"⚠️ Could not save session file to PostgreSQL: {save_err}")
            
            # 🔐 MODE: Initialize Telethon for secret chat support!
            try:
                from userbot_telethon_secret import telethon_secret_chat
                
                phone = userbot_config.get_dict().get('phone_number')
                
                telethon_initialized = await telethon_secret_chat.initialize(
                    int(api_id),
                    api_hash,
                    phone
                )
                if telethon_initialized:
                    logger.info("🔐 ✅ Telethon secret chat initialized!")
                else:
                    logger.warning("⚠️ Telethon secret chat initialization failed (will use regular Pyrogram delivery)")
            except Exception as telethon_err:
                logger.warning(f"⚠️ Telethon secret chat init error (non-critical, will fall back to regular delivery): {telethon_err}")
            
            # Save session string for future use
            if not session_string:
                new_session = await self.client.export_session_string()
                save_session_string(new_session)
                logger.info("✅ Session string saved to database")
            
            # 🔍 SCOUT MODE: Setup keyword detection handlers
            try:
                from userbot_scout import setup_scout_handlers
                userbot_id = 1  # Legacy userbot ID
                setup_scout_handlers(self.client, userbot_id)
                logger.info("🔍 ✅ Scout mode handlers registered!")
            except Exception as scout_err:
                logger.warning(f"⚠️ Scout mode setup error (non-critical): {scout_err}")
            
            # Start auto-reconnect monitor if enabled
            if userbot_config.auto_reconnect and not self.reconnect_task:
                self.reconnect_task = asyncio.create_task(self._monitor_connection())
            
            return True
            
        except AuthKeyUnregistered:
            logger.error("❌ Session expired - need to re-authenticate")
            await asyncio.to_thread(update_connection_status, False, "Session expired")
            return False
            
        except (UserDeactivated, UserDeactivatedBan):
            logger.error("❌ User account deactivated or banned")
            await asyncio.to_thread(update_connection_status, False, "Account deactivated")
            return False
            
        except ApiIdInvalid:
            logger.error("❌ Invalid API ID or Hash")
            await asyncio.to_thread(update_connection_status, False, "Invalid API credentials")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error initializing userbot: {e}", exc_info=True)
            await asyncio.to_thread(update_connection_status, False, f"Error: {str(e)[:100]}")
            return False
            
        finally:
            self.is_initializing = False
    
    async def disconnect(self) -> bool:
        """Disconnect userbot"""
        try:
            if self.reconnect_task:
                self.reconnect_task.cancel()
                self.reconnect_task = None
            
            if self.client:
                # Check if client is already disconnected to prevent "already terminated" error
                try:
                    if hasattr(self.client, 'is_connected') and self.client.is_connected:
                        await self.client.stop()
                        logger.info("✅ Userbot disconnected")
                    else:
                        logger.info("ℹ️ Client already disconnected, skipping stop()")
                except ConnectionError as e:
                    if "already terminated" in str(e).lower():
                        logger.info("ℹ️ Client already terminated, safe to ignore")
                    else:
                        raise
            
            self.is_connected = False
            await asyncio.to_thread(update_connection_status, False, "Manually disconnected")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error disconnecting userbot: {e}", exc_info=True)
            return False
    
    async def reconnect(self) -> bool:
        """Reconnect userbot"""
        logger.info("🔄 Reconnecting userbot...")
        await self.disconnect()
        await asyncio.sleep(2)
        return await self.initialize()
    
    async def _monitor_connection(self):
        """Monitor connection and auto-reconnect if needed"""
        logger.info("👁️ Connection monitor started")
        
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                if not self.client or not self.is_connected:
                    continue
                
                # Check if still connected
                try:
                    await self.client.get_me()
                except Exception as e:
                    logger.warning(f"⚠️ Connection lost: {e}")
                    self.is_connected = False
                    await asyncio.to_thread(update_connection_status, False, "Connection lost")
                    
                    if userbot_config.auto_reconnect:
                        logger.info("🔄 Auto-reconnecting...")
                        await self.reconnect()
                
            except asyncio.CancelledError:
                logger.info("Connection monitor stopped")
                break
            except Exception as e:
                logger.error(f"❌ Error in connection monitor: {e}", exc_info=True)
    
    async def start_phone_auth(self, phone_number: str) -> dict:
        """Start phone authentication process"""
        if not PYROGRAM_AVAILABLE:
            return {'success': False, 'error': 'Pyrogram not installed'}
        
        try:
            api_id = userbot_config.api_id
            api_hash = userbot_config.api_hash
            
            if not api_id or not api_hash:
                return {'success': False, 'error': 'API credentials not set'}
            
            # Create temporary client for authentication
            temp_client = Client(
                name="temp_auth",
                api_id=int(api_id),
                api_hash=api_hash,
                phone_number=phone_number,
                workdir="./userbot_data",
                in_memory=True
            )
            
            # Connect and send code
            await temp_client.connect()
            sent_code = await temp_client.send_code(phone_number)
            
            self._setup_phone_code_hash = sent_code.phone_code_hash
            self.client = temp_client
            
            logger.info(f"✅ Verification code sent to {phone_number}")
            
            return {
                'success': True,
                'phone_code_hash': sent_code.phone_code_hash,
                'message': 'Verification code sent'
            }
            
        except PhoneNumberInvalid:
            return {'success': False, 'error': 'Invalid phone number'}
        except FloodWait as e:
            return {'success': False, 'error': f'Too many attempts. Wait {e.value} seconds'}
        except Exception as e:
            logger.error(f"❌ Error starting phone auth: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def verify_phone_code(self, phone_number: str, code: str) -> dict:
        """Verify phone code and complete authentication"""
        if not self.client or not self._setup_phone_code_hash:
            return {'success': False, 'error': 'Authentication not started'}
        
        try:
            # Sign in with code
            await self.client.sign_in(
                phone_number=phone_number,
                phone_code_hash=self._setup_phone_code_hash,
                phone_code=code
            )
            
            # Get user info
            me = await self.client.get_me()
            
            # Export and save session
            session_string = await self.client.export_session_string()
            save_session_string(session_string)
            
            logger.info(f"✅ Authentication successful: @{me.username or me.first_name}")
            
            # Stop temporary client safely
            try:
                if self.client and not self.client.is_connected:
                    # Client already disconnected, just clean up
                    pass
                else:
                    await self.client.stop()
            except Exception as e:
                logger.warning(f"⚠️ Error stopping temp client (safe to ignore): {e}")
            
            # Reset temp variables
            self.client = None
            self._setup_phone_code_hash = None
            
            return {
                'success': True,
                'username': me.username or me.first_name,
                'user_id': me.id
            }
            
        except PhoneCodeInvalid:
            return {'success': False, 'error': 'Invalid verification code'}
        except PhoneCodeExpired:
            return {'success': False, 'error': 'Verification code expired'}
        except SessionPasswordNeeded:
            return {'success': False, 'error': '2FA enabled. Please disable it first'}
        except Exception as e:
            logger.error(f"❌ Error verifying code: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def send_message(
        self,
        user_id: int,
        text: str,
        parse_mode: str = None,
        disable_web_page_preview: bool = True
    ) -> dict:
        """Send text message to user"""
        if not self.is_connected or not self.client:
            return {'success': False, 'error': 'Userbot not connected'}
        
        try:
            message = await self.client.send_message(
                chat_id=user_id,
                text=text,
                parse_mode=parse_mode,
                disable_web_page_preview=disable_web_page_preview
            )
            
            logger.info(f"✅ Message sent to user {user_id}")
            return {'success': True, 'message_id': message.id}
            
        except FloodWait as e:
            logger.warning(f"⚠️ FloodWait: {e.value} seconds")
            return {'success': False, 'error': f'Rate limited. Wait {e.value}s'}
        except Exception as e:
            logger.error(f"❌ Error sending message: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def send_photo(
        self,
        user_id: int,
        photo_path: str,
        caption: str = None,
        ttl_seconds: int = None
    ) -> dict:
        """Send photo to user with optional TTL"""
        if not self.is_connected or not self.client:
            return {'success': False, 'error': 'Userbot not connected'}
        
        try:
            message = await self.client.send_photo(
                chat_id=user_id,
                photo=photo_path,
                caption=caption,
                ttl_seconds=ttl_seconds
            )
            
            logger.info(f"✅ Photo sent to user {user_id}")
            return {'success': True, 'message_id': message.id}
            
        except FloodWait as e:
            logger.warning(f"⚠️ FloodWait: {e.value} seconds")
            return {'success': False, 'error': f'Rate limited. Wait {e.value}s'}
        except Exception as e:
            logger.error(f"❌ Error sending photo: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def send_video(
        self,
        user_id: int,
        video_path: str,
        caption: str = None,
        ttl_seconds: int = None
    ) -> dict:
        """Send video to user with optional TTL"""
        if not self.is_connected or not self.client:
            return {'success': False, 'error': 'Userbot not connected'}
        
        try:
            message = await self.client.send_video(
                chat_id=user_id,
                video=video_path,
                caption=caption,
                ttl_seconds=ttl_seconds
            )
            
            logger.info(f"✅ Video sent to user {user_id}")
            return {'success': True, 'message_id': message.id}
            
        except FloodWait as e:
            logger.warning(f"⚠️ FloodWait: {e.value} seconds")
            return {'success': False, 'error': f'Rate limited. Wait {e.value}s'}
        except Exception as e:
            logger.error(f"❌ Error sending video: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def send_document(
        self,
        user_id: int,
        document_path: str,
        caption: str = None
    ) -> dict:
        """Send document to user"""
        if not self.is_connected or not self.client:
            return {'success': False, 'error': 'Userbot not connected'}
        
        try:
            message = await self.client.send_document(
                chat_id=user_id,
                document=document_path,
                caption=caption
            )
            
            logger.info(f"✅ Document sent to user {user_id}")
            return {'success': True, 'message_id': message.id}
            
        except FloodWait as e:
            logger.warning(f"⚠️ FloodWait: {e.value} seconds")
            return {'success': False, 'error': f'Rate limited. Wait {e.value}s'}
        except Exception as e:
            logger.error(f"❌ Error sending document: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
    
    async def get_user_info(self, user_id: int) -> Optional[dict]:
        """Get user information"""
        if not self.is_connected or not self.client:
            return None
        
        try:
            user = await self.client.get_users(user_id)
            return {
                'id': user.id,
                'username': user.username,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'is_bot': user.is_bot
            }
        except Exception as e:
            logger.error(f"❌ Error getting user info: {e}", exc_info=True)
            return None
    
    async def deliver_product_via_secret_chat(
        self,
        buyer_user_id: int,
        product_data: dict,
        order_id: str,
        telegram_bot = None
    ) -> dict:
        """
        🚀 MODE: Deliver product using Saved Messages storage + Main Bot delivery
        
        This prevents media corruption and PEER_ID_INVALID by:
        1. Uploading media to userbot's Saved Messages (preserves quality, safe storage)
        2. Getting new file_ids from Saved Messages
        3. Using MAIN BOT to send to buyer (no PEER_ID_INVALID!)
        4. Cleaning up Saved Messages after 6 hours
        
        Args:
            buyer_user_id: Telegram user ID of buyer
            product_data: Dict with keys: product_id, product_name, size, city, district, price, media_items
            order_id: Unique order identifier
            telegram_bot: Main Telegram bot instance for sending messages
            
        Returns:
            dict: {'success': bool, 'error': str (if failed), 'media_file_ids': [list of new file_ids]}
        """
        if not self.is_connected or not self.client:
            logger.error("❌ Userbot not connected for secret chat delivery")
            return {'success': False, 'error': 'Userbot not connected'}
        
        try:
            product_id = product_data.get('product_id')
            product_name = product_data.get('product_name', 'Product')
            media_items = product_data.get('media_items', [])
            
            logger.info(f"🔐 Starting TELETHON SECRET CHAT delivery for user {buyer_user_id}, product {product_id}")
            
            # 🔐 Try to use Telethon for TRUE SECRET CHAT delivery!
            try:
                from userbot_telethon_secret import telethon_secret_chat
                
                if not telethon_secret_chat.is_connected:
                    logger.warning("⚠️ Telethon not connected, trying to initialize...")
                    
                    phone = userbot_config.get_dict().get('phone_number')
                    
                    if phone:
                        telethon_initialized = await telethon_secret_chat.initialize(
                            int(userbot_config.api_id),
                            userbot_config.api_hash,
                            phone
                        )
                        
                        if not telethon_initialized:
                            logger.error("❌ Telethon not available - admin needs to set up Telethon in admin panel")
                            return {
                                'success': False,
                                'error': 'Telethon secret chat not configured. Admin must set up Telethon authentication in admin panel.'
                            }
                    else:
                        logger.error("❌ No phone number available")
                        return {
                            'success': False,
                            'error': 'No phone available for secret chat'
                        }
                
                # Prepare media for Telethon
                media_binary_items = []
                
                if media_items:
                    for idx, media_item in enumerate(media_items, 1):
                        media_binary = media_item.get('media_binary')
                        media_type = media_item.get('media_type')
                        
                        if not media_binary:
                            logger.warning(f"⚠️ No binary data for media item {idx}")
                            continue
                        
                        media_binary_items.append({
                            'media_type': media_type,
                            'media_binary': media_binary,
                            'filename': f"product_{product_id}_{idx}.{'jpg' if media_type == 'photo' else 'mp4'}"
                        })
                
                # Call Telethon secret chat delivery
                logger.info(f"🔐 Calling Telethon SECRET CHAT delivery...")
                success, message = await telethon_secret_chat.deliver_via_secret_chat(
                    buyer_user_id=buyer_user_id,
                    product_data=product_data,
                    media_binary_items=media_binary_items,
                    order_id=order_id
                )
                
                if success:
                    logger.info(f"✅ TELETHON SECRET CHAT: {message}")
                    return {
                        'success': True,
                        'media_count': len(media_binary_items),
                        'message': message
                    }
                else:
                    logger.error(f"❌ TELETHON SECRET CHAT: {message}")
                    return {
                        'success': False,
                        'error': message
                    }
                    
            except Exception as telethon_err:
                logger.error(f"❌ Telethon error: {telethon_err}", exc_info=True)
                return {
                    'success': False,
                    'error': f'Telethon delivery failed: {str(telethon_err)}'
                }
            
        except Exception as e:
            logger.error(f"❌ Secret chat delivery failed: {e}", exc_info=True)
            return {
                'success': False,
                'error': f'Delivery error: {str(e)}'
            }


    
    async def _cleanup_saved_messages_later(self, message_ids: list, delay_hours: int = 6):
        """Cleanup Saved Messages after specified delay"""
        await asyncio.sleep(delay_hours * 3600)
        
        try:
            if self.is_connected and self.client:
                await self.client.delete_messages(
                    chat_id='me',  # Saved Messages
                    message_ids=message_ids
                )
                logger.info(f"🗑️ Cleaned up {len(message_ids)} messages from Saved Messages")
        except Exception as e:
            logger.error(f"❌ Failed to cleanup saved messages: {e}")

# Global userbot manager instance
userbot_manager = UserbotManager()

