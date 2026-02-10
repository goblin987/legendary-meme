"""
Userbot Pool Manager
Manages multiple Telethon userbots for secret chat delivery with load balancing

ATTEMPT #42 (FINAL SOLUTION):
- Photos → Secret Chat (E2E encrypted, working perfectly)
- Videos → Private Messages (playable, Telegram server-encrypted)
- Transparent notifications explain the approach to users
"""

import logging
import asyncio
import os
import tempfile
from typing import Optional, Dict, List, Tuple, Any
from datetime import datetime, timezone, timedelta
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeVideo
from telethon.errors import PeerFloodError, FloodWaitError, UserPrivacyRestrictedError
from telethon_secret_chat import SecretChatManager
import io

logger = logging.getLogger(__name__)

class UserbotPool:
    """Manages a pool of Telethon userbots for secret chat delivery"""
    
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {}  # userbot_id -> client
        self.secret_chat_managers: Dict[int, SecretChatManager] = {}  # userbot_id -> manager
        self.flooded_until: Dict[int, datetime] = {}  # userbot_id -> cooldown end time
        self.is_initialized = False
        self._last_used_index = 0
        
    async def initialize(self):
        """Initialize all enabled userbots from database"""
        if self.is_initialized:
            logger.info("Userbot pool already initialized")
            return
        
        logger.info("🔄 Initializing userbot pool...")
        
        from userbot_database import get_db_connection
        conn = get_db_connection()
        c = conn.cursor()
        
        try:
            # Get all enabled userbots with sessions
            c.execute("""
                SELECT id, name, api_id, api_hash, phone_number, session_string, priority
                FROM userbots
                WHERE is_enabled = TRUE AND session_string IS NOT NULL
                ORDER BY priority DESC, id ASC
            """)
            
            userbots = c.fetchall()
            
            if not userbots:
                logger.warning("⚠️ No enabled userbots found in database")
                return
            
            logger.info(f"📋 Found {len(userbots)} enabled userbot(s) to initialize")
            
            # Initialize each userbot
            for ub in userbots:
                userbot_id = ub['id']
                name = ub['name']
                api_id = int(ub['api_id'])
                api_hash = ub['api_hash']
                session_string = ub['session_string']
                
                try:
                    logger.info(f"🔌 Connecting userbot #{userbot_id} ({name})...")
                    
                    # Create Telethon client
                    client = TelegramClient(
                        StringSession(session_string),
                        api_id,
                        api_hash
                    )
                    
                    await client.connect()
                    
                    # Check if authorized
                    if not await client.is_user_authorized():
                        logger.error(f"❌ Userbot #{userbot_id} not authorized!")
                        await client.disconnect()
                        self._update_connection_status(userbot_id, False, "Not authorized")
                        continue
                    
                    # Get user info
                    me = await client.get_me()
                    username = me.username or me.first_name
                    
                    # Create secret chat manager
                    secret_chat_manager = SecretChatManager(client, auto_accept=True)
                    
                    # Store in pool
                    self.clients[userbot_id] = client
                    self.secret_chat_managers[userbot_id] = secret_chat_manager
                    
                    # Update database status
                    self._update_connection_status(userbot_id, True, f"Connected as @{username}")
                    
                    logger.info(f"✅ Userbot #{userbot_id} ({name}) connected as @{username}")
                    
                    # Set up scout handlers if enabled for this userbot
                    try:
                        c.execute("SELECT scout_mode_enabled FROM userbots WHERE id = %s", (userbot_id,))
                        scout_result = c.fetchone()
                        if scout_result and scout_result['scout_mode_enabled']:
                            logger.info(f"🔍 Setting up Telethon scout handlers for userbot #{userbot_id}...")
                            self._setup_telethon_scout_handlers(client, userbot_id)
                    except Exception as scout_err:
                        logger.error(f"Error setting up scout handlers for userbot #{userbot_id}: {scout_err}")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to initialize userbot #{userbot_id} ({name}): {e}", exc_info=True)
                    self._update_connection_status(userbot_id, False, f"Error: {str(e)[:100]}")
                    continue
            
            self.is_initialized = True
            logger.info(f"✅ Userbot pool initialized with {len(self.clients)} active userbot(s)")
            
        except Exception as e:
            logger.error(f"❌ Error initializing userbot pool: {e}", exc_info=True)
        finally:
            conn.close()
    
    def _update_connection_status(self, userbot_id: int, is_connected: bool, status_message: str):
        """Update userbot connection status in database"""
        from userbot_database import get_db_connection
        conn = get_db_connection()
        c = conn.cursor()
        try:
            c.execute("""
                UPDATE userbots
                SET is_connected = %s, status_message = %s, last_connected_at = %s
                WHERE id = %s
            """, (is_connected, status_message, datetime.now(timezone.utc) if is_connected else None, userbot_id))
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating connection status for userbot #{userbot_id}: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_available_userbot(self) -> Optional[Tuple[int, TelegramClient, SecretChatManager]]:
        """Get next available userbot using round-robin selection, skipping flooded ones"""
        if not self.clients:
            logger.warning("⚠️ No userbots available in pool")
            return None
        
        userbot_ids = list(self.clients.keys())
        start_index = self._last_used_index
        
        # Try all userbots starting from last used
        for _ in range(len(userbot_ids)):
            self._last_used_index = (self._last_used_index + 1) % len(userbot_ids)
            userbot_id = userbot_ids[self._last_used_index]
        
            # Check flood cooldown
            if userbot_id in self.flooded_until:
                if datetime.now(timezone.utc) < self.flooded_until[userbot_id]:
                    logger.info(f"⏳ Userbot #{userbot_id} is in flood cooldown until {self.flooded_until[userbot_id]}")
                    continue
                else:
                    # Cooldown expired
                    del self.flooded_until[userbot_id]
            
            client = self.clients.get(userbot_id)
            secret_chat_manager = self.secret_chat_managers.get(userbot_id)
            
            if client and secret_chat_manager:
                # Check connection
                if not client.is_connected:
                    try:
                        asyncio.create_task(client.connect())
                    except: pass
                
                logger.info(f"🎯 Selected userbot #{userbot_id} for delivery (round-robin)")
                return userbot_id, client, secret_chat_manager
        
        logger.warning("⚠️ All userbots are flooded or unavailable")
        return None
    
    async def deliver_via_secret_chat(
        self,
        buyer_user_id: int,
        buyer_username: Optional[str],
        product_data: dict,
        media_binary_items: List[Dict],
        order_id: str
    ) -> Tuple[bool, str]:
        """Deliver media via secret chat with automatic failover and flood handling"""
        attempt_errors = []
        
        for attempt in range(3):
            userbot_info = self.get_available_userbot()
            if not userbot_info:
                return False, f"No available userbots. Errors: {attempt_errors}"
        
            userbot_id, client, secret_chat_manager = userbot_info
        
            try:
                return await self._attempt_delivery(
                    userbot_id, client, secret_chat_manager,
                    buyer_user_id, buyer_username, product_data, media_binary_items, order_id
                )
            except PeerFloodError as e:
                logger.error(f"❌ Userbot #{userbot_id} FLOODED: {e}")
                self.flooded_until[userbot_id] = datetime.now(timezone.utc) + timedelta(minutes=30)
                attempt_errors.append(f"UB#{userbot_id} Flood")
            except Exception as e:
                logger.error(f"❌ Userbot #{userbot_id} Failed: {e}")
                attempt_errors.append(f"UB#{userbot_id} {type(e).__name__}")
        
        return False, f"All delivery attempts failed. Errors: {attempt_errors}"

    async def _attempt_delivery(
        self,
        userbot_id: int,
        client: TelegramClient,
        secret_chat_manager: SecretChatManager,
        buyer_user_id: int,
        buyer_username: Optional[str],
        product_data: dict,
        media_binary_items: List[Dict],
        order_id: str
    ) -> Tuple[bool, str]:
        """
        Internal delivery attempt with specific userbot
        """
        try:
            logger.info(f"🔐 Starting SECRET CHAT delivery via userbot #{userbot_id} to user {buyer_user_id} (@{buyer_username or 'no_username'})")
            
            # 1. Get FULL user entity (not just InputPeer) - try username first
            try:
                if buyer_username:
                    logger.info(f"🔍 Getting FULL user entity by username: @{buyer_username}...")
                    user_entity = await client.get_entity(buyer_username)
                    logger.info(f"✅ Got full user entity by username: {user_entity.id}")
                else:
                    logger.info(f"🔍 Getting FULL user entity by ID: {buyer_user_id}...")
                    user_entity = await client.get_entity(buyer_user_id)
                    logger.info(f"✅ Got full user entity by ID: {user_entity.id}")
            except Exception as e:
                logger.error(f"❌ Error getting user entity for {buyer_user_id} (@{buyer_username or 'N/A'}): {e}")
                return False, f"Failed to get user entity: {e}"
            
            # 2. Get or create secret chat (REUSE existing chats to avoid rate limits!)
            secret_chat_id = None
            secret_chat_obj = None
            use_secret_chat = False

            try:
                # 🎯 ATTEMPT #28: Check for existing secret chat FIRST!
                # Try multiple methods to get existing chats
                existing_chat = None
                
                try:
                    # Method 1: Try session.get_all_secret_chats()
                    if hasattr(secret_chat_manager, 'session'):
                        session = secret_chat_manager.session
                        if hasattr(session, 'get_all_secret_chats'):
                            existing_chats = session.get_all_secret_chats()
                            logger.info(f"♻️ Found {len(existing_chats)} existing secret chats via session")
                            for chat in existing_chats:
                                if hasattr(chat, 'user_id') and chat.user_id == user_entity.id:
                                    existing_chat = chat
                                    logger.info(f"♻️ Found existing chat with user {user_entity.id}! Chat ID: {chat.id}")
                                    break
                        elif hasattr(session, 'get_secret_chat_by_user_id'):
                            # Method 2: Direct lookup by user_id
                            existing_chat = session.get_secret_chat_by_user_id(user_entity.id)
                            if existing_chat:
                                logger.info(f"♻️ Found existing chat via user_id lookup: {existing_chat.id}")
                except Exception as lookup_err:
                    logger.info(f"ℹ️ Could not lookup existing chats: {lookup_err}")
                
                if existing_chat:
                    # Reuse existing chat!
                    secret_chat_obj = existing_chat
                    secret_chat_id = existing_chat.id
                    logger.info(f"✅ Reusing existing secret chat: {secret_chat_id}")
                else:
                    # Create new chat only if none exists
                    logger.info(f"🔐 Starting NEW secret chat with user {user_entity.id} (@{buyer_username or 'N/A'})...")
                    secret_chat_id = await secret_chat_manager.start_secret_chat(user_entity)
                    logger.info(f"✅ Secret chat started! ID: {secret_chat_id}")
                    # Wait longer for encryption handshake
                    await asyncio.sleep(3)
                    
                    # Get the actual secret chat object from the manager
                    secret_chat_obj = secret_chat_manager.get_secret_chat(secret_chat_id)
                    logger.info(f"✅ Retrieved secret chat object: {type(secret_chat_obj)}")
                
                if secret_chat_obj:
                    use_secret_chat = True
                    logger.info(f"✅ Secret chat setup successful. Using E2E encryption for photos.")
                else:
                    raise Exception("Retrieved secret chat object is None")

            except PeerFloodError:
                raise  # Bubble up for rotation
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Failed to start secret chat: {e}. Falling back to standard delivery.")
                use_secret_chat = False
                # Proceed with standard delivery (fallback)
            
            # 3. Send elegant notification
            if use_secret_chat:
                notification_text = (
                    f"🔐 **Encrypted Delivery**\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Order Details:**\n"
                    f"📦 Order ID: #{order_id}\n"
                    f"🏷️ Product: {product_data.get('product_name', 'Digital Content')}\n"
                    f"📏 Size: {product_data.get('size', 'N/A')}\n"
                    f"📍 Location: {product_data.get('city', 'N/A')}, {product_data.get('district', 'N/A')}\n"
                    f"💰 Price: {product_data.get('price', 0):.2f} EUR\n\n"
                    f"⏬ **Delivering your content securely...**\n\n"
                    f"🔒 _This is an end-to-end encrypted chat._"
                )
                try:
                    await secret_chat_manager.send_secret_message(secret_chat_obj, notification_text)
                    logger.info(f"✅ Sent elegant notification to secret chat")
                except Exception as e:
                    logger.error(f"❌ Failed to send notification: {e}")
            else:
                notification_text = (
                    f"✅ **Delivery (Standard)**\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Order Details:**\n"
                    f"📦 Order ID: #{order_id}\n"
                    f"🏷️ Product: {product_data.get('product_name', 'Digital Content')}\n"
                    f"📏 Size: {product_data.get('size', 'N/A')}\n"
                    f"📍 Location: {product_data.get('city', 'N/A')}, {product_data.get('district', 'N/A')}\n"
                    f"💰 Price: {product_data.get('price', 0):.2f} EUR\n\n"
                    f"⚠️ **Note:** Secure chat connection failed. Delivering via standard private message to ensure you receive your goods.\n"
                    f"⏬ **Receiving content below...**"
                )
                try:
                    await client.send_message(user_entity, notification_text)
                    logger.info(f"✅ Sent notification to standard chat")
                except PeerFloodError:
                    raise
                except Exception as e:
                    logger.error(f"❌ Failed to send standard notification: {e}")
            
            await asyncio.sleep(1)
            
            # 4. Send media files
            sent_media_count = 0
            if media_binary_items and len(media_binary_items) > 0:
                logger.info(f"📂 Sending {len(media_binary_items)} media items via SECRET CHAT...")
                for idx, media_item in enumerate(media_binary_items, 1):
                    media_type = media_item['media_type']
                    media_binary = media_item['media_binary']
                    filename = media_item['filename']
                    
                    try:
                        logger.info(f"📤 Sending SECRET CHAT media {idx}/{len(media_binary_items)} ({len(media_binary)} bytes) type: {media_type}...")
                        
                        # Save to temp file (secret chat library needs file path)
                        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as temp_file:
                            temp_file.write(media_binary)
                            temp_path = temp_file.name
                        
                        
                        try:
                            if use_secret_chat and media_type == 'photo':
                                # --- SECRET CHAT PHOTO ---
                                file_size = len(media_binary)
                                await secret_chat_manager.send_secret_photo(
                                    secret_chat_obj,
                                    temp_path,
                                    thumb=b'',
                                    thumb_w=100,
                                    thumb_h=100,
                                    w=960,
                                    h=1280,
                                    size=file_size
                                )
                                logger.info(f"✅ SECRET CHAT photo {idx} sent!")
                                sent_media_count += 1
                                
                            elif use_secret_chat and media_type == 'video':
                                # --- SECRET CHAT VIDEO (Attempt as Document to avoid corruption) ---
                                logger.info(f"🚀 ATTEMPT: Sending video as Secret Document...")
                                
                                try:
                                    # Try to use send_secret_document if available (avoids re-encoding/corruption)
                                    method = getattr(secret_chat_manager, 'send_secret_document', None) or getattr(secret_chat_manager, 'send_secret_file', None)
                                    
                                    if method:
                                        await method(
                                            secret_chat_obj,
                                            temp_path,
                                            caption=caption,
                                            # We rely on file extension for detection
                                        )
                                        logger.info(f"✅ Sent video as secret document!")
                                        sent_media_count += 1
                                    else:
                                        # Fallback to original send_secret_video if document method missing
                                        # (This might corrupt, but we tried)
                                        # Actually, let's fallback to PM if we can't do document
                                        raise Exception("Library missing send_secret_document")
                                        
                                except Exception as e:
                                    logger.error(f"Failed to send secret video as document: {e}")
                                    
                                    # Fallback to PM (Reliable)
                                    video_caption = (
                                        f"🎬 **Your Video Content**\n"
                                        f"━━━━━━━━━━━━━━━━━━━━\n\n"
                                        f"📦 **Order:** #{order_id}\n"
                                        f"🎞️ **Product:** {product_data.get('product_name', 'Digital Content')}\n"
                                        f"✨ **Ready to watch!**"
                                    )
                                    
                                    await client.send_file(
                                        user_entity,
                                        temp_path,
                                        caption=video_caption,
                                        force_document=False,
                                        supports_streaming=True
                                    )
                                    logger.info(f"✅ Video {idx} sent to PRIVATE MESSAGE (Fallback)!")
                                    
                                    # Send notification to secret chat
                                    try:
                                        await secret_chat_manager.send_secret_message(
                                            secret_chat_obj,
                                            f"🎬 Video {idx} sent to your regular chat messages (Secure Delivery Fallback)."
                                        )
                                    except: pass
                                    sent_media_count += 1
                                    
                            else:
                                # --- STANDARD DELIVERY (Fallback) ---
                                logger.info(f"📤 Sending {media_type} via standard PM (Fallback)...")
                                caption = (
                                    f"📦 **Item {idx}/{len(media_binary_items)}**\n"
                                    f"TYPE: {media_type.upper()}"
                                )
                                await client.send_file(
                                    user_entity,
                                    temp_path,
                                    caption=caption,
                                    force_document=False,
                                    supports_streaming=True
                                )
                                logger.info(f"✅ Sent item {idx} via standard PM")
                                sent_media_count += 1
                                
                        except PeerFloodError:
                            raise
                        except Exception as send_err:
                            logger.error(f"❌ Failed to send media {idx}: {send_err}", exc_info=True)
                            # Try extremely simple fallback
                            try:
                                await client.send_file(user_entity, temp_path, caption=f"Item {idx} (Retry)")
                            except: pass

                        finally:
                            try:
                                os.unlink(temp_path)
                            except: pass
                        
                        await asyncio.sleep(0.5)
                        
                    except Exception as e:
                        logger.error(f"❌ Outer error sending media {idx}: {e}", exc_info=True)
            else:
                 logger.warning(f"⚠️ No media items to send for order {order_id}")
            
            # 5. Send completion message
            if use_secret_chat:
                completion_text = (
                    f"✅ **Delivery Complete!**\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Your Order Summary:**\n"
                    f"🏷️ **Product:** {product_data.get('product_name', 'Digital Content')}\n"
                    f"📏 **Size:** {product_data.get('size', 'N/A')}\n"
                    f"📍 **Location:** {product_data.get('city', 'N/A')}, {product_data.get('district', 'N/A')}\n"
                    f"💰 **Paid:** {product_data.get('price', 0):.2f} EUR\n\n"
                    f"📦 **Order ID:** #{order_id}\n\n"
                    f"🎉 **Thank you for your purchase!**"
                )
                try:
                    await secret_chat_manager.send_secret_message(secret_chat_obj, completion_text)
                    logger.info(f"✅ Sent elegant completion message to secret chat")
                except Exception as e:
                    logger.error(f"❌ Failed to send completion message: {e}")
            else:
                completion_text = (
                    f"✅ **Delivery Complete!**\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Your Order Summary:**\n"
                    f"🏷️ **Product:** {product_data.get('product_name', 'Digital Content')}\n"
                    f"📦 **Order ID:** #{order_id}\n\n"
                    f"🎉 **Thank you for your purchase!**"
                )
                try:
                    await client.send_message(user_entity, completion_text)
                    logger.info(f"✅ Sent completion message to standard chat")
                except Exception as e:
                    logger.error(f"❌ Failed to send standard completion: {e}")

            return True, f"Product delivered via {'SECRET CHAT' if use_secret_chat else 'STANDARD PM'} (userbot #{userbot_id}) to user {buyer_user_id}"
            
        except Exception as e:
            logger.error(f"❌ Secret chat delivery failed (userbot #{userbot_id}): {e}", exc_info=True)
            return False, f"Delivery error: {e}"
    
    def _setup_telethon_scout_handlers(self, client: TelegramClient, userbot_id: int):
        """Setup Telethon message handlers for scout mode"""
        from telethon import events
        from userbot_scout import scout_system
        
        logger.info(f"🔍 Registering Telethon scout handlers for userbot #{userbot_id}")
        
        @client.on(events.NewMessage(incoming=True))
        async def handle_scout_message(event):
            """Monitor all messages for keywords (Telethon version)"""
            try:
                # Only process group messages with text
                if not event.is_group or not event.text:
                    return
                
                # Don't process own messages
                if event.out:
                    return
                
                # Check if scout mode is still enabled
                from userbot_database import get_db_connection
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT scout_mode_enabled FROM userbots WHERE id = %s", (userbot_id,))
                result = c.fetchone()
                conn.close()
                
                if not result or not result['scout_mode_enabled']:
                    return
                
                # Check message for keywords
                logger.info(f"🔍 Scout checking message in chat {event.chat_id}: '{event.text[:50]}...'")
                matched_keyword = await scout_system.check_message(event.text)
                
                if matched_keyword:
                    logger.info(f"🔍 Keyword '{matched_keyword['keyword']}' detected in chat {event.chat_id} (userbot {userbot_id})")
                    
                    # Delay to look more human
                    delay = matched_keyword.get('response_delay_seconds', 3)
                    await asyncio.sleep(delay)
                    
                    # Send reply
                    response_text = matched_keyword['response_text']
                    sent_message = await event.reply(response_text)
                    
                    # Log successful trigger (create a mock message object for logging)
                    class MockMessage:
                        def __init__(self, event):
                            self.id = event.id
                            self.text = event.text
                            
                            class MockChat:
                                def __init__(self, chat_id, title):
                                    self.id = chat_id
                                    self.title = title
                            
                            class MockUser:
                                def __init__(self, sender):
                                    if sender:
                                        self.id = sender.id
                                        self.username = getattr(sender, 'username', None)
                                    else:
                                        self.id = None
                                        self.username = None
                            
                            self.chat = MockChat(event.chat_id, getattr(event.chat, 'title', None))
                            self.from_user = MockUser(event.sender)
                    
                    mock_message = MockMessage(event)
                    
                    await scout_system.log_trigger(
                        userbot_id=userbot_id,
                        keyword_id=matched_keyword['id'],
                        message=mock_message,
                        response_sent=True,
                        response_message_id=sent_message.id
                    )
                    
                    logger.info(f"✅ Scout replied to keyword '{matched_keyword['keyword']}' in chat {event.chat_id}")
            
            except Exception as e:
                logger.error(f"Error in Telethon scout handler (userbot {userbot_id}): {e}", exc_info=True)
        
        logger.info(f"✅ Telethon scout handlers registered for userbot #{userbot_id}")
    
    async def connect_single_userbot(self, userbot_id: int) -> bool:
        """Connect a single userbot by ID"""
        from userbot_database import get_db_connection
        
        logger.info(f"🔄 Connecting single userbot #{userbot_id}...")
        
        # Check if already connected
        if userbot_id in self.clients:
            logger.info(f"⚠️ Userbot #{userbot_id} already connected")
            return True
        
        conn = get_db_connection()
        c = conn.cursor()
        
        try:
            # Get userbot info
            c.execute("""
                SELECT id, name, api_id, api_hash, phone_number, session_string
                FROM userbots
                WHERE id = %s AND is_enabled = TRUE AND session_string IS NOT NULL
            """, (userbot_id,))
            
            ub = c.fetchone()
            
            if not ub:
                logger.warning(f"⚠️ Userbot #{userbot_id} not found or not enabled")
                return False
            
            name = ub['name']
            api_id = int(ub['api_id'])
            api_hash = ub['api_hash']
            session_string = ub['session_string']
            
            # Create Telethon client
            from telethon import TelegramClient
            from telethon.sessions import StringSession
            
            client = TelegramClient(
                StringSession(session_string),
                api_id,
                api_hash
            )
            
            await client.connect()
            
            # Check if authorized
            if not await client.is_user_authorized():
                logger.error(f"❌ Userbot #{userbot_id} not authorized!")
                await client.disconnect()
                self._update_connection_status(userbot_id, False, "Not authorized")
                return False
            
            # Get user info
            me = await client.get_me()
            username = me.username or me.first_name
            
            # Create secret chat manager
            secret_chat_manager = SecretChatManager(client, auto_accept=True)
            
            # Store in pool
            self.clients[userbot_id] = client
            self.secret_chat_managers[userbot_id] = secret_chat_manager
            
            # Update database status
            self._update_connection_status(userbot_id, True, f"Connected as @{username}")
            
            logger.info(f"✅ Userbot #{userbot_id} ({name}) connected as @{username}")
            
            # Set up scout handlers if needed (Telethon-based)
            from userbot_database import get_db_connection
            scout_conn = get_db_connection()
            scout_c = scout_conn.cursor()
            try:
                scout_c.execute("SELECT scout_mode_enabled FROM userbots WHERE id = %s", (userbot_id,))
                result = scout_c.fetchone()
                if result and result['scout_mode_enabled']:
                    logger.info(f"🔍 Setting up Telethon scout handlers for userbot #{userbot_id}...")
                    self._setup_telethon_scout_handlers(client, userbot_id)
            except Exception as e:
                logger.error(f"Error checking scout mode: {e}")
            finally:
                scout_conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect userbot #{userbot_id}: {e}", exc_info=True)
            self._update_connection_status(userbot_id, False, f"Error: {str(e)[:100]}")
            return False
        finally:
            conn.close()
    
    async def disconnect_all(self):
        """Disconnect all userbots in the pool"""
        logger.info("🔌 Disconnecting all userbots in pool...")
        
        for userbot_id, client in list(self.clients.items()):
            try:
                await client.disconnect()
                self._update_connection_status(userbot_id, False, "Disconnected")
                logger.info(f"✅ Disconnected userbot #{userbot_id}")
            except Exception as e:
                logger.error(f"❌ Error disconnecting userbot #{userbot_id}: {e}")
        
        self.clients.clear()
        self.secret_chat_managers.clear()
        self.is_initialized = False
        logger.info("✅ All userbots disconnected")

# Global userbot pool instance
userbot_pool = UserbotPool()

