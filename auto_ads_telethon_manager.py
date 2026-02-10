"""
Auto Ads Telethon Client Manager
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Handles Telethon client management for auto ads campaigns including session management,
message forwarding, and bridge channel integration.

Version: 2.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import os
import time
from typing import Optional, Dict, Any, List
from telethon import TelegramClient
from telethon.tl.types import MessageEntityCustomEmoji, MessageEntityBold, MessageEntityItalic, MessageEntityMention
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError

logger = logging.getLogger(__name__)

class AutoAdsTelethonManager:
    """Telethon client manager for auto ads campaign operations"""
    
    def __init__(self):
        self.clients: Dict[str, TelegramClient] = {}
        # Use persistent disk if available, otherwise local directory
        if os.path.exists('/data'):
            self.session_dir = "/data/auto_ads_sessions"
        else:
            self.session_dir = "auto_ads_sessions"
        os.makedirs(self.session_dir, exist_ok=True)
    
    async def get_client(self, account_data: Dict[str, Any]) -> Optional[TelegramClient]:
        """Get or create a Telethon client for the given account with improved error handling"""
        account_id = str(account_data['id'])
        
        # Check if existing client is still valid
        if account_id in self.clients:
            client = self.clients[account_id]
            try:
                # Test if client is still authorized and connected
                if client.is_connected() and await client.is_user_authorized():
                    # Test with a simple API call to ensure it's working
                    await client.get_me()
                    logger.info(f"✅ Existing client for account {account_id} is valid and authorized")
                    return client
                else:
                    logger.warning(f"⚠️ Existing client for account {account_id} is not authorized, recreating...")
                    await client.disconnect()
                    del self.clients[account_id]
            except Exception as e:
                logger.warning(f"⚠️ Existing client for account {account_id} failed test: {e}, recreating...")
                try:
                    await client.disconnect()
                except:
                    pass
                del self.clients[account_id]
        
        try:
            # Check if we have a stored session string
            if account_data.get('session_string'):
                # Use StringSession for headless environments
                from telethon.sessions import StringSession
                session_str = account_data['session_string']
                
                # Validate session string
                logger.info(f"🔧 DEBUG: Account {account_id} session_string type: {type(session_str)}")
                logger.info(f"🔧 DEBUG: Account {account_id} session_string length: {len(session_str) if session_str else 'None'}")
                
                if not session_str or not isinstance(session_str, str):
                    logger.error(f"❌ Invalid session_string for account {account_id}: {type(session_str)} - {repr(session_str)}")
                    return None
                
                # Clean session string (remove whitespace)
                session_str = session_str.strip()
                
                if not session_str:
                    logger.error(f"❌ Empty session_string for account {account_id}")
                    return None
                
                try:
                    # Check if session_str is base64 encoded session data
                    if session_str.startswith('U1FMaXRlIGZvcm1hdCAz') or len(session_str) > 1000:
                        logger.info(f"🔄 Detected base64 session data for account {account_id}, converting to session file")
                        # This is base64 encoded session data, not a StringSession string
                        import base64
                        session_name = f"aa_{account_id}"
                        session_path = os.path.join(self.session_dir, f"{session_name}.session")
                        
                        # Decode and write session data to file
                        try:
                            session_data = base64.b64decode(session_str)
                            with open(session_path, 'wb') as f:
                                f.write(session_data)
                            
                            # Use session file instead of StringSession
                            client = TelegramClient(
                                session_path,
                                account_data['api_id'],
                                account_data['api_hash']
                            )
                            logger.info(f"✅ Created client from base64 session data for account {account_id}")
                        except Exception as decode_error:
                            logger.error(f"❌ Failed to decode base64 session data for account {account_id}: {decode_error}")
                            return None
                    else:
                        # This is a proper StringSession string
                        client = TelegramClient(
                            StringSession(session_str),
                            account_data['api_id'],
                            account_data['api_hash']
                        )
                        logger.info(f"✅ Created client from StringSession for account {account_id}")
                        
                except Exception as session_error:
                    logger.error(f"❌ Failed to create client for account {account_id}: {session_error}")
                    return None
            else:
                logger.error(f"❌ No session_string available for account {account_id}")
                return None
            
            # Connect with retry mechanism
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    await client.connect()
                    logger.info(f"✅ Client connected successfully (attempt {attempt + 1}/{max_retries})")
                    break
                except Exception as connect_error:
                    logger.warning(f"⚠️ Connection attempt {attempt + 1}/{max_retries} failed: {connect_error}")
                    if attempt == max_retries - 1:
                        logger.error(f"❌ Failed to connect after {max_retries} attempts")
                        return None
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
            
            # Ensure client is properly initialized for cross-context usage
            try:
                # Check if already authorized first
                if not await client.is_user_authorized():
                    logger.error(f"❌ Account {account_id} is not authorized - cannot authenticate in headless environment")
                    await client.disconnect()
                    return None
                
                # Test the client by getting self info to ensure it's working
                me = await client.get_me()
                logger.info(f"✅ Client connected and authorized for {me.first_name} (ID: {me.id})")
            except Exception as test_error:
                logger.error(f"❌ Client connection test failed for account {account_id}: {test_error}")
                await client.disconnect()
                return None
            
            # Store client for reuse
            self.clients[account_id] = client
            logger.info(f"✅ Created auto ads Telethon client for account {account_data.get('account_name', account_id)}")
            
            return client
            
        except Exception as e:
            logger.error(f"❌ Failed to create Telethon client for account {account_id}: {e}")
            return None
    
    async def forward_message_to_targets(self, client: TelegramClient, source_chat_id: int,
                                        message_id: int, target_chats: List[str]) -> Dict[str, Any]:
        """Forward a message to multiple target chats"""
        results = {'successful': [], 'failed': []}
        
        for target_chat in target_chats:
            try:
                # Get target entity
                target_entity = await client.get_entity(target_chat)
                
                # Forward message
                forwarded = await client.forward_messages(
                    entity=target_entity,
                    messages=message_id,
                    from_peer=source_chat_id
                )
                
                if forwarded:
                    results['successful'].append(target_chat)
                    logger.info(f"✅ Forwarded message to {target_chat}")
                else:
                    results['failed'].append(target_chat)
                    logger.warning(f"❌ Failed to forward message to {target_chat}")
                    
            except FloodWaitError as flood_error:
                wait_time = flood_error.seconds
                logger.warning(f"⏳ FloodWaitError for {target_chat}: waiting {wait_time} seconds")
                results['failed'].append(target_chat)
                
            except Exception as e:
                logger.error(f"❌ Error forwarding to {target_chat}: {e}")
                results['failed'].append(target_chat)
        
        return results
    
    async def send_text_message(self, client: TelegramClient, target_chat: str,
                               message_text: str, buttons=None) -> bool:
        """Send a text message to a target chat"""
        try:
            target_entity = await client.get_entity(target_chat)
            
            # Convert buttons if provided
            telethon_buttons = None
            if buttons:
                telethon_buttons = self._convert_buttons_to_telethon(buttons)
            
            sent_message = await client.send_message(
                entity=target_entity,
                message=message_text,
                buttons=telethon_buttons
            )
            
            if sent_message:
                logger.info(f"✅ Sent text message to {target_chat}")
                return True
            else:
                logger.warning(f"❌ Failed to send message to {target_chat}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error sending message to {target_chat}: {e}")
            return False
    
    def _convert_buttons_to_telethon(self, buttons: List[List[Dict]]):
        """Convert button data to Telethon button format"""
        from telethon.tl.custom import Button
        
        telethon_buttons = []
        for row in buttons:
            button_row = []
            for button in row:
                if button.get('url'):
                    button_row.append(Button.url(button['text'], button['url']))
                else:
                    button_row.append(Button.inline(button['text'], button.get('callback_data', '')))
            telethon_buttons.append(button_row)
        
        return telethon_buttons
    
    async def validate_and_reconnect_client(self, account_id: str, client: TelegramClient) -> bool:
        """Validate client and reconnect if necessary"""
        try:
            # Check if client is connected
            if not client.is_connected():
                logger.info(f"🔄 Client {account_id} not connected, attempting to reconnect...")
                await client.connect()
            
            # Check if client is authorized
            if not await client.is_user_authorized():
                logger.error(f"❌ Client {account_id} not authorized")
                return False
            
            # Test with a simple API call
            await client.get_me()
            logger.info(f"✅ Client {account_id} validation successful")
            return True
            
        except Exception as e:
            logger.error(f"❌ Client {account_id} validation failed: {e}")
            return False
    
    async def get_validated_client(self, account_data: Dict[str, Any]) -> Optional[TelegramClient]:
        """Get a validated client, recreating if necessary"""
        account_id = str(account_data['id'])
        
        # Try to get existing client and validate it
        if account_id in self.clients:
            client = self.clients[account_id]
            if await self.validate_and_reconnect_client(account_id, client):
                return client
            else:
                # Remove invalid client
                logger.warning(f"⚠️ Removing invalid client for account {account_id}")
                try:
                    await client.disconnect()
                except:
                    pass
                del self.clients[account_id]
        
        # Create new client if needed
        return await self.get_client(account_data)
    
    async def cleanup(self):
        """Cleanup all clients"""
        for client in self.clients.values():
            try:
                await client.disconnect()
            except:
                pass
        self.clients.clear()

# Global instance
auto_ads_telethon_manager = AutoAdsTelethonManager()


