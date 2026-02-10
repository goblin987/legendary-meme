"""
Auto Ads Bump Service
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Service for managing automated ad campaigns with scheduling and anti-ban protection.
Simplified version adapted for PostgreSQL integration.

Version: 2.0.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import asyncio
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from auto_ads_database import AutoAdsDatabase
from auto_ads_telethon_manager import auto_ads_telethon_manager
from auto_ads_config import AutoAdsConfig

logger = logging.getLogger(__name__)

class AutoAdsBumpService:
    """Service for managing automated ad campaigns"""
    
    def __init__(self, bot_instance=None):
        self.db = AutoAdsDatabase()
        self.telethon_manager = auto_ads_telethon_manager
        self.bot_instance = bot_instance
        self.active_campaigns = {}
    
    def get_user_campaigns(self, user_id: int) -> List[Dict]:
        """Get all campaigns for a user"""
        return self.db.get_user_campaigns(user_id)
    
    def get_all_campaigns(self) -> List[Dict]:
        """Get all campaigns (for admins)"""
        return self.db.get_all_campaigns()
    
    def get_campaign(self, campaign_id: int) -> Optional[Dict]:
        """Get campaign by ID"""
        return self.db.get_campaign(campaign_id)
    
    def delete_campaign(self, campaign_id: int) -> bool:
        """Delete a campaign"""
        try:
            self.db.delete_campaign(campaign_id)
            return True
        except Exception as e:
            logger.error(f"Error deleting campaign: {e}")
            return False
    
    def toggle_campaign(self, campaign_id: int) -> bool:
        """Toggle campaign active status"""
        try:
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                return False
            
            new_status = not campaign['is_active']
            self.db.update_campaign_status(campaign_id, new_status)
            return True
        except Exception as e:
            logger.error(f"Error toggling campaign: {e}")
            return False
    
    def add_campaign(self, user_id: int, account_id: int, campaign_name: str,
                    ad_content: any, target_chats: list, buttons: list = None,
                    schedule_type: str = 'once', schedule_time: str = None) -> int:
        """Add a new campaign"""
        return self.db.add_campaign(
            user_id=user_id,
            account_id=account_id,
            campaign_name=campaign_name,
            ad_content=ad_content,
            target_chats=target_chats,
            buttons=buttons,
            schedule_type=schedule_type,
            schedule_time=schedule_time
        )
    
    async def execute_campaign(self, campaign_id: int) -> Dict[str, any]:
        """Execute a campaign immediately"""
        results = {
            'success': False,
            'message': '',
            'sent_count': 0,
            'failed_count': 0,
            'details': []
        }
        
        try:
            # Get campaign
            campaign = self.get_campaign(campaign_id)
            if not campaign:
                results['message'] = "Campaign not found"
                return results
            
            # Get account
            account = self.db.get_account(campaign['account_id'])
            if not account:
                results['message'] = "Account not found"
                return results
            
            # Get Telethon client
            client = await self.telethon_manager.get_validated_client(account)
            if not client:
                results['message'] = "Failed to connect account"
                return results
            
            # Extract campaign data
            ad_content = campaign['ad_content']
            target_chats_raw = campaign['target_chats']
            buttons = campaign.get('buttons')
            
            logger.info(f"🔍 DEBUG: Campaign {campaign_id} buttons from DB: {buttons}")
            logger.info(f"🔍 DEBUG: Buttons type: {type(buttons)}")
            if buttons:
                logger.info(f"🔍 DEBUG: Buttons length: {len(buttons)}")
            
            # Resolve target chats - if "all", get actual groups
            target_chats = await self._resolve_target_chats(client, target_chats_raw)
            
            if not target_chats:
                return {
                    'success': False,
                    'message': 'No target groups found. Make sure the userbot is added to some groups.',
                    'sent_count': 0,
                    'failed_count': 0,
                    'details': []
                }
            
            # Execute based on content type
            if isinstance(ad_content, dict):
                if ad_content.get('bridge_channel'):
                    # Forward from bridge channel
                    results = await self._execute_bridge_forward(
                        client, ad_content, target_chats, buttons
                    )
                elif ad_content.get('forwarded_message'):
                    # Forward existing message
                    results = await self._execute_message_forward(
                        client, ad_content, target_chats
                    )
                else:
                    # Send text message
                    text = ad_content.get('text', '')
                    results = await self._execute_text_message(
                        client, text, target_chats, buttons
                    )
            else:
                # Simple text message
                results = await self._execute_text_message(
                    client, str(ad_content), target_chats, buttons
                )
            
            # Update campaign stats
            if results['success']:
                self.db.update_campaign_last_run(campaign_id)
                results['message'] = f"Campaign executed: {results['sent_count']} successful, {results['failed_count']} failed"
            
            return results
            
        except Exception as e:
            logger.error(f"Error executing campaign {campaign_id}: {e}")
            results['message'] = f"Error: {str(e)}"
            return results
    
    async def _resolve_target_chats(self, client, target_chats_raw: list) -> list:
        """Resolve target chats - convert 'all' to actual group list"""
        try:
            # Check if target is "all groups"
            if target_chats_raw == ['all'] or target_chats_raw == 'all' or 'all' in target_chats_raw:
                logger.info("🔍 Resolving 'all' to actual groups...")
                
                # Get all dialogs and filter groups
                dialogs = await client.get_dialogs()
                target_entities = []
                
                for dialog in dialogs:
                    # Only include groups (not channels)
                    if dialog.is_group and not getattr(dialog.entity, 'broadcast', True):
                        target_entities.append(dialog.entity)
                        logger.info(f"✅ Found group: {dialog.name} (ID: {dialog.id})")
                
                logger.info(f"✅ Resolved 'all' to {len(target_entities)} groups")
                return target_entities
            
            else:
                # Specific chats - resolve to entities
                target_entities = []
                for chat in target_chats_raw:
                    try:
                        if isinstance(chat, str):
                            entity = await client.get_entity(chat)
                            target_entities.append(entity)
                        else:
                            # Already an entity or ID
                            target_entities.append(chat)
                    except Exception as e:
                        logger.warning(f"⚠️ Could not resolve chat {chat}: {e}")
                        
                return target_entities
                
        except Exception as e:
            logger.error(f"❌ Error resolving target chats: {e}")
            return []
    
    async def _execute_bridge_forward(self, client, ad_content: dict, 
                                     target_chats: list, buttons: list = None) -> Dict:
        """Execute campaign by forwarding from bridge channel"""
        results = {'success': True, 'sent_count': 0, 'failed_count': 0, 'details': []}
        
        try:
            # Check if bridge link was properly parsed
            if 'error' in ad_content:
                results['success'] = False
                results['message'] = ad_content['error']
                return results
                
            if 'bridge_channel_entity' not in ad_content or 'bridge_message_id' not in ad_content:
                results['success'] = False
                results['message'] = 'Invalid bridge channel link. Please use format: https://t.me/c/channelid/messageid'
                return results
                
            source_chat = ad_content['bridge_channel_entity']
            message_id = ad_content['bridge_message_id']
            
            # Get source entity once
            source_entity = await client.get_entity(source_chat)
            
            # Get the original message from bridge channel
            logger.info(f"📎 Bridge mode - fetching original message from storage channel")
            
            try:
                original_message = await client.get_messages(source_entity, ids=message_id)
                if not original_message:
                    results['success'] = False
                    results['message'] = 'Could not fetch original message from bridge channel'
                    return results
                
                logger.info(f"✅ Retrieved original message from bridge channel")
                logger.info(f"Message has media: {bool(original_message.media)}")
                logger.info(f"Message has buttons: {bool(original_message.buttons)}")
            except Exception as e:
                logger.error(f"❌ Failed to get original message: {e}")
                results['success'] = False
                results['message'] = f'Failed to get message from bridge: {str(e)}'
                return results
            
            # SOLUTION: Use Bot API to create a NEW message WITH BUTTONS in bridge channel, then forward it
            message_to_forward = message_id  # Default: forward original
            
            if buttons and self.bot_instance:
                try:
                    logger.info(f"🤖 Using Bot API to add {len(buttons)} inline button(s) to bridge message")
                    
                    # Create inline keyboard markup using Bot API
                    keyboard = []
                    for btn in buttons:
                        keyboard.append([InlineKeyboardButton(btn['text'], url=btn['url'])])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    logger.info(f"✅ Created InlineKeyboardMarkup with {len(keyboard)} button(s)")
                    
                    # Send a NEW message with buttons to the bridge channel using Bot API
                    if original_message.media:
                        # For media messages, we need to copy the media
                        # Download media to temp location then upload with buttons
                        logger.info(f"📥 Downloading media from original message...")
                        media_file = await client.download_media(original_message, file=bytes)
                        
                        logger.info(f"📤 Uploading media with buttons to bridge channel using Bot API...")
                        bot_msg = await self.bot_instance.send_photo(
                            chat_id=int(source_chat),
                            photo=media_file,
                            caption=original_message.message or "",
                            reply_markup=reply_markup
                        )
                        message_to_forward = bot_msg.message_id
                        logger.info(f"✅ Created bridge message with buttons (ID: {message_to_forward})")
                    else:
                        # For text messages, just send text with buttons
                        logger.info(f"📤 Sending text with buttons to bridge channel using Bot API...")
                        bot_msg = await self.bot_instance.send_message(
                            chat_id=int(source_chat),
                            text=original_message.message or "",
                            reply_markup=reply_markup
                        )
                        message_to_forward = bot_msg.message_id
                        logger.info(f"✅ Created bridge message with buttons (ID: {message_to_forward})")
                        
                except Exception as e:
                    logger.error(f"❌ Failed to create bridge message with buttons using Bot API: {e}")
                    import traceback
                    logger.error(f"❌ Traceback: {traceback.format_exc()}")
                    logger.warning(f"⚠️ Falling back to forwarding original message without buttons")
                    message_to_forward = message_id
            
            # Now forward the message (with buttons if we created one) to all target chats
            logger.info(f"🚀 Forwarding message {message_to_forward} from bridge to {len(target_chats)} groups")
            
            for target_chat in target_chats:
                try:
                    # Add delay between messages (anti-ban)
                    if results['sent_count'] > 0:
                        delay = self._get_safe_delay()
                        logger.info(f"Waiting {delay:.1f}s before next message...")
                        await asyncio.sleep(delay)
                    
                    # target_chat is already an entity from _resolve_target_chats
                    # Get a display name for logging
                    chat_name = getattr(target_chat, 'title', None) or getattr(target_chat, 'username', None) or str(getattr(target_chat, 'id', target_chat))
                    
                    # FORWARD the message from bridge channel (preserves buttons!)
                    logger.info(f"📨 Forwarding message to {chat_name}")
                    sent = await client.forward_messages(
                        entity=target_chat,
                        messages=message_to_forward,
                        from_peer=source_entity
                    )
                    
                    if sent:
                        logger.info(f"✅ Forwarded message with buttons to {chat_name}")
                        logger.info(f"🔍 DEBUG: Forwarded message ID: {sent.id if hasattr(sent, 'id') else sent[0].id if isinstance(sent, list) else 'Unknown'}")
                        results['sent_count'] += 1
                        results['details'].append(f"✅ {chat_name}")
                    else:
                        results['failed_count'] += 1
                        results['details'].append(f"❌ {chat_name}")
                        
                except Exception as e:
                    chat_name = getattr(target_chat, 'title', None) or str(getattr(target_chat, 'id', 'unknown'))
                    results['failed_count'] += 1
                    results['details'].append(f"❌ {chat_name}: {str(e)}")
                    logger.error(f"Failed to forward to {chat_name}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in bridge forward execution: {e}")
            results['success'] = False
            results['message'] = str(e)
            return results
    
    async def _execute_message_forward(self, client, ad_content: dict, 
                                      target_chats: list) -> Dict:
        """Execute campaign by forwarding existing message"""
        results = {'success': True, 'sent_count': 0, 'failed_count': 0, 'details': []}
        
        try:
            source_chat = ad_content['original_chat_id']
            message_id = ad_content['original_message_id']
            
            for target_chat in target_chats:
                try:
                    # Add delay between messages
                    if results['sent_count'] > 0:
                        delay = self._get_safe_delay()
                        await asyncio.sleep(delay)
                    
                    # Forward message
                    success = await self.telethon_manager.forward_message_to_targets(
                        client, source_chat, message_id, [target_chat]
                    )
                    
                    if success['successful']:
                        results['sent_count'] += 1
                        results['details'].append(f"✅ {target_chat}")
                    else:
                        results['failed_count'] += 1
                        results['details'].append(f"❌ {target_chat}")
                        
                except Exception as e:
                    results['failed_count'] += 1
                    results['details'].append(f"❌ {target_chat}: {str(e)}")
                    logger.error(f"Failed to forward to {target_chat}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in message forward execution: {e}")
            results['success'] = False
            results['message'] = str(e)
            return results
    
    async def _execute_text_message(self, client, text: str, 
                                   target_chats: list, buttons: list = None) -> Dict:
        """Execute campaign by sending text message"""
        results = {'success': True, 'sent_count': 0, 'failed_count': 0, 'details': []}
        
        try:
            # Convert buttons to Telethon format if provided
            telethon_buttons = None
            if buttons:
                try:
                    from telethon import Button
                    button_rows = []
                    for btn in buttons:
                        button_rows.append([Button.url(btn['text'], btn['url'])])
                    telethon_buttons = button_rows
                except Exception as e:
                    logger.error(f"Error creating buttons: {e}")
            
            for target_chat in target_chats:
                try:
                    # Add delay between messages
                    if results['sent_count'] > 0:
                        delay = self._get_safe_delay()
                        await asyncio.sleep(delay)
                    
                    # target_chat is already an entity
                    chat_name = getattr(target_chat, 'title', None) or getattr(target_chat, 'username', None) or str(getattr(target_chat, 'id', target_chat))
                    
                    # Send message directly to entity
                    await client.send_message(
                        target_chat,
                        text,
                        buttons=telethon_buttons
                    )
                    
                    results['sent_count'] += 1
                    results['details'].append(f"✅ {chat_name}")
                    logger.info(f"✅ Sent to {chat_name}")
                        
                except Exception as e:
                    chat_name = getattr(target_chat, 'title', None) or str(getattr(target_chat, 'id', 'unknown'))
                    results['failed_count'] += 1
                    results['details'].append(f"❌ {chat_name}: {str(e)}")
                    logger.error(f"Failed to send to {chat_name}: {e}")
            
            return results
            
        except Exception as e:
            logger.error(f"Error in text message execution: {e}")
            results['success'] = False
            results['message'] = str(e)
            return results
    
    def _get_safe_delay(self) -> float:
        """Get a safe random delay between messages (anti-ban)"""
        min_delay = AutoAdsConfig.MIN_DELAY_BETWEEN_MESSAGES
        max_delay = AutoAdsConfig.MAX_DELAY_BETWEEN_MESSAGES
        
        # Use random delay for unpredictable timing
        base_delay = random.uniform(min_delay, max_delay)
        
        # Add occasional longer pauses (5% chance of 1.5x delay for moderate speed)
        # Reduced from 10% and 2x to keep campaigns fast
        if random.random() < 0.05:
            base_delay *= 1.5
            logger.info(f"🛡️ ANTI-BAN: Extended delay for natural behavior")
        
        return base_delay
    
    async def cleanup(self):
        """Cleanup resources"""
        await self.telethon_manager.cleanup()


