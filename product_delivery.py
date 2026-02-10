"""
Product Delivery System for Secret Chats
Handles delivering products via userbot with self-destructing messages
"""

import logging
import asyncio
import os
from typing import Dict, List, Optional
from datetime import datetime, timezone

from userbot_manager import userbot_manager
from userbot_config import userbot_config
from userbot_database import log_delivery, save_secret_chat, get_secret_chat_id

logger = logging.getLogger(__name__)

async def deliver_product_via_userbot(
    user_id: int,
    product_data: Dict,
    order_id: str,
    context=None
) -> Dict:
    """
    Deliver product to user via userbot with secret chat
    
    Args:
        user_id: Telegram user ID
        product_data: Dictionary containing product information
        order_id: Unique order identifier
        context: Telegram bot context (for fallback)
    
    Returns:
        Dictionary with delivery status
    """
    
    # Check system setting for secret chat delivery
    from utils import get_bot_setting
    secret_chat_enabled = get_bot_setting("secret_chat_delivery_enabled", "false").lower() == "true"
    
    if not secret_chat_enabled:
        logger.info("ℹ️ Secret chat delivery DISABLED in system settings, using bot chat fallback")
        return {'success': False, 'error': 'Secret chat delivery disabled', 'fallback': True}
    
    # Check if userbot is enabled and connected
    if not userbot_config.is_enabled():
        logger.info("ℹ️ Userbot delivery disabled, using fallback")
        return {'success': False, 'error': 'Userbot disabled', 'fallback': True}
    
    if not userbot_manager.is_connected:
        logger.warning("⚠️ Userbot not connected, using fallback")
        await log_delivery(user_id, order_id, 'failed', 'Userbot not connected')
        return {'success': False, 'error': 'Userbot not connected', 'fallback': True}
    
    logger.info(f"🚀 Starting userbot delivery to user {user_id} (Order: {order_id})")
    
    # Get TTL from config
    ttl_seconds = userbot_config.secret_chat_ttl
    max_retries = userbot_config.max_retries
    retry_delay = userbot_config.retry_delay
    
    # Retry logic
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries}")
                await asyncio.sleep(retry_delay * attempt)  # Exponential backoff
            
            # Send welcome message
            welcome_msg = (
                "🔐 **Your Secret Delivery is Ready!**\n\n"
                "Your product details are being sent securely.\n"
                f"⏰ These messages will self-destruct in {ttl_seconds // 3600} hours.\n\n"
                f"📦 Order ID: `{order_id}`"
            )
            
            result = await userbot_manager.send_message(
                user_id=user_id,
                text=welcome_msg,
                parse_mode='markdown'
            )
            
            if not result['success']:
                raise Exception(result.get('error', 'Failed to send welcome message'))
            
            await asyncio.sleep(1)  # Small delay between messages
            
            # Send product media if available
            media_sent = False
            if 'media_path' in product_data and product_data['media_path']:
                media_path = product_data['media_path']
                
                if os.path.exists(media_path):
                    file_extension = os.path.splitext(media_path)[1].lower()
                    
                    # Determine media type and send
                    if file_extension in ['.jpg', '.jpeg', '.png', '.gif']:
                        result = await userbot_manager.send_photo(
                            user_id=user_id,
                            photo_path=media_path,
                            caption=f"📦 {product_data.get('name', 'Product')}",
                            ttl_seconds=ttl_seconds
                        )
                    elif file_extension in ['.mp4', '.mov', '.avi']:
                        result = await userbot_manager.send_video(
                            user_id=user_id,
                            video_path=media_path,
                            caption=f"📦 {product_data.get('name', 'Product')}",
                            ttl_seconds=ttl_seconds
                        )
                    else:
                        result = await userbot_manager.send_document(
                            user_id=user_id,
                            document_path=media_path,
                            caption=f"📦 {product_data.get('name', 'Product')}"
                        )
                    
                    if result['success']:
                        media_sent = True
                        logger.info(f"✅ Media sent to user {user_id}")
                    else:
                        logger.warning(f"⚠️ Failed to send media: {result.get('error')}")
            
            await asyncio.sleep(1)
            
            # Send product details
            details_msg = _format_product_details(product_data, order_id)
            
            result = await userbot_manager.send_message(
                user_id=user_id,
                text=details_msg,
                parse_mode='markdown'
            )
            
            if not result['success']:
                raise Exception(result.get('error', 'Failed to send details'))
            
            # Log successful delivery
            await log_delivery(user_id, order_id, 'success')
            
            logger.info(f"✅ Product delivered successfully to user {user_id}")
            
            return {
                'success': True,
                'media_sent': media_sent,
                'messages_sent': 3 if media_sent else 2
            }
            
        except Exception as e:
            logger.error(f"❌ Delivery attempt {attempt + 1} failed: {e}")
            
            if attempt == max_retries - 1:
                # Final attempt failed
                await log_delivery(user_id, order_id, 'failed', str(e))
                return {
                    'success': False,
                    'error': str(e),
                    'fallback': True
                }
    
    # Should not reach here
    return {'success': False, 'error': 'Unknown error', 'fallback': True}

def _format_product_details(product_data: Dict, order_id: str) -> str:
    """Format product details message"""
    
    msg = "📦 **Product Details**\n\n"
    
    # Product name
    if 'name' in product_data:
        msg += f"🏷️ **Name:** {product_data['name']}\n"
    
    # Product type
    if 'type' in product_data:
        msg += f"📋 **Type:** {product_data['type']}\n"
    
    # Size/Weight
    if 'size' in product_data:
        msg += f"⚖️ **Size:** {product_data['size']}\n"
    
    # Location
    if 'city' in product_data:
        msg += f"🏙️ **City:** {product_data['city']}\n"
    
    if 'district' in product_data:
        msg += f"🏘️ **District:** {product_data['district']}\n"
    
    # Price
    if 'price' in product_data:
        msg += f"💰 **Price:** {product_data['price']} EUR\n"
    
    # Quantity
    if 'quantity' in product_data:
        msg += f"🔢 **Quantity:** {product_data['quantity']}\n"
    
    # Order info
    msg += f"\n📋 **Order ID:** `{order_id}`\n"
    msg += f"📅 **Delivered:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
    
    # Self-destruct notice
    ttl_hours = userbot_config.secret_chat_ttl // 3600
    msg += f"\n⏰ **This message will self-destruct in {ttl_hours} hours**"
    
    return msg

async def deliver_basket_via_userbot(
    user_id: int,
    basket_items: List[Dict],
    total_amount: float,
    order_id: str,
    context=None
) -> Dict:
    """
    Deliver multiple products (basket) to user via userbot
    
    Args:
        user_id: Telegram user ID
        basket_items: List of product dictionaries
        total_amount: Total order amount
        order_id: Unique order identifier
        context: Telegram bot context (for fallback)
    
    Returns:
        Dictionary with delivery status
    """
    
    # Check system setting for secret chat delivery
    from utils import get_bot_setting
    secret_chat_enabled = get_bot_setting("secret_chat_delivery_enabled", "false").lower() == "true"
    
    if not secret_chat_enabled:
        logger.info("ℹ️ Secret chat delivery DISABLED in system settings, using bot chat fallback")
        return {'success': False, 'error': 'Secret chat delivery disabled', 'fallback': True}
    
    # Check if userbot is enabled and connected
    if not userbot_config.is_enabled():
        logger.info("ℹ️ Userbot delivery disabled, using fallback")
        return {'success': False, 'error': 'Userbot disabled', 'fallback': True}
    
    if not userbot_manager.is_connected:
        logger.warning("⚠️ Userbot not connected, using fallback")
        await log_delivery(user_id, order_id, 'failed', 'Userbot not connected')
        return {'success': False, 'error': 'Userbot not connected', 'fallback': True}
    
    logger.info(f"🚀 Starting basket delivery to user {user_id} (Order: {order_id})")
    
    ttl_seconds = userbot_config.secret_chat_ttl
    max_retries = userbot_config.max_retries
    retry_delay = userbot_config.retry_delay
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries}")
                await asyncio.sleep(retry_delay * attempt)
            
            # Send welcome message
            welcome_msg = (
                "🔐 **Your Secret Delivery is Ready!**\n\n"
                f"📦 You have **{len(basket_items)}** items in this order.\n"
                f"💰 Total: **{total_amount:.2f} EUR**\n\n"
                f"⏰ These messages will self-destruct in {ttl_seconds // 3600} hours.\n\n"
                f"📦 Order ID: `{order_id}`"
            )
            
            result = await userbot_manager.send_message(
                user_id=user_id,
                text=welcome_msg,
                parse_mode='markdown'
            )
            
            if not result['success']:
                raise Exception(result.get('error', 'Failed to send welcome message'))
            
            await asyncio.sleep(1)
            
            # Send each product
            media_count = 0
            for idx, item in enumerate(basket_items, 1):
                # Send media if available
                if 'media_path' in item and item['media_path'] and os.path.exists(item['media_path']):
                    media_path = item['media_path']
                    file_extension = os.path.splitext(media_path)[1].lower()
                    
                    caption = f"📦 Item {idx}/{len(basket_items)}: {item.get('name', 'Product')}"
                    
                    if file_extension in ['.jpg', '.jpeg', '.png', '.gif']:
                        result = await userbot_manager.send_photo(
                            user_id=user_id,
                            photo_path=media_path,
                            caption=caption,
                            ttl_seconds=ttl_seconds
                        )
                    elif file_extension in ['.mp4', '.mov', '.avi']:
                        result = await userbot_manager.send_video(
                            user_id=user_id,
                            video_path=media_path,
                            caption=caption,
                            ttl_seconds=ttl_seconds
                        )
                    else:
                        result = await userbot_manager.send_document(
                            user_id=user_id,
                            document_path=media_path,
                            caption=caption
                        )
                    
                    if result['success']:
                        media_count += 1
                
                # Send product details
                details_msg = f"📦 **Item {idx}/{len(basket_items)}**\n\n"
                details_msg += _format_product_details(item, order_id)
                
                result = await userbot_manager.send_message(
                    user_id=user_id,
                    text=details_msg,
                    parse_mode='markdown'
                )
                
                if not result['success']:
                    logger.warning(f"⚠️ Failed to send details for item {idx}")
                
                await asyncio.sleep(1)  # Delay between items
            
            # Send completion message
            completion_msg = (
                "✅ **Delivery Complete!**\n\n"
                f"📦 All {len(basket_items)} items delivered.\n"
                f"💰 Total: **{total_amount:.2f} EUR**\n\n"
                "Thank you for your purchase! 🎉"
            )
            
            await userbot_manager.send_message(
                user_id=user_id,
                text=completion_msg,
                parse_mode='markdown'
            )
            
            # Log successful delivery
            await log_delivery(user_id, order_id, 'success')
            
            logger.info(f"✅ Basket delivered successfully to user {user_id}")
            
            return {
                'success': True,
                'items_delivered': len(basket_items),
                'media_sent': media_count
            }
            
        except Exception as e:
            logger.error(f"❌ Basket delivery attempt {attempt + 1} failed: {e}")
            
            if attempt == max_retries - 1:
                await log_delivery(user_id, order_id, 'failed', str(e))
                return {
                    'success': False,
                    'error': str(e),
                    'fallback': True
                }
    
    return {'success': False, 'error': 'Unknown error', 'fallback': True}

async def test_userbot_delivery(admin_user_id: int) -> Dict:
    """Test userbot delivery by sending a test message to admin"""
    
    if not userbot_manager.is_connected:
        return {'success': False, 'error': 'Userbot not connected'}
    
    try:
        test_msg = (
            "🧪 **Userbot Test Message**\n\n"
            "This is a test delivery from your userbot.\n"
            "If you're seeing this, the userbot is working correctly! ✅\n\n"
            f"🕐 Test time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        )
        
        result = await userbot_manager.send_message(
            user_id=admin_user_id,
            text=test_msg,
            parse_mode='markdown'
        )
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Test delivery failed: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}

